package kernel

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"testbot/core/sdk"
)

type phase uint8

const (
	initializing phase = iota
	active
	closing
)

type resource struct {
	label, kind string
	dispose     sdk.Disposer
	pending     bool
	released    bool
	attempted   bool
	disposing   bool
	dirty       error
	done        chan struct{}
}

// 锁顺序固定为runtime.mu → scope.mu，持有作用域锁时不得进入运行时
type scope struct {
	mu             sync.Mutex
	runtime        *Runtime
	id             string
	gen            uint64
	run            context.Context
	cancel         context.CancelCauseFunc
	phase          phase
	finished       bool
	required       map[string]*serviceEntry
	provided       map[string]*serviceEntry
	resources      []*resource
	changed        chan struct{}
	initDone       bool
	initErr        error
	initEnd        chan struct{}
	failure        error
	calls          map[uint64]string
	nextCall       uint64
	cleanupRunning bool
	cleanupEnd     chan struct{}
}

func newScope(rt *Runtime, rec *record) *scope {
	run, cancel := context.WithCancelCause(context.WithValue(rt.root, executionOwnerKey{}, rt))
	s := &scope{
		runtime: rt, id: rec.def.Manifest.ID, gen: rt.nextGeneration,
		run: run, cancel: cancel, phase: initializing,
		required: make(map[string]*serviceEntry), provided: make(map[string]*serviceEntry),
		changed: make(chan struct{}), initEnd: make(chan struct{}), calls: make(map[uint64]string),
	}
	for _, key := range rec.def.Manifest.Required {
		s.required[key.Name] = rt.services[key.Name]
	}
	return s
}

func (s *scope) changedLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// 已结束的短任务和订阅不保留历史句柄，避免长期运行时持续增长
func (s *scope) pruneLocked() {
	kept := s.resources[:0]
	for _, r := range s.resources {
		if !r.released || r.disposing {
			kept = append(kept, r)
		}
	}
	clear(s.resources[len(kept):])
	s.resources = kept
}

func (s *scope) freezeLocked() {
	if s.phase != closing {
		s.phase = closing
		s.changedLocked()
	}
	// 初始化也是受管任务，关闭入口时必须通知它退出
	if !s.initDone {
		s.cancel(sdk.ErrContextClosed)
	}
}

func (s *scope) deferResource(label string, dispose sdk.Disposer) error {
	if label == "" || dispose == nil {
		return sdk.ErrInvalid
	}
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return sdk.ErrContextClosed
	}
	s.resources = append(s.resources, &resource{label: label, kind: "disposer", dispose: dispose})
	isClosing := s.phase == closing
	s.changedLocked()
	s.mu.Unlock()
	if isClosing {
		// 取消期间返回的资源仍归旧代，不能交给下一代回收
		s.runtime.notify()
		return sdk.ErrContextClosed
	}
	return nil
}

func (s *scope) task(label, kind string, fn sdk.Task) (sdk.CancelFunc, error) {
	if label == "" || fn == nil {
		return nil, sdk.ErrInvalid
	}
	s.mu.Lock()
	if s.phase == closing || s.finished {
		s.mu.Unlock()
		return nil, sdk.ErrContextClosed
	}
	run, cancel := context.WithCancel(s.run)
	var once sync.Once
	stop := func() { once.Do(cancel) }
	r := &resource{label: label, kind: kind, pending: true, dispose: func(context.Context) error { stop(); return nil }}
	s.resources = append(s.resources, r)
	s.changedLocked()
	s.mu.Unlock()
	go func() {
		err := invoke(func() error { return fn(run) })
		cancelled := run.Err() != nil
		stop()
		if err != nil && !(cancelled && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded))) {
			s.runtime.fail(s, fmt.Errorf("task %s: %w", label, err))
		}
		s.mu.Lock()
		r.pending = false
		r.released = true
		r.dirty = nil
		s.pruneLocked()
		s.changedLocked()
		s.mu.Unlock()
		s.runtime.notify()
	}()
	return stop, nil
}

func (s *scope) enterLocked(label string) (uint64, bool) {
	if s.phase != active || s.finished {
		return 0, false
	}
	s.nextCall++
	s.calls[s.nextCall] = label
	s.changedLocked()
	return s.nextCall, true
}

func (s *scope) leave(token uint64) {
	s.mu.Lock()
	delete(s.calls, token)
	s.changedLocked()
	s.mu.Unlock()
	s.runtime.notify()
}

func (s *scope) cleanLocked() bool {
	if !s.initDone || len(s.calls) > 0 || s.cleanupRunning {
		return false
	}
	for _, r := range s.resources {
		if !r.released || r.disposing {
			return false
		}
	}
	return true
}

func (s *scope) beginCleanup(retry bool) <-chan struct{} {
	s.mu.Lock()
	if s.cleanupRunning {
		done := s.cleanupEnd
		s.mu.Unlock()
		return done
	}
	work := false
	for _, r := range s.resources {
		if !r.released && !r.disposing && (!r.attempted || retry && r.dirty != nil) {
			work = true
		}
	}
	if !work {
		done := make(chan struct{})
		close(done)
		s.mu.Unlock()
		return done
	}
	s.cleanupRunning = true
	s.cleanupEnd = make(chan struct{})
	done := s.cleanupEnd
	items := append([]*resource(nil), s.resources...)
	s.mu.Unlock()
	s.cancel(sdk.ErrContextClosed)
	go func() {
		for i := len(items) - 1; i >= 0; i-- {
			s.dispose(items[i], retry)
		}
		s.mu.Lock()
		s.cleanupRunning = false
		close(done)
		s.changedLocked()
		s.mu.Unlock()
		s.runtime.notify()
	}()
	return done
}

func (s *scope) dispose(r *resource, retry bool) {
	s.mu.Lock()
	if r.released || r.disposing || r.attempted && !(retry && r.dirty != nil) {
		s.mu.Unlock()
		return
	}
	r.attempted, r.disposing = true, true
	r.done = make(chan struct{})
	done := r.done
	s.changedLocked()
	s.mu.Unlock()
	cleanup, cancel := context.WithTimeout(context.WithValue(context.Background(), executionOwnerKey{}, s.runtime), s.runtime.options.DisposeTimeout)
	defer cancel()
	go func() {
		err := invoke(func() error { return r.dispose(cleanup) })
		s.mu.Lock()
		r.disposing = false
		r.dirty = err
		if err == nil && !r.pending {
			r.released = true
		}
		s.pruneLocked()
		close(done)
		s.changedLocked()
		s.mu.Unlock()
		s.runtime.notify()
	}()
	select {
	case <-done:
	case <-cleanup.Done():
		s.mu.Lock()
		if r.disposing {
			r.dirty = ErrCleanupTimeout
			s.changedLocked()
		}
		s.mu.Unlock()
	}
}

func (s *scope) awaitCleanup(ctx context.Context, retry bool) error {
	s.cancel(sdk.ErrContextClosed)
	// 宿主停止等待后，清理仍继续，未退出的清理任务仍保留记录
	done := s.beginCleanup(retry)
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	timer := time.NewTimer(s.runtime.options.DisposeTimeout)
	defer timer.Stop()
	for {
		s.mu.Lock()
		clean := s.cleanLocked()
		change := s.changed
		// 已失败的disposer只接受显式重试
		failed := false
		for _, r := range s.resources {
			if r.dirty != nil && !r.disposing {
				failed = true
			}
		}
		s.mu.Unlock()
		if clean {
			return nil
		}
		if failed {
			return s.cleanupError()
		}
		select {
		case <-change:
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return s.cleanupError()
		}
	}
}

func (s *scope) cleanupError() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var failures []error
	if !s.initDone {
		failures = append(failures, fmt.Errorf("initializer: %w", ErrCleanupTimeout))
	}
	for _, label := range s.calls {
		failures = append(failures, fmt.Errorf("%s: %w", label, ErrCleanupTimeout))
	}
	for _, r := range s.resources {
		if r.released && !r.disposing {
			continue
		}
		err := r.dirty
		if err == nil {
			err = ErrCleanupTimeout
		}
		failures = append(failures, fmt.Errorf("%s: %w", r.label, err))
	}
	return errors.Join(failures...)
}

func (s *scope) resourcesLocked() []ResourceStatus {
	var result []ResourceStatus
	if !s.initDone {
		result = append(result, ResourceStatus{Label: "initializer", Kind: "initializer", Dirty: s.phase == closing})
	}
	for _, r := range s.resources {
		if !r.released || r.disposing {
			result = append(result, ResourceStatus{Label: r.label, Kind: r.kind, Dirty: s.phase == closing, Error: r.dirty})
		}
	}
	for _, label := range s.calls {
		result = append(result, ResourceStatus{Label: label, Kind: "callback", Dirty: s.phase == closing})
	}
	return result
}
