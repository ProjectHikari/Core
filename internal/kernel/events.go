package kernel

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"testbot/core/sdk"
)

type subscription struct {
	key     sdk.EventKey
	owner   *scope
	handler sdk.Handler
	visible bool
	removed bool
}

func (c *pluginContext) On(key sdk.EventKey, handler sdk.Handler) (sdk.CancelFunc, error) {
	if !validEvent(key) || handler == nil {
		return nil, sdk.ErrInvalid
	}
	s, rt := c.scope, c.scope.runtime
	rt.mu.Lock()
	s.mu.Lock()
	if s.phase == closing || s.finished {
		s.mu.Unlock()
		rt.mu.Unlock()
		return nil, sdk.ErrContextClosed
	}
	for _, existing := range rt.listeners[key.Name] {
		if existing.key != key {
			s.mu.Unlock()
			rt.mu.Unlock()
			return nil, fmt.Errorf("%w: event %s", sdk.ErrContractMismatch, key.Name)
		}
	}
	sub := &subscription{key: key, owner: s, handler: handler, visible: s.phase == active}
	rt.listeners[key.Name] = append(rt.listeners[key.Name], sub)
	r := &resource{label: "listener: " + key.Name, kind: "listener"}
	var once sync.Once
	stop := func() {
		once.Do(func() {
			rt.mu.Lock()
			rt.removeSubscriptionLocked(sub)
			s.mu.Lock()
			r.released = true
			s.pruneLocked()
			s.changedLocked()
			s.mu.Unlock()
			rt.mu.Unlock()
			rt.notify()
		})
	}
	r.dispose = func(context.Context) error { stop(); return nil }
	s.resources = append(s.resources, r)
	s.changedLocked()
	s.mu.Unlock()
	rt.mu.Unlock()
	return stop, nil
}

func (rt *Runtime) removeSubscriptionLocked(sub *subscription) {
	if sub.removed {
		return
	}
	sub.removed, sub.visible = true, false
	items := rt.listeners[sub.key.Name]
	for i, item := range items {
		if item == sub {
			items = append(items[:i], items[i+1:]...)
			break
		}
	}
	if len(items) == 0 {
		delete(rt.listeners, sub.key.Name)
	} else {
		rt.listeners[sub.key.Name] = items
	}
}

func (c *pluginContext) Emit(ctx context.Context, key sdk.EventKey, value any) error {
	return c.dispatch(ctx, key, value, sdk.EmitMode)
}

func (c *pluginContext) Parallel(ctx context.Context, key sdk.EventKey, value any) error {
	return c.dispatch(ctx, key, value, sdk.ParallelMode)
}

func (c *pluginContext) dispatch(ctx context.Context, key sdk.EventKey, value any, mode sdk.DispatchMode) error {
	if ctx == nil || !validEvent(key) {
		return sdk.ErrInvalid
	}
	if key.Mode != mode || !valueMatches(key.Type, value, true) {
		return sdk.ErrContractMismatch
	}
	s, rt := c.scope, c.scope.runtime
	rt.mu.Lock()
	s.mu.Lock()
	token, ok := s.enterLocked("event dispatch: " + key.Name)
	s.mu.Unlock()
	if !ok {
		rt.mu.Unlock()
		return sdk.ErrNotRunning
	}
	var snapshot []*subscription
	for _, sub := range rt.listeners[key.Name] {
		if sub.key != key {
			rt.mu.Unlock()
			s.leave(token)
			return sdk.ErrContractMismatch
		}
		if sub.visible && !sub.removed {
			snapshot = append(snapshot, sub)
		}
	}
	rt.mu.Unlock()
	defer s.leave(token)
	// 分发请求或发送方退出时，通知正在执行的listener取消
	run, cancel := context.WithCancel(ctx)
	stopEmitter := context.AfterFunc(s.run, cancel)
	defer func() { stopEmitter(); cancel() }()
	results := make([]error, len(snapshot))
	call := func(i int, sub *subscription) {
		if run.Err() != nil {
			results[i] = run.Err()
			return
		}
		rt.mu.Lock()
		sub.owner.mu.Lock()
		lease, admitted := uint64(0), false
		if sub.visible && !sub.removed {
			lease, admitted = sub.owner.enterLocked("listener callback: " + key.Name)
		}
		sub.owner.mu.Unlock()
		rt.mu.Unlock()
		if !admitted {
			return
		}
		listenerRun, stop := context.WithCancel(context.WithValue(run, executionOwnerKey{}, rt))
		unhook := context.AfterFunc(sub.owner.run, stop)
		err := invoke(func() error { return sub.handler(listenerRun, value) })
		unhook()
		stop()
		sub.owner.leave(lease)
		if err != nil {
			results[i] = &ListenerError{PluginID: sub.owner.id, Event: key.Name, Index: i, Err: err}
		}
	}
	if mode == sdk.EmitMode {
		for i, sub := range snapshot {
			call(i, sub)
		}
	} else {
		var wait sync.WaitGroup
		for i, sub := range snapshot {
			wait.Go(func() { call(i, sub) })
		}
		wait.Wait()
	}
	// 本轮listener全部处理后再隔离故障插件，避免panic截断同一快照
	reported := make(map[*scope]bool)
	for i, err := range results {
		var panicErr *PanicError
		if errors.As(err, &panicErr) && !reported[snapshot[i].owner] {
			owner := snapshot[i].owner
			reported[owner] = true
			var failures []error
			for j, result := range results {
				if snapshot[j].owner == owner && errors.As(result, &panicErr) {
					failures = append(failures, result)
				}
			}
			rt.fail(owner, errors.Join(failures...))
		}
	}
	return errors.Join(results...)
}
