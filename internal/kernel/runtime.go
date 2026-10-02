package kernel

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"testbot/core/sdk"
)

type serviceEntry struct {
	key   sdk.ServiceKey
	value any
	owner *scope
}

type record struct {
	def            sdk.Definition
	order          uint64
	wanted         bool
	state          State
	generation     uint64
	scope          *scope
	failure        error
	lastErr        error
	pending        bool
	restart        bool
	unload         bool
	dependencyStop bool
}

// 生命周期操作串行，事件分发独立执行
type Runtime struct {
	mu             sync.Mutex
	gate           chan struct{}
	root           context.Context
	cancel         context.CancelFunc
	wake           chan struct{}
	coordinatorEnd chan struct{}
	options        Options
	records        map[string]*record
	services       map[string]*serviceEntry
	listeners      map[string][]*subscription
	nextGeneration uint64
	nextOrder      uint64
	closed         bool
}

func New(options ...Options) *Runtime {
	opt := Options{StartTimeout: 30 * time.Second, DisposeTimeout: 5 * time.Second}
	if len(options) > 0 {
		if options[0].StartTimeout > 0 {
			opt.StartTimeout = options[0].StartTimeout
		}
		if options[0].DisposeTimeout > 0 {
			opt.DisposeTimeout = options[0].DisposeTimeout
		}
	}
	root, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		gate: make(chan struct{}, 1), root: root, cancel: cancel,
		wake: make(chan struct{}, 1), coordinatorEnd: make(chan struct{}), options: opt,
		records: make(map[string]*record), services: make(map[string]*serviceEntry),
		listeners: make(map[string][]*subscription),
	}
	go rt.coordinate()
	return rt
}

func (rt *Runtime) acquire(ctx context.Context) error {
	if ctx == nil {
		return sdk.ErrInvalid
	}
	if owner, _ := ctx.Value(executionOwnerKey{}).(*Runtime); owner == rt {
		return ErrLifecycleReentrant
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case rt.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			rt.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (rt *Runtime) release() { <-rt.gate }

func (rt *Runtime) notify() {
	select {
	case rt.wake <- struct{}{}:
	default:
	}
}

func (rt *Runtime) coordinate() {
	defer close(rt.coordinatorEnd)
	for {
		select {
		case <-rt.root.Done():
			return
		case <-rt.wake:
			if err := rt.acquire(rt.root); err != nil {
				return
			}
			// 错误保存在对应插件的Status中，一个插件失败不停止协调器
			rt.reconcile(rt.root, nil)
			rt.completeClose()
			rt.release()
		}
	}
}

// 只登记，不启用，校验失败不修改注册表
func (rt *Runtime) Register(def sdk.Definition) error {
	if err := rt.acquire(context.Background()); err != nil {
		return err
	}
	defer rt.release()
	if !validName(def.Manifest.ID) || def.Factory == nil {
		return sdk.ErrInvalid
	}
	def.Manifest.Provides = append([]sdk.ServiceKey(nil), def.Manifest.Provides...)
	def.Manifest.Required = append([]sdk.ServiceKey(nil), def.Manifest.Required...)
	def.Manifest.Optional = append([]sdk.ServiceKey(nil), def.Manifest.Optional...)
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return ErrRuntimeClosed
	}
	if rt.records[def.Manifest.ID] != nil {
		return fmt.Errorf("%w: plugin %s", sdk.ErrConflict, def.Manifest.ID)
	}
	contracts := make(map[string]reflect.Type)
	providers := make(map[string]string)
	for _, rec := range rt.records {
		for _, list := range [][]sdk.ServiceKey{rec.def.Manifest.Provides, rec.def.Manifest.Required, rec.def.Manifest.Optional} {
			for _, key := range list {
				contracts[key.Name] = key.Type
			}
		}
		for _, key := range rec.def.Manifest.Provides {
			providers[key.Name] = rec.def.Manifest.ID
		}
	}
	seenDependencies := make(map[string]bool)
	for i, list := range [][]sdk.ServiceKey{def.Manifest.Provides, def.Manifest.Required, def.Manifest.Optional} {
		seen := make(map[string]bool)
		for _, key := range list {
			if !validService(key) || seen[key.Name] {
				return fmt.Errorf("%w: service declaration %s", sdk.ErrInvalid, key.Name)
			}
			seen[key.Name] = true
			if t := contracts[key.Name]; t != nil && t != key.Type {
				return fmt.Errorf("%w: service %s", sdk.ErrContractMismatch, key.Name)
			}
			contracts[key.Name] = key.Type
			if i == 0 {
				if owner := providers[key.Name]; owner != "" {
					return fmt.Errorf("%w: service %s provided by %s", sdk.ErrConflict, key.Name, owner)
				}
				providers[key.Name] = def.Manifest.ID
			} else {
				if seenDependencies[key.Name] {
					return fmt.Errorf("%w: overlapping dependencies %s", sdk.ErrInvalid, key.Name)
				}
				seenDependencies[key.Name] = true
			}
		}
	}
	all := make(map[string]*record, len(rt.records)+1)
	for id, rec := range rt.records {
		all[id] = rec
	}
	rec := &record{def: def, state: Registered, order: rt.nextOrder + 1}
	all[def.Manifest.ID] = rec
	if _, err := topology(all); err != nil {
		return err
	}
	rt.nextOrder++
	rt.records[def.Manifest.ID] = rec
	rt.notify()
	return nil
}

// topology只按required查找provider，optional不影响生命周期
func topology(records map[string]*record) ([]*record, error) {
	providers := make(map[string]*record)
	var ordered []*record
	for _, rec := range records {
		ordered = append(ordered, rec)
		for _, key := range rec.def.Manifest.Provides {
			providers[key.Name] = rec
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].order < ordered[j].order })
	marks := make(map[*record]uint8)
	var result []*record
	var visit func(*record) error
	visit = func(rec *record) error {
		if marks[rec] == 1 {
			return fmt.Errorf("%w: %s", ErrDependencyCycle, rec.def.Manifest.ID)
		}
		if marks[rec] == 2 {
			return nil
		}
		marks[rec] = 1
		for _, key := range rec.def.Manifest.Required {
			if provider := providers[key.Name]; provider != nil {
				if err := visit(provider); err != nil {
					return err
				}
			}
		}
		marks[rec] = 2
		result = append(result, rec)
		return nil
	}
	for _, rec := range ordered {
		if err := visit(rec); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func dependsOn(consumer, provider *record) bool {
	for _, need := range consumer.def.Manifest.Required {
		if declared(provider.def.Manifest.Provides, need) {
			return true
		}
	}
	return false
}

func (rt *Runtime) closureLocked(root *record) map[*record]bool {
	result := map[*record]bool{root: true}
	for changed := true; changed; {
		changed = false
		for _, consumer := range rt.records {
			if result[consumer] {
				continue
			}
			for provider := range result {
				if dependsOn(consumer, provider) {
					result[consumer], changed = true, true
					break
				}
			}
		}
	}
	return result
}

// 先撤下整条依赖链的入口，上游资源是否可释放另行判断
func (rt *Runtime) freezeLocked(root *record) {
	for rec := range rt.closureLocked(root) {
		if rec.scope == nil {
			continue
		}
		rec.pending, rec.state = true, Stopping
		if rec != root {
			rec.dependencyStop = true
		}
		s := rec.scope
		s.mu.Lock()
		s.freezeLocked()
		s.mu.Unlock()
		for name, entry := range rt.services {
			if entry.owner == s {
				delete(rt.services, name)
			}
		}
		for _, items := range rt.listeners {
			for _, sub := range items {
				if sub.owner == s {
					sub.visible = false
				}
			}
		}
	}
}

func (rt *Runtime) fail(s *scope, err error) {
	if err == nil {
		return
	}
	rt.mu.Lock()
	rec := rt.records[s.id]
	if rec != nil && rec.scope == s {
		rec.failure = errors.Join(rec.failure, err)
		rec.lastErr = rec.failure
		s.mu.Lock()
		s.failure = errors.Join(s.failure, err)
		s.mu.Unlock()
		rt.freezeLocked(rec)
	}
	rt.mu.Unlock()
	rt.notify()
}

func (rt *Runtime) missingLocked(rec *record) []sdk.ServiceKey {
	var missing []sdk.ServiceKey
	for _, key := range rec.def.Manifest.Required {
		if rt.services[key.Name] == nil {
			missing = append(missing, key)
		}
	}
	return missing
}

// 依赖缺失时保持Waiting，不算启动失败
func (rt *Runtime) Start(ctx context.Context, id string) error {
	if err := rt.acquire(ctx); err != nil {
		return err
	}
	defer rt.release()
	rt.mu.Lock()
	rec := rt.records[id]
	if rt.closed {
		rt.mu.Unlock()
		return ErrRuntimeClosed
	}
	if rec == nil {
		rt.mu.Unlock()
		return ErrPluginNotFound
	}
	if rec.pending || rec.state == Quarantined {
		err := errors.Join(ErrQuarantined, rec.lastErr)
		rt.mu.Unlock()
		return err
	}
	rec.wanted = true
	if rec.scope == nil {
		rec.failure, rec.lastErr = nil, nil
		rec.state = Waiting
	}
	rt.mu.Unlock()
	rt.reconcile(ctx, nil)
	return rt.operationResult(ctx, id)
}

// 手动停止清除启用意图，依赖失效停止的其他插件仍保持启用
func (rt *Runtime) Stop(ctx context.Context, id string) error {
	return rt.stop(ctx, id, false, false)
}

// Unload完成资源回收后才移除定义和Service提供声明
func (rt *Runtime) Unload(ctx context.Context, id string) error {
	return rt.stop(ctx, id, true, false)
}

// Reload完整回收旧代后，用同一Factory创建新实例
func (rt *Runtime) Reload(ctx context.Context, id string) error {
	return rt.stop(ctx, id, false, true)
}

func (rt *Runtime) stop(ctx context.Context, id string, unload, restart bool) error {
	if err := rt.acquire(ctx); err != nil {
		return err
	}
	defer rt.release()
	rt.mu.Lock()
	rec := rt.records[id]
	if rt.closed && restart {
		rt.mu.Unlock()
		return ErrRuntimeClosed
	}
	if rec == nil {
		rt.mu.Unlock()
		return ErrPluginNotFound
	}
	if restart && rec.pending {
		err := errors.Join(ErrQuarantined, rec.lastErr)
		rt.mu.Unlock()
		return err
	}
	rec.wanted, rec.unload, rec.restart = restart, unload, restart
	if restart && rec.scope == nil {
		rec.failure, rec.lastErr, rec.restart = nil, nil, false
	}
	rt.freezeLocked(rec)
	if rec.scope == nil {
		if rec.failure == nil {
			rec.state = Stopped
		}
		if unload {
			delete(rt.records, id)
		}
	}
	rt.mu.Unlock()
	rt.reconcile(ctx, nil)
	return rt.operationResult(ctx, id)
}

// 重试清理不会清除插件的启动失败记录
func (rt *Runtime) RetryCleanup(ctx context.Context, id string) error {
	if err := rt.acquire(ctx); err != nil {
		return err
	}
	defer rt.release()
	rt.mu.Lock()
	rec := rt.records[id]
	if rec == nil {
		rt.mu.Unlock()
		return ErrPluginNotFound
	}
	retry := rt.closureLocked(rec)
	rt.mu.Unlock()
	rt.reconcile(ctx, retry)
	rt.completeClose()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rec.scope != nil && rec.pending {
		return errors.Join(ErrQuarantined, rec.lastErr, ctx.Err())
	}
	return ctx.Err()
}

func (rt *Runtime) operationResult(ctx context.Context, id string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rec := rt.records[id]
	if rec == nil {
		return ctx.Err()
	}
	if rec.pending {
		return errors.Join(ErrQuarantined, rec.lastErr, ctx.Err())
	}
	if rec.state == Failed {
		return errors.Join(rec.failure, ctx.Err())
	}
	return ctx.Err()
}

func (rt *Runtime) reconcile(ctx context.Context, retry map[*record]bool) {
	rt.mu.Lock()
	order, _ := topology(rt.records) // Register已保证图无环
	rt.mu.Unlock()
	for i := len(order) - 1; i >= 0; i-- {
		rec := order[i]
		rt.mu.Lock()
		if !rec.pending || rec.scope == nil {
			rt.mu.Unlock()
			continue
		}
		var blocker string
		for _, dependent := range order {
			if dependent.scope != nil && dependsOn(dependent, rec) {
				blocker = dependent.def.Manifest.ID
				break
			}
		}
		if blocker != "" {
			rec.state = Quarantined
			rec.lastErr = errors.Join(rec.failure, fmt.Errorf("%w: waiting for dependent %s", ErrQuarantined, blocker))
			rt.mu.Unlock()
			continue
		}
		s := rec.scope
		rt.mu.Unlock()
		err := s.awaitCleanup(ctx, retry[rec])
		rt.mu.Lock()
		s.mu.Lock()
		clean := s.cleanLocked()
		if clean {
			s.finished = true
			clear(s.required)
			clear(s.provided)
			s.resources = nil
		}
		s.mu.Unlock()
		if !clean {
			rec.state = Quarantined
			if err == nil {
				err = s.cleanupError()
			}
			rec.lastErr = errors.Join(rec.failure, err)
		} else {
			rec.scope, rec.pending = nil, false
			rec.dependencyStop = false
			if rec.restart {
				rec.failure, rec.restart = nil, false
			}
			rec.lastErr = rec.failure
			switch {
			case rec.failure != nil:
				rec.state = Failed
			case rec.wanted:
				rec.state = Waiting
			default:
				rec.state = Stopped
			}
			if rec.unload {
				delete(rt.records, rec.def.Manifest.ID)
			}
		}
		rt.mu.Unlock()
	}
	for _, rec := range order {
		rt.mu.Lock()
		eligible := !rt.closed && rec.wanted && rec.failure == nil && rec.scope == nil && !rec.pending && !rec.unload
		if eligible && len(rt.missingLocked(rec)) > 0 {
			rec.state = Waiting
			eligible = false
		}
		rt.mu.Unlock()
		if eligible && ctx.Err() == nil {
			rt.startOne(ctx, rec)
		}
	}
	// 新启动失败也要在本次操作结束前尝试回滚
	rt.mu.Lock()
	needCleanup := false
	for _, rec := range rt.records {
		if rec.pending && rec.state == Stopping {
			needCleanup = true
			break
		}
	}
	rt.mu.Unlock()
	if needCleanup {
		rt.reconcile(ctx, nil)
	}
}

func (rt *Runtime) startOne(ctx context.Context, rec *record) {
	rt.mu.Lock()
	if rec.scope != nil || rec.pending || len(rt.missingLocked(rec)) != 0 || rec.failure != nil || !rec.wanted || rt.closed {
		rt.mu.Unlock()
		return
	}
	rt.nextGeneration++
	s := newScope(rt, rec)
	rec.scope, rec.state, rec.generation = s, Starting, s.gen
	rt.mu.Unlock()
	go func() {
		err := invoke(func() error {
			plugin, err := rec.def.Factory()
			if err != nil {
				return err
			}
			if plugin == nil || reflect.ValueOf(plugin).Kind() == reflect.Pointer && reflect.ValueOf(plugin).IsNil() {
				return fmt.Errorf("%w: nil plugin instance", sdk.ErrInvalid)
			}
			return plugin.Start(&pluginContext{s})
		})
		s.mu.Lock()
		s.initErr, s.initDone = err, true
		close(s.initEnd)
		s.changedLocked()
		s.mu.Unlock()
		rt.notify()
	}()
	timer := time.NewTimer(rt.options.StartTimeout)
	defer timer.Stop()
	var err error
	select {
	case <-s.initEnd:
		s.mu.Lock()
		err = s.initErr
		s.mu.Unlock()
	case <-s.run.Done():
		err = context.Cause(s.run)
	case <-ctx.Done():
		err = ctx.Err()
	case <-timer.C:
		err = ErrStartTimeout
	}
	if err != nil {
		rt.startFailure(s, err)
		return
	}
	rt.mu.Lock()
	s.mu.Lock()
	if s.phase == closing || s.failure != nil || rec.pending || ctx.Err() != nil {
		err = errors.Join(s.failure, ctx.Err(), sdk.ErrContextClosed)
	}
	for _, key := range rec.def.Manifest.Required {
		if rt.services[key.Name] != s.required[key.Name] {
			err = errors.Join(err, fmt.Errorf("required service changed: %s", key.Name))
		}
	}
	for _, key := range rec.def.Manifest.Provides {
		if s.provided[key.Name] == nil {
			err = errors.Join(err, fmt.Errorf("declared service not provided: %s", key.Name))
		}
	}
	if err == nil {
		// Service和listener的公开在同一把锁下完成
		for name, entry := range s.provided {
			rt.services[name] = entry
		}
		for _, items := range rt.listeners {
			for _, sub := range items {
				if sub.owner == s && !sub.removed {
					sub.visible = true
				}
			}
		}
		s.phase, rec.state = active, Running
		s.changedLocked()
	}
	s.mu.Unlock()
	rt.mu.Unlock()
	if err != nil {
		rt.startFailure(s, err)
	}
}

func (rt *Runtime) startFailure(s *scope, err error) {
	rt.mu.Lock()
	rec := rt.records[s.id]
	interrupted := rec != nil && rec.scope == s && rec.dependencyStop && rec.failure == nil &&
		(errors.Is(err, sdk.ErrContextClosed) || errors.Is(err, context.Canceled))
	rt.mu.Unlock()
	if interrupted {
		// 依赖撤回引起的初始化取消，不锁定为需要手动重试的故障
		rt.notify()
		return
	}
	rt.fail(s, err)
}

// 未释放的资源保留诊断和重试入口
func (rt *Runtime) Close(ctx context.Context) error {
	if err := rt.acquire(ctx); err != nil {
		return err
	}
	defer rt.release()
	rt.mu.Lock()
	rt.closed = true
	for _, rec := range rt.records {
		rec.wanted = false
		rt.freezeLocked(rec)
	}
	rt.mu.Unlock()
	rt.reconcile(ctx, nil)
	rt.completeClose()
	rt.mu.Lock()
	var failures []error
	for _, rec := range rt.records {
		if rec.scope != nil {
			failures = append(failures, fmt.Errorf("%s: %w", rec.def.Manifest.ID, errors.Join(ErrQuarantined, rec.lastErr)))
		}
	}
	rt.mu.Unlock()
	if len(failures) == 0 {
		<-rt.coordinatorEnd
	}
	return errors.Join(append(failures, ctx.Err())...)
}

func (rt *Runtime) completeClose() {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if !rt.closed {
		return
	}
	for _, rec := range rt.records {
		if rec.scope != nil {
			return
		}
	}
	clear(rt.records)
	rt.cancel()
}

// Status返回快照，调用方修改切片不会影响Runtime
func (rt *Runtime) Status(id string) (Status, bool) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rec := rt.records[id]
	if rec == nil {
		return Status{}, false
	}
	return rt.statusLocked(rec), true
}

func (rt *Runtime) statusLocked(rec *record) Status {
	result := Status{
		ID: rec.def.Manifest.ID, State: rec.state, Enabled: rec.wanted,
		Generation: rec.generation, Missing: rt.missingLocked(rec), Error: rec.lastErr,
	}
	if rec.scope != nil {
		rec.scope.mu.Lock()
		result.Resources = rec.scope.resourcesLocked()
		rec.scope.mu.Unlock()
	}
	return result
}

func (rt *Runtime) Snapshot() []Status {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	var records []*record
	for _, rec := range rt.records {
		records = append(records, rec)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].order < records[j].order })
	result := make([]Status, 0, len(records))
	for _, rec := range records {
		result = append(result, rt.statusLocked(rec))
	}
	return result
}

// 取消但未退出的任务仍计入资源统计
func (rt *Runtime) Stats() Stats {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	result := Stats{Plugins: len(rt.records), Services: len(rt.services)}
	for _, items := range rt.listeners {
		for _, sub := range items {
			if sub.visible && !sub.removed {
				result.Listeners++
			}
		}
	}
	for _, rec := range rt.records {
		if rec.scope == nil {
			continue
		}
		rec.scope.mu.Lock()
		for _, resource := range rec.scope.resourcesLocked() {
			result.Resources++
			if resource.Dirty {
				result.Dirty++
			}
			switch resource.Kind {
			case "task":
				result.Tasks++
			case "timer":
				result.Timers++
			case "callback":
				result.Inflight++
			}
		}
		rec.scope.mu.Unlock()
	}
	return result
}
