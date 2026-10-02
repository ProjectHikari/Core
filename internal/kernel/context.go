package kernel

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"testbot/core/sdk"
)

type pluginContext struct{ scope *scope }

var _ sdk.Context = (*pluginContext)(nil)

func (c *pluginContext) Context() context.Context { return c.scope.run }
func (c *pluginContext) PluginID() string         { return c.scope.id }
func (c *pluginContext) Generation() uint64       { return c.scope.gen }
func (c *pluginContext) Defer(label string, dispose sdk.Disposer) error {
	return c.scope.deferResource(label, dispose)
}
func (c *pluginContext) Go(label string, task sdk.Task) (sdk.CancelFunc, error) {
	return c.scope.task(label, "task", task)
}

func (c *pluginContext) waitActive(run context.Context) error {
	s := c.scope
	for {
		s.mu.Lock()
		state, changed := s.phase, s.changed
		s.mu.Unlock()
		if state == active {
			return nil
		}
		if state == closing {
			return sdk.ErrContextClosed
		}
		select {
		case <-run.Done():
			return run.Err()
		case <-changed:
		}
	}
}

func (c *pluginContext) After(label string, delay time.Duration, task sdk.Task) (sdk.CancelFunc, error) {
	if delay < 0 || task == nil {
		return nil, sdk.ErrInvalid
	}
	return c.scope.task(label, "timer", func(run context.Context) error {
		if err := c.waitActive(run); err != nil {
			return nil
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-run.Done():
			return nil
		case <-timer.C:
			return c.timerCall(run, label, task)
		}
	})
}

func (c *pluginContext) Every(label string, interval time.Duration, task sdk.Task) (sdk.CancelFunc, error) {
	if interval <= 0 || task == nil {
		return nil, sdk.ErrInvalid
	}
	return c.scope.task(label, "timer", func(run context.Context) error {
		if err := c.waitActive(run); err != nil {
			return nil
		}
		timer := time.NewTicker(interval)
		defer timer.Stop()
		for {
			select {
			case <-run.Done():
				return nil
			case <-timer.C:
				if err := c.timerCall(run, label, task); err != nil {
					return err
				}
				c.scope.mu.Lock()
				closed := c.scope.phase == closing
				c.scope.mu.Unlock()
				if closed {
					return nil
				}
			}
		}
	})
}

func (c *pluginContext) timerCall(run context.Context, label string, task sdk.Task) error {
	s := c.scope
	s.mu.Lock()
	token, ok := s.enterLocked("timer callback: " + label)
	s.mu.Unlock()
	if !ok {
		return nil
	}
	defer s.leave(token)
	return task(run)
}

func validName(name string) bool           { return name != "" && strings.TrimSpace(name) == name }
func validService(key sdk.ServiceKey) bool { return validName(key.Name) && key.Type != nil }
func validEvent(key sdk.EventKey) bool {
	return validName(key.Name) && key.Type != nil && (key.Mode == sdk.EmitMode || key.Mode == sdk.ParallelMode)
}

func valueMatches(t reflect.Type, value any, allowNil bool) bool {
	if value == nil {
		if !allowNil {
			return false
		}
		switch t.Kind() {
		case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
			return true
		}
		return false
	}
	v := reflect.ValueOf(value)
	if !v.Type().AssignableTo(t) {
		return false
	}
	if !allowNil {
		switch v.Kind() {
		case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface:
			return !v.IsNil()
		}
	}
	return true
}

func declared(keys []sdk.ServiceKey, key sdk.ServiceKey) bool {
	for _, candidate := range keys {
		if candidate == key {
			return true
		}
	}
	return false
}

func (c *pluginContext) Provide(key sdk.ServiceKey, value any) error {
	if !validService(key) {
		return sdk.ErrInvalid
	}
	if !valueMatches(key.Type, value, false) {
		return fmt.Errorf("%w: service %s", sdk.ErrContractMismatch, key.Name)
	}
	s, rt := c.scope, c.scope.runtime
	rt.mu.Lock()
	s.mu.Lock()
	defer rt.mu.Unlock()
	defer s.mu.Unlock()
	rec := rt.records[s.id]
	if rec == nil || rec.scope != s || s.phase == closing || s.finished {
		return sdk.ErrContextClosed
	}
	if s.phase != initializing {
		return sdk.ErrNotRunning // Service提供属于启动事务
	}
	if !declared(rec.def.Manifest.Provides, key) {
		return fmt.Errorf("%w: provide %s", sdk.ErrUndeclared, key.Name)
	}
	if s.provided[key.Name] != nil {
		return fmt.Errorf("%w: service %s", sdk.ErrConflict, key.Name)
	}
	entry := &serviceEntry{key: key, value: value, owner: s}
	s.provided[key.Name] = entry
	s.resources = append(s.resources, &resource{
		label: "service: " + key.Name, kind: "service",
		dispose: func(context.Context) error {
			rt.mu.Lock()
			if rt.services[key.Name] == entry {
				delete(rt.services, key.Name)
			}
			s.mu.Lock()
			if s.provided[key.Name] == entry {
				delete(s.provided, key.Name)
			}
			s.mu.Unlock()
			rt.mu.Unlock()
			return nil
		},
	})
	s.changedLocked()
	return nil
}

func (c *pluginContext) Lookup(key sdk.ServiceKey) (any, bool, error) {
	if !validService(key) {
		return nil, false, sdk.ErrInvalid
	}
	s, rt := c.scope, c.scope.runtime
	rt.mu.Lock()
	s.mu.Lock()
	defer rt.mu.Unlock()
	defer s.mu.Unlock()
	rec := rt.records[s.id]
	if rec == nil || rec.scope != s || s.phase == closing || s.finished {
		return nil, false, sdk.ErrContextClosed
	}
	if declared(rec.def.Manifest.Provides, key) {
		entry := s.provided[key.Name]
		if entry == nil {
			return nil, false, nil
		}
		return entry.value, true, nil
	}
	if declared(rec.def.Manifest.Required, key) {
		entry := s.required[key.Name]
		if entry == nil {
			return nil, false, nil
		}
		return entry.value, true, nil
	}
	if declared(rec.def.Manifest.Optional, key) {
		entry := rt.services[key.Name]
		if entry == nil {
			return nil, false, nil
		}
		return entry.value, true, nil
	}
	return nil, false, fmt.Errorf("%w: lookup %s", sdk.ErrUndeclared, key.Name)
}
