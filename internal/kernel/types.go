package kernel

import (
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"testbot/core/sdk"
)

var (
	ErrPluginNotFound     = errors.New("plugin not found")
	ErrRuntimeClosed      = errors.New("runtime is closed")
	ErrQuarantined        = errors.New("activation is quarantined")
	ErrDependencyCycle    = errors.New("required dependency cycle")
	ErrStartTimeout       = errors.New("plugin initialization timed out")
	ErrCleanupTimeout     = errors.New("resource cleanup timed out")
	ErrLifecycleReentrant = errors.New("lifecycle operation from plugin execution context")
)

type State string

const (
	Registered  State = "Registered"
	Waiting     State = "Waiting"
	Starting    State = "Starting"
	Running     State = "Running"
	Stopping    State = "Stopping"
	Stopped     State = "Stopped"
	Failed      State = "Failed"
	Quarantined State = "Quarantined"
)

// Options中的非正数使用默认值
type Options struct {
	StartTimeout   time.Duration
	DisposeTimeout time.Duration
}

type ResourceStatus struct {
	Label string
	Kind  string
	Dirty bool
	Error error
}

type Status struct {
	ID         string
	State      State
	Enabled    bool
	Generation uint64
	Missing    []sdk.ServiceKey
	Error      error
	Resources  []ResourceStatus
}

type Stats struct {
	Plugins, Services, Listeners, Tasks, Timers, Resources, Inflight, Dirty int
}

type PanicError struct {
	Value any
	Stack []byte
}

func (p *PanicError) Error() string { return fmt.Sprintf("plugin panic: %v", p.Value) }

func invoke(fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = &PanicError{Value: value, Stack: debug.Stack()}
		}
	}()
	return fn()
}

type ListenerError struct {
	PluginID string
	Event    string
	Index    int
	Err      error
}

func (e *ListenerError) Error() string {
	return fmt.Sprintf("event %s listener %d (%s): %v", e.Event, e.Index, e.PluginID, e.Err)
}

func (e *ListenerError) Unwrap() error { return e.Err }

type executionOwnerKey struct{}
