package core

import "testbot/core/internal/kernel"

type Runtime = kernel.Runtime

type Options = kernel.Options

type State = kernel.State

type Status = kernel.Status

type ResourceStatus = kernel.ResourceStatus

type Stats = kernel.Stats

type PanicError = kernel.PanicError

type ListenerError = kernel.ListenerError

const (
	Registered  = kernel.Registered
	Waiting     = kernel.Waiting
	Starting    = kernel.Starting
	Running     = kernel.Running
	Stopping    = kernel.Stopping
	Stopped     = kernel.Stopped
	Failed      = kernel.Failed
	Quarantined = kernel.Quarantined
)

var (
	ErrPluginNotFound     = kernel.ErrPluginNotFound
	ErrRuntimeClosed      = kernel.ErrRuntimeClosed
	ErrQuarantined        = kernel.ErrQuarantined
	ErrDependencyCycle    = kernel.ErrDependencyCycle
	ErrStartTimeout       = kernel.ErrStartTimeout
	ErrCleanupTimeout     = kernel.ErrCleanupTimeout
	ErrLifecycleReentrant = kernel.ErrLifecycleReentrant
)

// 宿主结束时必须调用Close
func New(options ...Options) *Runtime { return kernel.New(options...) }
