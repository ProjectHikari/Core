package sdk

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
)

var (
	ErrInvalid          = errors.New("invalid contract or argument")
	ErrContextClosed    = errors.New("plugin context is closed")
	ErrNotRunning       = errors.New("plugin is not running")
	ErrConflict         = errors.New("registration conflict")
	ErrContractMismatch = errors.New("contract type or mode mismatch")
	ErrUndeclared       = errors.New("service is not declared by plugin")
)

// 注册时复制声明，Required控制生命周期，Optional仅允许查询
type Manifest struct {
	ID       string
	Provides []ServiceKey
	Required []ServiceKey
	Optional []ServiceKey
}

// 每代创建新实例，外部资源在Start中登记
type Factory func() (Plugin, error)

type Definition struct {
	Manifest Manifest
	Factory  Factory
}

type Plugin interface {
	Start(Context) error
}

type StartFunc func(Context) error

func (f StartFunc) Start(ctx Context) error { return f(ctx) }

// 幂等取消，不等待执行结束，实际退出另行追踪
type CancelFunc func()

// 返回nil确认释放完成，失败后需支持重试
type Disposer func(context.Context) error

type Task func(context.Context) error

// 并发分发时载荷只读
type Handler func(context.Context, any) error

// Context属于一代实例，不随宿主Start请求结束而失效
type Context interface {
	Context() context.Context
	PluginID() string
	Generation() uint64
	Defer(label string, dispose Disposer) error
	Go(label string, task Task) (CancelFunc, error)
	After(label string, delay time.Duration, task Task) (CancelFunc, error)
	Every(label string, interval time.Duration, task Task) (CancelFunc, error)
	Provide(ServiceKey, any) error
	Lookup(ServiceKey) (any, bool, error)
	On(EventKey, Handler) (CancelFunc, error)
	Emit(context.Context, EventKey, any) error
	Parallel(context.Context, EventKey, any) error
}

type ServiceKey struct {
	Name string
	Type reflect.Type
}

type Service[T any] struct{ key ServiceKey }

func NewService[T any](name string) Service[T] {
	return Service[T]{ServiceKey{name, reflect.TypeFor[T]()}}
}

func (s Service[T]) Key() ServiceKey { return s.key }

// 仅在启动事务内提供Service
func Provide[T any](ctx Context, service Service[T], value T) error {
	return ctx.Provide(service.key, value)
}

// 只能查询已声明的Service，缺失时返回found=false
func Lookup[T any](ctx Context, service Service[T]) (T, bool, error) {
	var zero T
	value, found, err := ctx.Lookup(service.key)
	if err != nil || !found {
		return zero, found, err
	}
	result, ok := value.(T)
	if !ok {
		return zero, false, fmt.Errorf("%w: service %s", ErrContractMismatch, service.key.Name)
	}
	return result, true, nil
}

type DispatchMode uint8

const (
	// 按订阅顺序同步执行
	EmitMode DispatchMode = iota + 1
	// 并发执行并等待全部完成
	ParallelMode
)

type EventKey struct {
	Name string
	Type reflect.Type
	Mode DispatchMode
}

type Event[T any] struct{ key EventKey }

// 同名事件的载荷类型和分发模式必须一致
func NewEvent[T any](name string, mode DispatchMode) Event[T] {
	return Event[T]{EventKey{name, reflect.TypeFor[T](), mode}}
}

func (e Event[T]) Key() EventKey { return e.key }

// 订阅随所属Context回收
func On[T any](ctx Context, event Event[T], handler func(context.Context, T) error) (CancelFunc, error) {
	if handler == nil {
		return nil, ErrInvalid
	}
	return ctx.On(event.key, func(run context.Context, payload any) error {
		value, ok := payload.(T)
		if !ok {
			// nil接口无法通过类型断言，可空载荷按零值传给listener
			if payload == nil && nullable(event.key.Type) {
				var zero T
				return handler(run, zero)
			}
			return ErrContractMismatch
		}
		return handler(run, value)
	})
}

// 单个listener失败不阻断本轮分发，错误统一汇总
func Emit[T any](ctx Context, run context.Context, event Event[T], payload T) error {
	return ctx.Emit(run, event.key, payload)
}

// 等待所有listener完成后汇总错误
func Parallel[T any](ctx Context, run context.Context, event Event[T], payload T) error {
	return ctx.Parallel(run, event.key, payload)
}

func nullable(t reflect.Type) bool {
	if t == nil {
		return false
	}
	switch t.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return true
	}
	return false
}
