package core_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/sdk"
)

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(2 * time.Second):
		t.Fatal("channel did not settle")
		var zero T
		return zero
	}
}

func TestEmitOrderAndErrorAggregation(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("ordered", sdk.EmitMode)
	var emitter sdk.Context
	var order []int
	first, second := errors.New("first"), errors.New("second")
	d := definition("plugin", func(ctx sdk.Context) error {
		emitter = ctx
		for i, err := range []error{first, second, nil} {
			if _, subscribeErr := sdk.On(ctx, event, func(context.Context, int) error { order = append(order, i); return err }); subscribeErr != nil {
				return subscribeErr
			}
		}
		return nil
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	err := sdk.Emit(emitter, background, event, 5)
	if !errors.Is(err, first) || !errors.Is(err, second) || fmt.Sprint(order) != "[0 1 2]" {
		t.Fatalf("order=%v error=%v", order, err)
	}
	status(t, rt, "plugin", core.Running)
	if err := sdk.Parallel(emitter, background, event, 5); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	if rt.Stats().Inflight != 0 {
		t.Fatal("dispatch lease remains")
	}
}

func TestParallelWaitsForEveryListener(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("parallel", sdk.ParallelMode)
	var emitter sdk.Context
	entered := make(chan int, 2)
	release := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var once [2]sync.Once
	t.Cleanup(func() {
		for i := range release {
			once[i].Do(func() { close(release[i]) })
		}
	})
	first, second := errors.New("first"), errors.New("second")
	d := definition("plugin", func(ctx sdk.Context) error {
		emitter = ctx
		for i, err := range []error{first, second} {
			if _, subErr := sdk.On(ctx, event, func(context.Context, int) error { entered <- i; <-release[i]; return err }); subErr != nil {
				return subErr
			}
		}
		return nil
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	result := make(chan error, 1)
	go func() { result <- sdk.Parallel(emitter, background, event, 1) }()
	a, b := receive(t, entered), receive(t, entered)
	if a == b {
		t.Fatal("listener entered twice")
	}
	once[1].Do(func() { close(release[1]) })
	select {
	case err := <-result:
		t.Fatalf("returned before all completed: %v", err)
	default:
	}
	once[0].Do(func() { close(release[0]) })
	err := receive(t, result)
	if !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatal(err)
	}
	if rt.Stats().Inflight != 0 {
		t.Fatal("parallel callbacks remain")
	}
}

func TestListenerSnapshotAndSelfUnsubscribe(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("snapshot", sdk.EmitMode)
	var emitter sdk.Context
	var unsubscribe sdk.CancelFunc
	var early, late int
	d := definition("plugin", func(ctx sdk.Context) error {
		emitter = ctx
		var err error
		unsubscribe, err = sdk.On(ctx, event, func(context.Context, int) error {
			early++
			unsubscribe()
			_, err := sdk.On(ctx, event, func(context.Context, int) error { late++; return nil })
			return err
		})
		return err
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	must(t, sdk.Emit(emitter, background, event, 1))
	if early != 1 || late != 0 {
		t.Fatal("dispatch did not use subscription snapshot")
	}
	must(t, sdk.Emit(emitter, background, event, 1))
	if early != 1 || late != 1 {
		t.Fatalf("early=%d late=%d", early, late)
	}
	unsubscribe()
}

func TestListenerPanicStillRunsRemainingSnapshot(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("panic", sdk.EmitMode)
	var emitter sdk.Context
	var last atomic.Int32
	d := definition("plugin", func(ctx sdk.Context) error {
		emitter = ctx
		if _, err := sdk.On(ctx, event, func(context.Context, int) error { panic("listener panic") }); err != nil {
			return err
		}
		_, err := sdk.On(ctx, event, func(context.Context, int) error { last.Add(1); return nil })
		return err
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	err := sdk.Emit(emitter, background, event, 1)
	var panicErr *core.PanicError
	if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
		t.Fatal(err)
	}
	if last.Load() != 1 {
		t.Fatal("panic prevented remaining listener")
	}
	eventually(t, func() bool { s, _ := rt.Status("plugin"); return s.State == core.Failed && len(s.Resources) == 0 })
}

func TestEventContractsAndNullablePayload(t *testing.T) {
	rt := runtimeFor(t)
	var emitter sdk.Context
	contract := sdk.NewEvent[any]("contract", sdk.EmitMode)
	var calls int
	d := definition("plugin", func(ctx sdk.Context) error {
		emitter = ctx
		_, err := sdk.On(ctx, contract, func(_ context.Context, payload any) error {
			if payload != nil {
				return errors.New("expected nil")
			}
			calls++
			return nil
		})
		return err
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	must(t, sdk.Emit(emitter, background, contract, nil))
	if calls != 1 {
		t.Fatal(calls)
	}
	wrong := sdk.NewEvent[int]("contract", sdk.EmitMode)
	if _, err := sdk.On(emitter, wrong, func(context.Context, int) error { return nil }); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	if err := sdk.Emit(emitter, background, wrong, 1); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	if err := emitter.Emit(background, contract.Key(), nil); err != nil {
		t.Fatal(err)
	}
}

func TestUnloadingWaitsForInflightListener(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("blocked", sdk.EmitMode)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var emitter sdk.Context
	must(t, rt.Register(definition("emitter", func(ctx sdk.Context) error { emitter = ctx; return nil })))
	must(t, rt.Register(definition("listener", func(ctx sdk.Context) error {
		_, err := sdk.On(ctx, event, func(context.Context, int) error { close(entered); <-release; return nil })
		return err
	})))
	must(t, rt.Start(background, "emitter"))
	must(t, rt.Start(background, "listener"))
	done := make(chan error, 1)
	go func() { done <- sdk.Emit(emitter, background, event, 1) }()
	receive(t, entered)
	if err := rt.Unload(background, "listener"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	status(t, rt, "listener", core.Quarantined)
	if rt.Stats().Listeners != 0 {
		t.Fatal("listener entry remains visible")
	}
	once.Do(func() { close(release) })
	must(t, receive(t, done))
	eventually(t, func() bool { _, exists := rt.Status("listener"); return !exists })
}
