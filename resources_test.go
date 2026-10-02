package core_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/sdk"
)

func TestTimersActivateAfterCommitAndCanCancelThemselves(t *testing.T) {
	rt := runtimeFor(t)
	ready, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var calls atomic.Int32
	d := definition("plugin", func(ctx sdk.Context) error {
		var cancel sdk.CancelFunc
		var err error
		cancel, err = ctx.After("self cancelling", 0, func(context.Context) error { cancel(); calls.Add(1); return nil })
		if err != nil {
			return err
		}
		close(ready)
		<-release
		return nil
	})
	must(t, rt.Register(d))
	done := make(chan error, 1)
	go func() { done <- rt.Start(background, "plugin") }()
	receive(t, ready)
	if calls.Load() != 0 {
		t.Fatal("timer ran before commit")
	}
	once.Do(func() { close(release) })
	must(t, receive(t, done))
	eventually(t, func() bool { return calls.Load() == 1 && rt.Stats().Resources == 0 })
}

func TestStoppingJoinsTimerCallback(t *testing.T) {
	rt := runtimeFor(t)
	entered := make(chan struct{})
	var calls atomic.Int32
	must(t, rt.Register(definition("plugin", func(ctx sdk.Context) error {
		_, err := ctx.Every("ticker", time.Millisecond, func(run context.Context) error {
			calls.Add(1)
			close(entered)
			<-run.Done()
			return run.Err()
		})
		return err
	})))
	must(t, rt.Start(background, "plugin"))
	receive(t, entered)
	must(t, rt.Stop(background, "plugin"))
	if calls.Load() != 1 || rt.Stats().Timers != 0 || rt.Stats().Inflight != 0 {
		t.Fatal(rt.Stats())
	}
	status(t, rt, "plugin", core.Stopped)
}

func TestRepeatedReloadAndResourceBaseline(t *testing.T) {
	rt := runtimeFor(t)
	key := sdk.NewService[int]("service")
	event := sdk.NewEvent[int]("event", sdk.EmitMode)
	var contexts []sdk.Context
	d := definition("plugin", func(ctx sdk.Context) error {
		contexts = append(contexts, ctx)
		if err := sdk.Provide(ctx, key, 1); err != nil {
			return err
		}
		if _, err := sdk.On(ctx, event, func(context.Context, int) error { return nil }); err != nil {
			return err
		}
		if _, err := ctx.Go("worker", func(run context.Context) error { <-run.Done(); return run.Err() }); err != nil {
			return err
		}
		_, err := ctx.Every("timer", time.Hour, func(context.Context) error { return nil })
		return err
	})
	d.Manifest.Provides = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	baseline := rt.Stats()
	for i := 0; i < 100; i++ {
		must(t, rt.Reload(background, "plugin"))
		if got := rt.Stats(); got != baseline {
			t.Fatalf("iteration %d: %+v, baseline %+v", i, got, baseline)
		}
	}
	old := contexts[0]
	if err := sdk.Provide(old, key, 10); !errors.Is(err, sdk.ErrContextClosed) {
		t.Fatal(err)
	}
	if _, err := sdk.On(old, event, func(context.Context, int) error { return nil }); !errors.Is(err, sdk.ErrContextClosed) {
		t.Fatal(err)
	}
	if _, err := old.After("late timer", 0, func(context.Context) error { return nil }); !errors.Is(err, sdk.ErrContextClosed) {
		t.Fatal(err)
	}
	if rt.Stats() != baseline {
		t.Fatal("stale Context changed new generation")
	}
	must(t, rt.Stop(background, "plugin"))
	if rt.Stats().Resources != 0 {
		t.Fatal(rt.Stats())
	}
}

func TestConcurrentLifecycleAndDispatch(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("concurrent", sdk.ParallelMode)
	var emitter sdk.Context
	var calls atomic.Int32
	must(t, rt.Register(definition("emitter", func(ctx sdk.Context) error { emitter = ctx; return nil })))
	must(t, rt.Register(definition("listener", func(ctx sdk.Context) error {
		_, err := sdk.On(ctx, event, func(context.Context, int) error { calls.Add(1); return nil })
		return err
	})))
	must(t, rt.Start(background, "emitter"))
	must(t, rt.Start(background, "listener"))
	must(t, sdk.Parallel(emitter, background, event, 1))
	errorsCh := make(chan error, 400)
	var wait sync.WaitGroup
	for i := 0; i < 2; i++ {
		wait.Go(func() {
			for j := 0; j < 50; j++ {
				if err := sdk.Parallel(emitter, background, event, j); err != nil {
					errorsCh <- err
				}
			}
		})
	}
	for i := 0; i < 2; i++ {
		wait.Go(func() {
			for j := 0; j < 20; j++ {
				if err := rt.Reload(background, "listener"); err != nil {
					errorsCh <- err
				}
				if err := rt.Stop(background, "listener"); err != nil {
					errorsCh <- err
				}
				if err := rt.Start(background, "listener"); err != nil {
					errorsCh <- err
				}
			}
		})
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	must(t, rt.Unload(background, "listener"))
	if rt.Stats().Listeners != 0 || rt.Stats().Inflight != 0 || rt.Stats().Resources != 0 || calls.Load() == 0 {
		t.Fatal(rt.Stats())
	}
}

type number interface{ Number() int }
type numberValue struct{}

func (*numberValue) Number() int { return 4 }

func TestServiceTypesAndUndeclaredAccess(t *testing.T) {
	rt := runtimeFor(t)
	key := sdk.NewService[number]("number")
	var saved sdk.Context
	d := definition("provider", func(ctx sdk.Context) error { saved = ctx; return sdk.Provide[number](ctx, key, &numberValue{}) })
	d.Manifest.Provides = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(d))
	must(t, rt.Start(background, "provider"))
	v, found, err := sdk.Lookup(saved, key)
	if err != nil || !found || v.Number() != 4 {
		t.Fatalf("%v %v %v", v, found, err)
	}
	if _, _, err := sdk.Lookup(saved, sdk.NewService[int]("undeclared")); !errors.Is(err, sdk.ErrUndeclared) {
		t.Fatal(err)
	}
	if err := saved.Provide(key.Key(), "wrong type"); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	nilKey := sdk.NewService[number]("nil")
	nilProvider := definition("nil", func(ctx sdk.Context) error { var value *numberValue; return sdk.Provide[number](ctx, nilKey, value) })
	nilProvider.Manifest.Provides = []sdk.ServiceKey{nilKey.Key()}
	must(t, rt.Register(nilProvider))
	if err := rt.Start(background, "nil"); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	duplicateKey := sdk.NewService[int]("duplicate")
	duplicate := definition("duplicate", func(ctx sdk.Context) error {
		if err := sdk.Provide(ctx, duplicateKey, 1); err != nil {
			return err
		}
		return sdk.Provide(ctx, duplicateKey, 2)
	})
	duplicate.Manifest.Provides = []sdk.ServiceKey{duplicateKey.Key()}
	must(t, rt.Register(duplicate))
	if err := rt.Start(background, "duplicate"); !errors.Is(err, sdk.ErrConflict) {
		t.Fatal(err)
	}
	if rt.Stats().Services != 1 {
		t.Fatal("failed provision remained public")
	}
}
