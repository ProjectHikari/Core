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

func TestCleanupContinuesAfterErrorAndPanic(t *testing.T) {
	rt := runtimeFor(t)
	var order []string
	var second, third atomic.Int32
	boom := errors.New("dispose failed")
	d := definition("plugin", func(ctx sdk.Context) error {
		if err := ctx.Defer("first", func(context.Context) error { order = append(order, "first"); return nil }); err != nil {
			return err
		}
		if err := ctx.Defer("second", func(context.Context) error {
			order = append(order, "second")
			if second.Add(1) == 1 {
				return boom
			}
			return nil
		}); err != nil {
			return err
		}
		return ctx.Defer("third", func(context.Context) error {
			order = append(order, "third")
			if third.Add(1) == 1 {
				panic("dispose panic")
			}
			return nil
		})
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	err := rt.Stop(background, "plugin")
	var panicErr *core.PanicError
	if !errors.Is(err, core.ErrQuarantined) || !errors.Is(err, boom) || !errors.As(err, &panicErr) {
		t.Fatal(err)
	}
	s := status(t, rt, "plugin", core.Quarantined)
	if len(s.Resources) != 2 || fmt.Sprint(order) != "[third second first]" {
		t.Fatalf("status=%+v order=%v", s, order)
	}
	if err := rt.Reload(background, "plugin"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	must(t, rt.RetryCleanup(background, "plugin"))
	status(t, rt, "plugin", core.Stopped)
	if fmt.Sprint(order) != "[third second first third second]" {
		t.Fatal(order)
	}
	if rt.Stats().Resources != 0 {
		t.Fatal(rt.Stats())
	}
}

func TestDirtyDependentRetainsTransitiveProviders(t *testing.T) {
	rt := runtimeFor(t)
	a, b := sdk.NewService[int]("a"), sdk.NewService[int]("b")
	var providerReleased, middleReleased atomic.Int32
	release, entered := make(chan struct{}), make(chan struct{}, 2)
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	p := definition("provider", func(ctx sdk.Context) error {
		if err := ctx.Defer("provider resource", func(context.Context) error { providerReleased.Add(1); return nil }); err != nil {
			return err
		}
		return sdk.Provide(ctx, a, 1)
	})
	p.Manifest.Provides = []sdk.ServiceKey{a.Key()}
	m := definition("middle", func(ctx sdk.Context) error {
		if err := ctx.Defer("middle resource", func(context.Context) error { middleReleased.Add(1); return nil }); err != nil {
			return err
		}
		return sdk.Provide(ctx, b, 2)
	})
	m.Manifest.Provides, m.Manifest.Required = []sdk.ServiceKey{b.Key()}, []sdk.ServiceKey{a.Key()}
	c := definition("consumer", func(ctx sdk.Context) error {
		_, err := ctx.Go("non-cooperative worker", func(context.Context) error { entered <- struct{}{}; <-release; return nil })
		return err
	})
	c.Manifest.Required = []sdk.ServiceKey{b.Key()}
	for _, d := range []sdk.Definition{p, m, c, definition("unrelated", func(sdk.Context) error { return nil })} {
		must(t, rt.Register(d))
	}
	for _, id := range []string{"consumer", "middle", "provider", "unrelated"} {
		must(t, rt.Start(background, id))
	}
	receive(t, entered)
	if err := rt.Stop(background, "provider"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	for _, id := range []string{"consumer", "middle", "provider"} {
		status(t, rt, id, core.Quarantined)
	}
	status(t, rt, "unrelated", core.Running)
	if providerReleased.Load() != 0 || middleReleased.Load() != 0 || rt.Stats().Services != 0 {
		t.Fatal("provider was freed while still needed")
	}
	once.Do(func() { close(release) })
	eventually(t, func() bool { s, _ := rt.Status("provider"); return s.State == core.Stopped })
	status(t, rt, "consumer", core.Waiting)
	status(t, rt, "middle", core.Waiting)
	if providerReleased.Load() != 1 || middleReleased.Load() != 1 {
		t.Fatal("provider cleanup did not resume")
	}
	must(t, rt.Start(background, "provider"))
	status(t, rt, "consumer", core.Running)
}

func TestLateCleanupAfterStartupTimeout(t *testing.T) {
	rt := core.New(core.Options{StartTimeout: 10 * time.Millisecond, DisposeTimeout: 10 * time.Millisecond})
	t.Cleanup(func() { must(t, rt.Close(background)) })
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var freed atomic.Int32
	d := definition("plugin", func(ctx sdk.Context) error {
		<-release
		return ctx.Defer("late resource", func(cleanup context.Context) error {
			if cleanup.Err() != nil {
				return cleanup.Err()
			}
			freed.Add(1)
			return nil
		})
	})
	must(t, rt.Register(d))
	if err := rt.Start(background, "plugin"); !errors.Is(err, core.ErrStartTimeout) || !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	status(t, rt, "plugin", core.Quarantined)
	once.Do(func() { close(release) })
	eventually(t, func() bool { s, _ := rt.Status("plugin"); return s.State == core.Failed && len(s.Resources) == 0 })
	if freed.Load() != 1 {
		t.Fatal("late resource was not reclaimed")
	}
}

func TestTimedOutDisposerRemainsTracked(t *testing.T) {
	rt := runtimeFor(t)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var blockedCalls, otherCalls atomic.Int32
	d := definition("plugin", func(ctx sdk.Context) error {
		if err := ctx.Defer("other", func(context.Context) error { otherCalls.Add(1); return nil }); err != nil {
			return err
		}
		return ctx.Defer("blocked", func(context.Context) error { blockedCalls.Add(1); <-release; return nil })
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	if err := rt.Stop(background, "plugin"); !errors.Is(err, core.ErrCleanupTimeout) {
		t.Fatal(err)
	}
	if otherCalls.Load() != 1 {
		t.Fatal("blocked disposer prevented other cleanup")
	}
	if err := rt.RetryCleanup(background, "plugin"); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	if blockedCalls.Load() != 1 {
		t.Fatal("retry duplicated an in-flight disposer")
	}
	once.Do(func() { close(release) })
	eventually(t, func() bool { s, _ := rt.Status("plugin"); return s.State == core.Stopped })
}

func TestManagedTaskFailuresAreIsolated(t *testing.T) {
	for _, kind := range []string{"error", "panic", "uncancelled cancellation"} {
		t.Run(kind, func(t *testing.T) {
			rt := runtimeFor(t)
			release := make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			var starts atomic.Int32
			boom := errors.New("task failed")
			d := definition("plugin", func(ctx sdk.Context) error {
				attempt := starts.Add(1)
				_, err := ctx.Go("worker", func(run context.Context) error {
					if attempt > 1 {
						<-run.Done()
						return run.Err()
					}
					<-release
					switch kind {
					case "panic":
						panic("task panic")
					case "uncancelled cancellation":
						return context.Canceled
					default:
						return boom
					}
				})
				return err
			})
			must(t, rt.Register(d))
			must(t, rt.Register(definition("unrelated", func(sdk.Context) error { return nil })))
			must(t, rt.Start(background, "plugin"))
			once.Do(func() { close(release) })
			eventually(t, func() bool { s, _ := rt.Status("plugin"); return s.State == core.Failed && len(s.Resources) == 0 })
			must(t, rt.Start(background, "unrelated"))
			if starts.Load() != 1 {
				t.Fatal("task failure automatically restarted plugin")
			}
			must(t, rt.Start(background, "plugin"))
			status(t, rt, "plugin", core.Running)
		})
	}
}

func TestFactoryPanicAndMissingPromise(t *testing.T) {
	rt := runtimeFor(t)
	d := sdk.Definition{Manifest: sdk.Manifest{ID: "panic"}, Factory: func() (sdk.Plugin, error) { panic("factory") }}
	must(t, rt.Register(d))
	var panicErr *core.PanicError
	if err := rt.Start(background, "panic"); !errors.As(err, &panicErr) {
		t.Fatal(err)
	}
	status(t, rt, "panic", core.Failed)
	key := sdk.NewService[int]("missing")
	d = definition("promise", func(sdk.Context) error { return nil })
	d.Manifest.Provides = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(d))
	if err := rt.Start(background, "promise"); err == nil {
		t.Fatal("missing promised service committed")
	}
	status(t, rt, "promise", core.Failed)
	if rt.Stats().Resources != 0 || rt.Stats().Services != 0 {
		t.Fatal(rt.Stats())
	}
}

func TestCloseRetainsDirtyRecordsUntilReleased(t *testing.T) {
	rt := runtimeFor(t)
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	must(t, rt.Register(definition("plugin", func(ctx sdk.Context) error {
		_, err := ctx.Go("blocked", func(context.Context) error { <-release; return nil })
		return err
	})))
	must(t, rt.Start(background, "plugin"))
	if err := rt.Close(background); !errors.Is(err, core.ErrQuarantined) {
		t.Fatal(err)
	}
	status(t, rt, "plugin", core.Quarantined)
	if err := rt.Start(background, "plugin"); !errors.Is(err, core.ErrRuntimeClosed) {
		t.Fatal(err)
	}
	if err := rt.Register(definition("new", func(sdk.Context) error { return nil })); !errors.Is(err, core.ErrRuntimeClosed) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	eventually(t, func() bool { return rt.Stats() == (core.Stats{}) })
	must(t, rt.Close(background))
}

func TestDependencyLossDuringInitializationReturnsToWaiting(t *testing.T) {
	rt := runtimeFor(t)
	key := sdk.NewService[int]("dependency")
	trigger, initializing := make(chan struct{}), make(chan struct{})
	var providerStarts, consumerStarts atomic.Int32
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(trigger) }) })
	provider := definition("provider", func(ctx sdk.Context) error {
		attempt := providerStarts.Add(1)
		if _, err := ctx.Go("worker", func(run context.Context) error {
			if attempt == 1 {
				select {
				case <-trigger:
					return errors.New("provider failure")
				case <-run.Done():
					return run.Err()
				}
			}
			<-run.Done()
			return run.Err()
		}); err != nil {
			return err
		}
		return sdk.Provide(ctx, key, 1)
	})
	provider.Manifest.Provides = []sdk.ServiceKey{key.Key()}
	consumer := definition("consumer", func(ctx sdk.Context) error {
		if consumerStarts.Add(1) == 1 {
			close(initializing)
			<-ctx.Context().Done()
			return ctx.Context().Err()
		}
		return nil
	})
	consumer.Manifest.Required = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(provider))
	must(t, rt.Register(consumer))
	must(t, rt.Start(background, "provider"))
	result := make(chan error, 1)
	go func() { result <- rt.Start(background, "consumer") }()
	receive(t, initializing)
	once.Do(func() { close(trigger) })
	must(t, receive(t, result))
	status(t, rt, "consumer", core.Waiting)
	status(t, rt, "provider", core.Failed)
	must(t, rt.Start(background, "provider"))
	status(t, rt, "consumer", core.Running)
	if consumerStarts.Load() != 2 {
		t.Fatal("dependent did not get a fresh activation")
	}
}

func TestPluginContextsRejectSynchronousLifecycleCalls(t *testing.T) {
	rt := runtimeFor(t)
	event := sdk.NewEvent[int]("reentry", sdk.EmitMode)
	var saved sdk.Context
	d := definition("plugin", func(ctx sdk.Context) error {
		saved = ctx
		if err := rt.Start(ctx.Context(), "plugin"); !errors.Is(err, core.ErrLifecycleReentrant) {
			return fmt.Errorf("Start reentry: %v", err)
		}
		if err := ctx.Defer("cleanup", func(run context.Context) error {
			if err := rt.Stop(run, "plugin"); !errors.Is(err, core.ErrLifecycleReentrant) {
				return fmt.Errorf("dispose reentry: %v", err)
			}
			return nil
		}); err != nil {
			return err
		}
		_, err := sdk.On(ctx, event, func(run context.Context, _ int) error {
			if err := rt.Reload(run, "plugin"); !errors.Is(err, core.ErrLifecycleReentrant) {
				return fmt.Errorf("listener reentry: %v", err)
			}
			return nil
		})
		return err
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	must(t, sdk.Emit(saved, background, event, 1))
	must(t, rt.Stop(background, "plugin"))
}

func TestCancelledStartupRequestReclaimsLateResources(t *testing.T) {
	rt := runtimeFor(t)
	ready, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	var freed atomic.Int32
	d := definition("plugin", func(ctx sdk.Context) error {
		close(ready)
		<-release
		return ctx.Defer("late", func(context.Context) error { freed.Add(1); return nil })
	})
	must(t, rt.Register(d))
	request, cancel := context.WithCancel(background)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Start(request, "plugin") }()
	receive(t, ready)
	cancel()
	if err := receive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	once.Do(func() { close(release) })
	eventually(t, func() bool { s, _ := rt.Status("plugin"); return s.State == core.Failed && len(s.Resources) == 0 })
	if freed.Load() != 1 {
		t.Fatal("cancelled request abandoned cleanup")
	}
}
