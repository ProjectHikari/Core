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

var background = context.Background()

func runtimeFor(t *testing.T) *core.Runtime {
	t.Helper()
	rt := core.New(core.Options{StartTimeout: 300 * time.Millisecond, DisposeTimeout: 30 * time.Millisecond})
	t.Cleanup(func() {
		if err := rt.Close(background); err != nil {
			t.Errorf("Close: %v", err)
		}
		if stats := rt.Stats(); stats != (core.Stats{}) {
			t.Errorf("resources remain after Close: %+v", stats)
		}
	})
	return rt
}

func definition(id string, start func(sdk.Context) error) sdk.Definition {
	return sdk.Definition{
		Manifest: sdk.Manifest{ID: id},
		Factory:  func() (sdk.Plugin, error) { return sdk.StartFunc(start), nil },
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func status(t *testing.T, rt *core.Runtime, id string, state core.State) core.Status {
	t.Helper()
	s, ok := rt.Status(id)
	if !ok || s.State != state {
		t.Fatalf("%s: got %+v, exists=%v, want %s", id, s, ok, state)
	}
	return s
}

func eventually(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not settle")
}

func TestLifecycleAndFreshReload(t *testing.T) {
	rt := runtimeFor(t)
	var contexts []sdk.Context
	var starts, disposals int
	d := definition("plugin", func(ctx sdk.Context) error {
		starts++
		contexts = append(contexts, ctx)
		return ctx.Defer("resource", func(context.Context) error { disposals++; return nil })
	})
	must(t, rt.Register(d))
	status(t, rt, "plugin", core.Registered)
	request, cancel := context.WithCancel(background)
	must(t, rt.Start(request, "plugin"))
	cancel()
	if contexts[0].Context().Err() != nil {
		t.Fatal("activation was bound to the completed Start request")
	}
	must(t, rt.Start(background, "plugin"))
	if starts != 1 {
		t.Fatal("idempotent Start ran twice")
	}
	must(t, rt.Reload(background, "plugin"))
	if contexts[0] == contexts[1] || contexts[0].Generation() == contexts[1].Generation() || disposals != 1 {
		t.Fatal("Reload reused an activation")
	}
	if _, err := contexts[0].Go("stale", func(context.Context) error { t.Error("stale task ran"); return nil }); !errors.Is(err, sdk.ErrContextClosed) {
		t.Fatalf("stale Go: %v", err)
	}
	must(t, rt.Stop(background, "plugin"))
	must(t, rt.Stop(background, "plugin"))
	status(t, rt, "plugin", core.Stopped)
	if disposals != 2 {
		t.Fatalf("disposals=%d", disposals)
	}
	must(t, rt.Unload(background, "plugin"))
	if _, ok := rt.Status("plugin"); ok {
		t.Fatal("unloaded definition remains")
	}
	if err := rt.Start(background, "plugin"); !errors.Is(err, core.ErrPluginNotFound) {
		t.Fatal(err)
	}
}

func TestWaitingAndDependencyChain(t *testing.T) {
	rt := runtimeFor(t)
	a := sdk.NewService[int]("a")
	b := sdk.NewService[int]("b")
	var order []string
	var mu sync.Mutex
	add := func(value string) { mu.Lock(); order = append(order, value); mu.Unlock() }
	var consumerContexts []sdk.Context
	provider := definition("provider", func(ctx sdk.Context) error {
		add("start provider")
		if err := ctx.Defer("provider", func(context.Context) error { add("stop provider"); return nil }); err != nil {
			return err
		}
		return sdk.Provide(ctx, a, 1)
	})
	provider.Manifest.Provides = []sdk.ServiceKey{a.Key()}
	middle := definition("middle", func(ctx sdk.Context) error {
		value, found, err := sdk.Lookup(ctx, a)
		if err != nil || !found || value != 1 {
			return fmt.Errorf("required a: %v %v %v", value, found, err)
		}
		add("start middle")
		if err := ctx.Defer("middle", func(context.Context) error { add("stop middle"); return nil }); err != nil {
			return err
		}
		return sdk.Provide(ctx, b, 2)
	})
	middle.Manifest.Required, middle.Manifest.Provides = []sdk.ServiceKey{a.Key()}, []sdk.ServiceKey{b.Key()}
	consumer := definition("consumer", func(ctx sdk.Context) error {
		consumerContexts = append(consumerContexts, ctx)
		add("start consumer")
		return ctx.Defer("consumer", func(context.Context) error { add("stop consumer"); return nil })
	})
	consumer.Manifest.Required = []sdk.ServiceKey{b.Key()}
	must(t, rt.Register(consumer))
	must(t, rt.Register(middle))
	must(t, rt.Register(provider))
	must(t, rt.Start(background, "consumer"))
	must(t, rt.Start(background, "middle"))
	if len(consumerContexts) != 0 {
		t.Fatal("waiting plugin executed")
	}
	status(t, rt, "consumer", core.Waiting)
	must(t, rt.Start(background, "provider"))
	status(t, rt, "consumer", core.Running)
	must(t, rt.Stop(background, "provider"))
	status(t, rt, "middle", core.Waiting)
	status(t, rt, "consumer", core.Waiting)
	must(t, rt.Start(background, "provider"))
	want := []string{"start provider", "start middle", "start consumer", "stop consumer", "stop middle", "stop provider", "start provider", "start middle", "start consumer"}
	mu.Lock()
	got := fmt.Sprint(order)
	mu.Unlock()
	if got != fmt.Sprint(want) {
		t.Fatalf("order=%s", got)
	}
	if consumerContexts[0].Generation() == consumerContexts[1].Generation() {
		t.Fatal("dependency recovery reused Context")
	}
	must(t, rt.Stop(background, "consumer"))
	must(t, rt.Reload(background, "provider"))
	if len(consumerContexts) != 2 {
		t.Fatal("manually stopped consumer restarted")
	}
}

func TestRegistrationTransactions(t *testing.T) {
	rt := runtimeFor(t)
	a, b := sdk.NewService[int]("a"), sdk.NewService[int]("b")
	d := definition("a", func(ctx sdk.Context) error { return sdk.Provide(ctx, a, 1) })
	d.Manifest.Provides, d.Manifest.Required = []sdk.ServiceKey{a.Key()}, []sdk.ServiceKey{b.Key()}
	must(t, rt.Register(d))
	d.Manifest.Required[0] = a.Key()
	cycle := definition("b", func(ctx sdk.Context) error { return sdk.Provide(ctx, b, 1) })
	cycle.Manifest.Provides, cycle.Manifest.Required = []sdk.ServiceKey{b.Key()}, []sdk.ServiceKey{a.Key()}
	if err := rt.Register(cycle); !errors.Is(err, core.ErrDependencyCycle) {
		t.Fatal(err)
	}
	if rt.Stats().Plugins != 1 {
		t.Fatal("failed registration mutated registry")
	}
	duplicate := definition("duplicate", func(sdk.Context) error { return nil })
	duplicate.Manifest.Provides = []sdk.ServiceKey{a.Key()}
	if err := rt.Register(duplicate); !errors.Is(err, sdk.ErrConflict) {
		t.Fatal(err)
	}
	wrong := definition("wrong", func(sdk.Context) error { return nil })
	wrong.Manifest.Optional = []sdk.ServiceKey{sdk.NewService[string]("a").Key()}
	if err := rt.Register(wrong); !errors.Is(err, sdk.ErrContractMismatch) {
		t.Fatal(err)
	}
	self := definition("self", func(sdk.Context) error { return nil })
	self.Manifest.Provides, self.Manifest.Required = []sdk.ServiceKey{b.Key()}, []sdk.ServiceKey{b.Key()}
	if err := rt.Register(self); !errors.Is(err, core.ErrDependencyCycle) {
		t.Fatal(err)
	}
}

func TestAtomicStartupRollback(t *testing.T) {
	rt := runtimeFor(t)
	service := sdk.NewService[int]("service")
	event := sdk.NewEvent[int]("event", sdk.EmitMode)
	var observer sdk.Context
	d := definition("observer", func(ctx sdk.Context) error { observer = ctx; return nil })
	d.Manifest.Optional = []sdk.ServiceKey{service.Key()}
	must(t, rt.Register(d))
	must(t, rt.Start(background, "observer"))
	ready, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	boom := errors.New("startup failed")
	var calls atomic.Int32
	var disposed []string
	d = definition("failing", func(ctx sdk.Context) error {
		if err := sdk.Provide(ctx, service, 1); err != nil {
			return err
		}
		if _, err := sdk.On(ctx, event, func(context.Context, int) error { calls.Add(1); return nil }); err != nil {
			return err
		}
		if err := sdk.Emit(ctx, background, event, 1); !errors.Is(err, sdk.ErrNotRunning) {
			return fmt.Errorf("startup Emit: %v", err)
		}
		if err := ctx.Defer("first", func(context.Context) error { disposed = append(disposed, "first"); return nil }); err != nil {
			return err
		}
		if err := ctx.Defer("second", func(context.Context) error { disposed = append(disposed, "second"); return nil }); err != nil {
			return err
		}
		taskReady := make(chan struct{})
		if _, err := ctx.Go("initializer worker", func(run context.Context) error { close(taskReady); <-run.Done(); return run.Err() }); err != nil {
			return err
		}
		<-taskReady
		close(ready)
		<-release
		return boom
	})
	d.Manifest.Provides = []sdk.ServiceKey{service.Key()}
	must(t, rt.Register(d))
	startResult := make(chan error, 1)
	go func() { startResult <- rt.Start(background, "failing") }()
	<-ready
	if _, found, err := sdk.Lookup(observer, service); err != nil || found {
		t.Fatalf("staged service visible: %v %v", found, err)
	}
	must(t, sdk.Emit(observer, background, event, 1))
	if calls.Load() != 0 || rt.Stats().Listeners != 0 {
		t.Fatal("staged listener visible")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-startResult; !errors.Is(err, boom) {
		t.Fatal(err)
	}
	status(t, rt, "failing", core.Failed)
	if fmt.Sprint(disposed) != "[second first]" {
		t.Fatal(disposed)
	}
	if stats := rt.Stats(); stats.Resources != 0 || stats.Services != 0 || stats.Listeners != 0 || stats.Tasks != 0 {
		t.Fatalf("rollback leaked: %+v", stats)
	}
}

func TestOptionalAndExplicitFailureRetry(t *testing.T) {
	rt := runtimeFor(t)
	key := sdk.NewService[int]("optional")
	var consumer sdk.Context
	var starts int
	c := definition("consumer", func(ctx sdk.Context) error { starts++; consumer = ctx; return nil })
	c.Manifest.Optional = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(c))
	must(t, rt.Start(background, "consumer"))
	p := definition("provider", func(ctx sdk.Context) error { return sdk.Provide(ctx, key, 7) })
	p.Manifest.Provides = []sdk.ServiceKey{key.Key()}
	must(t, rt.Register(p))
	must(t, rt.Start(background, "provider"))
	if v, found, err := sdk.Lookup(consumer, key); v != 7 || !found || err != nil {
		t.Fatalf("%v %v %v", v, found, err)
	}
	must(t, rt.Reload(background, "provider"))
	must(t, rt.Unload(background, "provider"))
	if starts != 1 {
		t.Fatal("optional dependency restarted consumer")
	}
	if _, found, err := sdk.Lookup(consumer, key); found || err != nil {
		t.Fatalf("%v %v", found, err)
	}
	var attempts atomic.Int32
	boom := errors.New("initial failure")
	f := definition("failure", func(sdk.Context) error {
		if attempts.Add(1) == 1 {
			return boom
		}
		return nil
	})
	must(t, rt.Register(f))
	if err := rt.Start(background, "failure"); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	must(t, rt.Reload(background, "consumer"))
	if attempts.Load() != 1 {
		t.Fatal("failed plugin automatically retried")
	}
	must(t, rt.Start(background, "failure"))
	status(t, rt, "failure", core.Running)
}

func TestReloadFailureDoesNotRestorePreviousInstance(t *testing.T) {
	rt := runtimeFor(t)
	var attempts, disposals int
	boom := errors.New("reload failed")
	d := definition("plugin", func(ctx sdk.Context) error {
		attempts++
		if err := ctx.Defer("resource", func(context.Context) error { disposals++; return nil }); err != nil {
			return err
		}
		if attempts == 2 {
			return boom
		}
		return nil
	})
	must(t, rt.Register(d))
	must(t, rt.Start(background, "plugin"))
	if err := rt.Reload(background, "plugin"); !errors.Is(err, boom) {
		t.Fatal(err)
	}
	status(t, rt, "plugin", core.Failed)
	if attempts != 2 || disposals != 2 || rt.Stats().Resources != 0 {
		t.Fatalf("attempts=%d disposals=%d", attempts, disposals)
	}
}
