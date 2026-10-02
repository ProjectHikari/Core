package kernel

import (
	"context"
	"sync"
	"testing"
	"time"

	"testbot/core/sdk"
)

func TestShortLivedResourcesDoNotAccumulate(t *testing.T) {
	rt := New()
	t.Cleanup(func() {
		if err := rt.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	var view *pluginContext
	if err := rt.Register(sdk.Definition{
		Manifest: sdk.Manifest{ID: "plugin"},
		Factory: func() (sdk.Plugin, error) {
			return sdk.StartFunc(func(ctx sdk.Context) error { view = ctx.(*pluginContext); return nil }), nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.Start(context.Background(), "plugin"); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wait.Add(1)
		if _, err := view.Go("short task", func(context.Context) error { wait.Done(); return nil }); err != nil {
			t.Fatal(err)
		}
	}
	wait.Wait()
	deadline := time.Now().Add(time.Second)
	for {
		view.scope.mu.Lock()
		retained := len(view.scope.resources)
		view.scope.mu.Unlock()
		if retained == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retained %d completed tasks", retained)
		}
		time.Sleep(time.Millisecond)
	}
}
