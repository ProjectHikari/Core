package core_test

import (
	"context"
	"fmt"

	"testbot/core"
	"testbot/core/sdk"
)

func Example() {
	rt := core.New()
	value := sdk.NewService[int]("example.value")
	event := sdk.NewEvent[int]("example.changed", sdk.EmitMode)
	var sender sdk.Context
	provider := sdk.Definition{
		Manifest: sdk.Manifest{ID: "provider", Provides: []sdk.ServiceKey{value.Key()}},
		Factory: func() (sdk.Plugin, error) {
			return sdk.StartFunc(func(ctx sdk.Context) error { sender = ctx; return sdk.Provide(ctx, value, 42) }), nil
		},
	}
	consumer := sdk.Definition{
		Manifest: sdk.Manifest{ID: "consumer", Required: []sdk.ServiceKey{value.Key()}},
		Factory: func() (sdk.Plugin, error) {
			return sdk.StartFunc(func(ctx sdk.Context) error {
				n, found, err := sdk.Lookup(ctx, value)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("value unavailable")
				}
				fmt.Println("service:", n)
				_, err = sdk.On(ctx, event, func(_ context.Context, payload int) error { fmt.Println("event:", payload); return nil })
				return err
			}), nil
		},
	}
	run := context.Background()
	for _, def := range []sdk.Definition{consumer, provider} {
		if err := rt.Register(def); err != nil {
			panic(err)
		}
	}
	if err := rt.Start(run, "consumer"); err != nil {
		panic(err)
	}
	state, _ := rt.Status("consumer")
	fmt.Println(state.State)
	if err := rt.Start(run, "provider"); err != nil {
		panic(err)
	}
	if err := sdk.Emit(sender, run, event, 7); err != nil {
		panic(err)
	}
	if err := rt.Close(run); err != nil {
		panic(err)
	}
	// Output:
	// Waiting
	// service: 42
	// event: 7
}
