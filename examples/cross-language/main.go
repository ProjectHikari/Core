package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"testbot/core"
	"testbot/core/bridge"
	"testbot/core/sdk"
)

func run(language string) (err error) {
	program, file := "python3", "echo.py"
	switch language {
	case "python":
	case "typescript":
		program, file = "node", "echo.mts"
	default:
		return fmt.Errorf("unknown language %s", language)
	}
	key := bridge.NewService("demo.echo")
	definition, err := bridge.Definition(bridge.Config{
		Manifest: sdk.Manifest{ID: "echo", Provides: []sdk.ServiceKey{key.Key()}},
		Command:  []string{program, filepath.Join("examples", "cross-language", file)},
		Env:      []string{"PYTHONDONTWRITEBYTECODE=1"},
	})
	if err != nil {
		return err
	}
	rt := core.New()
	defer func() { err = errors.Join(err, rt.Close(context.Background())) }()
	if err = rt.Register(definition); err != nil {
		return err
	}
	if err = rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "caller", Required: []sdk.ServiceKey{key.Key()}}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(ctx sdk.Context) error {
			service, _, err := sdk.Lookup(ctx, key)
			if err != nil {
				return err
			}
			result, err := service.Call(ctx.Context(), "echo", json.RawMessage(`{"message":"你好"}`))
			if err == nil {
				fmt.Println(string(result))
			}
			return err
		}), nil
	}}); err != nil {
		return err
	}
	if err = rt.Start(context.Background(), "caller"); err != nil {
		return err
	}
	return rt.Start(context.Background(), "echo")
}

func main() {
	language := flag.String("language", "python", "plugin language: python or typescript")
	flag.Parse()
	if err := run(*language); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
