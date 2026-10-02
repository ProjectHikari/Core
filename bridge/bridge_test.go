package bridge_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/bridge"
	"testbot/core/sdk"
)

var (
	hostService   = bridge.NewService("test.host")
	remoteService = bridge.NewService("test.remote")
	changed       = sdk.NewEvent[json.RawMessage]("test.changed", sdk.EmitMode)
	parallel      = sdk.NewEvent[json.RawMessage]("test.parallel", sdk.ParallelMode)
)

func require(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func command(t *testing.T, language string) []string {
	t.Helper()
	program, filename := "node", "plugin.mts"
	if language == "python" {
		program, filename = "python3", "plugin.py"
	}
	path, err := exec.LookPath(program)
	if err != nil {
		t.Skip(program + " unavailable")
	}
	file, err := filepath.Abs(filepath.Join("testdata", filename))
	require(t, err)
	return []string{path, file}
}

func config(t *testing.T, language string, env ...string) bridge.Config {
	return bridge.Config{Manifest: sdk.Manifest{ID: language, Provides: []sdk.ServiceKey{remoteService.Key()}, Required: []sdk.ServiceKey{hostService.Key()}}, Command: command(t, language), Events: []sdk.Event[json.RawMessage]{changed, parallel}, Env: append([]string{"PYTHONDONTWRITEBYTECODE=1"}, env...), StopTimeout: 100 * time.Millisecond, CallTimeout: 2 * time.Second}
}

func host(t *testing.T, rt *core.Runtime) sdk.Context {
	t.Helper()
	var ctx sdk.Context
	require(t, rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "host", Provides: []sdk.ServiceKey{hostService.Key()}}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(c sdk.Context) error {
			ctx = c
			return sdk.Provide[bridge.Service](c, hostService, bridge.ServiceFunc(func(context.Context, string, json.RawMessage) (json.RawMessage, error) {
				return json.RawMessage("41"), nil
			}))
		}), nil
	}}))
	require(t, rt.Start(context.Background(), "host"))
	return ctx
}

func consumer(t *testing.T, rt *core.Runtime, key sdk.Service[bridge.Service]) func() bridge.Service {
	t.Helper()
	var value bridge.Service
	require(t, rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "consumer", Required: []sdk.ServiceKey{key.Key()}}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(c sdk.Context) error {
			var err error
			value, _, err = sdk.Lookup(c, key)
			return err
		}), nil
	}}))
	require(t, rt.Start(context.Background(), "consumer"))
	return func() bridge.Service { return value }
}

func waitState(t *testing.T, rt *core.Runtime, id string, want core.State) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		state, _ := rt.Status(id)
		if state.State == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	status, _ := rt.Status(id)
	t.Fatalf("want %s: %+v", want, status)
}

func info(t *testing.T, service bridge.Service) map[string]any {
	t.Helper()
	data, err := service.Call(context.Background(), "info", nil)
	require(t, err)
	var result map[string]any
	require(t, json.Unmarshal(data, &result))
	return result
}

func TestLanguagesLifecycleAndCommunication(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		t.Run(language, func(t *testing.T) {
			rt := core.New()
			t.Cleanup(func() { require(t, rt.Close(context.Background())) })
			file := filepath.Join(t.TempDir(), "resource")
			cfg := config(t, language, "RESOURCE_FILE="+file)
			def, err := bridge.Definition(cfg)
			require(t, err)
			require(t, rt.Register(def))
			require(t, rt.Start(context.Background(), language))
			waitState(t, rt, language, core.Waiting)
			if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("waiting plugin executed")
			}
			value := consumer(t, rt, remoteService)
			hostContext := host(t, rt)
			before := info(t, value())
			if before["startValue"] != float64(41) {
				t.Fatal(before)
			}
			data, err := value().Call(context.Background(), "echo", json.RawMessage(`{"hello":"世界"}`))
			require(t, err)
			var echoed map[string]string
			require(t, json.Unmarshal(data, &echoed))
			if echoed["hello"] != "世界" {
				t.Fatal(string(data))
			}
			require(t, sdk.Emit(hostContext, context.Background(), changed, json.RawMessage(`"go"`)))
			_, err = value().Call(context.Background(), "emit", json.RawMessage(`"remote"`))
			require(t, err)
			seen := info(t, value())["seen"].([]any)
			if len(seen) != 2 || seen[0] != "go" || seen[1] != "remote" {
				t.Fatal(seen)
			}
			_, err = value().Call(context.Background(), "parallel", nil)
			require(t, err)
			for _, method := range []string{"fail", "wrongMode", "undeclared"} {
				_, err := value().Call(context.Background(), method, nil)
				if err == nil {
					t.Fatal("missing error", method)
				}
				waitState(t, rt, language, core.Running)
			}
			run, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			_, err = value().Call(run, "slow", nil)
			cancel()
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			_, err = value().Call(context.Background(), "echo", nil)
			require(t, err)
			baseline := rt.Stats()
			for range 5 {
				require(t, rt.Reload(context.Background(), language))
				waitState(t, rt, language, core.Running)
			}
			after := info(t, value())
			if after["generation"] == before["generation"] || after["pid"] == before["pid"] {
				t.Fatal("instance reused", before, after)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) && rt.Stats() != baseline {
				time.Sleep(time.Millisecond)
			}
			if rt.Stats() != baseline {
				t.Fatalf("resource baseline: %+v -> %+v", baseline, rt.Stats())
			}
			require(t, rt.Stop(context.Background(), "host"))
			waitState(t, rt, language, core.Waiting)
			if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("resource retained", err)
			}
			require(t, rt.Start(context.Background(), "host"))
			waitState(t, rt, language, core.Running)
			require(t, rt.Unload(context.Background(), language))
			if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("resource retained", err)
			}
		})
	}
}

func TestStartupRollbackAndCleanupRetry(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		for _, mode := range []string{"FAIL_START", "FAIL_CLEANUP"} {
			t.Run(language+"/"+mode, func(t *testing.T) {
				rt := core.New()
				host(t, rt)
				file := filepath.Join(t.TempDir(), "resource")
				cfg := config(t, language, "RESOURCE_FILE="+file, mode+"=1")
				def, err := bridge.Definition(cfg)
				require(t, err)
				require(t, rt.Register(def))
				err = rt.Start(context.Background(), language)
				if mode == "FAIL_START" {
					if err == nil {
						t.Fatal("startup succeeded")
					}
					waitState(t, rt, language, core.Failed)
				} else {
					require(t, err)
					if err = rt.Stop(context.Background(), language); err == nil {
						t.Fatal("cleanup succeeded")
					}
					waitState(t, rt, language, core.Quarantined)
					if err = rt.Reload(context.Background(), language); !errors.Is(err, core.ErrQuarantined) {
						t.Fatal(err)
					}
					require(t, rt.RetryCleanup(context.Background(), language))
					waitState(t, rt, language, core.Stopped)
				}
				if _, err = os.Stat(file); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("resource retained", err)
				}
				require(t, rt.Close(context.Background()))
				if rt.Stats() != (core.Stats{}) {
					t.Fatal(rt.Stats())
				}
			})
		}
	}
}

func TestProcessFaultsAndInitializationTimeout(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		modes := []string{"crash", "fault", "timeout"}
		if language == "python" {
			modes = append(modes, "cancelError")
		}
		for _, mode := range modes {
			t.Run(language+"/"+mode, func(t *testing.T) {
				rt := core.New(core.Options{StartTimeout: 200 * time.Millisecond, DisposeTimeout: time.Second})
				host(t, rt)
				cfg := config(t, language)
				if mode == "timeout" {
					cfg.Env = append(cfg.Env, "HANG_START=1")
				}
				def, err := bridge.Definition(cfg)
				require(t, err)
				require(t, rt.Register(def))
				value := consumer(t, rt, remoteService)
				err = rt.Start(context.Background(), language)
				if mode == "timeout" {
					if !errors.Is(err, core.ErrStartTimeout) {
						t.Fatal(err)
					}
				} else {
					require(t, err)
					_, err = value().Call(context.Background(), mode, nil)
					if mode != "crash" {
						require(t, err)
					}
				}
				waitState(t, rt, language, core.Failed)
				waitState(t, rt, "consumer", core.Waiting)
				require(t, rt.Close(context.Background()))
				if rt.Stats() != (core.Stats{}) {
					t.Fatal(rt.Stats())
				}
			})
		}
	}
}

func TestPythonToTypeScriptService(t *testing.T) {
	rt := core.New()
	t.Cleanup(func() { require(t, rt.Close(context.Background())) })
	host(t, rt)
	py := config(t, "python")
	pyDef, err := bridge.Definition(py)
	require(t, err)
	require(t, rt.Register(pyDef))
	require(t, rt.Start(context.Background(), "python"))
	tsKey := bridge.NewService("test.ts")
	ts := config(t, "typescript", "SERVICE_NAME=test.ts", "HOST_SERVICE=test.remote")
	ts.Manifest.Provides, ts.Manifest.Required = []sdk.ServiceKey{tsKey.Key()}, []sdk.ServiceKey{remoteService.Key()}
	tsDef, err := bridge.Definition(ts)
	require(t, err)
	require(t, rt.Register(tsDef))
	require(t, rt.Start(context.Background(), "typescript"))
	value := consumer(t, rt, tsKey)
	if info(t, value())["startValue"] != float64(42) {
		t.Fatal("cross language call failed")
	}
}

func TestInvalidConfig(t *testing.T) {
	for _, cfg := range []bridge.Config{{}, {Command: []string{"node"}, CallTimeout: -1}, {Command: []string{"node"}, MaxMessageBytes: 1}, {Command: []string{"node"}, Manifest: sdk.Manifest{Provides: []sdk.ServiceKey{sdk.NewService[int]("bad").Key()}}}, {Command: []string{"node"}, Events: []sdk.Event[json.RawMessage]{changed, changed}}} {
		if _, err := bridge.Definition(cfg); err == nil {
			t.Fatal("invalid config accepted", cfg)
		}
	}
}

func TestBlockedRemoteCallIsReaped(t *testing.T) {
	rt := core.New()
	host(t, rt)
	cfg := config(t, "typescript")
	def, err := bridge.Definition(cfg)
	require(t, err)
	require(t, rt.Register(def))
	require(t, rt.Start(context.Background(), "typescript"))
	value := consumer(t, rt, remoteService)
	pid := int(info(t, value())["pid"].(float64))
	run, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	_, err = value().Call(run, "ignore", nil)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	require(t, rt.Close(context.Background()))
	if _, err := os.Stat(filepath.Join("/proc", strconv.Itoa(pid))); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("process still exists", pid, err)
	}
}

func TestRemoteRequestRetainsRequiredProviderUntilGoCallExits(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		t.Run(language, func(t *testing.T) {
			rt := core.New(core.Options{DisposeTimeout: 40 * time.Millisecond})
			entered, release, destroyed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var enterOnce, releaseOnce sync.Once
			t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); require(t, rt.Close(context.Background())) })
			require(t, rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "host", Provides: []sdk.ServiceKey{hostService.Key()}}, Factory: func() (sdk.Plugin, error) {
				return sdk.StartFunc(func(ctx sdk.Context) error {
					if err := ctx.Defer("provider", func(context.Context) error { close(destroyed); return nil }); err != nil {
						return err
					}
					return sdk.Provide[bridge.Service](ctx, hostService, bridge.ServiceFunc(func(_ context.Context, method string, _ json.RawMessage) (json.RawMessage, error) {
						if method == "slow" {
							enterOnce.Do(func() { close(entered) })
							<-release
						}
						return json.RawMessage("41"), nil
					}))
				}), nil
			}}))
			require(t, rt.Start(context.Background(), "host"))
			cfg := config(t, language)
			cfg.StopTimeout = 10 * time.Millisecond
			def, err := bridge.Definition(cfg)
			require(t, err)
			require(t, rt.Register(def))
			require(t, rt.Start(context.Background(), language))
			value := consumer(t, rt, remoteService)
			callDone := make(chan struct{})
			go func() { _, _ = value().Call(context.Background(), "hostSlow", nil); close(callDone) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("host call not entered")
			}
			if err := rt.Stop(context.Background(), "host"); err == nil {
				t.Fatal("stop ignored running request")
			}
			waitState(t, rt, language, core.Quarantined)
			select {
			case <-destroyed:
				t.Fatal("provider destroyed while request running")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			waitState(t, rt, "host", core.Stopped)
			select {
			case <-callDone:
			case <-time.After(time.Second):
				t.Fatal("remote caller not released")
			}
			select {
			case <-destroyed:
			case <-time.After(time.Second):
				t.Fatal("provider retained after request exit")
			}
		})
	}
}

func TestTimedOutRemoteDisposerDoesNotRunTwice(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		t.Run(language, func(t *testing.T) {
			rt := core.New(core.Options{DisposeTimeout: 30 * time.Millisecond})
			host(t, rt)
			dir := t.TempDir()
			file, attempts := filepath.Join(dir, "resource"), filepath.Join(dir, "attempts")
			cfg := config(t, language, "RESOURCE_FILE="+file, "SLOW_CLEANUP=1", "CLEANUP_ATTEMPTS="+attempts)
			cfg.StopTimeout = 10 * time.Millisecond
			def, err := bridge.Definition(cfg)
			require(t, err)
			require(t, rt.Register(def))
			require(t, rt.Start(context.Background(), language))
			if err := rt.Stop(context.Background(), language); err == nil {
				t.Fatal("cleanup timeout not reported")
			}
			waitState(t, rt, language, core.Quarantined)
			_ = rt.RetryCleanup(context.Background(), language)
			data, err := os.ReadFile(attempts)
			require(t, err)
			if string(data) != "1" {
				t.Fatal("disposer executed twice", string(data))
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				status, _ := rt.Status(language)
				pending := false
				for _, resource := range status.Resources {
					if resource.Label == "file" {
						pending = true
					}
				}
				if !pending {
					break
				}
				time.Sleep(time.Millisecond)
			}
			if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("cleanup did not finish", err)
			}
			require(t, rt.RetryCleanup(context.Background(), language))
			waitState(t, rt, language, core.Stopped)
			data, err = os.ReadFile(attempts)
			require(t, err)
			if string(data) != "1" {
				t.Fatal("disposer executed twice", string(data))
			}
			require(t, rt.Close(context.Background()))
		})
	}
}

func TestLateRemoteResourceBelongsToClosingGeneration(t *testing.T) {
	for _, language := range []string{"typescript", "python"} {
		t.Run(language, func(t *testing.T) {
			rt := core.New()
			host(t, rt)
			file := filepath.Join(t.TempDir(), "late")
			cfg := config(t, language, "LATE_FILE="+file)
			def, err := bridge.Definition(cfg)
			require(t, err)
			require(t, rt.Register(def))
			require(t, rt.Start(context.Background(), language))
			value := consumer(t, rt, remoteService)
			_, err = value().Call(context.Background(), "late", nil)
			require(t, err)
			deadline := time.Now().Add(time.Second)
			for info(t, value())["lateReady"] != true {
				if time.Now().After(deadline) {
					t.Fatal("late task not started")
				}
				time.Sleep(time.Millisecond)
			}
			_ = rt.Stop(context.Background(), language)
			deadline = time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				_ = rt.RetryCleanup(context.Background(), language)
				status, _ := rt.Status(language)
				if status.State == core.Stopped {
					break
				}
				time.Sleep(time.Millisecond)
			}
			waitState(t, rt, language, core.Stopped)
			if _, err := os.Stat(file); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("late resource retained", err)
			}
			require(t, rt.Close(context.Background()))
		})
	}
}
