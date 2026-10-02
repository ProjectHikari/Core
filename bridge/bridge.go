package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"time"

	"testbot/core/sdk"
)

// 方法和JSON数据结构由能力契约定义
type Service interface {
	Call(context.Context, string, json.RawMessage) (json.RawMessage, error)
}

type ServiceFunc func(context.Context, string, json.RawMessage) (json.RawMessage, error)

func (f ServiceFunc) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
	return f(ctx, method, params)
}

func NewService(name string) sdk.Service[Service] { return sdk.NewService[Service](name) }

// 宿主声明契约范围，子进程不能自行扩大依赖
type Config struct {
	Manifest sdk.Manifest
	Command  []string
	Dir      string
	Env      []string
	Events   []sdk.Event[json.RawMessage]
	Stderr   io.Writer
	// 超时用于单次调用和进程退出等待，不改变Core的初始化超时
	CallTimeout     time.Duration
	StopTimeout     time.Duration
	MaxMessageBytes int
	MaxInflight     int
}

func Definition(config Config) (sdk.Definition, error) {
	if len(config.Command) == 0 || strings.TrimSpace(config.Command[0]) == "" {
		return sdk.Definition{}, sdk.ErrInvalid
	}
	if config.CallTimeout < 0 || config.StopTimeout < 0 || config.MaxMessageBytes < 0 || config.MaxInflight < 0 {
		return sdk.Definition{}, sdk.ErrInvalid
	}
	if config.CallTimeout == 0 {
		config.CallTimeout = 30 * time.Second
	}
	if config.StopTimeout == 0 {
		config.StopTimeout = time.Second
	}
	if config.MaxMessageBytes == 0 {
		config.MaxMessageBytes = 1 << 20
	}
	if config.MaxInflight == 0 {
		config.MaxInflight = 128
	}
	if config.MaxMessageBytes < 256 || config.MaxMessageBytes > 16<<20 {
		return sdk.Definition{}, sdk.ErrInvalid
	}
	if config.Stderr == nil {
		config.Stderr = os.Stderr
	}
	config.Command = append([]string(nil), config.Command...)
	config.Env = append([]string(nil), config.Env...)
	config.Manifest.Provides = append([]sdk.ServiceKey(nil), config.Manifest.Provides...)
	config.Manifest.Required = append([]sdk.ServiceKey(nil), config.Manifest.Required...)
	config.Manifest.Optional = append([]sdk.ServiceKey(nil), config.Manifest.Optional...)
	config.Events = append([]sdk.Event[json.RawMessage](nil), config.Events...)
	serviceType := reflect.TypeFor[Service]()
	for _, keys := range [][]sdk.ServiceKey{config.Manifest.Provides, config.Manifest.Required, config.Manifest.Optional} {
		for _, key := range keys {
			if key.Type != serviceType {
				return sdk.Definition{}, fmt.Errorf("%w: bridge service %s must use bridge.Service", sdk.ErrContractMismatch, key.Name)
			}
		}
	}
	events := make(map[string]sdk.EventKey)
	for _, event := range config.Events {
		key := event.Key()
		if key.Name == "" || strings.TrimSpace(key.Name) != key.Name || (key.Mode != sdk.EmitMode && key.Mode != sdk.ParallelMode) {
			return sdk.Definition{}, sdk.ErrInvalid
		}
		if _, found := events[key.Name]; found {
			return sdk.Definition{}, fmt.Errorf("%w: event %s", sdk.ErrConflict, key.Name)
		}
		events[key.Name] = key
	}
	manifest := config.Manifest
	manifest.Provides = append([]sdk.ServiceKey(nil), manifest.Provides...)
	manifest.Required = append([]sdk.ServiceKey(nil), manifest.Required...)
	manifest.Optional = append([]sdk.ServiceKey(nil), manifest.Optional...)
	return sdk.Definition{Manifest: manifest, Factory: func() (sdk.Plugin, error) {
		return &processPlugin{config: config}, nil
	}}, nil
}

type processPlugin struct{ config Config }

type session struct {
	config    Config
	ctx       sdk.Context
	cmd       *exec.Cmd
	peer      *peer
	exited    chan struct{}
	mu        sync.Mutex
	exitErr   error
	stopping  bool
	controls  map[string]sdk.CancelFunc
	disposers map[string]bool
	events    map[string]sdk.EventKey
}

func (p *processPlugin) Start(ctx sdk.Context) error {
	s := &session{config: p.config, ctx: ctx, exited: make(chan struct{}), controls: make(map[string]sdk.CancelFunc), disposers: make(map[string]bool), events: make(map[string]sdk.EventKey)}
	for _, event := range p.config.Events {
		s.events[event.Key().Name] = event.Key()
	}
	s.cmd = exec.Command(p.config.Command[0], p.config.Command[1:]...)
	s.cmd.Dir, s.cmd.Env, s.cmd.Stderr = p.config.Dir, append(os.Environ(), p.config.Env...), p.config.Stderr
	prepareProcess(s.cmd)
	input, err := s.cmd.StdinPipe()
	if err != nil {
		return err
	}
	// 自己持有读端，避免Cmd.Wait提前关闭仍有响应未读完的管道
	output, childOutput, err := os.Pipe()
	if err != nil {
		input.Close()
		return err
	}
	s.cmd.Stdout = childOutput
	if err = s.cmd.Start(); err != nil {
		input.Close()
		output.Close()
		childOutput.Close()
		return err
	}
	childOutput.Close()
	s.peer = newPeer(input, output, s.config, ctx, s.handle)
	context.AfterFunc(ctx.Context(), func() {
		_ = s.peer.send(message{Type: "scope_cancel", ID: "scope"})
	})
	go func() {
		err := s.cmd.Wait()
		s.mu.Lock()
		s.exitErr = err
		s.mu.Unlock()
		close(s.exited)
	}()
	// 即使取消发生在启动管道期间，进程也必须有回收路径
	if err = ctx.Defer("bridge process", s.dispose); err != nil {
		s.forceStop()
		return err
	}
	if err = s.peer.start(); err != nil {
		return err
	}
	if _, err = ctx.Go("bridge process monitor", func(context.Context) error {
		<-s.exited
		s.mu.Lock()
		stopping, cause := s.stopping, s.exitErr
		s.mu.Unlock()
		if stopping {
			return nil
		}
		if cause == nil {
			cause = errors.New("unexpected process exit")
		}
		_ = killProcess(s.cmd)
		s.peer.close(cause)
		return cause
	}); err != nil {
		return err
	}
	params := map[string]any{"protocol": 1, "plugin_id": ctx.PluginID(), "generation": ctx.Generation(), "provides": names(p.config.Manifest.Provides), "required": names(p.config.Manifest.Required), "optional": names(p.config.Manifest.Optional), "events": eventModes(s.events), "max_message_bytes": s.config.MaxMessageBytes, "max_inflight": s.config.MaxInflight}
	_, err = s.peer.call(ctx.Context(), "start", params)
	return err
}

func names(keys []sdk.ServiceKey) []string {
	names := make([]string, len(keys))
	for i, key := range keys {
		names[i] = key.Name
	}
	return names
}

func eventModes(keys map[string]sdk.EventKey) map[string]string {
	modes := make(map[string]string)
	for name, key := range keys {
		if key.Mode == sdk.EmitMode {
			modes[name] = "emit"
		} else {
			modes[name] = "parallel"
		}
	}
	return modes
}

func (s *session) forceStop() {
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	s.peer.close(errors.New("bridge stopped"))
	killProcess(s.cmd)
	<-s.exited
}

func (s *session) dispose(ctx context.Context) error {
	s.mu.Lock()
	left := len(s.disposers)
	s.mu.Unlock()
	if left > 0 {
		// 保留进程供RetryCleanup重试，不能用进程退出掩盖外部补偿失败
		select {
		case <-s.peer.done:
		default:
			return fmt.Errorf("%d remote disposers remain", left)
		}
	}
	s.mu.Lock()
	s.stopping = true
	s.mu.Unlock()
	stop, cancel := context.WithTimeout(ctx, s.config.StopTimeout)
	defer cancel()
	select {
	case <-s.exited:
	default:
		_, _ = s.peer.call(stop, "shutdown", nil)
	}
	s.mu.Lock()
	left = len(s.disposers)
	s.mu.Unlock()
	if left > 0 {
		select {
		case <-s.peer.done:
		default:
			return fmt.Errorf("%d late remote disposers remain", left)
		}
	}
	// shutdown不能证明退出，只有Wait完成才释放进程资源
	s.peer.close(errors.New("bridge stopped"))
	if err := killProcess(s.cmd); err != nil {
		return err
	}
	select {
	case <-s.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type operation struct {
	Name     string          `json:"name"`
	Method   string          `json:"method"`
	Callback string          `json:"callback"`
	Label    string          `json:"label"`
	DelayMS  float64         `json:"delay_ms"`
	Params   json.RawMessage `json:"params"`
	Payload  json.RawMessage `json:"payload"`
	Message  string          `json:"message"`
}

func (s *session) key(name string) (sdk.ServiceKey, error) {
	for _, keys := range [][]sdk.ServiceKey{s.config.Manifest.Provides, s.config.Manifest.Required, s.config.Manifest.Optional} {
		for _, key := range keys {
			if key.Name == name {
				return key, nil
			}
		}
	}
	return sdk.ServiceKey{}, fmt.Errorf("%w: service %s", sdk.ErrUndeclared, name)
}

func (s *session) handle(run context.Context, method string, data json.RawMessage) (any, error) {
	var op operation
	if err := json.Unmarshal(data, &op); err != nil {
		return nil, err
	}
	switch method {
	case "fault":
		_, err := s.ctx.Go("remote task: "+op.Label, func(context.Context) error { return errors.New(op.Message) })
		return nil, err
	case "provide":
		key, err := s.key(op.Name)
		if err != nil {
			return nil, err
		}
		service := ServiceFunc(func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, error) {
			return s.peer.call(ctx, "service", map[string]any{"name": op.Name, "method": method, "params": raw(params)})
		})
		return nil, s.ctx.Provide(key, service)
	case "lookup", "call":
		key, err := s.key(op.Name)
		if err != nil {
			return nil, err
		}
		value, found, err := s.ctx.Lookup(key)
		if err != nil {
			return nil, err
		}
		if method == "lookup" {
			return found, nil
		}
		if !found {
			return nil, fmt.Errorf("service %s unavailable", op.Name)
		}
		return value.(Service).Call(run, op.Method, raw(op.Params))
	case "emit", "parallel", "on":
		key, found := s.events[op.Name]
		if !found {
			return nil, fmt.Errorf("%w: event %s", sdk.ErrUndeclared, op.Name)
		}
		if method == "emit" {
			return nil, s.ctx.Emit(run, key, raw(op.Payload))
		}
		if method == "parallel" {
			return nil, s.ctx.Parallel(run, key, raw(op.Payload))
		}
		return s.control(op.Callback, func() (sdk.CancelFunc, error) {
			return s.ctx.On(key, func(run context.Context, payload any) error {
				_, err := s.peer.call(run, "callback", map[string]any{"callback": op.Callback, "payload": payload})
				return err
			})
		})
	case "after", "every":
		if op.DelayMS < 0 || op.DelayMS > float64((24*time.Hour)/time.Millisecond) || (method == "every" && op.DelayMS == 0) {
			return nil, sdk.ErrInvalid
		}
		fn := func(run context.Context) error {
			_, err := s.peer.call(run, "callback", map[string]any{"callback": op.Callback, "payload": nil})
			if method == "after" {
				s.mu.Lock()
				delete(s.controls, op.Callback)
				s.mu.Unlock()
			}
			return err
		}
		return s.control(op.Callback, func() (sdk.CancelFunc, error) {
			if method == "after" {
				return s.ctx.After(op.Label, time.Duration(op.DelayMS*float64(time.Millisecond)), fn)
			}
			return s.ctx.Every(op.Label, time.Duration(op.DelayMS*float64(time.Millisecond)), fn)
		})
	case "cancel":
		s.mu.Lock()
		cancel := s.controls[op.Callback]
		delete(s.controls, op.Callback)
		s.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return nil, nil
	case "defer":
		if op.Callback == "" {
			return nil, sdk.ErrInvalid
		}
		s.mu.Lock()
		if s.disposers[op.Callback] {
			s.mu.Unlock()
			return nil, sdk.ErrConflict
		}
		s.disposers[op.Callback] = true
		s.mu.Unlock()
		err := s.ctx.Defer(op.Label, func(cleanup context.Context) error {
			_, err := s.peer.callMode(cleanup, "dispose", map[string]any{"callback": op.Callback}, true)
			if err == nil {
				s.mu.Lock()
				delete(s.disposers, op.Callback)
				s.mu.Unlock()
			}
			return err
		})
		if err != nil && !errors.Is(err, sdk.ErrContextClosed) {
			s.mu.Lock()
			delete(s.disposers, op.Callback)
			s.mu.Unlock()
		}
		// 进程和reader尚未退出，ContextClosed表示旧作用域已接收晚归资源
		if errors.Is(err, sdk.ErrContextClosed) {
			err = nil
		}
		return nil, err
	default:
		return nil, fmt.Errorf("unknown method %s", method)
	}
}

func (s *session) control(id string, create func() (sdk.CancelFunc, error)) (any, error) {
	if id == "" {
		return nil, sdk.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.controls[id]; found {
		return nil, sdk.ErrConflict
	}
	cancel, err := create()
	if err == nil {
		s.controls[id] = cancel
	}
	return nil, err
}

func raw(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("null")
	}
	return value
}
