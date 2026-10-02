package bridge

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"testbot/core/sdk"
)

var (
	ErrBusy   = errors.New("bridge inflight limit reached")
	ErrClosed = errors.New("bridge transport closed")
)

type RemoteError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *RemoteError) Error() string { return e.Code + ": " + e.Message }

type message struct {
	Type   string          `json:"type"`
	ID     string          `json:"id"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RemoteError    `json:"error,omitempty"`
}

type peer struct {
	input    io.WriteCloser
	output   io.ReadCloser
	config   Config
	ctx      sdk.Context
	handle   func(context.Context, string, json.RawMessage) (any, error)
	mu       sync.Mutex
	next     uint64
	pending  map[string]chan message
	incoming map[string]context.CancelFunc
	cause    error
	done     chan struct{}
	writes   chan []byte
}

func newPeer(input io.WriteCloser, output io.ReadCloser, config Config, ctx sdk.Context, handle func(context.Context, string, json.RawMessage) (any, error)) *peer {
	return &peer{input: input, output: output, config: config, ctx: ctx, handle: handle, pending: make(map[string]chan message), incoming: make(map[string]context.CancelFunc), done: make(chan struct{}), writes: make(chan []byte, config.MaxInflight+16)}
}

func (p *peer) start() error {
	if _, err := p.ctx.Go("bridge writer", func(context.Context) error {
		err := p.writeLoop()
		if err != nil {
			p.close(err)
		}
		if p.ctx.Context().Err() != nil {
			return nil
		}
		return err
	}); err != nil {
		return err
	}
	_, err := p.ctx.Go("bridge reader", func(context.Context) error {
		err := p.readLoop()
		p.close(err)
		if p.ctx.Context().Err() != nil {
			return nil
		}
		return err
	})
	return err
}

func (p *peer) close(cause error) {
	p.mu.Lock()
	if p.cause != nil {
		p.mu.Unlock()
		return
	}
	if cause == nil {
		cause = ErrClosed
	}
	p.cause = fmt.Errorf("%w: %w", ErrClosed, cause)
	close(p.done)
	for _, cancel := range p.incoming {
		cancel()
	}
	clear(p.pending)
	p.mu.Unlock()
	// 关闭管道也会解除阻塞中的Write，取消context本身做不到这一点
	p.input.Close()
	p.output.Close()
}

func (p *peer) send(msg message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	if len(data) > p.config.MaxMessageBytes {
		return errors.New("bridge message too large")
	}
	data = append(data, '\n')
	select {
	case <-p.done:
		return p.closedError()
	default:
	}
	select {
	case p.writes <- data:
		return nil
	case <-p.done:
		return p.closedError()
	default:
		return ErrBusy
	}
}

func (p *peer) closedError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cause
}

func (p *peer) writeLoop() error {
	for {
		select {
		case <-p.done:
			return nil
		case data := <-p.writes:
			for len(data) > 0 {
				n, err := p.input.Write(data)
				if err != nil {
					return err
				}
				if n == 0 {
					return io.ErrShortWrite
				}
				data = data[n:]
			}
		}
	}
}

func (p *peer) call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	return p.callMode(ctx, method, params, false)
}

func (p *peer) callMode(ctx context.Context, method string, params any, awaitCancelled bool) (json.RawMessage, error) {
	if ctx == nil {
		return nil, sdk.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	run, cancel := context.WithTimeout(ctx, p.config.CallTimeout)
	defer cancel()
	data, err := json.Marshal(params)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.cause != nil {
		err := p.cause
		p.mu.Unlock()
		return nil, err
	}
	if len(p.pending) >= p.config.MaxInflight {
		p.mu.Unlock()
		return nil, ErrBusy
	}
	p.next++
	id := fmt.Sprintf("g%d", p.next)
	response := make(chan message, 1)
	p.pending[id] = response
	p.mu.Unlock()
	if err := p.send(message{Type: "request", ID: id, Method: method, Params: data}); err != nil {
		p.mu.Lock()
		delete(p.pending, id)
		p.mu.Unlock()
		return nil, err
	}
	select {
	case result := <-response:
		if result.Error != nil {
			return nil, result.Error
		}
		return raw(result.Result), nil
	case <-p.done:
		return nil, p.closedError()
	case <-run.Done():
		// 只请求取消，保留槽位直到真实响应或进程退出，避免积累无人等待的调用
		if err := p.send(message{Type: "cancel", ID: id}); err != nil {
			p.close(err)
		}
		if awaitCancelled {
			// disposer继续计入执行中，直到对端确认完成，避免重试产生第二次执行
			select {
			case result := <-response:
				if result.Error != nil {
					return nil, result.Error
				}
				return raw(result.Result), nil
			case <-p.done:
				return nil, p.closedError()
			}
		}
		return nil, run.Err()
	}
}

func (p *peer) readLoop() error {
	scanner := bufio.NewScanner(p.output)
	scanner.Buffer(make([]byte, min(4096, p.config.MaxMessageBytes)), p.config.MaxMessageBytes+1)
	for scanner.Scan() {
		var msg message
		if err := json.Unmarshal(scanner.Bytes(), &msg); err != nil {
			return fmt.Errorf("invalid bridge message: %w", err)
		}
		if msg.ID == "" || len(msg.ID) > 128 {
			return errors.New("invalid bridge request id")
		}
		switch msg.Type {
		case "response":
			p.mu.Lock()
			reply := p.pending[msg.ID]
			delete(p.pending, msg.ID)
			p.mu.Unlock()
			if reply != nil {
				reply <- msg
			}
		case "cancel":
			p.mu.Lock()
			cancel := p.incoming[msg.ID]
			p.mu.Unlock()
			if cancel != nil {
				cancel()
			}
		case "request":
			if msg.Method == "" || len(msg.Method) > 128 {
				return errors.New("invalid bridge method")
			}
			if err := p.accept(msg); err != nil {
				return err
			}
		default:
			return errors.New("invalid bridge message type")
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return io.EOF
}

func (p *peer) respond(id string, value any, err error) error {
	msg := message{Type: "response", ID: id}
	if err != nil {
		msg.Error = &RemoteError{Code: "error", Message: err.Error()}
	} else {
		data, encodeErr := json.Marshal(value)
		if encodeErr != nil {
			msg.Error = &RemoteError{Code: "serialization", Message: encodeErr.Error()}
		} else {
			msg.Result = data
		}
	}
	if err := p.send(msg); err != nil {
		// 响应过大时返回明确错误，不能让对端一直等到超时
		return p.send(message{Type: "response", ID: id, Error: &RemoteError{Code: "transport", Message: err.Error()}})
	}
	return nil
}

func (p *peer) accept(msg message) error {
	p.mu.Lock()
	if _, exists := p.incoming[msg.ID]; exists {
		p.mu.Unlock()
		return errors.New("duplicate bridge request id")
	}
	if len(p.incoming) >= p.config.MaxInflight {
		p.mu.Unlock()
		return p.respond(msg.ID, nil, ErrBusy)
	}
	run, cancel := context.WithTimeout(p.ctx.Context(), p.config.CallTimeout)
	p.incoming[msg.ID] = cancel
	p.mu.Unlock()
	finish := func() {
		cancel()
		p.mu.Lock()
		delete(p.incoming, msg.ID)
		p.mu.Unlock()
	}
	if msg.Method == "defer" {
		// reader仍受旧代追踪，允许关闭期间返回的资源补登记清理
		value, err := p.handle(run, msg.Method, raw(msg.Params))
		finish()
		return p.respond(msg.ID, value, err)
	}
	_, err := p.ctx.Go("bridge request: "+msg.Method, func(context.Context) (err error) {
		defer finish()
		// 本地Service的panic交给Core的受管任务处理，同时解除对端等待
		defer func() {
			if value := recover(); value != nil {
				_ = p.respond(msg.ID, nil, fmt.Errorf("service panic: %v", value))
				panic(value)
			}
		}()
		var value any
		callErr := run.Err()
		if callErr == nil {
			value, callErr = p.handle(run, msg.Method, raw(msg.Params))
		}
		if err := p.respond(msg.ID, value, callErr); err != nil {
			p.close(err)
			return err
		}
		return nil
	})
	if err != nil {
		finish()
		return p.respond(msg.ID, nil, err)
	}
	return nil
}
