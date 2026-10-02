package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"testbot/core"
	"testbot/core/sdk"
)

func testPeer(t *testing.T, handle func(context.Context, string, json.RawMessage) (any, error)) (*peer, net.Conn) {
	t.Helper()
	local, remote := net.Pipe()
	rt := core.New()
	var p *peer
	err := rt.Register(sdk.Definition{Manifest: sdk.Manifest{ID: "wire"}, Factory: func() (sdk.Plugin, error) {
		return sdk.StartFunc(func(ctx sdk.Context) error {
			p = newPeer(local, local, Config{CallTimeout: time.Second, MaxMessageBytes: 1024, MaxInflight: 1}, ctx, handle)
			if err := ctx.Defer("transport", func(context.Context) error { p.close(ErrClosed); return nil }); err != nil {
				return err
			}
			return p.start()
		}), nil
	}})
	if err == nil {
		err = rt.Start(context.Background(), "wire")
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		remote.Close()
		if err := rt.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	if err := remote.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return p, remote
}

func readFrame(t *testing.T, decoder *json.Decoder) message {
	t.Helper()
	var msg message
	if err := decoder.Decode(&msg); err != nil {
		t.Fatal(err)
	}
	return msg
}

func writeFrame(t *testing.T, encoder *json.Encoder, msg message) {
	t.Helper()
	if err := encoder.Encode(msg); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledCallsRetainAdmissionUntilResponse(t *testing.T) {
	p, wire := testPeer(t, nil)
	decoder, encoder := json.NewDecoder(wire), json.NewEncoder(wire)
	result := make(chan error, 1)
	run, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	go func() { _, err := p.call(run, "echo", nil); result <- err }()
	request := readFrame(t, decoder)
	if err := <-result; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if cancel := readFrame(t, decoder); cancel.Type != "cancel" || cancel.ID != request.ID {
		t.Fatal(cancel)
	}
	if _, err := p.call(context.Background(), "echo", nil); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	writeFrame(t, encoder, message{Type: "response", ID: request.ID, Result: json.RawMessage("42")})
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		p.mu.Lock()
		count := len(p.pending)
		p.mu.Unlock()
		if count == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("completed call retained slot")
}

func TestCleanupCallWaitsForActualRemoteCompletion(t *testing.T) {
	p, wire := testPeer(t, nil)
	decoder, encoder := json.NewDecoder(wire), json.NewEncoder(wire)
	result := make(chan error, 1)
	run, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	go func() { _, err := p.callMode(run, "dispose", nil, true); result <- err }()
	request := readFrame(t, decoder)
	if cancel := readFrame(t, decoder); cancel.Type != "cancel" {
		t.Fatal(cancel)
	}
	select {
	case err := <-result:
		t.Fatal("disposer finished before remote response", err)
	default:
	}
	writeFrame(t, encoder, message{Type: "response", ID: request.ID, Result: json.RawMessage("null")})
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestInboundAdmissionRemainsUntilHandlerReturns(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	p, wire := testPeer(t, func(context.Context, string, json.RawMessage) (any, error) { close(entered); <-release; return 42, nil })
	decoder, encoder := json.NewDecoder(wire), json.NewEncoder(wire)
	writeFrame(t, encoder, message{Type: "request", ID: "c1", Method: "call", Params: json.RawMessage("null")})
	<-entered
	writeFrame(t, encoder, message{Type: "request", ID: "c2", Method: "call", Params: json.RawMessage("null")})
	busy := readFrame(t, decoder)
	if busy.ID != "c2" || busy.Error == nil {
		t.Fatal(busy)
	}
	writeFrame(t, encoder, message{Type: "cancel", ID: "c1"})
	p.mu.Lock()
	count := len(p.incoming)
	p.mu.Unlock()
	if count != 1 {
		t.Fatal("cancel removed active invocation", count)
	}
	close(release)
	response := readFrame(t, decoder)
	if response.ID != "c1" || string(response.Result) != "42" {
		t.Fatal(response)
	}
}

func TestOversizedOutgoingCallDoesNotRetainSlot(t *testing.T) {
	p, _ := testPeer(t, nil)
	if _, err := p.call(context.Background(), "big", make([]int, 1024)); err == nil {
		t.Fatal("oversized frame accepted")
	}
	p.mu.Lock()
	count := len(p.pending)
	p.mu.Unlock()
	if count != 0 {
		t.Fatal("unsent request retained slot")
	}
}
