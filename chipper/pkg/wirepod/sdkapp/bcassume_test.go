package sdkapp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fforchino/vector-go-sdk/pkg/vectorpb"
)

type fakeControlStream struct {
	ctx   context.Context
	mu    sync.Mutex
	sent  []*vectorpb.BehaviorControlRequest
	resps chan *vectorpb.BehaviorControlResponse
	fail  chan error
}

func newFakeControlStream(ctx context.Context) *fakeControlStream {
	return &fakeControlStream{
		ctx:   ctx,
		resps: make(chan *vectorpb.BehaviorControlResponse, 8),
		fail:  make(chan error, 1),
	}
}

func (f *fakeControlStream) Send(r *vectorpb.BehaviorControlRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, r)
	return nil
}

func (f *fakeControlStream) Recv() (*vectorpb.BehaviorControlResponse, error) {
	select {
	case r := <-f.resps:
		return r, nil
	case err := <-f.fail:
		return nil, err
	case <-f.ctx.Done():
		return nil, f.ctx.Err()
	}
}

func (f *fakeControlStream) log() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, r := range f.sent {
		if cr := r.GetControlRequest(); cr != nil {
			out = append(out, "req:"+cr.Priority.String())
		} else if r.GetControlRelease() != nil {
			out = append(out, "release")
		}
	}
	return out
}

func grantedEvent() *vectorpb.BehaviorControlResponse {
	return &vectorpb.BehaviorControlResponse{
		ResponseType: &vectorpb.BehaviorControlResponse_ControlGrantedResponse{
			ControlGrantedResponse: &vectorpb.ControlGrantedResponse{},
		},
	}
}

func TestPriorityMapping(t *testing.T) {
	cases := map[string]vectorpb.ControlRequest_Priority{
		"":                    vectorpb.ControlRequest_DEFAULT,
		"default":             vectorpb.ControlRequest_DEFAULT,
		"high":                vectorpb.ControlRequest_OVERRIDE_BEHAVIORS,
		"override_behaviors":  vectorpb.ControlRequest_OVERRIDE_BEHAVIORS,
		"reserve":             vectorpb.ControlRequest_RESERVE_CONTROL,
		" reserve_control ":   vectorpb.ControlRequest_RESERVE_CONTROL,
	}
	for in, want := range cases {
		if got := bcPriority(in); got != want {
			t.Errorf("bcPriority(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestAssumeControlWithOpenerGranted(t *testing.T) {
	var task robotTask
	var stream *fakeControlStream
	err := assumeControlWithOpener(context.Background(), &task, context.Background(), "default", time.Second,
		func(ctx context.Context) (bcControlStream, error) {
			stream = newFakeControlStream(ctx)
			stream.resps <- grantedEvent()
			return stream, nil
		})
	if err != nil {
		t.Fatalf("expected grant, got %v", err)
	}
	if !task.running() {
		t.Fatal("expected control hold task to remain active after grant")
	}
	task.stop()
	deadline := time.Now().Add(time.Second)
	for task.running() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if task.running() {
		t.Fatal("expected task to stop after release")
	}
}

func TestAssumeControlWithOpenerStreamErrorAndTimeout(t *testing.T) {
	var task robotTask
	err := assumeControlWithOpener(context.Background(), &task, context.Background(), "default", time.Second,
		func(context.Context) (bcControlStream, error) {
			return nil, errors.New("dial failed")
		})
	if err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("expected dial error, got %v", err)
	}
	if task.running() {
		t.Fatal("expected hold stopped after stream open error")
	}

	errTimeout := assumeControlWithOpener(context.Background(), &task, context.Background(), "default", 40*time.Millisecond,
		func(ctx context.Context) (bcControlStream, error) {
			return newFakeControlStream(ctx), nil
		})
	if errTimeout == nil || !strings.Contains(errTimeout.Error(), "timed out") {
		t.Fatalf("expected grant timeout, got %v", errTimeout)
	}
	deadline := time.Now().Add(time.Second)
	for task.running() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if task.running() {
		t.Fatal("expected hold stopped after timeout")
	}
}
