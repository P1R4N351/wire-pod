package sdkapp

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/fforchino/vector-go-sdk/pkg/vectorpb"
)

func TestHandleSayTextWithFn(t *testing.T) {
	var captured string
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api-sdk/say_text?serial=0dd1cafe&text="+url.QueryEscape("  Hello world  "), nil)
	handleSayTextWithFn(rec, req, context.Background(), func(_ context.Context, r *vectorpb.SayTextRequest) (*vectorpb.SayTextResponse, error) {
		captured = r.Text
		return &vectorpb.SayTextResponse{State: vectorpb.SayTextResponse_FINISHED}, nil
	})
	if rec.Body.String() != "success" || captured != "Hello world" {
		t.Fatalf("expected success with trimmed text, got body=%q text=%q", rec.Body.String(), captured)
	}

	recEmpty := httptest.NewRecorder()
	handleSayTextWithFn(recEmpty, httptest.NewRequest(http.MethodGet, "/api-sdk/say_text?serial=0dd1cafe&text=%20%20", nil), context.Background(),
		func(_ context.Context, _ *vectorpb.SayTextRequest) (*vectorpb.SayTextResponse, error) {
			t.Fatal("sayFn should not run for empty text")
			return nil, nil
		})
	if !strings.Contains(recEmpty.Body.String(), "error: text is required") {
		t.Fatalf("expected empty refusal, got %q", recEmpty.Body.String())
	}

	recLong := httptest.NewRecorder()
	longReq := httptest.NewRequest(http.MethodGet, "/api-sdk/say_text?serial=0dd1cafe&text="+strings.Repeat("a", 600), nil)
	handleSayTextWithFn(recLong, longReq, context.Background(),
		func(_ context.Context, _ *vectorpb.SayTextRequest) (*vectorpb.SayTextResponse, error) {
			t.Fatal("sayFn should not run for oversized text")
			return nil, nil
		})
	if !strings.Contains(recLong.Body.String(), "error: text is too long") {
		t.Fatalf("expected length refusal, got %q", recLong.Body.String())
	}

	recErr := httptest.NewRecorder()
	handleSayTextWithFn(recErr, httptest.NewRequest(http.MethodGet, "/api-sdk/say_text?serial=0dd1cafe&text=hi", nil), context.Background(),
		func(_ context.Context, _ *vectorpb.SayTextRequest) (*vectorpb.SayTextResponse, error) {
			return nil, errors.New("rpc unavailable")
		})
	if !strings.Contains(recErr.Body.String(), "error: rpc unavailable") {
		t.Fatalf("expected RPC error propagation, got %q", recErr.Body.String())
	}

	recInv := httptest.NewRecorder()
	handleSayTextWithFn(recInv, httptest.NewRequest(http.MethodGet, "/api-sdk/say_text?serial=0dd1cafe&text=hi", nil), context.Background(),
		func(ctx context.Context, _ *vectorpb.SayTextRequest) (*vectorpb.SayTextResponse, error) {
			if _, hasDeadline := ctx.Deadline(); !hasDeadline {
				t.Fatal("expected deadline on say context")
			}
			return &vectorpb.SayTextResponse{State: vectorpb.SayTextResponse_INVALID}, nil
		})
	if !strings.Contains(recInv.Body.String(), "rejected") {
		t.Fatalf("expected INVALID rejection error, got %q", recInv.Body.String())
	}
}

func TestHandleCloudIntentWithFn(t *testing.T) {
	recOK := httptest.NewRecorder()
	handleCloudIntentWithFn(recOK, httptest.NewRequest(http.MethodGet, "/api-sdk/cloud_intent?serial=0dd1cafe&intent=explore_start", nil), context.Background(),
		func(ctx context.Context, r *vectorpb.AppIntentRequest) (*vectorpb.AppIntentResponse, error) {
			if _, hasDeadline := ctx.Deadline(); !hasDeadline {
				t.Fatal("expected deadline on intent context")
			}
			if r.Intent != "explore_start" {
				t.Fatalf("unexpected intent %q", r.Intent)
			}
			return &vectorpb.AppIntentResponse{}, nil
		})
	if recOK.Body.String() != "done" {
		t.Fatalf("expected done, got %q", recOK.Body.String())
	}

	recErr := httptest.NewRecorder()
	handleCloudIntentWithFn(recErr, httptest.NewRequest(http.MethodGet, "/api-sdk/cloud_intent?serial=0dd1cafe&intent=explore_start", nil), context.Background(),
		func(_ context.Context, _ *vectorpb.AppIntentRequest) (*vectorpb.AppIntentResponse, error) {
			return nil, errors.New("intent error")
		})
	if !strings.Contains(recErr.Body.String(), "error: intent error") {
		t.Fatalf("expected error response, got %q", recErr.Body.String())
	}
}

func TestUpdateSettingsWithClientTimeoutAndErrors(t *testing.T) {
	if sdkSettingsClient.Timeout != 10*time.Second {
		t.Fatalf("expected 10s sdkSettingsClient timeout, got %v", sdkSettingsClient.Timeout)
	}
	srvOK := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/update_settings" || r.Header.Get("Authorization") != "Bearer tok" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		var payload map[string]interface{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil || payload["update_settings"] != true {
			http.Error(w, "bad payload", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srvOK.Close()

	if err := updateSettingsWithClient(srvOK.Client(), srvOK.URL, "tok", map[string]interface{}{"master_volume": 2}); err != nil {
		t.Fatalf("expected success, got %v", err)
	}

	srvFail := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "denied", http.StatusInternalServerError)
	}))
	defer srvFail.Close()

	if err := updateSettingsWithClient(srvFail.Client(), srvFail.URL, "tok", map[string]interface{}{"master_volume": 2}); err == nil || !strings.Contains(err.Error(), "http 500") {
		t.Fatalf("expected http 500 error, got %v", err)
	}

	srvClosed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := srvClosed.URL
	srvClosed.Close()
	if err := updateSettingsWithClient(nil, closedURL, "tok", map[string]interface{}{}); err == nil {
		t.Fatal("expected error for unreachable server, got nil")
	}
}
