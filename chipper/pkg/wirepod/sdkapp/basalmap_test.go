package sdkapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

type fakeConsoleBot struct {
	vars  map[string]string
	calls []string
}

func newFakeStockBot() *fakeConsoleBot {
	return &fakeConsoleBot{
		vars: map[string]string{
			"MergeOldMaps":           "false",
			"CliffTimeout_ms":        "1200000.000000",
			"ProxTimeout_ms":         "600000.000000",
			"VisionTimeout_ms":       "120000.000000",
			"UnrecognizedTimeout_ms": "20000.000000",
			"TimeoutUpdatePeriod_ms": "5000.000000",
		},
	}
}

func (f *fakeConsoleBot) handle(_ context.Context, _ string, port int, pq string) (int, string, error) {
	f.calls = append(f.calls, fmt.Sprintf("%d:%s", port, pq))
	if port != basalEnginePort {
		return http.StatusNotFound, "wrong port", nil
	}
	u, err := url.Parse(pq)
	if err != nil {
		return http.StatusBadRequest, "bad url", nil
	}
	q := u.Query()
	switch u.Path {
	case "/consolevarget":
		v, ok := f.vars[q.Get("key")]
		if !ok {
			return http.StatusOK, "Variable not found<br>\n", nil
		}
		return http.StatusOK, v + "<br>\n", nil
	case "/consolevarset":
		k := q.Get("key")
		if _, ok := f.vars[k]; !ok {
			return http.StatusOK, "Variable not found<br>\n", nil
		}
		f.vars[k] = q.Get("value")
		return http.StatusOK, q.Get("value") + "<br>\n", nil
	default:
		return http.StatusNotFound, "not found", nil
	}
}

func resetBasalTestStore(t *testing.T) {
	t.Helper()
	tmp := filepath.Join(t.TempDir(), "basalMap.json")
	basalStoreMu.Lock()
	basalEntries = make(map[string]basalDiskEntry)
	basalLoaded = true
	basalStatePath = tmp
	basalStoreMu.Unlock()
}

func doBasalMap(t *testing.T, method, query string, getter basalConsoleGetter) (*httptest.ResponseRecorder, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(method, "/api-sdk/basal_map?"+query, nil)
	rec := httptest.NewRecorder()
	handleBasalMapWith(rec, req, "00112233", "192.0.2.10", getter)
	var body map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("answer is not JSON: %q", rec.Body.String())
	}
	return rec, body
}

func TestBasalMapEnableDisableAndNoSaveConsoleVars(t *testing.T) {
	resetBasalTestStore(t)
	defer resetBasalTestStore(t)

	bot := newFakeStockBot()
	rec, body := doBasalMap(t, http.MethodGet, "serial=00112233", bot.handle)
	if rec.Code != http.StatusOK || body["ok"] != true || body["enabled"] != false ||
		body["oskr_supported"] != true || body["robot_applied"] != false || body["drift"] != false {
		t.Fatalf("initial GET mismatch: code=%d body=%v", rec.Code, body)
	}

	bot.calls = nil
	rec, body = doBasalMap(t, http.MethodPost, "serial=00112233&enable=true", bot.handle)
	if rec.Code != http.StatusOK || body["ok"] != true || body["enabled"] != true ||
		body["robot_applied"] != true || body["merge_old_maps"] != false || body["drift"] != false {
		t.Fatalf("POST enable mismatch: code=%d body=%v", rec.Code, body)
	}
	if bot.vars["CliffTimeout_ms"] != "86400000" || bot.vars["ProxTimeout_ms"] != "86400000" ||
		bot.vars["VisionTimeout_ms"] != "21600000" || bot.vars["UnrecognizedTimeout_ms"] != "3600000" ||
		bot.vars["MergeOldMaps"] != "false" {
		t.Fatalf("unexpected bot consolevars: %v", bot.vars)
	}
	for _, c := range bot.calls {
		if strings.Contains(c, "SaveConsoleVars") {
			t.Fatalf("SaveConsoleVars must never be called: %v", c)
		}
	}

	rec, body = doBasalMap(t, http.MethodPost, "serial=00112233&enable=false", bot.handle)
	if rec.Code != http.StatusOK || body["enabled"] != false || body["robot_applied"] != false {
		t.Fatalf("POST disable mismatch: code=%d body=%v", rec.Code, body)
	}
	if bot.vars["CliffTimeout_ms"] != "1200000" || bot.vars["ProxTimeout_ms"] != "600000" ||
		bot.vars["VisionTimeout_ms"] != "120000" || bot.vars["UnrecognizedTimeout_ms"] != "20000" {
		t.Fatalf("stock restore failed: %v", bot.vars)
	}
}

func TestBasalMapValidationAndNonOSKR(t *testing.T) {
	resetBasalTestStore(t)
	defer resetBasalTestStore(t)

	bot := newFakeStockBot()
	for _, badQ := range []string{
		"serial=00112233&enable=yes",
		"serial=00112233&enable=true&cliff_ms=999",
		"serial=00112233&enable=false&cliff_ms=999",
		"serial=00112233&enable=true&prox_ms=86400001",
		"serial=00112233&enable=true&vision_ms=NaN",
		"serial=00112233&enable=true&unrecognized_ms=10.5",
	} {
		rec, body := doBasalMap(t, http.MethodPost, badQ, bot.handle)
		if rec.Code != http.StatusBadRequest || body["ok"] != false {
			t.Fatalf("expected 400 for %q, got %d %v", badQ, rec.Code, body)
		}
	}

	rec, _ := doBasalMap(t, http.MethodDelete, "serial=00112233", bot.handle)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}

	// Non-OSKR robot (:8888 connection refused) must still save disk desire and report oskr_supported=false.
	refused := func(_ context.Context, _ string, _ int, _ string) (int, string, error) {
		return 0, "", syscall.ECONNREFUSED
	}
	rec, body := doBasalMap(t, http.MethodPost, "serial=00445566&enable=true", refused)
	if rec.Code != http.StatusAccepted || body["ok"] != true || body["pod_saved"] != true ||
		body["oskr_supported"] != false || body["robot_applied"] != false {
		t.Fatalf("non-OSKR POST mismatch: code=%d body=%v", rec.Code, body)
	}
	rec, body = doBasalMap(t, http.MethodGet, "serial=00445566", refused)
	if rec.Code != http.StatusOK || body["ok"] != true || body["oskr_supported"] != false || body["enabled"] != true {
		t.Fatalf("non-OSKR GET mismatch: code=%d body=%v", rec.Code, body)
	}

	// Generic timeout must fail with 502 instead of claiming non-OSKR.
	netFail := func(_ context.Context, _ string, _ int, _ string) (int, string, error) {
		return 0, "", errors.New("i/o timeout")
	}
	rec, body = doBasalMap(t, http.MethodGet, "serial=00112233", netFail)
	if rec.Code != http.StatusBadGateway || body["ok"] != false {
		t.Fatalf("timeout GET mismatch: code=%d body=%v", rec.Code, body)
	}
}

func TestBasalMapPersistsReloadsAndReconcilesRebootDrift(t *testing.T) {
	resetBasalTestStore(t)
	defer resetBasalTestStore(t)

	bot := newFakeStockBot()
	rec, _ := doBasalMap(t, http.MethodPost,
		"serial=00112233&enable=true&cliff_ms=43200000&vision_ms=7200000", bot.handle)
	if rec.Code != http.StatusOK {
		t.Fatalf("custom POST failed: %d", rec.Code)
	}

	// Wipe RAM store to simulate wire-pod restart.
	basalStoreMu.Lock()
	basalEntries = make(map[string]basalDiskEntry)
	basalLoaded = false
	basalStoreMu.Unlock()

	entry, ok := getBasalDiskEntry("VIC:00112233")
	if !ok || !entry.Enabled || entry.CliffTimeoutMs != 43200000 || entry.VisionTimeoutMs != 7200000 {
		t.Fatalf("reloaded disk entry mismatch: ok=%v entry=%+v", ok, entry)
	}

	// Simulate robot reboot resetting consolevars to stock + MergeOldMaps=true accidentally set.
	bot = newFakeStockBot()
	bot.vars["MergeOldMaps"] = "true"
	lookup := func(esn string) (string, error) {
		if esn == "00112233" {
			return "192.0.2.10", nil
		}
		return "", errors.New("unknown")
	}
	reconcileBasalMapOnce(context.Background(), lookup, bot.handle)
	if bot.vars["MergeOldMaps"] != "false" || bot.vars["CliffTimeout_ms"] != "43200000" ||
		bot.vars["VisionTimeout_ms"] != "7200000" {
		t.Fatalf("reconcile did not restore profile: %v", bot.vars)
	}

	// Second pass with aligned robot must issue reads only, zero writes.
	bot.calls = nil
	reconcileBasalMapOnce(context.Background(), lookup, bot.handle)
	for _, c := range bot.calls {
		if strings.Contains(c, "consolevarset") {
			t.Fatalf("unexpected write when already aligned: %v", c)
		}
	}
}

func TestBasalMapFailsClosedOnBrokenReadback(t *testing.T) {
	resetBasalTestStore(t)
	defer resetBasalTestStore(t)

	staleReadback := func(_ context.Context, _ string, _ int, pq string) (int, string, error) {
		if strings.HasPrefix(pq, "/consolevarset?") {
			u, _ := url.Parse(pq)
			return http.StatusOK, u.Query().Get("value") + "<br>\n", nil
		}
		if strings.Contains(pq, "MergeOldMaps") {
			return http.StatusOK, "false<br>\n", nil
		}
		// Fault injection: subsequent consolevarget still reports stock timeout (failed apply on robot).
		return http.StatusOK, "1200000<br>\n", nil
	}
	rec, body := doBasalMap(t, http.MethodPost, "serial=00112233&enable=true", staleReadback)
	if rec.Code != http.StatusBadGateway || body["ok"] != false || body["pod_saved"] != true {
		t.Fatalf("expected 502 fail-closed on readback mismatch, got %d %v", rec.Code, body)
	}
}
