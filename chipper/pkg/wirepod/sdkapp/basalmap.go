package sdkapp

// Basal MemoryMap persistence endpoint and reboot reconciler for wire-pod.
//
// GET  /api-sdk/basal_map?serial=<esn>
// POST /api-sdk/basal_map?serial=<esn>&enable=true|false[&cliff_ms=...&prox_ms=...&vision_ms=...&unrecognized_ms=...]
//
// Raises vic-engine (:8888) MapComponent timeout consolevars while keeping MergeOldMaps=false
// (MergeOldMaps=true folds Unknown leaves across origin merges via QuadTree::Merge) and
// avoiding SaveConsoleVars (which persists transient Cryosleep vars into consoleVars.ini).
// Persistence lives on the pod in <JdocsDir>/basalMap.json and is reconciled every 30 s.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/kercre123/wire-pod/chipper/pkg/logger"
	"github.com/kercre123/wire-pod/chipper/pkg/vars"
)

const (
	basalMinTimeoutMs     int64         = 1000
	basalMaxTimeoutMs     int64         = 86400000 // 24 h; safely exact in float32 & uint32
	basalMatchToleranceMs float64       = 1000.0
	basalTotalTimeout     time.Duration = 10 * time.Second
	basalReconcilePeriod  time.Duration = 30 * time.Second
	basalMaxReconcileESNs int           = 64
	basalStockCliffMs     int64         = 1200000
	basalStockProxMs      int64         = 600000
	basalStockVisionMs    int64         = 120000
	basalStockUnrecMs     int64         = 20000
	basalDefaultCliffMs   int64         = 86400000
	basalDefaultProxMs    int64         = 86400000
	basalDefaultVisionMs  int64         = 21600000
	basalDefaultUnrecMs   int64         = 3600000
)

type basalProfile struct {
	CliffTimeoutMs        int64 `json:"cliff_timeout_ms"`
	ProxTimeoutMs         int64 `json:"prox_timeout_ms"`
	VisionTimeoutMs       int64 `json:"vision_timeout_ms"`
	UnrecognizedTimeoutMs int64 `json:"unrecognized_timeout_ms"`
}

type basalDiskEntry struct {
	Enabled               bool  `json:"enabled"`
	CliffTimeoutMs        int64 `json:"cliff_timeout_ms"`
	ProxTimeoutMs         int64 `json:"prox_timeout_ms"`
	VisionTimeoutMs       int64 `json:"vision_timeout_ms"`
	UnrecognizedTimeoutMs int64 `json:"unrecognized_timeout_ms"`
	UpdatedAt             int64 `json:"updated_at"`
}

type basalConsoleSnapshot struct {
	MergeOldMaps          bool
	CliffTimeoutMs        int64
	ProxTimeoutMs         int64
	VisionTimeoutMs       int64
	UnrecognizedTimeoutMs int64
	TimeoutUpdatePeriodMs int64
}

var (
	basalStatePath string
	basalLoaded    bool
	basalStartOnce sync.Once
	basalStoreMu   sync.RWMutex
	basalEntries   = make(map[string]basalDiskEntry)
	basalActuateMu sync.Mutex
	errBasalRefuse = errors.New("robot_console_refused")
	errBasalBadVal = errors.New("robot_console_invalid_value")
)

func basalDefaultProfile() basalProfile {
	return basalProfile{
		CliffTimeoutMs:        basalDefaultCliffMs,
		ProxTimeoutMs:         basalDefaultProxMs,
		VisionTimeoutMs:       basalDefaultVisionMs,
		UnrecognizedTimeoutMs: basalDefaultUnrecMs,
	}
}

func basalStockProfile() basalProfile {
	return basalProfile{
		CliffTimeoutMs:        basalStockCliffMs,
		ProxTimeoutMs:         basalStockProxMs,
		VisionTimeoutMs:       basalStockVisionMs,
		UnrecognizedTimeoutMs: basalStockUnrecMs,
	}
}

func basalStateFilePath() string {
	if basalStatePath != "" {
		return basalStatePath
	}
	if strings.TrimSpace(vars.JdocsDir) == "" {
		return ""
	}
	return filepath.Join(vars.JdocsDir, "basalMap.json")
}

func validateBasalProfile(p basalProfile) error {
	vals := [4]int64{p.CliffTimeoutMs, p.ProxTimeoutMs, p.VisionTimeoutMs, p.UnrecognizedTimeoutMs}
	for _, v := range vals {
		if v < basalMinTimeoutMs || v > basalMaxTimeoutMs {
			return fmt.Errorf("timeout %d out of range [%d..%d] ms", v, basalMinTimeoutMs, basalMaxTimeoutMs)
		}
	}
	return nil
}

func ensureBasalLoadedLocked() {
	if basalLoaded {
		return
	}
	basalLoaded = true
	path := basalStateFilePath()
	if path == "" {
		return
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var disk map[string]basalDiskEntry
	if err := json.Unmarshal(raw, &disk); err != nil {
		logger.Println("basal_map: ignoring malformed state file: " + err.Error())
		return
	}
	for k, v := range disk {
		esn := basalNormalizeESN(k)
		if esn == "" {
			continue
		}
		p := basalProfile{v.CliffTimeoutMs, v.ProxTimeoutMs, v.VisionTimeoutMs, v.UnrecognizedTimeoutMs}
		if validateBasalProfile(p) != nil {
			continue
		}
		basalEntries[esn] = v
	}
}

func saveBasalLocked() error {
	path := basalStateFilePath()
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(basalEntries, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0o600); err != nil {
		return err
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func getBasalDiskEntry(device string) (basalDiskEntry, bool) {
	esn := basalNormalizeESN(device)
	if esn == "" {
		return basalDiskEntry{}, false
	}
	basalStoreMu.Lock()
	defer basalStoreMu.Unlock()
	ensureBasalLoadedLocked()
	entry, ok := basalEntries[esn]
	return entry, ok
}

func setBasalDiskEntry(device string, entry basalDiskEntry) error {
	esn := basalNormalizeESN(device)
	if esn == "" {
		return errBasalNoRobot
	}
	basalStoreMu.Lock()
	defer basalStoreMu.Unlock()
	ensureBasalLoadedLocked()
	basalEntries[esn] = entry
	return saveBasalLocked()
}

func basalEnabledSnapshot() []basalDiskSnapshotItem {
	basalStoreMu.Lock()
	ensureBasalLoadedLocked()
	esns := make([]string, 0, len(basalEntries))
	for esn, entry := range basalEntries {
		if entry.Enabled {
			esns = append(esns, esn)
		}
	}
	sort.Strings(esns)
	if len(esns) > basalMaxReconcileESNs {
		esns = esns[:basalMaxReconcileESNs]
	}
	items := make([]basalDiskSnapshotItem, 0, len(esns))
	for _, esn := range esns {
		entry := basalEntries[esn]
		items = append(items, basalDiskSnapshotItem{
			esn: esn,
			profile: basalProfile{
				CliffTimeoutMs:        entry.CliffTimeoutMs,
				ProxTimeoutMs:         entry.ProxTimeoutMs,
				VisionTimeoutMs:       entry.VisionTimeoutMs,
				UnrecognizedTimeoutMs: entry.UnrecognizedTimeoutMs,
			},
		})
	}
	basalStoreMu.Unlock()
	return items
}

type basalDiskSnapshotItem struct {
	esn     string
	profile basalProfile
}

func isBasalConnRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection refused")
}

func parseBasalConsoleFloat(raw string) (int64, error) {
	cleaned := strings.ReplaceAll(raw, "<br>", " ")
	fields := strings.Fields(strings.TrimSpace(cleaned))
	if len(fields) != 1 {
		return 0, errBasalBadVal
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, errBasalBadVal
	}
	rounded := math.Round(v)
	if rounded < float64(math.MinInt64/2) || rounded > float64(math.MaxInt64/2) {
		return 0, errBasalBadVal
	}
	return int64(rounded), nil
}

func readBasalConsoleBool(ctx context.Context, ip string, key string, getter basalConsoleGetter) (bool, error) {
	code, body, err := getter(ctx, ip, basalEnginePort, "/consolevarget?key="+key)
	if err != nil {
		if isBasalConnRefused(err) {
			return false, errBasalRefuse
		}
		return false, errBasalConsoleFailed
	}
	if code != http.StatusOK {
		return false, errBasalConsoleFailed
	}
	val, perr := basalParseConsoleBool(body)
	if perr != nil {
		return false, errBasalBadVal
	}
	return val, nil
}

func readBasalConsoleInt(ctx context.Context, ip string, key string, getter basalConsoleGetter) (int64, error) {
	code, body, err := getter(ctx, ip, basalEnginePort, "/consolevarget?key="+key)
	if err != nil {
		if isBasalConnRefused(err) {
			return 0, errBasalRefuse
		}
		return 0, errBasalConsoleFailed
	}
	if code != http.StatusOK {
		return 0, errBasalConsoleFailed
	}
	val, perr := parseBasalConsoleFloat(body)
	if perr != nil {
		return 0, errBasalBadVal
	}
	return val, nil
}

func readBasalConsoleSnapshot(ctx context.Context, ip string, getter basalConsoleGetter) (basalConsoleSnapshot, error) {
	var snap basalConsoleSnapshot
	mergeVal, err := readBasalConsoleBool(ctx, ip, "MergeOldMaps", getter)
	if err != nil {
		return snap, err
	}
	snap.MergeOldMaps = mergeVal
	keys := [5]string{"CliffTimeout_ms", "ProxTimeout_ms", "VisionTimeout_ms", "UnrecognizedTimeout_ms", "TimeoutUpdatePeriod_ms"}
	dst := [5]*int64{&snap.CliffTimeoutMs, &snap.ProxTimeoutMs, &snap.VisionTimeoutMs, &snap.UnrecognizedTimeoutMs, &snap.TimeoutUpdatePeriodMs}
	for i, key := range keys {
		v, kerr := readBasalConsoleInt(ctx, ip, key, getter)
		if kerr != nil {
			return snap, kerr
		}
		*dst[i] = v
	}
	return snap, nil
}

func setBasalConsoleBool(ctx context.Context, ip string, key string, want bool, getter basalConsoleGetter) error {
	valStr := "false"
	if want {
		valStr = "true"
	}
	code, body, err := getter(ctx, ip, basalEnginePort, fmt.Sprintf("/consolevarset?key=%s&value=%s", key, valStr))
	if err != nil {
		if isBasalConnRefused(err) {
			return errBasalRefuse
		}
		return errBasalConsoleFailed
	}
	if code != http.StatusOK {
		return errBasalConsoleFailed
	}
	got, perr := basalParseConsoleBool(body)
	if perr != nil || got != want {
		return errBasalBadVal
	}
	return nil
}

func setBasalConsoleInt(ctx context.Context, ip string, key string, want int64, getter basalConsoleGetter) error {
	code, body, err := getter(ctx, ip, basalEnginePort, fmt.Sprintf("/consolevarset?key=%s&value=%d", key, want))
	if err != nil {
		if isBasalConnRefused(err) {
			return errBasalRefuse
		}
		return errBasalConsoleFailed
	}
	if code != http.StatusOK {
		return errBasalConsoleFailed
	}
	got, perr := parseBasalConsoleFloat(body)
	if perr != nil || math.Abs(float64(got-want)) > basalMatchToleranceMs {
		return errBasalBadVal
	}
	return nil
}

func basalProfileMatches(snap basalConsoleSnapshot, want basalProfile) bool {
	if snap.MergeOldMaps {
		return false
	}
	pairs := [4][2]int64{
		{snap.CliffTimeoutMs, want.CliffTimeoutMs},
		{snap.ProxTimeoutMs, want.ProxTimeoutMs},
		{snap.VisionTimeoutMs, want.VisionTimeoutMs},
		{snap.UnrecognizedTimeoutMs, want.UnrecognizedTimeoutMs},
	}
	for _, p := range pairs {
		if math.Abs(float64(p[0]-p[1])) > basalMatchToleranceMs {
			return false
		}
	}
	return true
}

func applyBasalConsoleProfile(ctx context.Context, ip string, want basalProfile, getter basalConsoleGetter) (basalConsoleSnapshot, error) {
	if err := validateBasalProfile(want); err != nil {
		return basalConsoleSnapshot{}, err
	}
	if err := setBasalConsoleBool(ctx, ip, "MergeOldMaps", false, getter); err != nil {
		return basalConsoleSnapshot{}, err
	}
	varsToSet := [4]struct {
		key string
		val int64
	}{
		{"CliffTimeout_ms", want.CliffTimeoutMs},
		{"ProxTimeout_ms", want.ProxTimeoutMs},
		{"VisionTimeout_ms", want.VisionTimeoutMs},
		{"UnrecognizedTimeout_ms", want.UnrecognizedTimeoutMs},
	}
	for _, item := range varsToSet {
		if err := setBasalConsoleInt(ctx, ip, item.key, item.val, getter); err != nil {
			return basalConsoleSnapshot{}, err
		}
	}
	snap, err := readBasalConsoleSnapshot(ctx, ip, getter)
	if err != nil {
		return basalConsoleSnapshot{}, err
	}
	if !basalProfileMatches(snap, want) {
		return snap, errBasalBadVal
	}
	return snap, nil
}

func reconcileBasalMapOnce(ctx context.Context, ipLookup func(string) (string, error), getter basalConsoleGetter) {
	if ctx == nil || ipLookup == nil || getter == nil {
		return
	}
	for _, item := range basalEnabledSnapshot() {
		ip, err := ipLookup(item.esn)
		if err != nil || strings.TrimSpace(ip) == "" {
			continue
		}
		basalActuateMu.Lock()
		entry, ok := getBasalDiskEntry(item.esn)
		if ok && entry.Enabled {
			stepCtx, cancel := context.WithTimeout(ctx, basalTotalTimeout)
			snap, rerr := readBasalConsoleSnapshot(stepCtx, ip, getter)
			if rerr == nil && !basalProfileMatches(snap, item.profile) {
				if _, aerr := applyBasalConsoleProfile(stepCtx, ip, item.profile, getter); aerr != nil {
					logger.Println("basal_map: reconcile apply failed for " + item.esn + ": " + aerr.Error())
				} else {
					logger.Println("basal_map: re-applied persistent timeouts for " + item.esn)
				}
			}
			cancel()
		}
		basalActuateMu.Unlock()
	}
}

// StartBasalMapFromDisk loads persisted basal timeout configs and reconciles OSKR robots every 30 s.
func StartBasalMapFromDisk() {
	basalStartOnce.Do(func() {
		basalStoreMu.Lock()
		ensureBasalLoadedLocked()
		basalStoreMu.Unlock()
		go func() {
			ticker := time.NewTicker(basalReconcilePeriod)
			defer ticker.Stop()
			for range ticker.C {
				reconcileBasalMapOnce(context.Background(), basalRobotIP, basalHttpConsoleGet)
			}
		}()
	})
}

func parseBasalIntParam(r *http.Request, name string, fallback int64) (int64, error) {
	raw := strings.TrimSpace(r.FormValue(name))
	if raw == "" {
		return fallback, nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v < basalMinTimeoutMs || v > basalMaxTimeoutMs {
		return 0, fmt.Errorf("%s must be an integer in [%d..%d]", name, basalMinTimeoutMs, basalMaxTimeoutMs)
	}
	return v, nil
}

func parseBasalPostProfile(r *http.Request, enable bool) (basalProfile, error) {
	def := basalDefaultProfile()
	cliff, err := parseBasalIntParam(r, "cliff_ms", def.CliffTimeoutMs)
	if err != nil {
		return basalProfile{}, err
	}
	prox, err := parseBasalIntParam(r, "prox_ms", def.ProxTimeoutMs)
	if err != nil {
		return basalProfile{}, err
	}
	vision, err := parseBasalIntParam(r, "vision_ms", def.VisionTimeoutMs)
	if err != nil {
		return basalProfile{}, err
	}
	unrec, err := parseBasalIntParam(r, "unrecognized_ms", def.UnrecognizedTimeoutMs)
	if err != nil {
		return basalProfile{}, err
	}
	if !enable {
		return basalStockProfile(), nil
	}
	return basalProfile{cliff, prox, vision, unrec}, nil
}

func basalStatusPayload(
	esn string,
	entry basalDiskEntry,
	hasEntry bool,
	oskrSupported bool,
	robotApplied bool,
	drift bool,
	snap *basalConsoleSnapshot,
) map[string]interface{} {
	want := basalProfile{entry.CliffTimeoutMs, entry.ProxTimeoutMs, entry.VisionTimeoutMs, entry.UnrecognizedTimeoutMs}
	if !hasEntry {
		want = basalStockProfile()
	}
	out := map[string]interface{}{
		"ok":                      true,
		"serial":                  esn,
		"enabled":                 hasEntry && entry.Enabled,
		"pod_saved":               hasEntry,
		"oskr_supported":          oskrSupported,
		"robot_applied":           robotApplied,
		"drift":                   drift,
		"merge_old_maps":          false,
		"desired_timeouts_ms":     want,
		"timeout_update_period_ms": nil,
	}
	if snap != nil {
		out["merge_old_maps"] = snap.MergeOldMaps
		out["timeout_update_period_ms"] = snap.TimeoutUpdatePeriodMs
		out["cliff_timeout_ms"] = snap.CliffTimeoutMs
		out["prox_timeout_ms"] = snap.ProxTimeoutMs
		out["vision_timeout_ms"] = snap.VisionTimeoutMs
		out["unrecognized_timeout_ms"] = snap.UnrecognizedTimeoutMs
		out["timeouts_ms"] = basalProfile{
			CliffTimeoutMs:        snap.CliffTimeoutMs,
			ProxTimeoutMs:         snap.ProxTimeoutMs,
			VisionTimeoutMs:       snap.VisionTimeoutMs,
			UnrecognizedTimeoutMs: snap.UnrecognizedTimeoutMs,
		}
	}
	return out
}

func handleBasalMap(w http.ResponseWriter, r *http.Request) {
	esn := basalNormalizeESN(r.FormValue("serial"))
	ip, err := basalRobotIP(esn)
	if err != nil {
		basalWriteJSON(w, http.StatusNotFound, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	handleBasalMapWith(w, r, esn, ip, basalHttpConsoleGet)
}

func handleBasalMapGet(w http.ResponseWriter, ctx context.Context, esn string, ip string, getter basalConsoleGetter) {
	entry, hasEntry := getBasalDiskEntry(esn)
	want := basalStockProfile()
	if hasEntry && entry.Enabled {
		want = basalProfile{entry.CliffTimeoutMs, entry.ProxTimeoutMs, entry.VisionTimeoutMs, entry.UnrecognizedTimeoutMs}
	}
	snap, err := readBasalConsoleSnapshot(ctx, ip, getter)
	if err != nil {
		if errors.Is(err, errBasalRefuse) {
			basalWriteJSON(w, http.StatusOK, basalStatusPayload(esn, entry, hasEntry, false, false, false, nil))
			return
		}
		basalWriteJSON(w, http.StatusBadGateway, map[string]interface{}{
			"ok": false, "error": err.Error(), "serial": esn, "pod_saved": hasEntry, "enabled": hasEntry && entry.Enabled,
		})
		return
	}
	applied := hasEntry && entry.Enabled && basalProfileMatches(snap, want)
	drift := (hasEntry && entry.Enabled && !applied) || snap.MergeOldMaps
	basalWriteJSON(w, http.StatusOK, basalStatusPayload(esn, entry, hasEntry, true, applied, drift, &snap))
}

func handleBasalMapPost(
	w http.ResponseWriter,
	r *http.Request,
	ctx context.Context,
	esn string,
	ip string,
	getter basalConsoleGetter,
) {
	var enable bool
	switch strings.TrimSpace(r.FormValue("enable")) {
	case "true":
		enable = true
	case "false":
		enable = false
	default:
		basalWriteJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": "enable must be true or false"})
		return
	}
	want, err := parseBasalPostProfile(r, enable)
	if err != nil {
		basalWriteJSON(w, http.StatusBadRequest, map[string]interface{}{"ok": false, "error": err.Error()})
		return
	}
	entry := basalDiskEntry{
		Enabled:               enable,
		CliffTimeoutMs:        want.CliffTimeoutMs,
		ProxTimeoutMs:         want.ProxTimeoutMs,
		VisionTimeoutMs:       want.VisionTimeoutMs,
		UnrecognizedTimeoutMs: want.UnrecognizedTimeoutMs,
		UpdatedAt:             time.Now().Unix(),
	}
	basalActuateMu.Lock()
	defer basalActuateMu.Unlock()
	if serr := setBasalDiskEntry(esn, entry); serr != nil {
		basalWriteJSON(w, http.StatusInternalServerError, map[string]interface{}{"ok": false, "error": "persist_failed: " + serr.Error()})
		return
	}
	snap, aerr := applyBasalConsoleProfile(ctx, ip, want, getter)
	if aerr != nil {
		if errors.Is(aerr, errBasalRefuse) {
			basalWriteJSON(w, http.StatusAccepted, basalStatusPayload(esn, entry, true, false, false, false, nil))
			return
		}
		basalWriteJSON(w, http.StatusBadGateway, map[string]interface{}{
			"ok": false, "error": aerr.Error(), "serial": esn, "pod_saved": true, "enabled": enable,
		})
		return
	}
	basalWriteJSON(w, http.StatusOK, basalStatusPayload(esn, entry, true, true, enable, false, &snap))
}

func handleBasalMapWith(w http.ResponseWriter, r *http.Request, esn string, ip string, getter basalConsoleGetter) {
	ctx, cancel := context.WithTimeout(r.Context(), basalTotalTimeout)
	defer cancel()
	switch r.Method {
	case http.MethodGet:
		handleBasalMapGet(w, ctx, esn, ip, getter)
	case http.MethodPost:
		handleBasalMapPost(w, r, ctx, esn, ip, getter)
	default:
		basalWriteJSON(w, http.StatusMethodNotAllowed, map[string]interface{}{"ok": false, "error": "use GET or POST"})
	}
}
