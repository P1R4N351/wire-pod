package sdkapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/kercre123/wire-pod/chipper/pkg/vars"
)

const (
	basalEnginePort     = 8888
	basalConsoleTimeout = 5 * time.Second
	basalMaxBodyBytes   = 65536
)

var (
	errBasalNoRobot       = errors.New("robot_not_found")
	errBasalConsoleFailed = errors.New("robot_console_failed")
	errBasalBadAnswer     = errors.New("robot_answer_not_understood")
)

type basalConsoleGetter func(ctx context.Context, ip string, port int, pathQuery string) (int, string, error)

func basalNormalizeESN(raw string) string {
	s := strings.ToLower(strings.TrimSpace(raw))
	return strings.TrimPrefix(s, "vic:")
}

func basalRobotIP(esn string) (string, error) {
	for _, bot := range vars.GetBotInfo().Robots {
		if strings.EqualFold(bot.Esn, esn) && bot.IPAddress != "" {
			host := bot.IPAddress
			if h, _, err := net.SplitHostPort(host); err == nil {
				host = h
			}
			return host, nil
		}
	}
	return "", errBasalNoRobot
}

func basalHttpConsoleGet(ctx context.Context, ip string, port int, pathQuery string) (int, string, error) {
	hostPort := net.JoinHostPort(ip, strconv.Itoa(port))
	url := fmt.Sprintf("http://%s%s", hostPort, pathQuery)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	client := &http.Client{Timeout: basalConsoleTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, basalMaxBodyBytes))
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

func basalParseConsoleBool(raw string) (bool, error) {
	cleaned := strings.ReplaceAll(raw, "<br>", " ")
	fields := strings.Fields(strings.ToLower(cleaned))
	if len(fields) != 1 {
		return false, errBasalBadAnswer
	}
	switch fields[0] {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, errBasalBadAnswer
	}
}

func basalWriteJSON(w http.ResponseWriter, statusCode int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(payload)
}
