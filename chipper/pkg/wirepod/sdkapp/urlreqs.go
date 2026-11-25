package sdkapp

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var transCfg = &http.Transport{
	TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
}

var sdkSettingsClient = &http.Client{
	Transport: transCfg,
	Timeout:   10 * time.Second,
}

func updateSettingsWithClient(client *http.Client, baseURL, guid string, settings map[string]interface{}) error {
	if client == nil {
		client = sdkSettingsClient
	}
	body, err := json.Marshal(map[string]interface{}{
		"update_settings": true,
		"settings":        settings,
	})
	if err != nil {
		return err
	}
	target := strings.TrimRight(baseURL, "/") + "/v1/update_settings"
	req, err := http.NewRequest(http.MethodPost, target, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+guid)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("update_settings http %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func updateSettings(robot *Robot, settings map[string]interface{}) error {
	if robot == nil {
		return fmt.Errorf("update_settings: nil robot")
	}
	return updateSettingsWithClient(sdkSettingsClient, "https://"+robot.Target, robot.GUID, settings)
}

func setCustomEyeColor(robot *Robot, hue string, sat string) error {
	h, err := strconv.ParseFloat(hue, 64)
	if err != nil {
		return fmt.Errorf("invalid hue: %w", err)
	}
	s, err := strconv.ParseFloat(sat, 64)
	if err != nil {
		return fmt.Errorf("invalid saturation: %w", err)
	}
	return updateSettings(robot, map[string]interface{}{
		"custom_eye_color": map[string]interface{}{
			"enabled":    true,
			"hue":        h,
			"saturation": s,
		},
	})
}

func setPresetEyeColor(robot *Robot, value string) error {
	color, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("invalid eye color: %w", err)
	}
	return updateSettings(robot, map[string]interface{}{
		"custom_eye_color": map[string]interface{}{"enabled": false},
		"eye_color":        color,
	})
}

func setSettingSDKstring(robot *Robot, setting string, value string) error {
	return updateSettings(robot, map[string]interface{}{setting: value})
}

func setSettingSDKintbool(robot *Robot, setting string, value string) error {
	if i, err := strconv.Atoi(value); err == nil {
		return updateSettings(robot, map[string]interface{}{setting: i})
	}
	if b, err := strconv.ParseBool(value); err == nil {
		return updateSettings(robot, map[string]interface{}{setting: b})
	}
	return fmt.Errorf("invalid value for %s: %q", setting, value)
}
