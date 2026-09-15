package main

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFindBinariesReportsMissingDependency(t *testing.T) {
	_, err := findBinaries(func(name string) (string, error) {
		if name == "streamlink" {
			return "", errors.New("not found")
		}
		return "/usr/bin/" + name, nil
	})
	if err == nil || !strings.Contains(err.Error(), "streamlink") {
		t.Fatalf("findBinaries() error = %v, want missing streamlink error", err)
	}
}

func TestRunLoadsConfigCreatesOutputAndStopsWithContext(t *testing.T) {
	tmp := t.TempDir()
	outputDir := filepath.Join(tmp, "recordings")
	configPath := filepath.Join(tmp, "config.json")
	configJSON := `{
		"channels": [{"name": "offline_channel", "max_recording_minutes": 0}],
		"output_dir": ` + quoteJSON(outputDir) + `,
		"check_interval_seconds": 1,
		"chunk_duration_minutes": 5
	}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	streamlink := writeTestExecutable(t, tmp, "streamlink", "#!/bin/sh\necho 'error: No playable streams found on this URL' >&2\nexit 1\n")
	ffmpeg := writeTestExecutable(t, tmp, "ffmpeg", "#!/bin/sh\nexit 0\n")
	lookPath := func(name string) (string, error) {
		if name == "streamlink" {
			return streamlink, nil
		}
		return ffmpeg, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var logs bytes.Buffer
	err := run(
		ctx,
		slog.New(slog.NewJSONHandler(&logs, nil)),
		configPath,
		lookPath,
		runtimeSettings{LogPath: "logs/log.json", LogRetentionDays: 5},
	)
	if err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if info, err := os.Stat(outputDir); err != nil || !info.IsDir() {
		t.Fatalf("output directory was not created: info=%v err=%v", info, err)
	}
	for _, want := range []string{
		`"msg":"Настройки загружены"`,
		`"name":"offline_channel"`,
		`"max_recording_minutes":0`,
		`"quality":"best"`,
		`"check_interval_seconds":1`,
		`"chunk_duration_minutes":5`,
		`"log_path":"logs/log.json"`,
		`"log_retention_days":5`,
	} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs = %q, want containing %q", logs.String(), want)
		}
	}
}

func quoteJSON(value string) string {
	return `"` + strings.ReplaceAll(value, `\`, `\\`) + `"`
}

func writeTestExecutable(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}
