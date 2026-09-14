package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadValidConfig(t *testing.T) {
	path := writeConfig(t, `{
		"channels": [
			{"name": "shroud", "max_recording_minutes": 180, "quality": "1080"},
			{"name": "xqc", "max_recording_minutes": 0}
		],
		"output_dir": "./recordings",
		"check_interval_seconds": 30,
		"chunk_duration_minutes": 5
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(cfg.Channels) != 2 {
		t.Fatalf("len(Channels) = %d, want 2", len(cfg.Channels))
	}
	if cfg.Channels[0].Name != "shroud" || cfg.Channels[0].MaxRecordingMinutes != 180 {
		t.Fatalf("Channels[0] = %+v", cfg.Channels[0])
	}
	if cfg.Channels[0].Quality != Quality1080 {
		t.Fatalf("Channels[0].Quality = %q, want %q", cfg.Channels[0].Quality, Quality1080)
	}
	if cfg.Channels[1].MaxRecordingMinutes != 0 {
		t.Fatalf("zero recording limit was not preserved: %+v", cfg.Channels[1])
	}
	if cfg.Channels[1].Quality != QualityBest {
		t.Fatalf("Channels[1].Quality = %q, want default %q", cfg.Channels[1].Quality, QualityBest)
	}
	if cfg.OutputDir != "./recordings" || cfg.CheckIntervalSeconds != 30 || cfg.ChunkDurationMinutes != 5 {
		t.Fatalf("unexpected config: %+v", cfg)
	}
}

func TestLoadDefaultsEmptyQualityToBest(t *testing.T) {
	path := writeConfig(t, `{
		"channels": [{"name": "shroud", "max_recording_minutes": 0, "quality": ""}],
		"output_dir": "./recordings",
		"check_interval_seconds": 30,
		"chunk_duration_minutes": 5
	}`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got := cfg.Channels[0].Quality; got != QualityBest {
		t.Fatalf("Quality = %q, want %q", got, QualityBest)
	}
}

func TestLoadRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{"unknown field", `{"channels":[{"name":"a","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1,"extra":true}`, "unknown field"},
		{"trailing data", `{"channels":[{"name":"a","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1} {}`, "trailing"},
		{"no channels", `{"channels":[],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "at least one channel"},
		{"empty channel", `{"channels":[{"name":"","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "channel name"},
		{"unsafe channel", `{"channels":[{"name":"../x","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "channel name"},
		{"duplicate channel case insensitive", `{"channels":[{"name":"Shroud","max_recording_minutes":0},{"name":"shroud","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "duplicate"},
		{"negative recording limit", `{"channels":[{"name":"a","max_recording_minutes":-1}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "max_recording_minutes"},
		{"unsupported quality", `{"channels":[{"name":"a","max_recording_minutes":0,"quality":"1440"}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "quality"},
		{"numeric quality", `{"channels":[{"name":"a","max_recording_minutes":0,"quality":720}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "quality"},
		{"null quality", `{"channels":[{"name":"a","max_recording_minutes":0,"quality":null}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":1}`, "quality must be a string"},
		{"empty output dir", `{"channels":[{"name":"a","max_recording_minutes":0}],"output_dir":"","check_interval_seconds":1,"chunk_duration_minutes":1}`, "output_dir"},
		{"zero check interval", `{"channels":[{"name":"a","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":0,"chunk_duration_minutes":1}`, "check_interval_seconds"},
		{"zero chunk duration", `{"channels":[{"name":"a","max_recording_minutes":0}],"output_dir":"x","check_interval_seconds":1,"chunk_duration_minutes":0}`, "chunk_duration_minutes"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tt.content))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Load() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
