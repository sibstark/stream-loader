package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type Quality string

const (
	QualityBest Quality = "best"
	Quality1080 Quality = "1080"
	Quality720  Quality = "720"
)

func (q *Quality) UnmarshalJSON(data []byte) error {
	var value *string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("quality must be a string: %w", err)
	}
	if value == nil {
		return errors.New("quality must be a string, not null")
	}
	*q = Quality(*value)
	return nil
}

type Channel struct {
	Name                string  `json:"name"`
	MaxRecordingMinutes int     `json:"max_recording_minutes"`
	Quality             Quality `json:"quality"`
}

type Config struct {
	Channels             []Channel `json:"channels"`
	OutputDir            string    `json:"output_dir"`
	CheckIntervalSeconds int       `json:"check_interval_seconds"`
	ChunkDurationMinutes int       `json:"chunk_duration_minutes"`
}

func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config %q: %w", path, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()

	var cfg Config
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", path, err)
	}
	for i := range cfg.Channels {
		if cfg.Channels[i].Quality == "" {
			cfg.Channels[i].Quality = QualityBest
		}
	}

	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return Config{}, fmt.Errorf("decode config %q: trailing JSON data", path)
		}
		return Config{}, fmt.Errorf("decode config %q: trailing data: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}
	return cfg, nil
}

func (cfg Config) validate() error {
	if len(cfg.Channels) == 0 {
		return errors.New("at least one channel is required")
	}

	seen := make(map[string]struct{}, len(cfg.Channels))
	for i, channel := range cfg.Channels {
		if !validChannelName(channel.Name) {
			return fmt.Errorf("channel name at index %d must contain only ASCII letters, digits, or underscores", i)
		}
		key := strings.ToLower(channel.Name)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate channel %q", channel.Name)
		}
		seen[key] = struct{}{}

		if err := validateMinutes("max_recording_minutes", channel.MaxRecordingMinutes, true); err != nil {
			return fmt.Errorf("channel %q: %w", channel.Name, err)
		}
		switch channel.Quality {
		case QualityBest, Quality1080, Quality720:
		default:
			return fmt.Errorf("channel %q: quality must be one of %q, %q, or %q", channel.Name, QualityBest, Quality1080, Quality720)
		}
	}

	if strings.TrimSpace(cfg.OutputDir) == "" {
		return errors.New("output_dir must not be empty")
	}
	if cfg.CheckIntervalSeconds <= 0 {
		return errors.New("check_interval_seconds must be greater than zero")
	}
	if time.Duration(cfg.CheckIntervalSeconds) > time.Duration(1<<63-1)/time.Second {
		return errors.New("check_interval_seconds is too large")
	}
	if err := validateMinutes("chunk_duration_minutes", cfg.ChunkDurationMinutes, false); err != nil {
		return err
	}
	return nil
}

func validChannelName(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			continue
		}
		return false
	}
	return true
}

func validateMinutes(field string, value int, zeroAllowed bool) error {
	if value < 0 || (!zeroAllowed && value == 0) {
		if zeroAllowed {
			return fmt.Errorf("%s must not be negative", field)
		}
		return fmt.Errorf("%s must be greater than zero", field)
	}
	if time.Duration(value) > time.Duration(1<<63-1)/time.Minute {
		return fmt.Errorf("%s is too large", field)
	}
	return nil
}
