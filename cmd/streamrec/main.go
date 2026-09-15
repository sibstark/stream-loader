package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/sibstark/stream-loader/internal/config"
	streamlogging "github.com/sibstark/stream-loader/internal/logging"
	"github.com/sibstark/stream-loader/internal/monitor"
	"github.com/sibstark/stream-loader/internal/recorder"
	"github.com/sibstark/stream-loader/internal/twitch"
)

const (
	configPath      = "config.json"
	logPath         = "logs/log.json"
	logRetentionEnv = "LOG_RETENTION_DAYS"
)

type binaryPaths struct {
	streamlink string
	ffmpeg     string
}

type runtimeSettings struct {
	LogPath          string
	LogRetentionDays int
}

func main() {
	os.Exit(mainExitCode())
}

func mainExitCode() int {
	consoleHandler := slog.NewTextHandler(os.Stderr, nil)
	bootstrapLogger := slog.New(consoleHandler)
	retentionDays, err := streamlogging.ParseRetentionDays(os.Getenv(logRetentionEnv))
	if err != nil {
		bootstrapLogger.Error("application failed", "error", err)
		return 1
	}
	logWriter, err := streamlogging.NewRotatingWriter(logPath, retentionDays)
	if err != nil {
		bootstrapLogger.Error("application failed", "error", err)
		return 1
	}
	logger := slog.New(streamlogging.NewFanoutHandler(
		consoleHandler,
		slog.NewJSONHandler(logWriter, nil),
	))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	exitCode := 0
	if err := run(
		ctx,
		logger,
		configPath,
		exec.LookPath,
		runtimeSettings{LogPath: logPath, LogRetentionDays: retentionDays},
	); err != nil {
		logger.Error("application failed", "error", err)
		exitCode = 1
	}
	if err := logWriter.Close(); err != nil {
		bootstrapLogger.Error("close log file", "error", err)
		return 1
	}
	return exitCode
}

func run(
	ctx context.Context,
	logger *slog.Logger,
	path string,
	lookPath func(string) (string, error),
	settings runtimeSettings,
) error {
	logger.Info("application starting", "config", path)

	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	logger.Info("Настройки загружены",
		"channels", cfg.Channels,
		"output_dir", cfg.OutputDir,
		"check_interval_seconds", cfg.CheckIntervalSeconds,
		"chunk_duration_minutes", cfg.ChunkDurationMinutes,
		"log_path", settings.LogPath,
		"log_retention_days", settings.LogRetentionDays,
	)
	binaries, err := findBinaries(lookPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return fmt.Errorf("create output directory %q: %w", cfg.OutputDir, err)
	}

	checker := twitch.NewClient(binaries.streamlink)
	streamRecorder := recorder.NewRunner(binaries.streamlink, binaries.ffmpeg)
	monitor.New(cfg, checker, streamRecorder, logger).Run(ctx)

	logger.Info("application stopped")
	return nil
}

func findBinaries(lookPath func(string) (string, error)) (binaryPaths, error) {
	streamlinkPath, err := lookPath("streamlink")
	if err != nil {
		return binaryPaths{}, fmt.Errorf("required executable streamlink was not found in PATH: %w", err)
	}
	ffmpegPath, err := lookPath("ffmpeg")
	if err != nil {
		return binaryPaths{}, fmt.Errorf("required executable ffmpeg was not found in PATH: %w", err)
	}
	return binaryPaths{streamlink: streamlinkPath, ffmpeg: ffmpegPath}, nil
}
