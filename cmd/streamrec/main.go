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
	"github.com/sibstark/stream-loader/internal/monitor"
	"github.com/sibstark/stream-loader/internal/recorder"
	"github.com/sibstark/stream-loader/internal/twitch"
)

const configPath = "config.json"

type binaryPaths struct {
	streamlink string
	ffmpeg     string
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger, configPath, exec.LookPath); err != nil {
		logger.Error("application failed", "error", err)
		os.Exit(1)
	}
}

func run(
	ctx context.Context,
	logger *slog.Logger,
	path string,
	lookPath func(string) (string, error),
) error {
	logger.Info("application starting", "config", path)

	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
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
