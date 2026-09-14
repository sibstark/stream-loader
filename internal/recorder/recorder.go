package recorder

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/sibstark/stream-loader/internal/config"
)

type Request struct {
	Channel       string
	Quality       config.Quality
	SessionDir    string
	ChunkDuration time.Duration
}

type Result struct {
	Err           error
	StreamlinkErr error
	FFmpegErr     error
}

type Runner struct {
	streamlinkPath string
	ffmpegPath     string
}

func NewRunner(streamlinkPath, ffmpegPath string) *Runner {
	return &Runner{streamlinkPath: streamlinkPath, ffmpegPath: ffmpegPath}
}

func (r *Runner) Record(ctx context.Context, request Request) Result {
	if request.ChunkDuration <= 0 {
		return Result{Err: fmt.Errorf("chunk duration must be greater than zero")}
	}
	streamlinkArgs, err := buildStreamlinkArgs(request)
	if err != nil {
		return Result{Err: err}
	}
	if err := os.MkdirAll(request.SessionDir, 0o755); err != nil {
		return Result{Err: fmt.Errorf("create session directory %q: %w", request.SessionDir, err)}
	}

	startNumber, err := nextSegmentNumber(request.SessionDir)
	if err != nil {
		return Result{Err: err}
	}

	streamlinkCtx, cancelStreamlink := context.WithCancel(ctx)
	defer cancelStreamlink()
	ffmpegCtx, cancelFFmpeg := context.WithCancel(ctx)
	defer cancelFFmpeg()

	streamlinkCmd := exec.CommandContext(streamlinkCtx, r.streamlinkPath, streamlinkArgs...)
	var streamlinkStderr bytes.Buffer
	streamlinkCmd.Stderr = &streamlinkStderr
	stream, err := streamlinkCmd.StdoutPipe()
	if err != nil {
		return Result{StreamlinkErr: fmt.Errorf("create streamlink output pipe: %w", err)}
	}

	segmentSeconds := int64(request.ChunkDuration / time.Second)
	outputPattern := filepath.Join(request.SessionDir, "%05d.ts")
	ffmpegCmd := exec.CommandContext(ffmpegCtx, r.ffmpegPath,
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-i", "pipe:0",
		"-map", "0:v?",
		"-map", "0:a?",
		"-c", "copy",
		"-f", "segment",
		"-segment_format", "mpegts",
		"-segment_time", strconv.FormatInt(segmentSeconds, 10),
		"-reset_timestamps", "1",
		"-segment_start_number", strconv.Itoa(startNumber),
		outputPattern,
	)
	ffmpegCmd.Stdin = stream
	var ffmpegStderr bytes.Buffer
	ffmpegCmd.Stderr = &ffmpegStderr

	if err := ffmpegCmd.Start(); err != nil {
		return Result{FFmpegErr: withDetails("start ffmpeg", err, ffmpegStderr.String())}
	}
	if err := streamlinkCmd.Start(); err != nil {
		cancelFFmpeg()
		_ = ffmpegCmd.Wait()
		return Result{StreamlinkErr: withDetails("start streamlink", err, streamlinkStderr.String())}
	}

	streamlinkDone := make(chan error, 1)
	ffmpegDone := make(chan error, 1)
	go func() { streamlinkDone <- streamlinkCmd.Wait() }()
	go func() { ffmpegDone <- ffmpegCmd.Wait() }()

	var streamlinkErr error
	var ffmpegErr error
	select {
	case streamlinkErr = <-streamlinkDone:
		ffmpegErr = <-ffmpegDone
	case ffmpegErr = <-ffmpegDone:
		_ = stream.Close()
		cancelStreamlink()
		<-streamlinkDone
	}

	return Result{
		StreamlinkErr: withDetails("streamlink", streamlinkErr, streamlinkStderr.String()),
		FFmpegErr:     withDetails("ffmpeg", ffmpegErr, ffmpegStderr.String()),
	}
}

func buildStreamlinkArgs(request Request) ([]string, error) {
	quality := request.Quality
	if quality == "" {
		quality = config.QualityBest
	}

	args := []string{"--stdout", "--loglevel", "error", "--progress", "no"}
	switch quality {
	case config.QualityBest:
		args = append(args, "https://www.twitch.tv/"+request.Channel, "best")
	case config.Quality1080:
		args = append(args,
			"--stream-sorting-excludes", ">1080p60",
			"https://www.twitch.tv/"+request.Channel,
			"best,best-unfiltered",
		)
	case config.Quality720:
		args = append(args,
			"--stream-sorting-excludes", ">720p60",
			"https://www.twitch.tv/"+request.Channel,
			"best,best-unfiltered",
		)
	default:
		return nil, fmt.Errorf("unsupported recording quality %q", quality)
	}
	return args, nil
}

func nextSegmentNumber(dir string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, fmt.Errorf("read session directory %q: %w", dir, err)
	}

	maxNumber := 0
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".ts" {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".ts")
		number, err := strconv.Atoi(base)
		if err != nil || number < 1 {
			continue
		}
		if number > maxNumber {
			maxNumber = number
		}
	}
	return maxNumber + 1, nil
}

func withDetails(operation string, err error, stderr string) error {
	if err == nil {
		return nil
	}
	details := strings.TrimSpace(stderr)
	if details == "" {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s: %w: %s", operation, err, details)
}
