package recorder

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sibstark/stream-loader/internal/config"
)

func TestRecordPipesStreamlinkIntoSegmentedFFmpeg(t *testing.T) {
	tmp := t.TempDir()
	streamlinkArgs := filepath.Join(tmp, "streamlink.args")
	ffmpegArgs := filepath.Join(tmp, "ffmpeg.args")
	t.Setenv("STREAMLINK_ARGS_FILE", streamlinkArgs)
	t.Setenv("FFMPEG_ARGS_FILE", ffmpegArgs)

	streamlink := writeExecutable(t, tmp, "streamlink", `#!/bin/sh
printf '%s\n' "$@" > "$STREAMLINK_ARGS_FILE"
printf 'fake transport stream'
`)
	ffmpeg := writeExecutable(t, tmp, "ffmpeg", `#!/bin/sh
printf '%s\n' "$@" > "$FFMPEG_ARGS_FILE"
cat >/dev/null
`)

	sessionDir := filepath.Join(tmp, "recordings", "shroud", "2026-09-14_20-30-00")
	if err := os.MkdirAll(sessionDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sessionDir, "00003.ts"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	result := NewRunner(streamlink, ffmpeg).Record(context.Background(), Request{
		Channel:       "shroud",
		SessionDir:    sessionDir,
		ChunkDuration: 5 * time.Minute,
	})
	if result.StreamlinkErr != nil || result.FFmpegErr != nil {
		t.Fatalf("Record() result = %+v", result)
	}

	wantStreamlink := []string{
		"--stdout", "--loglevel", "error", "--progress", "no",
		"https://www.twitch.tv/shroud", "best",
	}
	if got := readLines(t, streamlinkArgs); !reflect.DeepEqual(got, wantStreamlink) {
		t.Fatalf("streamlink args = %q, want %q", got, wantStreamlink)
	}
	wantFFmpeg := []string{
		"-nostdin", "-hide_banner", "-loglevel", "error",
		"-i", "pipe:0",
		"-map", "0:v?", "-map", "0:a?", "-c", "copy",
		"-f", "segment", "-segment_format", "mpegts",
		"-segment_time", "300", "-reset_timestamps", "1",
		"-segment_start_number", "4",
		filepath.Join(sessionDir, "%05d.ts"),
	}
	if got := readLines(t, ffmpegArgs); !reflect.DeepEqual(got, wantFFmpeg) {
		t.Fatalf("ffmpeg args = %q, want %q", got, wantFFmpeg)
	}
}

func TestRecordSelectsConfiguredQuality(t *testing.T) {
	tests := []struct {
		name    string
		quality config.Quality
		limit   string
	}{
		{name: "1080", quality: config.Quality1080, limit: ">1080p60"},
		{name: "720", quality: config.Quality720, limit: ">720p60"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			streamlinkArgs := filepath.Join(tmp, "streamlink.args")
			t.Setenv("STREAMLINK_ARGS_FILE", streamlinkArgs)
			streamlink := writeExecutable(t, tmp, "streamlink", `#!/bin/sh
printf '%s\n' "$@" > "$STREAMLINK_ARGS_FILE"
printf 'fake transport stream'
`)
			ffmpeg := writeExecutable(t, tmp, "ffmpeg", "#!/bin/sh\ncat >/dev/null\n")

			result := NewRunner(streamlink, ffmpeg).Record(context.Background(), Request{
				Channel:       "shroud",
				Quality:       tt.quality,
				SessionDir:    filepath.Join(tmp, "session"),
				ChunkDuration: time.Minute,
			})
			if result.Err != nil || result.StreamlinkErr != nil || result.FFmpegErr != nil {
				t.Fatalf("Record() result = %+v", result)
			}

			want := []string{
				"--stdout", "--loglevel", "error", "--progress", "no",
				"--stream-sorting-excludes", tt.limit,
				"https://www.twitch.tv/shroud", "best,best-unfiltered",
			}
			if got := readLines(t, streamlinkArgs); !reflect.DeepEqual(got, want) {
				t.Fatalf("streamlink args = %q, want %q", got, want)
			}
		})
	}
}

func TestRecordReportsStreamlinkAndFFmpegErrorsSeparately(t *testing.T) {
	tests := []struct {
		name          string
		streamlink    string
		ffmpeg        string
		wantStreamErr bool
		wantFFmpegErr bool
		wantDetail    string
	}{
		{
			name:          "streamlink fails",
			streamlink:    "#!/bin/sh\necho 'streamlink broke' >&2\nexit 7\n",
			ffmpeg:        "#!/bin/sh\ncat >/dev/null\n",
			wantStreamErr: true,
			wantDetail:    "streamlink broke",
		},
		{
			name:          "ffmpeg fails",
			streamlink:    "#!/bin/sh\nprintf 'data'\n",
			ffmpeg:        "#!/bin/sh\ncat >/dev/null\necho 'ffmpeg broke' >&2\nexit 8\n",
			wantFFmpegErr: true,
			wantDetail:    "ffmpeg broke",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmp := t.TempDir()
			result := NewRunner(
				writeExecutable(t, tmp, "streamlink", tt.streamlink),
				writeExecutable(t, tmp, "ffmpeg", tt.ffmpeg),
			).Record(context.Background(), Request{
				Channel:       "channel",
				SessionDir:    filepath.Join(tmp, "session"),
				ChunkDuration: time.Minute,
			})

			if (result.StreamlinkErr != nil) != tt.wantStreamErr {
				t.Fatalf("StreamlinkErr = %v, wanted error: %v", result.StreamlinkErr, tt.wantStreamErr)
			}
			if (result.FFmpegErr != nil) != tt.wantFFmpegErr {
				t.Fatalf("FFmpegErr = %v, wanted error: %v", result.FFmpegErr, tt.wantFFmpegErr)
			}
			combined := resultErrorText(result)
			if !strings.Contains(combined, tt.wantDetail) {
				t.Fatalf("errors = %q, want detail %q", combined, tt.wantDetail)
			}
		})
	}
}

func TestRecordStopsBothProcessesWhenContextIsCanceled(t *testing.T) {
	tmp := t.TempDir()
	streamlink := writeExecutable(t, tmp, "streamlink", "#!/bin/sh\nwhile :; do printf x; done\n")
	ffmpeg := writeExecutable(t, tmp, "ffmpeg", "#!/bin/sh\nwhile IFS= read -r line; do :; done\n")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	started := time.Now()
	result := NewRunner(streamlink, ffmpeg).Record(ctx, Request{
		Channel:       "channel",
		SessionDir:    filepath.Join(tmp, "session"),
		ChunkDuration: time.Minute,
	})
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Record() returned after %v, want prompt cancellation", elapsed)
	}
	if result.StreamlinkErr == nil && result.FFmpegErr == nil {
		t.Fatalf("Record() result = %+v, want canceled process error", result)
	}
}

func writeExecutable(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSpace(string(data)), "\n")
}

func resultErrorText(result Result) string {
	var parts []string
	if result.StreamlinkErr != nil {
		parts = append(parts, result.StreamlinkErr.Error())
	}
	if result.FFmpegErr != nil {
		parts = append(parts, result.FFmpegErr.Error())
	}
	return strings.Join(parts, " ")
}
