package logging

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseRetentionDays(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    int
		wantErr bool
	}{
		{name: "default", value: "", want: 7},
		{name: "five days", value: "5", want: 5},
		{name: "seven days", value: "7", want: 7},
		{name: "zero", value: "0", wantErr: true},
		{name: "negative", value: "-1", wantErr: true},
		{name: "not a number", value: "week", wantErr: true},
		{name: "duration overflow", value: "1000000000", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseRetentionDays(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseRetentionDays(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Fatalf("ParseRetentionDays(%q) = %d, want %d", tt.value, got, tt.want)
			}
		})
	}
}

func TestFanoutHandlerWritesToEveryHandler(t *testing.T) {
	var textOutput bytes.Buffer
	var jsonOutput bytes.Buffer
	logger := slog.New(NewFanoutHandler(
		slog.NewTextHandler(&textOutput, nil),
		slog.NewJSONHandler(&jsonOutput, nil),
	))

	logger.InfoContext(context.Background(), "test event", "account", "asmadey")

	for name, output := range map[string]string{
		"text": textOutput.String(),
		"json": jsonOutput.String(),
	} {
		if !strings.Contains(output, "test event") || !strings.Contains(output, "asmadey") {
			t.Fatalf("%s output = %q, want message and account", name, output)
		}
	}
}

func TestRotatingWriterRotatesAtUTCDayBoundary(t *testing.T) {
	now := time.Date(2026, 9, 15, 23, 59, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "log.json")
	writer, err := newRotatingWriter(path, 7, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("day-one\n")); err != nil {
		t.Fatal(err)
	}

	now = time.Date(2026, 9, 16, 0, 1, 0, 0, time.UTC)
	if _, err := writer.Write([]byte("day-two\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	assertFileContent(t, filepath.Join(filepath.Dir(path), "log-2026-09-15.json"), "day-one\n")
	assertFileContent(t, path, "day-two\n")
}

func TestRotatingWriterArchivesStaleCurrentLogOnStartup(t *testing.T) {
	now := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "log.json")
	if err := os.WriteFile(path, []byte("previous-day\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	previousDay := time.Date(2026, 9, 15, 22, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, previousDay, previousDay); err != nil {
		t.Fatal(err)
	}

	writer, err := newRotatingWriter(path, 7, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	assertFileContent(t, filepath.Join(filepath.Dir(path), "log-2026-09-15.json"), "previous-day\n")
	assertFileContent(t, path, "")
}

func TestRotatingWriterDeletesOnlyExpiredArchives(t *testing.T) {
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	oldArchive := filepath.Join(dir, "log-2026-09-01.json")
	recentArchive := filepath.Join(dir, "log-2026-09-14.json")
	unrelated := filepath.Join(dir, "important.json")
	for _, path := range []string{oldArchive, recentArchive, unrelated} {
		if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chtimes(oldArchive, now, now); err != nil {
		t.Fatal(err)
	}
	oldModificationTime := now.Add(-30 * 24 * time.Hour)
	if err := os.Chtimes(recentArchive, oldModificationTime, oldModificationTime); err != nil {
		t.Fatal(err)
	}

	writer, err := newRotatingWriter(filepath.Join(dir, "log.json"), 7, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(oldArchive); !os.IsNotExist(err) {
		t.Fatalf("expired archive still exists: %v", err)
	}
	for _, path := range []string{recentArchive, unrelated} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("preserved file %q: %v", path, err)
		}
	}
}

func TestRotatingWriterKeepsWritingAfterRotationFailure(t *testing.T) {
	now := time.Date(2026, 9, 15, 23, 59, 0, 0, time.UTC)
	dir := t.TempDir()
	path := filepath.Join(dir, "log.json")
	writer, err := newRotatingWriter(path, 7, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("before-failure\n")); err != nil {
		t.Fatal(err)
	}

	writer.rename = func(_, _ string) error { return errors.New("injected rename failure") }
	now = time.Date(2026, 9, 16, 0, 1, 0, 0, time.UTC)
	if _, err := writer.Write([]byte("during-failure\n")); err == nil {
		t.Fatal("Write() error = nil, want rotation error")
	}
	writer.rename = os.Rename
	if _, err := writer.Write([]byte("after-recovery\n")); err != nil {
		t.Fatalf("Write() after rotation failure error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	assertFileContent(t, filepath.Join(dir, "log-2026-09-15.json"), "before-failure\nduring-failure\n")
	assertFileContent(t, path, "after-recovery\n")
}

func TestRotatingWriterPreservesBothLogsWhenArchiveAlreadyExists(t *testing.T) {
	now := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	dir := t.TempDir()
	path := filepath.Join(dir, "log.json")
	archivePath := filepath.Join(dir, "log-2026-09-15.json")
	if err := os.WriteFile(archivePath, []byte("existing-archive\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("stale-current\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	previousDay := time.Date(2026, 9, 15, 22, 0, 0, 0, time.UTC)
	if err := os.Chtimes(path, previousDay, previousDay); err != nil {
		t.Fatal(err)
	}

	writer, err := newRotatingWriter(path, 7, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	assertFileContent(t, archivePath, "existing-archive\n")
	assertFileContent(t, filepath.Join(dir, "log-2026-09-15-2.json"), "stale-current\n")
}

func TestRotatingWriterSupportsConcurrentWrites(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log.json")
	writer, err := newRotatingWriter(path, 7, func() time.Time {
		return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	})
	if err != nil {
		t.Fatal(err)
	}

	var workers sync.WaitGroup
	for i := range 100 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := fmt.Fprintf(writer, "line-%d\n", i); err != nil {
				t.Errorf("Write() error = %v", err)
			}
		}()
	}
	workers.Wait()
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	lines := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if lines != 100 {
		t.Fatalf("line count = %d, want 100", lines)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != want {
		t.Fatalf("file %q = %q, want %q", path, got, want)
	}
}
