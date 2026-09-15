package logging

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RotatingWriter struct {
	mu            sync.Mutex
	path          string
	dir           string
	archivePrefix string
	archiveSuffix string
	retention     time.Duration
	now           func() time.Time
	rename        func(string, string) error
	day           string
	file          *os.File
	closed        bool
}

func NewRotatingWriter(path string, retentionDays int) (*RotatingWriter, error) {
	return newRotatingWriter(path, retentionDays, time.Now)
}

func newRotatingWriter(path string, retentionDays int, now func() time.Time) (*RotatingWriter, error) {
	if retentionDays <= 0 {
		return nil, fmt.Errorf("retention days must be greater than zero")
	}
	if now == nil {
		return nil, fmt.Errorf("clock must not be nil")
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log directory %q: %w", dir, err)
	}
	ext := filepath.Ext(path)
	writer := &RotatingWriter{
		path:          path,
		dir:           dir,
		archivePrefix: strings.TrimSuffix(filepath.Base(path), ext) + "-",
		archiveSuffix: ext,
		retention:     time.Duration(retentionDays) * 24 * time.Hour,
		now:           now,
		rename:        os.Rename,
	}

	currentTime := now().UTC()
	writer.day = dayKey(currentTime)
	if info, err := os.Stat(path); err == nil && info.Size() > 0 && dayKey(info.ModTime().UTC()) != writer.day {
		if err := writer.archiveCurrent(dayKey(info.ModTime().UTC())); err != nil {
			return nil, err
		}
	} else if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("inspect current log %q: %w", path, err)
	}
	if err := writer.removeExpired(currentTime); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open current log %q: %w", path, err)
	}
	writer.file = file
	return writer, nil
}

func (w *RotatingWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, os.ErrClosed
	}
	if w.file == nil {
		if err := w.openCurrent(w.day); err != nil {
			return 0, err
		}
	}

	currentTime := w.now().UTC()
	currentDay := dayKey(currentTime)
	var rotationErr error
	if currentDay != w.day {
		rotationErr = w.rotate(currentTime, currentDay)
		if w.file == nil {
			return 0, rotationErr
		}
	}
	written, writeErr := w.file.Write(data)
	return written, errors.Join(rotationErr, writeErr)
}

func (w *RotatingWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		w.closed = true
		return nil
	}
	err := w.file.Close()
	w.file = nil
	w.closed = true
	return err
}

func (w *RotatingWriter) rotate(currentTime time.Time, currentDay string) error {
	previousDay := w.day
	if err := w.file.Close(); err != nil {
		w.file = nil
		return errors.Join(
			fmt.Errorf("close current log %q: %w", w.path, err),
			w.openCurrent(previousDay),
		)
	}
	w.file = nil
	if info, err := os.Stat(w.path); err == nil && info.Size() > 0 {
		if err := w.archiveCurrent(w.day); err != nil {
			return errors.Join(err, w.openCurrent(previousDay))
		}
	} else if err != nil && !os.IsNotExist(err) {
		return errors.Join(
			fmt.Errorf("inspect current log %q: %w", w.path, err),
			w.openCurrent(previousDay),
		)
	}
	w.day = currentDay
	cleanupErr := w.removeExpired(currentTime)
	openErr := w.openCurrent(currentDay)
	return errors.Join(cleanupErr, openErr)
}

func (w *RotatingWriter) openCurrent(day string) error {
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open current log %q: %w", w.path, err)
	}
	w.file = file
	w.day = day
	return nil
}

func (w *RotatingWriter) archiveCurrent(day string) error {
	for sequence := 1; ; sequence++ {
		name := w.archivePrefix + day
		if sequence > 1 {
			name += "-" + strconv.Itoa(sequence)
		}
		archivePath := filepath.Join(w.dir, name+w.archiveSuffix)
		if _, err := os.Stat(archivePath); err == nil {
			continue
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("inspect log archive %q: %w", archivePath, err)
		}
		if err := w.rename(w.path, archivePath); err != nil {
			return fmt.Errorf("archive current log as %q: %w", archivePath, err)
		}
		return nil
	}
}

func (w *RotatingWriter) removeExpired(currentTime time.Time) error {
	entries, err := os.ReadDir(w.dir)
	if err != nil {
		return fmt.Errorf("read log directory %q: %w", w.dir, err)
	}
	currentDay := currentTime.UTC()
	cutoff := time.Date(currentDay.Year(), currentDay.Month(), currentDay.Day(), 0, 0, 0, 0, time.UTC).Add(-w.retention)
	for _, entry := range entries {
		archiveDay, ok := w.archiveDay(entry.Name())
		if entry.IsDir() || !ok {
			continue
		}
		if !archiveDay.Before(cutoff) {
			continue
		}
		path := filepath.Join(w.dir, entry.Name())
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove expired log archive %q: %w", path, err)
		}
	}
	return nil
}

func (w *RotatingWriter) archiveDay(name string) (time.Time, bool) {
	if !strings.HasPrefix(name, w.archivePrefix) || !strings.HasSuffix(name, w.archiveSuffix) {
		return time.Time{}, false
	}
	archiveID := strings.TrimSuffix(strings.TrimPrefix(name, w.archivePrefix), w.archiveSuffix)
	if len(archiveID) < len("2006-01-02") {
		return time.Time{}, false
	}
	date := archiveID[:len("2006-01-02")]
	sequence := archiveID[len("2006-01-02"):]
	if sequence != "" {
		value, err := strconv.Atoi(strings.TrimPrefix(sequence, "-"))
		if !strings.HasPrefix(sequence, "-") || err != nil || value < 2 {
			return time.Time{}, false
		}
	}
	day, err := time.Parse("2006-01-02", date)
	return day, err == nil
}

func dayKey(value time.Time) string {
	return value.UTC().Format("2006-01-02")
}
