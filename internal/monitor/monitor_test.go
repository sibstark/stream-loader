package monitor

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sibstark/stream-loader/internal/config"
	"github.com/sibstark/stream-loader/internal/recorder"
)

type fakeChecker struct {
	mu         sync.RWMutex
	online     bool
	err        error
	delay      time.Duration
	checks     chan bool
	probeError chan error
}

func (f *fakeChecker) IsOnline(ctx context.Context, _ string) (bool, error) {
	f.mu.RLock()
	online, err, delay := f.online, f.err, f.delay
	f.mu.RUnlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-timer.C:
		}
	}
	select {
	case f.checks <- online:
	default:
	}
	if err != nil && f.probeError != nil {
		select {
		case f.probeError <- err:
		default:
		}
	}
	return online, err
}

func (f *fakeChecker) set(online bool, err error) {
	f.mu.Lock()
	f.online, f.err = online, err
	f.mu.Unlock()
}

func (f *fakeChecker) setDelay(delay time.Duration) {
	f.mu.Lock()
	f.delay = delay
	f.mu.Unlock()
}

type recorderCall struct {
	request  recorder.Request
	deadline time.Time
}

type fakeRecorder struct {
	mu          sync.Mutex
	calls       []recorderCall
	started     chan recorderCall
	firstResult *recorder.Result
	active      int
	maxActive   int
}

func (f *fakeRecorder) Record(ctx context.Context, request recorder.Request) recorder.Result {
	deadline, _ := ctx.Deadline()
	call := recorderCall{request: request, deadline: deadline}
	f.mu.Lock()
	index := len(f.calls)
	f.calls = append(f.calls, call)
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()
	f.started <- call

	var result recorder.Result
	if index == 0 && f.firstResult != nil {
		result = *f.firstResult
	} else {
		<-ctx.Done()
	}

	f.mu.Lock()
	f.active--
	f.mu.Unlock()
	return result
}

func TestChannelDoesNotStartTwoRecordersConcurrently(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 20)}
	recorderFake := &fakeRecorder{started: make(chan recorderCall, 10)}
	monitor := testMonitor(checker, recorderFake, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()

	waitCall(t, recorderFake.started)
	waitChecks(t, checker.checks, 4)
	recorderFake.mu.Lock()
	callCount, maxActive := len(recorderFake.calls), recorderFake.maxActive
	recorderFake.mu.Unlock()
	if callCount != 1 || maxActive != 1 {
		t.Fatalf("recorder calls = %d, max active = %d; want 1 and 1", callCount, maxActive)
	}

	cancel()
	waitDone(t, done)
}

func TestMonitorPassesChannelQualityToRecorder(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 20)}
	recorderFake := &fakeRecorder{started: make(chan recorderCall, 10)}
	cfg := config.Config{
		Channels: []config.Channel{{
			Name:                "shroud",
			MaxRecordingMinutes: 0,
			Quality:             config.Quality720,
		}},
		OutputDir:            "recordings",
		CheckIntervalSeconds: 1,
		ChunkDurationMinutes: 5,
	}
	m := New(cfg, checker, recorderFake, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.intervalUnit = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()

	call := waitCall(t, recorderFake.started)
	if call.request.Quality != config.Quality720 {
		t.Fatalf("recorder quality = %q, want %q", call.request.Quality, config.Quality720)
	}

	cancel()
	waitDone(t, done)
}

func TestRecordingStartLogIncludesQuality(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 20)}
	recorderFake := &fakeRecorder{started: make(chan recorderCall, 10)}
	cfg := config.Config{
		Channels: []config.Channel{{
			Name:                "shroud",
			MaxRecordingMinutes: 0,
			Quality:             config.Quality1080,
		}},
		OutputDir:            "recordings",
		CheckIntervalSeconds: 1,
		ChunkDurationMinutes: 5,
	}
	var logs bytes.Buffer
	m := New(cfg, checker, recorderFake, slog.New(slog.NewTextHandler(&logs, nil)))
	m.intervalUnit = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()

	waitCall(t, recorderFake.started)
	cancel()
	waitDone(t, done)
	if !strings.Contains(logs.String(), "quality=1080") {
		t.Fatalf("recording log = %q, want quality attribute", logs.String())
	}
}

func TestLimitBlocksRestartUntilOfflineThenOnline(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 50)}
	recorderFake := &fakeRecorder{started: make(chan recorderCall, 10)}
	monitor := testMonitor(checker, recorderFake, 1)
	monitor.durationUnit = 35 * time.Millisecond
	monitor.retryDelay = 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()

	waitCall(t, recorderFake.started)
	time.Sleep(80 * time.Millisecond)
	assertNoCall(t, recorderFake.started, 30*time.Millisecond)

	checker.set(false, nil)
	waitUntilStatus(t, checker.checks, false)
	checker.set(true, nil)
	waitUntilStatus(t, checker.checks, true)
	second := waitCall(t, recorderFake.started)
	if second.request.SessionDir == "" {
		t.Fatal("second recording has empty session directory")
	}

	cancel()
	waitDone(t, done)
}

func TestUnexpectedExitRetriesInSameSessionWithOriginalDeadline(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 50)}
	recorderFake := &fakeRecorder{
		started:     make(chan recorderCall, 10),
		firstResult: &recorder.Result{StreamlinkErr: errors.New("streamlink crashed")},
	}
	monitor := testMonitor(checker, recorderFake, 1)
	monitor.durationUnit = 300 * time.Millisecond
	monitor.retryDelay = 15 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()

	first := waitCall(t, recorderFake.started)
	second := waitCall(t, recorderFake.started)
	if first.request.SessionDir != second.request.SessionDir {
		t.Fatalf("retry session directory = %q, want %q", second.request.SessionDir, first.request.SessionDir)
	}
	if first.deadline.IsZero() || !first.deadline.Equal(second.deadline) {
		t.Fatalf("retry deadline = %v, want original %v", second.deadline, first.deadline)
	}

	cancel()
	waitDone(t, done)
}

func TestFailedRetryProbeDoesNotStartRecorderFromStaleOnlineState(t *testing.T) {
	probeErrors := make(chan error, 10)
	checker := &fakeChecker{online: true, checks: make(chan bool, 50), probeError: probeErrors}
	recorderFake := &fakeRecorder{
		started:     make(chan recorderCall, 10),
		firstResult: &recorder.Result{StreamlinkErr: errors.New("streamlink crashed")},
	}
	monitor := testMonitor(checker, recorderFake, 0)
	monitor.intervalUnit = time.Second
	monitor.retryDelay = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()

	waitCall(t, recorderFake.started)
	checker.set(true, errors.New("temporary probe failure"))
	select {
	case <-probeErrors:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed retry probe")
	}
	assertNoCall(t, recorderFake.started, 10*time.Millisecond)

	checker.set(true, nil)
	waitCall(t, recorderFake.started)
	cancel()
	waitDone(t, done)
}

func TestRetryProbeCrossingDeadlineDoesNotStartExpiredAttempt(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 50)}
	recorderFake := &fakeRecorder{
		started:     make(chan recorderCall, 10),
		firstResult: &recorder.Result{StreamlinkErr: errors.New("streamlink crashed")},
	}
	monitor := testMonitor(checker, recorderFake, 1)
	monitor.intervalUnit = time.Second
	monitor.durationUnit = 50 * time.Millisecond
	monitor.retryDelay = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { monitor.Run(ctx); close(done) }()

	waitCall(t, recorderFake.started)
	checker.setDelay(60 * time.Millisecond)
	assertNoCall(t, recorderFake.started, 120*time.Millisecond)

	cancel()
	waitDone(t, done)
}

type perChannelRecorder struct {
	started chan string
}

func (f *perChannelRecorder) Record(ctx context.Context, request recorder.Request) recorder.Result {
	f.started <- request.Channel
	if request.Channel == "broken" {
		return recorder.Result{Err: errors.New("broken recorder")}
	}
	<-ctx.Done()
	return recorder.Result{}
}

func TestFailureOnOneChannelDoesNotBlockAnother(t *testing.T) {
	checker := &fakeChecker{online: true, checks: make(chan bool, 50)}
	recorderFake := &perChannelRecorder{started: make(chan string, 20)}
	cfg := config.Config{
		Channels: []config.Channel{
			{Name: "broken", MaxRecordingMinutes: 0},
			{Name: "healthy", MaxRecordingMinutes: 0},
		},
		OutputDir: "recordings", CheckIntervalSeconds: 1, ChunkDurationMinutes: 5,
	}
	m := New(cfg, checker, recorderFake, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.intervalUnit = 5 * time.Millisecond
	m.retryDelay = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { m.Run(ctx); close(done) }()

	seen := map[string]bool{}
	deadline := time.After(time.Second)
	for !seen["broken"] || !seen["healthy"] {
		select {
		case channel := <-recorderFake.started:
			seen[channel] = true
		case <-deadline:
			t.Fatalf("started channels = %v, want both channels", seen)
		}
	}

	cancel()
	waitDone(t, done)
}

func testMonitor(checker StatusChecker, recorderFake StreamRecorder, maxMinutes int) *Monitor {
	cfg := config.Config{
		Channels:             []config.Channel{{Name: "shroud", MaxRecordingMinutes: maxMinutes}},
		OutputDir:            "recordings",
		CheckIntervalSeconds: 1,
		ChunkDurationMinutes: 5,
	}
	m := New(cfg, checker, recorderFake, slog.New(slog.NewTextHandler(io.Discard, nil)))
	m.intervalUnit = 5 * time.Millisecond
	m.retryDelay = 10 * time.Millisecond
	return m
}

func waitCall(t *testing.T, calls <-chan recorderCall) recorderCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for recorder call")
		return recorderCall{}
	}
}

func assertNoCall(t *testing.T, calls <-chan recorderCall, duration time.Duration) {
	t.Helper()
	select {
	case call := <-calls:
		t.Fatalf("unexpected recorder call: %+v", call)
	case <-time.After(duration):
	}
}

func waitChecks(t *testing.T, checks <-chan bool, count int) {
	t.Helper()
	for range count {
		waitUntilCheck(t, checks)
	}
}

func waitUntilCheck(t *testing.T, checks <-chan bool) {
	t.Helper()
	select {
	case <-checks:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for status check")
	}
}

func waitUntilStatus(t *testing.T, checks <-chan bool, want bool) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		select {
		case got := <-checks:
			if got == want {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for online=%v status check", want)
		}
	}
}

func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("monitor did not stop")
	}
}
