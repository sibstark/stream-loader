package monitor

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/sibstark/stream-loader/internal/config"
	"github.com/sibstark/stream-loader/internal/recorder"
)

const defaultRetryDelay = 5 * time.Second

type StatusChecker interface {
	IsOnline(ctx context.Context, channel string) (bool, error)
}

type StreamRecorder interface {
	Record(ctx context.Context, request recorder.Request) recorder.Result
}

type Monitor struct {
	config       config.Config
	checker      StatusChecker
	recorder     StreamRecorder
	logger       *slog.Logger
	intervalUnit time.Duration
	durationUnit time.Duration
	retryDelay   time.Duration
	now          func() time.Time
}

func New(cfg config.Config, checker StatusChecker, streamRecorder StreamRecorder, logger *slog.Logger) *Monitor {
	if logger == nil {
		logger = slog.Default()
	}
	return &Monitor{
		config:       cfg,
		checker:      checker,
		recorder:     streamRecorder,
		logger:       logger,
		intervalUnit: time.Second,
		durationUnit: time.Minute,
		retryDelay:   defaultRetryDelay,
		now:          time.Now,
	}
}

func (m *Monitor) Run(ctx context.Context) {
	var workers sync.WaitGroup
	workers.Add(len(m.config.Channels))
	for _, channel := range m.config.Channels {
		channel := channel
		go func() {
			defer workers.Done()
			m.runChannel(ctx, channel)
		}()
	}
	workers.Wait()
}

type streamSession struct {
	ctx          context.Context
	cancel       context.CancelFunc
	dir          string
	limited      bool
	offlineEnded bool
	attempts     int
}

func (m *Monitor) runChannel(ctx context.Context, channel config.Channel) {
	ticker := time.NewTicker(time.Duration(m.config.CheckIntervalSeconds) * m.intervalUnit)
	defer ticker.Stop()

	var known bool
	var online bool
	var recording bool
	var session *streamSession
	var sessionDone <-chan struct{}
	var recordDone <-chan recorder.Result
	var retryTimer *time.Timer
	var retryC <-chan time.Time

	stopRetry := func() {
		if retryTimer != nil {
			if !retryTimer.Stop() {
				select {
				case <-retryTimer.C:
				default:
				}
			}
		}
		retryTimer = nil
		retryC = nil
	}

	clearSession := func() {
		if session != nil {
			session.cancel()
		}
		session = nil
		sessionDone = nil
		stopRetry()
	}

	markLimitReached := func() bool {
		if session == nil || !errors.Is(session.ctx.Err(), context.DeadlineExceeded) {
			return session != nil && session.limited
		}
		if !session.limited {
			session.limited = true
			stopRetry()
			m.logger.Info("maximum recording duration reached",
				"channel", channel.Name,
				"max_recording_minutes", channel.MaxRecordingMinutes,
			)
		}
		return true
	}

	startAttempt := func() {
		resultCh := make(chan recorder.Result, 1)
		recordDone = resultCh
		recording = true
		session.attempts++
		attempt := session.attempts
		quality := channel.Quality
		if quality == "" {
			quality = config.QualityBest
		}
		request := recorder.Request{
			Channel:       channel.Name,
			Quality:       quality,
			SessionDir:    session.dir,
			ChunkDuration: time.Duration(m.config.ChunkDurationMinutes) * time.Minute,
		}
		if attempt == 1 {
			m.logger.Info("recording started", "channel", channel.Name, "quality", quality, "path", session.dir)
		} else {
			m.logger.Info("retrying recording", "channel", channel.Name, "quality", quality, "attempt", attempt, "path", session.dir)
		}
		m.logger.Info("Идет запись", "account", channel.Name, "quality", quality, "path", session.dir)
		go func(sessionCtx context.Context) {
			resultCh <- m.recorder.Record(sessionCtx, request)
		}(session.ctx)
	}

	startSession := func() {
		startedAt := m.now()
		var sessionCtx context.Context
		var cancel context.CancelFunc
		if channel.MaxRecordingMinutes > 0 {
			sessionCtx, cancel = context.WithTimeout(ctx, time.Duration(channel.MaxRecordingMinutes)*m.durationUnit)
		} else {
			sessionCtx, cancel = context.WithCancel(ctx)
		}
		session = &streamSession{
			ctx:    sessionCtx,
			cancel: cancel,
			dir: filepath.Join(
				m.config.OutputDir,
				channel.Name,
				startedAt.Format("2006-01-02_15-04-05"),
			),
		}
		sessionDone = sessionCtx.Done()
		startAttempt()
	}

	reconcile := func() {
		if !known || !online || recording || retryC != nil {
			return
		}
		if markLimitReached() {
			return
		}
		if session == nil {
			startSession()
			return
		}
		if session.offlineEnded {
			clearSession()
			startSession()
			return
		}
		if !session.limited {
			startAttempt()
		}
	}

	setStatus := func(isOnline bool) {
		changed := !known || online != isOnline
		known = true
		online = isOnline
		if changed {
			if online {
				m.logger.Info("channel online", "channel", channel.Name)
			} else {
				m.logger.Info("channel offline", "channel", channel.Name)
			}
		}
		if !online && session != nil {
			session.offlineEnded = true
			session.cancel()
			stopRetry()
			if !recording {
				clearSession()
			}
		}
	}

	checkStatus := func() bool {
		m.logger.Info("Проверяем online", "account", channel.Name)
		isOnline, err := m.checker.IsOnline(ctx, channel.Name)
		if err != nil {
			if ctx.Err() == nil {
				m.logger.Error("streamlink status check failed", "channel", channel.Name, "error", err)
			}
			return false
		}
		if !isOnline {
			m.logger.Info(channel.Name+" не онлайн", "account", channel.Name)
		} else if recording && session != nil {
			quality := channel.Quality
			if quality == "" {
				quality = config.QualityBest
			}
			m.logger.Info("Идет запись", "account", channel.Name, "quality", quality, "path", session.dir)
		}
		setStatus(isOnline)
		return true
	}

	scheduleRetry := func() {
		stopRetry()
		retryTimer = time.NewTimer(m.retryDelay)
		retryC = retryTimer.C
		m.logger.Info("recording retry scheduled", "channel", channel.Name, "delay", m.retryDelay)
	}

	if checkStatus() {
		reconcile()
	}

	for {
		select {
		case <-ctx.Done():
			stopRetry()
			if session != nil {
				session.cancel()
			}
			if recording {
				<-recordDone
			}
			return

		case <-ticker.C:
			if checkStatus() {
				reconcile()
			}

		case <-retryC:
			retryTimer = nil
			retryC = nil
			if checkStatus() {
				reconcile()
			} else if session != nil && online && !recording && !markLimitReached() {
				scheduleRetry()
			}

		case <-sessionDone:
			sessionDone = nil
			markLimitReached()

		case result := <-recordDone:
			recordDone = nil
			recording = false
			markLimitReached()
			expectedStop := ctx.Err() != nil || session == nil || session.limited || session.offlineEnded || session.ctx.Err() != nil
			if !expectedStop {
				if result.Err != nil {
					m.logger.Error("recorder failed", "channel", channel.Name, "error", result.Err)
				}
				if result.StreamlinkErr != nil {
					m.logger.Error("streamlink failed", "channel", channel.Name, "error", result.StreamlinkErr)
				}
				if result.FFmpegErr != nil {
					m.logger.Error("ffmpeg failed", "channel", channel.Name, "error", result.FFmpegErr)
				}
			}

			reason := "stream ended"
			if session != nil && session.limited {
				reason = "maximum duration reached"
			} else if session != nil && session.offlineEnded {
				reason = "channel offline"
			} else if result.Err != nil || result.StreamlinkErr != nil || result.FFmpegErr != nil {
				reason = "recorder error"
			}
			m.logger.Info("recording stopped", "channel", channel.Name, "reason", reason)

			if session != nil && session.offlineEnded {
				clearSession()
				reconcile()
				continue
			}
			if session != nil && !session.limited && online && ctx.Err() == nil {
				scheduleRetry()
			}
		}
	}
}
