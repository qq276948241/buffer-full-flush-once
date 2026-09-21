package flushlog

import (
	"os"
	"strconv"
	"sync"
	"time"
)

// RetryPolicy decides what happens to buffered bytes when the outlet fails a
// flush.
type RetryPolicy int

const (
	// DropRemaining drops the bytes that did not reach the outlet after the
	// failed write. No further write is attempted.
	DropRemaining RetryPolicy = iota
	// RetryOnce retries the failed write exactly one more time. If the retry
	// also fails, the buffered bytes are dropped and the error is returned;
	// there is never a third attempt.
	RetryOnce
)

// defaultBufferSize is large enough that a single short log line never needs
// a flush in the middle of an ordinary write.
const defaultBufferSize = 64 * 1024

// Option configures a Logger at build time.
type Option func(*config)

type config struct {
	outlet      Outlet
	format      Format
	bufferSize  int
	syncEvery   bool
	retryPolicy RetryPolicy
	now         func() time.Time
}

// WithOutlet sends output to w. Defaults to os.Stdout. When several loggers
// share one writer, wrap it once with SharedOutlet and pass the result to
// each of them.
func WithOutlet(w Outlet) Option {
	return func(c *config) { c.outlet = w }
}

// WithFormat pins the encoding format. It cannot change after New.
func WithFormat(f Format) Option {
	return func(c *config) { c.format = f }
}

// WithBufferSize sets the flush threshold in bytes. A zero or negative value
// keeps the default.
func WithBufferSize(n int) Option {
	return func(c *config) { c.bufferSize = n }
}

// WithSyncEach makes every Log flush before it returns, so the next record
// never starts while the previous one is still buffered.
func WithSyncEach() Option {
	return func(c *config) { c.syncEvery = true }
}

// WithRetryOnce retries a failed outlet write exactly once instead of
// dropping the remaining bytes immediately.
func WithRetryOnce() Option {
	return func(c *config) { c.retryPolicy = RetryOnce }
}

// withClock overrides the time source; used by tests.
func withClock(now func() time.Time) Option {
	return func(c *config) { c.now = now }
}

// Logger encodes records, buffers the bytes, and flushes them to its outlet.
// A Logger is safe for concurrent use. Its format is fixed for life.
type Logger struct {
	mu          sync.Mutex
	enc         encoder
	out         Outlet
	buf         []byte
	cap         int
	syncEvery   bool
	retryPolicy RetryPolicy
	now         func() time.Time
	closed      bool
	closing     bool
}

// New builds a Logger. The outlet defaults to os.Stdout and the format
// defaults to JSON. The returned Logger must be Closed when done so that any
// bytes still buffered are flushed.
func New(opts ...Option) *Logger {
	c := config{
		outlet:     SharedOutlet(os.Stdout),
		format:     JSONFormat,
		bufferSize: defaultBufferSize,
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(&c)
	}
	if c.bufferSize <= 0 {
		c.bufferSize = defaultBufferSize
	}
	return &Logger{
		enc:         newEncoder(c.format),
		out:         c.outlet,
		buf:         make([]byte, 0, c.bufferSize),
		cap:         c.bufferSize,
		syncEvery:   c.syncEvery,
		retryPolicy: c.retryPolicy,
		now:         c.now,
	}
}

// Log encodes one record and appends it to the buffer. If encoding fails no
// bytes are written at all, so a record can never land half-encoded. If the
// buffer reaches its capacity, or SyncEach is enabled, the buffer is
// flushed. A flush failure is returned: the record is not reported as
// persisted.
func (l *Logger) Log(msg string, fields ...Field) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.closed || l.closing {
		return ClosedError{Closing: l.closing && !l.closed}
	}

	encoded, err := l.enc.appendRecord(nil, l.now(), msg, fields)
	if err != nil {
		return err
	}

	l.buf = append(l.buf, encoded...)

	if l.syncEvery || len(l.buf) >= l.cap {
		return l.flushLocked()
	}
	return nil
}

// Sync flushes every buffered byte to the outlet. When the outlet fails and
// bytes remain, the error is a *RemainingBytesError carrying the outlet's
// original error.
func (l *Logger) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ClosedError{}
	}
	return l.flushLocked()
}

// Close flushes remaining bytes and marks the Logger closed. Any write that
// races an in-progress close fails with ClosedError instead of touching the
// closing outlet. Writes after close also fail.
func (l *Logger) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ClosedError{}
	}
	l.closing = true
	flushErr := l.flushLocked()
	l.closing = false
	l.closed = true
	l.mu.Unlock()
	return flushErr
}

// flushLocked writes the whole buffer to the outlet. On failure it retries
// exactly once when the policy says so; otherwise, or when the retry also
// fails, the remaining bytes are dropped from the buffer and reported. It
// must be called with l.mu held.
func (l *Logger) flushLocked() error {
	if len(l.buf) == 0 {
		return nil
	}

	pending := l.buf
	attempts := 1
	if l.retryPolicy == RetryOnce {
		attempts = 2
	}

	var writeErr error
	for i := 0; i < attempts; i++ {
		n, err := l.out.Write(pending)
		if err != nil {
			writeErr = err
			pending = pending[n:]
			continue
		}
		if n == len(pending) {
			pending = nil
			break
		}
		writeErr = errShortWrite{Written: n, Total: len(pending)}
		pending = pending[n:]
	}

	remaining := len(pending)
	// A successful (or fully delivered) flush leaves the buffer empty so the
	// next record cannot carry a tail from the previous one.
	l.buf = l.buf[:0]
	if remaining > 0 {
		return &RemainingBytesError{Remaining: remaining, Err: writeErr}
	}
	return nil
}

type errShortWrite struct {
	Written int
	Total   int
}

func (e errShortWrite) Error() string {
	return "flushlog: short write: " + strconv.Itoa(e.Written) + " of " + strconv.Itoa(e.Total) + " bytes"
}
