// Package buffered provides a small logger that encodes structured fields
// into bytes, buffers them, and flushes the buffer to a sink.
//
// The pipeline is a single path: encode fields to bytes, append the bytes
// to the buffer, flush when the buffer is full (or on every entry when
// FlushEvery is set), and surface any error to the caller.
package buffered

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// EmptyMarker is the agreed-upon marker written for keys whose value is
// empty. Readers can distinguish an empty value from a missing key.
const EmptyMarker = "<empty>"

// DefaultBufferSize is used when Options.BufferSize is not positive.
const DefaultBufferSize = 4096

// DefaultTimeFormat is the single time layout used for all time values in
// an entry unless the caller picks another one at construction time.
const DefaultTimeFormat = time.RFC3339

// ErrClosed is returned by writes that race with or follow Close.
var ErrClosed = errors.New("buffered: logger already closed")

// Format selects the encoding. It is fixed at construction time; to use a
// different format, build a new Logger.
type Format int

const (
	// FormatJSON encodes each entry as one JSON object per line.
	FormatJSON Format = iota
	// FormatLogfmt encodes each entry as space-separated key=value pairs.
	FormatLogfmt
)

// FlushError reports a flush that could not deliver all buffered bytes.
// Remaining is the number of bytes left undelivered; Err is the sink's
// original error, passed through unchanged.
type FlushError struct {
	Remaining int
	Err       error
}

func (e *FlushError) Error() string {
	return fmt.Sprintf("buffered: flush failed with %d bytes remaining: %v", e.Remaining, e.Err)
}

func (e *FlushError) Unwrap() error { return e.Err }

// Field is a single key/value pair. An unsupported Value causes the whole
// entry to fail encoding before anything is written.
type Field struct {
	Key   string
	Value interface{}
}

// String returns a string field.
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Time returns a time field, rendered with the logger's single time format.
func Time(key string, value time.Time) Field { return Field{Key: key, Value: value} }

// Error returns an error field.
func Error(err error) Field { return Field{Key: "error", Value: err} }

// Options configures a Logger at construction time. None of these can be
// changed afterwards; build a new Logger instead.
type Options struct {
	// Format selects the encoding. Defaults to FormatJSON.
	Format Format
	// Sink receives flushed bytes. Nil means os.Stdout.
	Sink io.Writer
	// BufferSize is the flush threshold in bytes. Defaults to DefaultBufferSize.
	BufferSize int
	// FlushEvery flushes after every entry: when one entry ends, its bytes
	// have already left the buffer.
	FlushEvery bool
	// RetryOnce selects the failure policy for the sink: retry one more
	// time instead of dropping the buffered bytes immediately. A second
	// failure stops; there is never a third attempt.
	RetryOnce bool
	// TimeFormat is the single layout for all time values in an entry.
	// Defaults to DefaultTimeFormat.
	TimeFormat string
}

// Logger encodes entries and flushes them to a sink. It is safe for
// concurrent use; bytes of two entries never interleave on the same line,
// even when two Loggers share one sink.
type Logger struct {
	format     Format
	sink       io.Writer
	size       int
	flushEvery bool
	retryOnce  bool
	timeFormat string

	mu     sync.Mutex
	buf    []byte
	closed bool
}

// sinkLocks serializes writes per sink so that two Loggers sharing one
// sink never interleave lines.
var sinkLocks sync.Map // io.Writer -> *sync.Mutex

func lockFor(w io.Writer) *sync.Mutex {
	l, _ := sinkLocks.LoadOrStore(w, &sync.Mutex{})
	return l.(*sync.Mutex)
}

// New builds a Logger. The format is fixed here and cannot change later.
func New(opts Options) *Logger {
	sink := opts.Sink
	if sink == nil {
		sink = os.Stdout
	}
	size := opts.BufferSize
	if size <= 0 {
		size = DefaultBufferSize
	}
	tf := opts.TimeFormat
	if tf == "" {
		tf = DefaultTimeFormat
	}
	return &Logger{
		format:     opts.Format,
		sink:       sink,
		size:       size,
		flushEvery: opts.FlushEvery,
		retryOnce:  opts.RetryOnce,
		timeFormat: tf,
	}
}

// Log encodes one entry and appends it to the buffer, flushing when the
// buffer is full or when FlushEvery is set. A failed encode writes nothing;
// a failed flush marks this entry as failed.
func (l *Logger) Log(fields ...Field) error {
	line, err := l.encode(fields)
	if err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}

	// A single entry larger than the buffer goes straight to the sink.
	if len(line) >= l.size {
		if err := l.flushLocked(); err != nil {
			return err
		}
		return l.writeSink(line)
	}
	if len(l.buf)+len(line) > l.size {
		if err := l.flushLocked(); err != nil {
			return err
		}
	}
	l.buf = append(l.buf, line...)
	if l.flushEvery {
		return l.flushLocked()
	}
	return nil
}

// Close flushes any remaining bytes, including a half-finished entry, and
// marks the logger closed. If the remainder cannot be flushed, the error
// reports how many bytes were left. Writes after Close fail with ErrClosed.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	l.closed = true
	return l.flushLocked()
}

// flushLocked delivers the whole buffer and empties it. On failure the
// caller's policy applies: drop the remainder, or retry exactly once and
// then stop. The buffer is empty afterwards either way; a failure reports
// how many bytes could not be delivered, wrapping the sink's own error.
func (l *Logger) flushLocked() error {
	if len(l.buf) == 0 {
		return nil
	}
	pending := l.buf
	l.buf = nil
	err := l.writeSink(pending)
	if err != nil && l.retryOnce {
		err = l.writeSink(pending) // one retry, never a third attempt
	}
	if err != nil {
		return &FlushError{Remaining: len(pending), Err: err}
	}
	return nil
}

// writeSink writes p to the sink, serializing against other Loggers that
// share the same sink. The sink's error is returned unchanged.
func (l *Logger) writeSink(p []byte) error {
	lock := lockFor(l.sink)
	lock.Lock()
	defer lock.Unlock()
	_, err := l.sink.Write(p)
	return err
}

// encode renders one entry into a fresh byte slice. Nothing is appended to
// the buffer until encoding has fully succeeded, so a failed encode never
// leaves a partial entry behind.
func (l *Logger) encode(fields []Field) ([]byte, error) {
	var sb strings.Builder
	switch l.format {
	case FormatJSON:
		sb.WriteByte('{')
		for i, f := range fields {
			if i > 0 {
				sb.WriteByte(',')
			}
			if err := l.encodeJSONPair(&sb, f); err != nil {
				return nil, err
			}
		}
		sb.WriteString("}\n")
	case FormatLogfmt:
		for i, f := range fields {
			if i > 0 {
				sb.WriteByte(' ')
			}
			if err := l.encodeLogfmtPair(&sb, f); err != nil {
				return nil, err
			}
		}
		sb.WriteByte('\n')
	default:
		return nil, fmt.Errorf("buffered: unknown format %d", l.format)
	}
	return []byte(sb.String()), nil
}

func (l *Logger) encodeJSONPair(sb *strings.Builder, f Field) error {
	sb.WriteString(strconv.Quote(f.Key))
	sb.WriteByte(':')
	s, err := l.valueString(f.Value)
	if err != nil {
		return err
	}
	sb.WriteString(strconv.Quote(s))
	return nil
}

func (l *Logger) encodeLogfmtPair(sb *strings.Builder, f Field) error {
	sb.WriteString(f.Key)
	sb.WriteByte('=')
	s, err := l.valueString(f.Value)
	if err != nil {
		return err
	}
	if strings.ContainsAny(s, " \t\"") {
		sb.WriteString(strconv.Quote(s))
	} else {
		sb.WriteString(s)
	}
	return nil
}

// valueString renders a value. Empty values become EmptyMarker so the key
// is visibly present. Time values use the logger's single time format.
func (l *Logger) valueString(v interface{}) (string, error) {
	switch t := v.(type) {
	case nil:
		return EmptyMarker, nil
	case string:
		if t == "" {
			return EmptyMarker, nil
		}
		return t, nil
	case time.Time:
		return t.Format(l.timeFormat), nil
	case error:
		return t.Error(), nil
	case fmt.Stringer:
		return t.String(), nil
	case bool:
		return strconv.FormatBool(t), nil
	case int:
		return strconv.Itoa(t), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	default:
		return "", fmt.Errorf("buffered: cannot encode value of type %T", v)
	}
}
