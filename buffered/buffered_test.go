package buffered

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestJSONEntryReachesSink(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true})
	if err := l.Log(String("msg", "hi"), Field{Key: "n", Value: 3}); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "{\"msg\":\"hi\",\"n\":\"3\"}\n" {
		t.Fatalf("unexpected output %q", got)
	}
}

func TestShortEntryWithoutBuffering(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, BufferSize: 1 << 20})
	if err := l.Log(String("a", "b")); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("should be buffered")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "\"a\":\"b\"") {
		t.Fatalf("short entry missing from sink: %q", out.String())
	}
}

func TestEmptyValueUsesMarker(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true, Format: FormatLogfmt})
	if err := l.Log(String("empty", ""), String("full", "x")); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "empty="+EmptyMarker) {
		t.Fatalf("empty key not written with marker: %q", got)
	}
	if !strings.Contains(got, "full=x") {
		t.Fatalf("missing full key: %q", got)
	}
}

func TestBufferFullFlushesOnce(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, BufferSize: 20})
	if err := l.Log(String("k", "12345678")); err != nil { // 14 bytes
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("should still be buffered")
	}
	if err := l.Log(String("k", "abcdefgh")); err != nil { // overflows -> flush
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "12345678") {
		t.Fatal("full buffer was not flushed")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "abcdefgh") {
		t.Fatal("second entry lost")
	}
}

func TestFlushEveryLeavesBufferEmpty(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true, BufferSize: 1 << 20})
	if err := l.Log(String("a", "1")); err != nil {
		t.Fatal(err)
	}
	l.mu.Lock()
	n := len(l.buf)
	l.mu.Unlock()
	if n != 0 {
		t.Fatal("buffer not empty after flush-every entry")
	}
	if err := l.Log(String("b", "2")); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 2 || lines[0] != "{\"a\":\"1\"}" || lines[1] != "{\"b\":\"2\"}" {
		t.Fatalf("entries interleaved or tailed: %q", out.String())
	}
}

type failWriter struct {
	err    error
	calls  int
	failAt int // fail calls while calls < failAt; -1 = always
}

func (w *failWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.failAt < 0 || w.calls <= w.failAt {
		return 0, w.err
	}
	return len(p), nil
}

func TestFlushFailureMarksEntryFailed(t *testing.T) {
	sinkErr := errors.New("disk on fire")
	w := &failWriter{err: sinkErr, failAt: -1}
	l := New(Options{Sink: w, FlushEvery: true})
	err := l.Log(String("a", "b"))
	if err == nil {
		t.Fatal("flush failure must fail the entry")
	}
	if !errors.Is(err, sinkErr) {
		t.Fatalf("sink error not passed through: %v", err)
	}
}

func TestEncodeFailureWritesNothing(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true})
	if err := l.Log(Field{Key: "bad", Value: func() {}}); err == nil {
		t.Fatal("expected encode error")
	}
	if out.Len() != 0 {
		t.Fatalf("partial entry written: %q", out.String())
	}
}

func TestRetryOnceThenStop(t *testing.T) {
	sinkErr := errors.New("nope")
	w := &failWriter{err: sinkErr, failAt: -1}
	l := New(Options{Sink: w, FlushEvery: true, RetryOnce: true})
	if err := l.Log(String("a", "b")); err == nil {
		t.Fatal("expected failure")
	}
	if w.calls != 2 {
		t.Fatalf("expected exactly 2 attempts (1 retry), got %d", w.calls)
	}
}

func TestRetryOnceSucceedsOnSecondAttempt(t *testing.T) {
	sinkErr := errors.New("transient")
	w := &failWriter{err: sinkErr, failAt: 1}
	var out bytes.Buffer
	l := New(Options{Sink: w, FlushEvery: true, RetryOnce: true})
	_ = out
	if err := l.Log(String("a", "b")); err != nil {
		t.Fatalf("retry should have succeeded: %v", err)
	}
	if w.calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", w.calls)
	}
}

func TestNoRetryDropsBufferedBytes(t *testing.T) {
	sinkErr := errors.New("boom")
	w := &failWriter{err: sinkErr, failAt: 1} // first flush fails, later writes ok
	l := New(Options{Sink: w, FlushEvery: true})
	if err := l.Log(String("a", "1")); err == nil {
		t.Fatal("expected failure")
	}
	if w.calls != 1 {
		t.Fatalf("must not retry without RetryOnce, got %d calls", w.calls)
	}
	l.mu.Lock()
	n := len(l.buf)
	l.mu.Unlock()
	if n != 0 {
		t.Fatal("failed flush must empty the buffer")
	}
}

func TestUniformTimeFormat(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true, TimeFormat: time.RFC3339})
	ts := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	if err := l.Log(Time("start", ts), Time("end", ts)); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if strings.Count(got, "2026-09-21T12:00:00Z") != 2 {
		t.Fatalf("time format not uniform: %q", got)
	}
}

func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	var mu sync.Mutex
	var out bytes.Buffer
	type lockedWriter struct{ ioWriter }
	_ = lockedWriter{}
	w := &syncWriter{mu: &mu, w: &out}
	l := New(Options{Sink: w, FlushEvery: true})
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = l.Log(String("g", strings.Repeat(string(rune('a'+g)), 20)))
			}
		}(g)
	}
	wg.Wait()
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		if !strings.HasPrefix(line, "{\"g\":\"") || !strings.HasSuffix(line, "\"}") {
			t.Fatalf("interleaved line: %q", line)
		}
		val := line[6 : len(line)-2]
		if strings.Trim(val, val[:1]) != "" {
			t.Fatalf("mixed bytes in one line: %q", line)
		}
	}
}

type ioWriter interface{ Write([]byte) (int, error) }

type syncWriter struct {
	mu *sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func TestTwoLoggersShareSinkNoInterleave(t *testing.T) {
	var mu sync.Mutex
	var out bytes.Buffer
	w := &syncWriter{mu: &mu, w: &out}
	l1 := New(Options{Sink: w, FlushEvery: true})
	l2 := New(Options{Sink: w, FlushEvery: true})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = l1.Log(String("who", strings.Repeat("x", 30)))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_ = l2.Log(String("who", strings.Repeat("y", 30)))
		}
	}()
	wg.Wait()
	for _, line := range strings.Split(strings.TrimRight(out.String(), "\n"), "\n") {
		val := line[8 : len(line)-2]
		if strings.Trim(val, val[:1]) != "" {
			t.Fatalf("lines from two loggers interleaved: %q", line)
		}
	}
}

func TestCloseFlushesRemainderAndBlocksWrites(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, BufferSize: 1 << 20})
	if err := l.Log(String("pending", "yes")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "pending") {
		t.Fatal("close did not flush remaining bytes")
	}
	if err := l.Log(String("late", "no")); !errors.Is(err, ErrClosed) {
		t.Fatalf("write after close must fail with ErrClosed, got %v", err)
	}
}

func TestCloseReportsRemainingBytes(t *testing.T) {
	sinkErr := errors.New("sink gone")
	w := &failWriter{err: sinkErr, failAt: -1}
	l := New(Options{Sink: w, BufferSize: 1 << 20})
	if err := l.Log(String("half", "entry")); err != nil {
		t.Fatal(err)
	}
	err := l.Close()
	var fe *FlushError
	if !errors.As(err, &fe) {
		t.Fatalf("expected FlushError, got %v", err)
	}
	if fe.Remaining != len("{\"half\":\"entry\"}\n") {
		t.Fatalf("wrong remaining byte count: %d", fe.Remaining)
	}
	if !errors.Is(err, sinkErr) {
		t.Fatal("original sink error must be preserved")
	}
}

func TestFormatFixedAtConstruction(t *testing.T) {
	var out bytes.Buffer
	l := New(Options{Sink: &out, FlushEvery: true, Format: FormatLogfmt})
	if err := l.Log(String("k", "v")); err != nil {
		t.Fatal(err)
	}
	if out.String() != "k=v\n" {
		t.Fatalf("format not honored: %q", out.String())
	}
	// A different format requires a new logger.
	var out2 bytes.Buffer
	l2 := New(Options{Sink: &out2, FlushEvery: true, Format: FormatJSON})
	if err := l2.Log(String("k", "v")); err != nil {
		t.Fatal(err)
	}
	if out2.String() != "{\"k\":\"v\"}\n" {
		t.Fatalf("unexpected: %q", out2.String())
	}
}
