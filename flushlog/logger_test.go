package flushlog

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func fixedClock() func() time.Time {
	t := time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC)
	return func() time.Time { return t }
}

// A short line with no buffering pressure still appears on the outlet.
func TestShortLineReachesOutlet(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), withClock(fixedClock()))
	if err := l.Log("hello"); err != nil {
		t.Fatalf("log: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !strings.Contains(out.String(), "hello") {
		t.Fatalf("short line missing from outlet: %q", out.String())
	}
}

func TestJSONEncodesKeysValuesAndNullMarker(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), withClock(fixedClock()))
	if err := l.Log("evt", String("name", "a"), Empty("trace"), Any("n", nil), Int("n2", 7)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &obj); err != nil {
		t.Fatalf("invalid json %q: %v", out.String(), err)
	}
	if obj["name"] != "a" || obj["n2"] != float64(7) {
		t.Fatalf("values missing: %#v", obj)
	}
	if v, ok := obj["trace"]; !ok || v != nil {
		t.Fatalf("empty key must be present as null, got %#v", obj)
	}
	if _, ok := obj["n"]; !ok {
		t.Fatalf("nil-valued key must not be dropped")
	}
	if obj["ts"] != "2026-09-21T10:00:00Z" {
		t.Fatalf("unexpected timestamp encoding: %v", obj["ts"])
	}
}

func TestLineFormatEncodesNullMarker(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), WithFormat(LineFormat), withClock(fixedClock()))
	if err := l.Log("evt", String("k", "v"), Empty("empty")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(out.String())
	if !strings.Contains(line, `k="v"`) || !strings.Contains(line, "empty=null") {
		t.Fatalf("line format missing value or null marker: %q", line)
	}
}

type failingWriter struct {
	err     error
	failFor int
	calls   int
	ok      bytes.Buffer
}

func (f *failingWriter) Write(p []byte) (int, error) {
	f.calls++
	if f.calls <= f.failFor {
		return 0, f.err
	}
	return f.ok.Write(p)
}

func TestFlushFailureIsReportedNotPretended(t *testing.T) {
	sentinel := errors.New("disk on fire")
	fw := &failingWriter{err: sentinel, failFor: 1}
	l := New(WithOutlet(fw), WithBufferSize(4))
	err := l.Log("abcdef")
	if err == nil {
		t.Fatal("flush failure must be returned")
	}
	var rbe *RemainingBytesError
	if !errors.As(err, &rbe) {
		t.Fatalf("want *RemainingBytesError, got %T: %v", err, err)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("caller outlet error must pass through unwrapped, got %v", err)
	}
	if rbe.Remaining == 0 {
		t.Fatal("remaining bytes must be reported")
	}
}

func TestRetryOnceSucceedsOnSecondAttempt(t *testing.T) {
	fw := &failingWriter{err: errors.New("transient"), failFor: 1}
	l := New(WithOutlet(fw), WithBufferSize(4), WithRetryOnce())
	if err := l.Log("abcdef"); err != nil {
		t.Fatalf("retry should recover: %v", err)
	}
	if fw.calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", fw.calls)
	}
}

func TestRetryOnceFailsStopsAfterSecondAttempt(t *testing.T) {
	fw := &failingWriter{err: errors.New("dead"), failFor: 10}
	l := New(WithOutlet(fw), WithBufferSize(4), WithRetryOnce())
	err := l.Log("abcdef")
	if err == nil {
		t.Fatal("persistent failure must be reported")
	}
	if fw.calls != 2 {
		t.Fatalf("must retry exactly once then stop, got %d attempts", fw.calls)
	}
}

func TestDropPolicyDoesNotRetry(t *testing.T) {
	fw := &failingWriter{err: errors.New("dead"), failFor: 10}
	l := New(WithOutlet(fw), WithBufferSize(4))
	_ = l.Log("abcdef")
	if fw.calls != 1 {
		t.Fatalf("drop policy must attempt once, got %d", fw.calls)
	}
}

// After a flush (successful or failed) the buffer is empty: the next record
// must not carry the previous record's tail.
func TestBufferEmptyAfterFlush(t *testing.T) {
	fw := &failingWriter{err: errors.New("dead"), failFor: 1}
	l := New(WithOutlet(fw), WithBufferSize(4))
	_ = l.Log("FIRST")
	if len(l.buf) != 0 {
		t.Fatalf("buffer must be empty after failed flush, has %d bytes", len(l.buf))
	}
	if err := l.Log("SECOND"); err != nil {
		t.Fatalf("second record should be independent: %v", err)
	}
	if err := l.Sync(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fw.ok.String(), "SECOND") || strings.Contains(fw.ok.String(), "FIRST") {
		t.Fatal("second flush must not carry first record's tail")
	}
}

type recordingWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (r *recordingWriter) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.Write(p)
}

func TestSyncEachFlushesBeforeNextRecord(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), WithSyncEach())
	if err := l.Log("one"); err != nil {
		t.Fatal(err)
	}
	if len(l.buf) != 0 {
		t.Fatal("sync-each must leave buffer empty before the next record starts")
	}
	if !strings.Contains(out.String(), "one") {
		t.Fatal("sync-each record must reach outlet immediately")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWriteAfterCloseFails(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out))
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	err := l.Log("late")
	var ce ClosedError
	if !errors.As(err, &ce) || ce.Closing {
		t.Fatalf("write after close must return closed error, got %v", err)
	}
	if strings.Contains(out.String(), "late") {
		t.Fatal("record after close must not reach the outlet")
	}
}

func TestCloseFlushesRemainingAndReportsCount(t *testing.T) {
	fw := &failingWriter{err: errors.New("dead"), failFor: 10}
	l := New(WithOutlet(fw), WithRetryOnce())
	if err := l.Log("leftover"); err != nil {
		t.Fatal(err)
	}
	err := l.Close()
	var rbe *RemainingBytesError
	if !errors.As(err, &rbe) {
		t.Fatalf("close must report remaining bytes, got %v", err)
	}
	if rbe.Remaining <= 0 {
		t.Fatal("remaining byte count must be positive")
	}
	if err := l.Close(); !errors.Is(err, ClosedError{}) {
		t.Fatalf("double close must return closed error, got %v", err)
	}
}

// Concurrent writes to one logger never interleave bytes inside a line.
func TestConcurrentWritesDoNotInterleave(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), WithSyncEach())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				_ = l.Log("goroutine", Int("g", g), Int("i", i))
			}
		}(g)
	}
	wg.Wait()
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("interleaved or corrupt line %q: %v", line, err)
		}
	}
}

// Two loggers sharing one outlet also cannot interleave lines.
func TestTwoLoggersSharedOutlet(t *testing.T) {
	shared := SharedOutlet(&bytes.Buffer{})
	a := New(WithOutlet(shared), WithSyncEach())
	b := New(WithOutlet(shared), WithSyncEach())
	var wg sync.WaitGroup
	write := func(l *Logger, tag string) {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			_ = l.Log("shared", String("tag", tag), Int("i", i))
		}
	}
	wg.Add(2)
	go write(a, "a")
	go write(b, "b")
	wg.Wait()
	_ = a.Close()
	_ = b.Close()

	if _, ok := shared.(*lockedOutlet); !ok {
		t.Fatal("SharedOutlet must return a locking outlet")
	}
	got := shared.(*lockedOutlet).w.(*bytes.Buffer).String()
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("interleaved shared line %q: %v", line, err)
		}
		if obj["msg"] != "shared" {
			t.Fatalf("corrupt record: %q", line)
		}
	}
}

type marshalFail struct{}

func (marshalFail) MarshalJSON() ([]byte, error) { return nil, errors.New("nope") }

func TestEncodeFailureWritesNothing(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out))
	before := len(l.buf)
	err := l.Log("bad", Any("v", marshalFail{}))
	if err == nil {
		t.Fatal("encoding failure must be returned")
	}
	if len(l.buf) != before {
		t.Fatal("encoding failure must not leave a partial record in the buffer")
	}
	if out.Len() != 0 {
		t.Fatal("encoding failure must not reach the outlet")
	}
	_ = l.Close()
}

func TestFormatIsFixedAndTimeUniform(t *testing.T) {
	l := New(WithFormat(LineFormat))
	if _, ok := l.enc.(lineEncoder); !ok {
		t.Fatal("format must be pinned at construction")
	}
	// TimeFormat is the one layout for the whole encoding surface.
	if TimeFormat != time.RFC3339Nano {
		t.Fatal("time layout changed")
	}
}

func TestBufferFlushesWhenFull(t *testing.T) {
	var out bytes.Buffer
	l := New(WithOutlet(&out), WithBufferSize(32))
	for i := 0; i < 10 && out.Len() == 0; i++ {
		if err := l.Log("fill", Int("i", i)); err != nil {
			t.Fatal(err)
		}
	}
	if out.Len() == 0 {
		t.Fatal("full buffer must auto-flush")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		var obj map[string]interface{}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("corrupt flushed line %q: %v", line, err)
		}
	}
}

func TestSharedOutletIdempotent(t *testing.T) {
	lo := SharedOutlet(&bytes.Buffer{})
	if SharedOutlet(lo) != lo {
		t.Fatal("re-wrapping a shared outlet must return the same instance")
	}
}

func TestWriteDuringCloseFailsAndNeverLands(t *testing.T) {
	fw := &blockingWriter{}
	fw.init()
	l := New(WithOutlet(SharedOutlet(fw)))
	// Put a byte in the buffer so Close has to flush, and make that flush
	// block while a competing write waits on the logger lock.
	if err := l.Log("buffered"); err != nil {
		t.Fatal(err)
	}

	closeErr := make(chan error, 1)
	go func() { closeErr <- l.Close() }()
	<-fw.entered // Close's flush is in progress, close flag is already set

	writeErr := make(chan error, 1)
	go func() { writeErr <- l.Log("late-during-close") }()
	// The competing Log must be waiting rather than touching the outlet.
	select {
	case err := <-writeErr:
		t.Fatalf("write during close must wait for close, got %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	close(fw.release)
	<-closeErr

	var ce ClosedError
	if err := <-writeErr; !errors.As(err, &ce) {
		t.Fatalf("write during/after close must fail closed, got %v", err)
	}
	fw.mu.Lock()
	got := fw.buf.String()
	fw.mu.Unlock()
	if strings.Contains(got, "late-during-close") {
		t.Fatal("write racing close must never land on the outlet")
	}
}

type blockingWriter struct {
	entered chan struct{}
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func (b *blockingWriter) init() {
	b.entered = make(chan struct{})
	b.release = make(chan struct{})
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	select {
	case <-b.release:
	default:
		close(b.entered)
		<-b.release
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func ExampleNew() {
	l := New(WithOutlet(&bytes.Buffer{}))
	_ = l.Log("user signed up", String("id", "42"), Empty("referrer"))
	_ = l.Close()
	fmt.Println("logged")
	// Output: logged
}
