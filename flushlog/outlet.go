package flushlog

import (
	"io"
	"sync"
)

// Outlet receives the encoded bytes. Any io.Writer satisfies it; callers may
// supply os.Stdout, a bytes.Buffer, a network connection, or any other
// writer whose errors they care about.
type Outlet = io.Writer

// lockedOutlet serializes writes from several Loggers sharing one outlet.
// Write calls happen under the same mutex used for Sync, so bytes of one
// flush can never interleave with bytes of another on a shared line.
type lockedOutlet struct {
	mu sync.Mutex
	w  io.Writer
}

// SharedOutlet wraps an outlet so that multiple Loggers writing to it cannot
// interleave their lines. The same returned outlet should be handed to every
// Logger that shares the underlying writer.
func SharedOutlet(w io.Writer) Outlet {
	if w == nil {
		panic("flushlog: nil outlet")
	}
	if _, ok := w.(*lockedOutlet); ok {
		return w
	}
	return &lockedOutlet{w: w}
}

func (l *lockedOutlet) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
