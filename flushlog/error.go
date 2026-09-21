package flushlog

import "fmt"

// ClosedError is returned when a write or flush is attempted after the
// Logger was closed.
type ClosedError struct {
	// Closing is true when the write lost the race against a Close already
	// in progress: the outlet was being shut down while the record came in.
	Closing bool
}

func (e ClosedError) Error() string {
	if e.Closing {
		return "flushlog: logger is closing"
	}
	return "flushlog: logger is closed"
}

// RemainingBytesError is returned from Close (or Sync) when bytes were left
// in the buffer because the outlet refused them. Remaining reports how many
// bytes could not be delivered, and Err is the outlet's original error,
// unwrapped rather than replaced by a generic write failure.
type RemainingBytesError struct {
	Remaining int
	Err       error
}

func (e *RemainingBytesError) Error() string {
	return fmt.Sprintf("flushlog: %d bytes remain after flush failure: %v", e.Remaining, e.Err)
}

func (e *RemainingBytesError) Unwrap() error { return e.Err }
