// Package flushlog provides a small, concurrency-safe structured logger that
// encodes records into bytes, buffers them, and flushes the buffer to an
// outlet when it fills up or when the caller asks for a flush.
//
// The four steps of every write happen on one code path: encode the fields
// into bytes, append those bytes to an in-memory buffer, flush the buffer to
// the outlet when it is full, and report every failure instead of pretending
// the record was persisted.
package flushlog
