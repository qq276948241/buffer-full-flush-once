package flushlog

// Field is one key/value pair attached to a record. Both key and value are
// always encoded. A nil Value is written as the explicit null marker rather
// than being dropped, so an empty field stays distinguishable from a missing
// key on the reading side.
type Field struct {
	Key   string
	Value interface{}
}

// String builds a string-valued field.
func String(key, value string) Field { return Field{Key: key, Value: value} }

// Int builds an int-valued field.
func Int(key string, value int) Field { return Field{Key: key, Value: value} }

// Bool builds a bool-valued field.
func Bool(key string, value bool) Field { return Field{Key: key, Value: value} }

// Any builds a field with an arbitrary value. Passing an untyped nil
// produces the explicit null marker.
func Any(key string, value interface{}) Field { return Field{Key: key, Value: value} }

// Empty builds a field whose value is explicitly empty. The key is still
// written; its value serializes to the null marker.
func Empty(key string) Field { return Field{Key: key, Value: nil} }
