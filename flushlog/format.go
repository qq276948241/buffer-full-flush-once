package flushlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// Format selects how records are turned into bytes. A Logger keeps the
// format it was built with for its whole lifetime; changing format requires
// building a new Logger.
type Format int

const (
	// JSONFormat encodes one JSON object per line.
	JSONFormat Format = iota
	// LineFormat encodes one logfmt-style "key=value" pair per field, one
	// record per line.
	LineFormat
)

// TimeFormat is the single layout used for every time field encoded by this
// package. Allowing per-field layouts would let one record mix numeric
// seconds with strings for the same kind of value, which is forbidden.
const TimeFormat = time.RFC3339Nano

// NullMarker is the explicit marker written for a nil value. In JSON output
// it is the literal null token; in line output it is the bare token "null".
// A reader can therefore tell a key was present-but-empty rather than
// missing: "key=null" is never omitted from the output.
const NullMarker = "null"

type encoder interface {
	appendRecord(dst []byte, t time.Time, msg string, fields []Field) ([]byte, error)
}

func newEncoder(f Format) encoder {
	switch f {
	case LineFormat:
		return lineEncoder{}
	default:
		return jsonEncoder{}
	}
}

type jsonEncoder struct{}

func (jsonEncoder) appendRecord(dst []byte, t time.Time, msg string, fields []Field) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)

	obj := map[string]interface{}{
		"ts":  t.Format(TimeFormat),
		"msg": msg,
	}
	for _, f := range fields {
		if _, exists := obj[f.Key]; exists {
			return nil, fmt.Errorf("flushlog: duplicate field key %q", f.Key)
		}
		if f.Value == nil {
			obj[f.Key] = nil
			continue
		}
		obj[f.Key] = f.Value
	}
	if err := enc.Encode(obj); err != nil {
		return nil, err
	}
	return append(dst, buf.Bytes()...), nil
}

type lineEncoder struct{}

func (lineEncoder) appendRecord(dst []byte, t time.Time, msg string, fields []Field) ([]byte, error) {
	seen := map[string]struct{}{}

	var err error
	dst = append(dst, "ts="...)
	dst = strconv.AppendQuote(dst, t.Format(TimeFormat))
	dst = append(dst, ' ')
	dst = append(dst, "msg="...)
	dst = strconv.AppendQuote(dst, msg)

	for _, f := range fields {
		if _, dup := seen[f.Key]; dup {
			return nil, fmt.Errorf("flushlog: duplicate field key %q", f.Key)
		}
		seen[f.Key] = struct{}{}

		dst = append(dst, ' ')
		dst = append(dst, f.Key...)
		dst = append(dst, '=')
		if f.Value == nil {
			dst = append(dst, NullMarker...)
			continue
		}
		dst, err = appendLineValue(dst, f.Value)
		if err != nil {
			return nil, err
		}
	}
	dst = append(dst, '\n')
	return dst, nil
}

func appendLineValue(dst []byte, v interface{}) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(dst, NullMarker...), nil
	case string:
		return strconv.AppendQuote(dst, x), nil
	case bool:
		return strconv.AppendBool(dst, x), nil
	case int:
		return strconv.AppendInt(dst, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(dst, x, 10), nil
	case float64:
		return strconv.AppendFloat(dst, x, 'g', -1, 64), nil
	case time.Time:
		return strconv.AppendQuote(dst, x.Format(TimeFormat)), nil
	case time.Duration:
		return strconv.AppendQuote(dst, x.String()), nil
	case error:
		return strconv.AppendQuote(dst, x.Error()), nil
	case fmt.Stringer:
		return strconv.AppendQuote(dst, x.String()), nil
	default:
		b, err := json.Marshal(x)
		if err != nil {
			return nil, err
		}
		return append(dst, b...), nil
	}
}
