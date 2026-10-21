package ingest

import (
	"encoding/binary"
	"errors"
	"math"
)

// The protobuf wire format (https://protobuf.dev/programming-guides/encoding/),
// read only, for the two request messages the doors take: OTLP's
// ExportLogsServiceRequest and Loki's PushRequest. Unknown fields are
// skipped, as protobuf requires.

var errWire = errors.New("protobuf: malformed message")

type pbuf struct{ b []byte }

func (p *pbuf) more() bool { return len(p.b) > 0 }

func (p *pbuf) varint() (uint64, error) {
	v, n := binary.Uvarint(p.b)
	if n <= 0 {
		return 0, errWire
	}
	p.b = p.b[n:]
	return v, nil
}

// field reads a tag: the field number and wire type.
func (p *pbuf) field() (int, int, error) {
	t, err := p.varint()
	if err != nil {
		return 0, 0, err
	}
	if t>>3 == 0 || t>>3 > math.MaxInt32 {
		return 0, 0, errWire
	}
	return int(t >> 3), int(t & 7), nil
}

func (p *pbuf) bytes() ([]byte, error) {
	n, err := p.varint()
	if err != nil || n > uint64(len(p.b)) {
		return nil, errWire
	}
	b := p.b[:n]
	p.b = p.b[n:]
	return b, nil
}

func (p *pbuf) fixed64() (uint64, error) {
	if len(p.b) < 8 {
		return 0, errWire
	}
	v := binary.LittleEndian.Uint64(p.b)
	p.b = p.b[8:]
	return v, nil
}

func (p *pbuf) fixed32() (uint32, error) {
	if len(p.b) < 4 {
		return 0, errWire
	}
	v := binary.LittleEndian.Uint32(p.b)
	p.b = p.b[4:]
	return v, nil
}

// lenField reads a length-delimited field's value (wire type 2: a string,
// bytes or an embedded message); any other wire type for such a field is
// malformed, as a protobuf parser holds it.
func (p *pbuf) lenField(wt int) ([]byte, error) {
	if wt != 2 {
		return nil, errWire
	}
	return p.bytes()
}

// varField reads a varint field's value (wire type 0: int32, int64,
// uint32, uint64, bool, enum).
func (p *pbuf) varField(wt int) (uint64, error) {
	if wt != 0 {
		return 0, errWire
	}
	return p.varint()
}

// skip passes over a field of wire type wt.
func (p *pbuf) skip(wt int) error {
	var err error
	switch wt {
	case 0:
		_, err = p.varint()
	case 1:
		_, err = p.fixed64()
	case 2:
		_, err = p.bytes()
	case 5:
		_, err = p.fixed32()
	default:
		err = errWire
	}
	return err
}

// each calls fn for every field of b; fn reads the field's value or
// returns errSkip to have it skipped.
func each(b []byte, fn func(p *pbuf, num, wt int) error) error {
	p := &pbuf{b}
	for p.more() {
		num, wt, err := p.field()
		if err != nil {
			return err
		}
		if err := fn(p, num, wt); err != nil {
			if !errors.Is(err, errSkip) {
				return err
			}
			if err := p.skip(wt); err != nil {
				return err
			}
		}
	}
	return nil
}

var errSkip = errors.New("skip")

// want checks a field's wire type.
func want(wt, w int) error {
	if wt != w {
		return errWire
	}
	return nil
}
