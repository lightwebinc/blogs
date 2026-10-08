package ingest

import (
	"encoding/binary"
	"errors"
)

// Snappy's block format (https://github.com/google/snappy/blob/main/format_description.txt),
// decode only: the body encoding of a protobuf Loki push. Decoding is
// bounded by the length the block declares, itself bounded by max.

var errSnappy = errors.New("snappy: malformed block")

func snappyDecode(src []byte, max int) ([]byte, error) {
	n, k := binary.Uvarint(src)
	if k <= 0 || n > uint64(max) {
		if k > 0 {
			return nil, errors.New("snappy: block expands over the request bound")
		}
		return nil, errSnappy
	}
	src = src[k:]
	dst := make([]byte, 0, n)
	for len(src) > 0 {
		tag := src[0]
		var length, offset int
		switch tag & 3 {
		case 0:
			length = int(tag >> 2)
			src = src[1:]
			if length >= 60 {
				extra := length - 59
				if len(src) < extra {
					return nil, errSnappy
				}
				length = 0
				for i := extra - 1; i >= 0; i-- {
					length = length<<8 | int(src[i])
				}
				src = src[extra:]
			}
			length++
			if length > len(src) || len(dst)+length > int(n) {
				return nil, errSnappy
			}
			dst = append(dst, src[:length]...)
			src = src[length:]
			continue
		case 1:
			if len(src) < 2 {
				return nil, errSnappy
			}
			length = 4 + int(tag>>2)&7
			offset = int(tag>>5)<<8 | int(src[1])
			src = src[2:]
		case 2:
			if len(src) < 3 {
				return nil, errSnappy
			}
			length = 1 + int(tag>>2)
			offset = int(binary.LittleEndian.Uint16(src[1:]))
			src = src[3:]
		case 3:
			if len(src) < 5 {
				return nil, errSnappy
			}
			length = 1 + int(tag>>2)
			offset = int(binary.LittleEndian.Uint32(src[1:]))
			src = src[5:]
		}
		if offset <= 0 || offset > len(dst) || len(dst)+length > int(n) {
			return nil, errSnappy
		}
		start := len(dst) - offset
		for i := 0; i < length; i++ {
			dst = append(dst, dst[start+i])
		}
	}
	if len(dst) != int(n) {
		return nil, errSnappy
	}
	return dst, nil
}
