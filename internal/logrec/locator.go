package logrec

import (
	"fmt"

	"github.com/lightwebinc/bcommon/cbor"
)

// Locator is a digest-only batch's pointer to its content: the SHA-256 of
// the content bytes (the hash a BRC-26 UHRP URL encodes), their length, and
// zero to MaxURLs HTTPS fetch hints. Every member is always present, so one
// locator has one encoding.
type Locator struct {
	Digest [32]byte // "digest": SHA-256 of the content bytes as fetched
	Length uint64   // "length": the content's length in bytes
	URLs   []string // "urls": fetch hints, possibly empty
	// Extra holds members this version does not define, whatever their
	// key type, preserved verbatim. A locator with any is unsupported: a
	// reader does not fetch through it, and the batch stays a valid link.
	Extra cbor.Map
}

// Unsupported reports whether the locator carries a member this version
// does not define.
func (l *Locator) Unsupported() bool { return len(l.Extra) > 0 }

func (l *Locator) validate() error {
	if l.Length < 1 || l.Length > MaxLocated {
		return fmt.Errorf("%w: length", ErrLocator)
	}
	if l.URLs == nil || len(l.URLs) > MaxURLs {
		return fmt.Errorf("%w: urls", ErrLocator)
	}
	for _, u := range l.URLs {
		if !validURL(u) {
			return fmt.Errorf("%w: url", ErrLocator)
		}
	}
	for _, p := range l.Extra {
		if k, ok := p.Key.(string); ok && (k == "digest" || k == "length" || k == "urls") {
			return fmt.Errorf("%w: member name", ErrLocator)
		}
	}
	return nil
}

func (l *Locator) value() cbor.Map {
	urls := make([]cbor.Value, len(l.URLs))
	for i, u := range l.URLs {
		urls[i] = u
	}
	m := cbor.Map{
		{Key: "digest", Val: l.Digest[:]},
		{Key: "length", Val: l.Length},
		{Key: "urls", Val: urls},
	}
	return append(m, l.Extra...)
}

// EncodeLocator writes a locator alone, as it appears under key 10.
func (l *Locator) Encode() ([]byte, error) {
	if err := l.validate(); err != nil {
		return nil, err
	}
	out, err := cbor.Encode(l.value())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCBOR, err)
	}
	return out, nil
}

// DecodeLocator parses a locator encoded alone.
func DecodeLocator(p []byte) (*Locator, error) {
	if len(p) > 2048 {
		return nil, ErrTooLarge
	}
	v, err := cbor.DecodeValue(p)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCBOR, err)
	}
	return decodeLocator(v)
}

func decodeLocator(v cbor.Value) (*Locator, error) {
	m, ok := v.(cbor.Map)
	if !ok {
		return nil, fmt.Errorf("%w: not a map", ErrLocator)
	}
	l := &Locator{}
	var seen int
	for _, p := range m {
		k, _ := p.Key.(string)
		switch k {
		case "digest":
			d, ok := p.Val.([]byte)
			if !ok || len(d) != 32 {
				return nil, fmt.Errorf("%w: digest", ErrLocator)
			}
			l.Digest = hash32(d)
			seen++
		case "length":
			n, ok := p.Val.(uint64)
			if !ok {
				return nil, fmt.Errorf("%w: length", ErrLocator)
			}
			l.Length = n
			seen++
		case "urls":
			a, ok := p.Val.([]cbor.Value)
			if !ok || len(a) > MaxURLs {
				return nil, fmt.Errorf("%w: urls", ErrLocator)
			}
			l.URLs = make([]string, 0, len(a))
			for _, e := range a {
				s, ok := e.(string)
				if !ok {
					return nil, fmt.Errorf("%w: url", ErrLocator)
				}
				l.URLs = append(l.URLs, s)
			}
			seen++
		default:
			// Includes a non-text key, whose k is "".
			l.Extra = append(l.Extra, p)
		}
	}
	if seen != 3 {
		return nil, fmt.Errorf("%w: a member is missing", ErrLocator)
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	return l, nil
}
