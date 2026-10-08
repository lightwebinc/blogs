package ingest

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// Syslog over a stream (RFC 6587 framing, RFC 5425 when over TLS): each
// frame is octet counted ("LEN SP MSG") or ends at a newline, decided per
// frame by its first byte, as rsyslog's imtcp decides. A message is parsed
// as RFC 5424; one that is not (an RFC 3164 sender) keeps its priority and
// is carried whole as the body.

// MaxSyslogFrame bounds one frame.
const MaxSyslogFrame = MaxRecordLine

var severities = []string{"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug"}

var facilities = []string{"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news", "uucp", "cron",
	"authpriv", "ftp", "ntp", "security", "console", "solaris-cron", "local0", "local1", "local2", "local3",
	"local4", "local5", "local6", "local7"}

// ReadFrame reads one syslog frame.
func ReadFrame(r *bufio.Reader) ([]byte, error) {
	c, err := r.Peek(1)
	if err != nil {
		return nil, err
	}
	if c[0] >= '1' && c[0] <= '9' {
		n := 0
		for {
			b, err := r.ReadByte()
			if err != nil {
				return nil, err
			}
			if b == ' ' {
				break
			}
			if b < '0' || b > '9' || n > MaxSyslogFrame {
				return nil, errors.New("syslog: a malformed octet count")
			}
			n = n*10 + int(b-'0')
		}
		if n > MaxSyslogFrame {
			return nil, fmt.Errorf("syslog: a frame of %d bytes is over the bound of %d", n, MaxSyslogFrame)
		}
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf, nil
	}
	line, err := r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return nil, fmt.Errorf("syslog: a line over the bound of %d", r.Size())
	}
	if err != nil && (len(line) == 0 || !errors.Is(err, io.EOF)) {
		return nil, err
	}
	out := make([]byte, len(line))
	copy(out, line)
	return []byte(strings.TrimRight(string(out), "\r\n")), nil
}

// ParseSyslog reads one message. now stamps a message with no time.
func ParseSyslog(msg []byte, now time.Time) (Record, error) {
	s := strings.TrimRight(string(msg), "\r\n")
	rec := Record{Labels: map[string]string{}, Attrs: map[string]string{}}
	if !strings.HasPrefix(s, "<") {
		return rec, errors.New("syslog: no priority")
	}
	end := strings.IndexByte(s, '>')
	if end < 2 || end > 4 {
		return rec, errors.New("syslog: a malformed priority")
	}
	pri, err := strconv.Atoi(s[1:end])
	if err != nil || pri < 0 || pri > 191 {
		return rec, errors.New("syslog: a malformed priority")
	}
	rec.Sev = severities[pri%8]
	rec.Attrs["facility"] = facilities[pri/8]
	s = s[end+1:]
	if !strings.HasPrefix(s, "1 ") {
		rec.Ts = now.UnixNano()
		rec.Labels["service_name"] = "syslog"
		rec.Body = s
		return rec, nil
	}
	s = s[2:]
	fields := make([]string, 5)
	for i := range fields {
		sp := strings.IndexByte(s, ' ')
		if sp < 0 {
			if i == 4 {
				fields[i], s = s, ""
				break
			}
			return rec, errors.New("syslog: a truncated RFC 5424 header")
		}
		fields[i], s = s[:sp], s[sp+1:]
	}
	ts, host, app, proc, msgid := fields[0], fields[1], fields[2], fields[3], fields[4]
	rec.Ts = now.UnixNano()
	if ts != "-" {
		t, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			return rec, fmt.Errorf("syslog: timestamp %q: %w", ts, err)
		}
		rec.Ts = t.UnixNano()
	}
	rec.Labels["service_name"] = "syslog"
	if app != "-" {
		rec.Labels["service_name"] = app
	}
	if host != "-" {
		rec.Labels["host"] = host
	}
	if proc != "-" {
		rec.Attrs["procid"] = proc
	}
	if msgid != "-" {
		rec.Attrs["msgid"] = msgid
	}
	rest, err := structuredData(s, rec.Attrs)
	if err != nil {
		return rec, err
	}
	rec.Body = strings.TrimPrefix(strings.TrimPrefix(rest, " "), "\ufeff")
	return rec, nil
}

// structuredData reads RFC 5424 structured data into attrs as
// sd.<id>.<param>, and returns what follows it.
func structuredData(s string, attrs map[string]string) (string, error) {
	if strings.HasPrefix(s, "-") {
		return s[1:], nil
	}
	for strings.HasPrefix(s, "[") {
		s = s[1:]
		sp := strings.IndexAny(s, " ]")
		if sp < 0 {
			return "", errors.New("syslog: unterminated structured data")
		}
		id := s[:sp]
		s = s[sp:]
		for strings.HasPrefix(s, " ") {
			s = s[1:]
			eq := strings.IndexByte(s, '=')
			if eq < 1 || len(s) < eq+2 || s[eq+1] != '"' {
				return "", errors.New("syslog: malformed structured data")
			}
			name := s[:eq]
			s = s[eq+2:]
			var b strings.Builder
			i := 0
			for ; i < len(s) && s[i] != '"'; i++ {
				if s[i] == '\\' && i+1 < len(s) && strings.IndexByte(`"\]`, s[i+1]) >= 0 {
					i++
				}
				b.WriteByte(s[i])
			}
			if i >= len(s) {
				return "", errors.New("syslog: unterminated structured data value")
			}
			attrs["sd."+id+"."+name] = b.String()
			s = s[i+1:]
		}
		if !strings.HasPrefix(s, "]") {
			return "", errors.New("syslog: unterminated structured data")
		}
		s = s[1:]
	}
	return s, nil
}
