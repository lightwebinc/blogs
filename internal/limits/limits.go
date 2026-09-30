// Package limits is the blogs command's policy limits: the defaults and
// hard caps of docs/limits.md, and the checks that apply them to a
// producer's and a reader's settings.
//
// None of these is a wire format or a signed bound (docs/frozen.md). The
// record bounds every reader accepts (spec section 11) are logrec's; the
// limits here are what a producer chooses to send and what a reader or an
// archive chooses to ask for, so a later version may change any of them
// without a new record version. A hard cap refuses the setting with an
// error naming the limit and why; a recommendation warns and goes on.
package limits

import (
	"fmt"
	"time"

	"github.com/lightwebinc/blogs/internal/logrec"
)

// The plane's object size. An object larger than one packet is carried as
// BRC-130 fragments at the fabric's MTU floor of 1280 bytes: less the IPv6
// (40), UDP (8) and fragment (104) headers, 1128 bytes of object each. Every
// lost fragment is repaired by NACK, and the reassembly of the whole object
// must finish within a fixed 10 s; the more fragments, the likelier one
// needs a second repair round and the object is abandoned. Measured: 12 %
// of 64-packet objects lost at 1 % loss with a 141 ms round trip, none of
// the 1- and 8-packet ones.
const (
	FragmentPayload = 1128
	PlanePackets    = 64
	// PlaneObjectAim is the largest carrier BEEF, in bytes, that stays
	// within PlanePackets fragments.
	PlaneObjectAim = FragmentPayload * PlanePackets
	// OpenObjectBound is BRC-149's bound on an object submitted on the open
	// path; MaxObjectBound the authenticated path's. No plane admits more.
	OpenObjectBound = 1 << 20
	MaxObjectBound  = 8 << 20
)

// Batch cutting.
const (
	// PlaneMaxBytes is the default bytes of lines per full-bytes lines
	// batch on the plane: with a 100-output funding tree and a proof, its
	// carrier's BEEF is 54,727 bytes, 49 fragments (measured). At
	// PlaneMaxBytesWarn it is 71,119 bytes, 64 fragments; above it a
	// carrier passes PlanePackets.
	PlaneMaxBytes     = 48 << 10
	PlaneMaxBytesWarn = 64 << 10
	// PlaneGzipMaxBytes is the default bytes of lines per gzip batch on the
	// plane; log lines compress several times over, so the carried content
	// is of the same order as PlaneMaxBytes.
	PlaneGzipMaxBytes = 256 << 10
	// UnicastMaxBytes is the default per full-bytes lines batch in unicast
	// mode: the record's carried bound, which each host takes over HTTP.
	UnicastMaxBytes = logrec.MaxContent
	// LargeMaxBytes is the default for gzip in unicast mode and for
	// digest-only in either mode, whose content does not ride the plane.
	LargeMaxBytes = 1 << 20

	DefaultMaxLines = 10000
	MaxLines        = 65536

	DefaultMaxWait = time.Second
	MinMaxWait     = 10 * time.Millisecond
	MaxMaxWait     = time.Hour
)

// Anchors.
const (
	DefaultInterval      = 10 * time.Minute
	MinInterval          = time.Minute
	MaxInterval          = 24 * time.Hour
	DefaultAnchorBatches = 4096
	MaxAnchorBatches     = logrec.MaxIntervalBatches
)

// Publishing.
const (
	// DefaultRate and MaxRate bound the batches a stream publishes per
	// second.
	DefaultRate = 2
	MaxRate     = 50
	// DefaultTreeCount and MaxTreeCount bound a batch funding tree's
	// outputs; every carrier's BEEF carries its tree. PlaneTreeCountWarn
	// is where the tree alone is about 10 fragments.
	DefaultTreeCount   = 100
	MaxTreeCount       = 1000
	PlaneTreeCountWarn = 250
	// MaxHosts is the most hosts a command is configured with.
	// UnicastHostsWarn is where a unicast producer's upload per object is
	// better done by the plane.
	MaxHosts         = 16
	UnicastHostsWarn = 5
	// MinTimeout and MaxTimeout bound one request.
	MinTimeout = time.Second
	MaxTimeout = 5 * time.Minute
	// MaxStreams is the most streams one producer home holds.
	MaxStreams = 64
)

// Readers and archives.
const (
	// DefaultRecheck is how many stored seqs an archive pass asks the host
	// about again, oldest checked first; MaxRecheck the most one pass may
	// be given (-recheck-all asks about every one).
	DefaultRecheck = 1024
	MaxRecheck     = 65536
	// MaxReadBatches is the most batches one read prints.
	MaxReadBatches = 65536
	// ContentDirFreeWarn is the free space under which a digest-only
	// producer is warned about its content directory.
	ContentDirFreeWarn = 1 << 30
)

// Producer is a producer's settings, as the checks see them.
type Producer struct {
	Plane         bool
	Hosts         int
	Need          int
	Digest        bool
	Gzip          bool
	MaxBytes      int
	MaxLines      int
	MaxWait       time.Duration
	Interval      time.Duration
	AnchorBatches int
	Rate          float64
	TreeCount     int
	ObjectBound   int
}

// DefaultMaxBytes is -max-bytes when none is given, by mode and profile.
func DefaultMaxBytes(plane, digest, gzip bool) int {
	switch {
	case digest:
		return LargeMaxBytes
	case plane && gzip:
		return PlaneGzipMaxBytes
	case plane:
		return PlaneMaxBytes
	case gzip:
		return LargeMaxBytes
	}
	return UnicastMaxBytes
}

func over(what string, v, limit any, why string) error {
	return fmt.Errorf("%s %v is over the limit of %v: %s (docs/limits.md)", what, v, limit, why)
}

func under(what string, v, limit any, why string) error {
	return fmt.Errorf("%s %v is under the limit of %v: %s (docs/limits.md)", what, v, limit, why)
}

// Check applies the hard caps, returning the first broken as an error, and
// the recommendations, returning each passed as a warning.
func (p Producer) Check() (warnings []string, err error) {
	byteCap, why := logrec.MaxExpanded, "the decoded bound every reader accepts"
	if !p.Digest && !p.Gzip {
		byteCap, why = logrec.MaxContent, "the carried content bound every reader accepts; a larger batch would be split anyway"
	}
	switch {
	case p.MaxBytes < 1:
		return nil, under("-max-bytes", p.MaxBytes, 1, "a batch holds at least one byte")
	case p.MaxBytes > byteCap:
		return nil, over("-max-bytes", p.MaxBytes, byteCap, why)
	case p.MaxLines < 1:
		return nil, under("-max-lines", p.MaxLines, 1, "a batch holds at least one line")
	case p.MaxLines > MaxLines:
		return nil, over("-max-lines", p.MaxLines, MaxLines, "a reader decodes and filters every line of a batch before it prints one")
	case p.MaxWait < MinMaxWait:
		return nil, under("-max-wait", p.MaxWait, MinMaxWait, "below it every line is a batch of its own, at about 50 sat each")
	case p.MaxWait > MaxMaxWait:
		return nil, over("-max-wait", p.MaxWait, MaxMaxWait, "a line waits this long to be published at all")
	case p.Interval < MinInterval:
		return nil, under("-interval", p.Interval, MinInterval, "one anchor is in flight at a time and each waits for a block, so a shorter interval anchors no sooner")
	case p.Interval > MaxInterval:
		return nil, over("-interval", p.Interval, MaxInterval, "a batch is unproven until its anchor mines")
	case p.AnchorBatches < 1:
		return nil, under("-anchor-batches", p.AnchorBatches, 1, "an anchor covers at least one batch")
	case p.AnchorBatches > MaxAnchorBatches:
		return nil, over("-anchor-batches", p.AnchorBatches, MaxAnchorBatches, "the batches per interval every reader accepts")
	case p.Rate <= 0:
		return nil, under("-max-rate", p.Rate, "0 (exclusive)", "a stream publishes at some rate")
	case p.Rate > MaxRate:
		return nil, over("-max-rate", p.Rate, MaxRate, "every subscribed host receives every batch, and a subscriber's delivery budget (hundreds of objects a second) is shared by all its topics")
	case p.TreeCount > MaxTreeCount:
		return nil, over("-tree-count", p.TreeCount, MaxTreeCount, "every carrier's BEEF carries its whole funding tree")
	case p.ObjectBound > MaxObjectBound:
		return nil, over("object_bound", p.ObjectBound, MaxObjectBound, "no plane admits a larger object (BRC-149)")
	case !p.Plane && p.Hosts > MaxHosts:
		return nil, over("hosts", p.Hosts, MaxHosts, "a unicast producer uploads every object to each host")
	}
	if p.Plane {
		if !p.Digest && !p.Gzip && p.MaxBytes > PlaneMaxBytesWarn {
			warnings = append(warnings, fmt.Sprintf("-max-bytes %d on the plane makes carriers of more than %d packets, which are lost far more often under loss and distance than small ones; %d is the default (docs/limits.md)", p.MaxBytes, PlanePackets, PlaneMaxBytes))
		}
		if p.TreeCount > PlaneTreeCountWarn {
			warnings = append(warnings, fmt.Sprintf("-tree-count %d on the plane adds about %d bytes of funding tree to every carrier; %d is the default (docs/limits.md)", p.TreeCount, p.TreeCount*49, DefaultTreeCount))
		}
		if p.ObjectBound > OpenObjectBound {
			warnings = append(warnings, fmt.Sprintf("object_bound %d is over the open path's bound of %d; only an authenticated submission path admits a larger object (docs/limits.md)", p.ObjectBound, OpenObjectBound))
		}
	} else {
		if p.Hosts > UnicastHostsWarn {
			warnings = append(warnings, fmt.Sprintf("%d hosts in mode unicast: the producer uploads every object %d times; beyond %d hosts the plane is what removes that cost (docs/limits.md)", p.Hosts, p.Hosts, UnicastHostsWarn))
		}
		if p.Need > 0 && p.Need < p.Hosts/2+1 {
			warnings = append(warnings, fmt.Sprintf("a quorum of %d of %d hosts is below a majority: an object counts as published while most hosts lack it, until a backfill (docs/limits.md)", p.Need, p.Hosts))
		}
	}
	if p.MaxWait > p.Interval {
		warnings = append(warnings, fmt.Sprintf("-max-wait %s is longer than -interval %s: a batch can wait longer than an anchor", p.MaxWait, p.Interval))
	}
	return warnings, nil
}

// Gap is the least time between two batches of a stream at rate batches
// per second.
func Gap(rate float64) time.Duration {
	if rate <= 0 {
		return 0
	}
	return time.Duration(float64(time.Second) / rate)
}
