package tlndb

import (
	"context"
	"errors"
	"time"
)

// ErrStatsStale is returned by Stats when the last Delete invalidated
// the running aggregate and the backend has not yet recomputed it.
// Callers may retry — recomputation is automatic on the next read.
var ErrStatsStale = errors.New("tlndb: running stats stale, recompute in progress")

// ErrInvalidValue is returned when a numeric query receives NaN or
// infinity, neither of which has a defined position in the index.
var ErrInvalidValue = errors.New("tlndb: invalid numeric value")

// DocIDSet is the iteration surface over query results. Backends may
// back it with roaring bitmaps; callers must not assume the underlying
// representation. Implementations must be safe for concurrent reads.
type DocIDSet interface {
	Len() int
	Contains(docID string) bool
	ForEach(fn func(docID string) bool)
}

// EmptyDocIDSet returns a DocIDSet with no members. Backends may
// return this sentinel from any lookup that finds nothing.
func EmptyDocIDSet() DocIDSet { return emptyDocIDSet{} }

type emptyDocIDSet struct{}

func (emptyDocIDSet) Len() int                       { return 0 }
func (emptyDocIDSet) Contains(string) bool           { return false }
func (emptyDocIDSet) ForEach(func(string) bool)      {}

// RangeOpts selects open or closed bounds for LookupNumericRange. The
// zero value is [min, max] inclusive on both sides.
type RangeOpts struct {
	MinExclusive bool
	MaxExclusive bool
}

// TemporalEvent is one entry in the temporal index. Backends return
// events sorted by At ascending.
type TemporalEvent struct {
	DocID string
	Type  string
	At    time.Time
}

// TemporalCluster is one contiguous group of events returned by
// ClusterQuery. First / Last bracket the span; Events lists the
// events that fell into the cluster, sorted by At ascending.
type TemporalCluster struct {
	First  time.Time
	Last   time.Time
	Events []TemporalEvent
}

// SequenceMatch is one item whose event log satisfied a SequenceJoin
// query. Events lists exactly the events that matched each step, in
// step order; len(Events) == len(steps).
type SequenceMatch struct {
	ItemID string
	Events []TemporalEvent
}

// GroupBucket is the value type for GroupCount: how many docs share
// a particular (itemID, attr, value) tuple and when the first / last
// were seen.
type GroupBucket struct {
	Count  int
	First  time.Time
	Last   time.Time
	DocIDs DocIDSet
}

// RunningStats is the value type for Stats. Stddev is computed by the
// caller as sqrt(M2 / (Count-1)) when Count > 1; callers must handle
// the Count <= 1 case themselves.
type RunningStats struct {
	Count int64
	Mean  float64
	M2    float64
	Min   float64
	Max   float64
}

// IndexedStore extends DocumentStore with the per-block lookup methods
// described in tln-language issue #27. Backends are free to implement
// only a subset, in which case unimplemented methods return a wrapped
// errors.ErrUnsupported.
type IndexedStore interface {
	DocumentStore

	// Lookup returns the set of documents whose extracted terms include
	// the given exact term.
	Lookup(ctx context.Context, entityID, term string) (DocIDSet, error)

	// LookupPrefix returns the union of all term bitmaps whose term
	// starts with the given prefix.
	LookupPrefix(ctx context.Context, entityID, prefix string) (DocIDSet, error)

	// LookupNumericRange returns documents whose attr-named field falls
	// in [min, max] (bounds adjusted by RangeOpts). NaN / ±Inf bounds
	// return ErrInvalidValue.
	LookupNumericRange(ctx context.Context, entityID, attr string, min, max float64, opts RangeOpts) (DocIDSet, error)

	// WindowQuery returns events for itemID whose type is in `types`
	// and whose At falls within `window` of at least one anchor event,
	// sorted ascending by time. An empty `types` slice means "any type".
	WindowQuery(ctx context.Context, entityID, itemID string, types []string, window time.Duration) ([]TemporalEvent, error)

	// ClusterQuery returns non-overlapping clusters of events for
	// (entityID, itemID) whose total span is at most `window` and whose
	// size is at least `minSize`. Server-side equivalent of the
	// client-side "fetch all events + count" pattern for "N+ records
	// within W" detect blocks. `window` of 0 means no upper bound.
	ClusterQuery(ctx context.Context, entityID, itemID string, types []string, window time.Duration, minSize int) ([]TemporalCluster, error)

	// SequenceJoin returns the items whose event log contains `steps`
	// in order, with total span at most `window`. Empty `itemIDs` scans
	// every item under entityID. Server-side equivalent of the
	// client-side matchesSequence pass over WindowQuery results, used
	// by detect blocks of the form "A followed_by B within N".
	SequenceJoin(ctx context.Context, entityID string, itemIDs, steps []string, window time.Duration) ([]SequenceMatch, error)

	// GroupCount returns the pre-aggregated counter for one
	// (itemID, attr, value) tuple. Returns a zero-valued GroupBucket
	// with Count == 0 if nothing matches.
	GroupCount(ctx context.Context, entityID, itemID, attr, value string) (GroupBucket, error)

	// Stats returns the running aggregate for one (entity, attr) pair.
	// Returns RunningStats{} with Count == 0 if nothing has ever been
	// recorded; returns ErrStatsStale if a recent Delete invalidated
	// the counter and the backend has not yet recomputed it.
	Stats(ctx context.Context, entityID, attr string) (RunningStats, error)

	// LastSeen returns the most recent At timestamp for the given
	// (itemID, recordType). The bool is false if no such record has
	// ever been written.
	LastSeen(ctx context.Context, entityID, itemID, recordType string) (time.Time, bool, error)

	// Ancestors returns categoryID's parent chain ordered from
	// immediate parent to root. Returns an empty slice for a root.
	Ancestors(ctx context.Context, entityID, categoryID string) ([]string, error)

	// Descendants returns the set of docIDs whose ancestor chain
	// includes rootID.
	Descendants(ctx context.Context, entityID, rootID string) (DocIDSet, error)
}
