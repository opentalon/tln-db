package bboltstore

import (
	"context"
	"fmt"
	"time"

	tlndb "github.com/opentalon/tln-db"

	roaring "github.com/RoaringBitmap/roaring/v2"
	bolt "go.etcd.io/bbolt"
)

// Lookup implements tlndb.IndexedStore — see indexed.go for the
// contract.
func (s *Store) Lookup(ctx context.Context, entityID, term string) (tlndb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = invIndexLookup(tx, entityID, term)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// LastSeen implements tlndb.IndexedStore.
func (s *Store) LastSeen(ctx context.Context, entityID, itemID, recordType string) (time.Time, bool, error) {
	if err := validateEntityID(entityID); err != nil {
		return time.Time{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	var (
		at    int64
		found bool
	)
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		at, found, err = absenceLookup(tx, entityID, itemID, recordType)
		return err
	}); err != nil {
		return time.Time{}, false, err
	}
	if !found {
		return time.Time{}, false, nil
	}
	return time.Unix(0, at), true, nil
}

// Stats implements tlndb.IndexedStore.
func (s *Store) Stats(ctx context.Context, entityID, attr string) (tlndb.RunningStats, error) {
	if err := validateEntityID(entityID); err != nil {
		return tlndb.RunningStats{}, err
	}
	if err := ctx.Err(); err != nil {
		return tlndb.RunningStats{}, err
	}
	v, err := statsRead(s.db, entityID, attr)
	if err != nil {
		return tlndb.RunningStats{}, err
	}
	return tlndb.RunningStats{
		Count: v.Count,
		Mean:  v.Mean,
		M2:    v.M2,
		Min:   v.Min,
		Max:   v.Max,
	}, nil
}

// Ancestors implements tlndb.IndexedStore.
func (s *Store) Ancestors(ctx context.Context, entityID, categoryID string) ([]string, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var chain []string
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		chain, err = closureReadAncestors(tx, entityID, categoryID)
		return err
	}); err != nil {
		return nil, err
	}
	return chain, nil
}

// Descendants implements tlndb.IndexedStore.
func (s *Store) Descendants(ctx context.Context, entityID, rootID string) (tlndb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = closureReadDescendants(tx, entityID, rootID)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// GroupCount implements tlndb.IndexedStore.
func (s *Store) GroupCount(ctx context.Context, entityID, itemID, attr, value string) (tlndb.GroupBucket, error) {
	if err := validateEntityID(entityID); err != nil {
		return tlndb.GroupBucket{}, err
	}
	if err := ctx.Err(); err != nil {
		return tlndb.GroupBucket{}, err
	}
	var (
		v  groupByValue
		bm *roaring.Bitmap
	)
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		v, bm, err = groupByRead(tx, entityID, itemID, attr, value)
		return err
	}); err != nil {
		return tlndb.GroupBucket{}, err
	}
	if v.Count == 0 {
		return tlndb.GroupBucket{}, nil
	}
	docIDs, err := s.materializeDocIDSet(entityID, bm)
	if err != nil {
		return tlndb.GroupBucket{}, err
	}
	return tlndb.GroupBucket{
		Count:  int(v.Count),
		First:  time.Unix(0, v.FirstSeen),
		Last:   time.Unix(0, v.LastSeen),
		DocIDs: docIDs,
	}, nil
}

// WindowQuery implements tlndb.IndexedStore. The `window` parameter
// is currently advisory: every matching event is returned in time
// order; callers apply windowing on the result. Kept in the signature
// for forward compatibility with a future server-side window filter.
func (s *Store) WindowQuery(ctx context.Context, entityID, itemID string, types []string, window time.Duration) ([]tlndb.TemporalEvent, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	_ = window
	var entries []temporalEntry
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		entries, err = temporalRead(tx, entityID, itemID, types)
		return err
	}); err != nil {
		return nil, err
	}
	out := make([]tlndb.TemporalEvent, len(entries))
	for i, e := range entries {
		out[i] = tlndb.TemporalEvent{DocID: e.DocID, Type: e.Type, At: time.Unix(0, e.At)}
	}
	return out, nil
}

// LookupNumericRange implements tlndb.IndexedStore.
func (s *Store) LookupNumericRange(ctx context.Context, entityID, attr string, min, max float64, opts tlndb.RangeOpts) (tlndb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = numIndexRange(tx, entityID, attr, min, max, opts.MinExclusive, opts.MaxExclusive)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// LookupPrefix implements tlndb.IndexedStore.
func (s *Store) LookupPrefix(ctx context.Context, entityID, prefix string) (tlndb.DocIDSet, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var bm *roaring.Bitmap
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		bm, err = invIndexLookupPrefix(tx, entityID, prefix)
		return err
	}); err != nil {
		return nil, err
	}
	return s.materializeDocIDSet(entityID, bm)
}

// materializeDocIDSet resolves every internalID in `bm` back to its
// string docID via the idmap reverse bucket and returns a frozen
// snapshot. The set is detached from any open transaction.
func (s *Store) materializeDocIDSet(entityID string, bm *roaring.Bitmap) (tlndb.DocIDSet, error) {
	if bm == nil || bm.IsEmpty() {
		return tlndb.EmptyDocIDSet(), nil
	}
	ids := make([]string, 0, bm.GetCardinality())
	if err := s.db.View(func(tx *bolt.Tx) error {
		iter := bm.Iterator()
		for iter.HasNext() {
			internalID := iter.Next()
			docID, ok, err := idmapReverse(tx, entityID, internalID)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("bboltstore: orphan internalID %d in bitmap", internalID)
			}
			ids = append(ids, docID)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &stringDocIDSet{ids: ids}, nil
}

// stringDocIDSet is a frozen, sorted-by-construction DocIDSet over a
// pre-materialized slice. Safe for concurrent reads.
type stringDocIDSet struct {
	ids []string
}

func (s *stringDocIDSet) Len() int { return len(s.ids) }

func (s *stringDocIDSet) Contains(docID string) bool {
	// Linear scan is fine for the sizes we expect in slice 1; if
	// callers need O(log n) we can swap in sort.Search later.
	for _, id := range s.ids {
		if id == docID {
			return true
		}
	}
	return false
}

func (s *stringDocIDSet) ForEach(fn func(docID string) bool) {
	for _, id := range s.ids {
		if !fn(id) {
			return
		}
	}
}

// AsSortedSlice is a backend convenience for callers that need the
// underlying slice (e.g. for joining with another set). The slice
// MUST NOT be mutated.
func (s *stringDocIDSet) AsSortedSlice() []string {
	return s.ids
}

var _ tlndb.IndexedStore = (*Store)(nil)
