package bboltstore

import (
	"context"
	"time"

	tlndb "github.com/opentalon/tln-db"

	bolt "go.etcd.io/bbolt"
)

// ClusterQuery walks the temporal index for (entityID, itemID) and
// returns non-overlapping clusters of events whose total span is at
// most `window` and whose size is at least `minSize`. Replaces the
// client-side "fetch all events + count" pattern for detect blocks of
// the form "N+ records same item within W".
//
// Algorithm (linear in event count):
//
//   - Filter events by `types` (empty = accept all).
//   - Greedy non-overlapping scan: for each starting index i, advance
//     j as far as events[j].At − events[i].At ≤ window allows.
//     If j-i+1 ≥ minSize, emit cluster [i..j] and resume at i = j+1.
//     Otherwise advance i by one.
//
// `window` of 0 is treated as "no upper bound" — every consecutive
// run of ≥ minSize matching events becomes a single cluster.
//
// `minSize` < 1 is normalised to 1.
func (s *Store) ClusterQuery(ctx context.Context, entityID, itemID string, types []string, window time.Duration, minSize int) ([]tlndb.TemporalCluster, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if minSize < 1 {
		minSize = 1
	}
	var entries []temporalEntry
	if err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		entries, err = temporalRead(tx, entityID, itemID, types)
		return err
	}); err != nil {
		return nil, err
	}
	return clusterScan(entries, window, minSize), nil
}

// clusterScan is the pure-function core of ClusterQuery — extracted so
// it can be unit-tested without bbolt. Callers must pass `entries`
// sorted by At ascending (temporalRead guarantees this).
func clusterScan(entries []temporalEntry, window time.Duration, minSize int) []tlndb.TemporalCluster {
	if len(entries) == 0 || minSize > len(entries) {
		return nil
	}
	var out []tlndb.TemporalCluster
	windowNanos := window.Nanoseconds()
	noUpper := windowNanos <= 0

	i := 0
	for i < len(entries) {
		j := i
		// Advance j as far as window allows.
		for j+1 < len(entries) {
			if !noUpper && entries[j+1].At-entries[i].At > windowNanos {
				break
			}
			j++
		}
		size := j - i + 1
		if size >= minSize {
			events := make([]tlndb.TemporalEvent, 0, size)
			for k := i; k <= j; k++ {
				events = append(events, tlndb.TemporalEvent{
					DocID: entries[k].DocID,
					Type:  entries[k].Type,
					At:    time.Unix(0, entries[k].At),
				})
			}
			out = append(out, tlndb.TemporalCluster{
				First:  time.Unix(0, entries[i].At),
				Last:   time.Unix(0, entries[j].At),
				Events: events,
			})
			i = j + 1
		} else {
			i++
		}
	}
	return out
}

