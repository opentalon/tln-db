package bboltstore

import (
	"bytes"
	"context"
	"time"

	tlndb "github.com/opentalon/tln-db"

	bolt "go.etcd.io/bbolt"
)

// SequenceJoin scans temporal indexes for one or more items and
// returns the items whose event log contains `steps` in order, with
// total span at most `window`. Empty `itemIDs` scans every item
// recorded under entityID.
//
// Matching algorithm — equivalent to tln-language's
// internal/executor/event_sequence.matchesSequence: for each starting
// event whose type equals steps[0], greedily walk forward looking for
// each subsequent step. The (first, last) span must be ≤ window;
// window=0 means no upper bound. First successful walk per item wins.
func (s *Store) SequenceJoin(ctx context.Context, entityID string, itemIDs, steps []string, window time.Duration) ([]tlndb.SequenceMatch, error) {
	if err := validateEntityID(entityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(steps) == 0 {
		return nil, nil
	}

	var items []string
	if len(itemIDs) > 0 {
		items = itemIDs
	} else {
		// Enumerate every item with a temporal bucket for this entity.
		prefix := []byte(temporalPrefix + entityID + ":")
		if err := s.db.View(func(tx *bolt.Tx) error {
			return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
				if bytes.HasPrefix(name, prefix) {
					items = append(items, string(name[len(prefix):]))
				}
				return nil
			})
		}); err != nil {
			return nil, err
		}
	}

	windowNanos := window.Nanoseconds()
	out := make([]tlndb.SequenceMatch, 0)
	for _, item := range items {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var entries []temporalEntry
		if err := s.db.View(func(tx *bolt.Tx) error {
			var err error
			entries, err = loadTemporal(tx, entityID, item)
			return err
		}); err != nil {
			return nil, err
		}
		matched := sequenceWalk(entries, steps, windowNanos)
		if matched == nil {
			continue
		}
		events := make([]tlndb.TemporalEvent, len(matched))
		for i, e := range matched {
			events[i] = tlndb.TemporalEvent{
				DocID: e.DocID,
				Type:  e.Type,
				At:    time.Unix(0, e.At),
			}
		}
		out = append(out, tlndb.SequenceMatch{ItemID: item, Events: events})
	}
	return out, nil
}

// sequenceWalk finds the first start index where events[start].Type ==
// steps[0] and the walker can collect one event per subsequent step,
// in order, with total span ≤ windowNanos. Returns the matched events
// in step order, or nil if no match exists. windowNanos ≤ 0 disables
// the upper bound.
func sequenceWalk(events []temporalEntry, steps []string, windowNanos int64) []temporalEntry {
	if len(steps) == 0 {
		return nil
	}
	noUpper := windowNanos <= 0
	for start := 0; start < len(events); start++ {
		if events[start].Type != steps[0] {
			continue
		}
		picked := make([]temporalEntry, 0, len(steps))
		picked = append(picked, events[start])
		for j := start + 1; j < len(events) && len(picked) < len(steps); j++ {
			if events[j].Type != steps[len(picked)] {
				continue
			}
			picked = append(picked, events[j])
		}
		if len(picked) != len(steps) {
			continue
		}
		span := picked[len(picked)-1].At - picked[0].At
		if noUpper || span <= windowNanos {
			return picked
		}
	}
	return nil
}
