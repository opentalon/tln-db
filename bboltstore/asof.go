package bboltstore

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

// histBucketPrefix names the per-entity history bucket. Each key is a
// docID; the value is a snappy-compressed JSON array of docVersion,
// appended in write order. Time-travel queries reconstruct a document's
// state by replaying this chain up to a target instant.
const histBucketPrefix = "hist:"

// docVersion is one entry in a document's history chain. At is the write
// instant in Unix nanoseconds. A Deleted version is a tombstone (the doc
// did not exist just after this instant). Data holds the raw document
// bytes for a live version.
type docVersion struct {
	At      int64  `json:"at"`
	Deleted bool   `json:"deleted,omitempty"`
	Data    []byte `json:"data,omitempty"` // raw doc bytes (base64 in JSON); may be opaque non-JSON
}

// appendDocHistory appends v to the history chain for (entityID, docID),
// creating the history bucket on first write. Called inside the same
// write transaction as the doc/meta mutation so history stays consistent
// with the live document.
func appendDocHistory(tx *bolt.Tx, entityID, docID string, v docVersion) error {
	hb, err := tx.CreateBucketIfNotExists([]byte(histBucketPrefix + entityID))
	if err != nil {
		return err
	}
	var versions []docVersion
	if raw := hb.Get([]byte(docID)); raw != nil {
		decoded, err := snappy.Decode(nil, raw)
		if err != nil {
			return fmt.Errorf("bboltstore: decode history for %q: %w", docID, err)
		}
		if err := json.Unmarshal(decoded, &versions); err != nil {
			return fmt.Errorf("bboltstore: decode history versions for %q: %w", docID, err)
		}
	}
	versions = append(versions, v)
	blob, err := json.Marshal(versions)
	if err != nil {
		return fmt.Errorf("bboltstore: encode history for %q: %w", docID, err)
	}
	return hb.Put([]byte(docID), snappy.Encode(nil, blob))
}

// QueryAsOf runs the same structured composer as Query, but against a
// reconstruction of the store as it existed at asOf. Every document under
// the entity is rebuilt from its history chain (the latest version whose
// write time is <= asOf); documents created after asOf, or deleted by
// then, are absent. Unlike Query, it does not use the inverted index —
// the index reflects current state, so time-travel must scan history.
//
// Backs the FactStore TimeTraveler capability. Documents written before
// history tracking existed have no chain and are therefore invisible to
// as-of queries.
func (s *Store) QueryAsOf(ctx context.Context, req QueryRequest, asOf time.Time) ([]QueryRow, error) {
	if err := validateEntityID(req.EntityID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(req.Where) == 0 {
		return nil, fmt.Errorf("bboltstore: query-as-of has no where clause")
	}
	cutoff := asOf.UnixNano()

	var rows []QueryRow
	err := s.db.View(func(tx *bolt.Tx) error {
		hb := tx.Bucket([]byte(histBucketPrefix + req.EntityID))
		if hb == nil {
			return nil
		}
		return hb.ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			doc, live, err := reconstructDocAsOf(v, cutoff)
			if err != nil {
				return err
			}
			if !live {
				return nil
			}
			bindings := map[string]any{"?e": parseQueryRecordID(string(k))}
			if !matchAllQuery(req.Where, doc, bindings) {
				return nil
			}
			if len(req.Aggregates) > 0 {
				rows = append(rows, bindingsToRow(bindings))
				return nil
			}
			row := make(QueryRow, len(req.Find))
			for i, name := range req.Find {
				row[i] = bindings[name]
			}
			rows = append(rows, row)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	if len(req.Aggregates) > 0 {
		return runQueryAggregates(rowsToBindings(rows), req.GroupBy, req.Aggregates), nil
	}
	return rows, nil
}

// reconstructDocAsOf decodes a history chain and returns the document's
// attributes as of cutoff. live is false when the doc did not exist then
// (no version at/before cutoff, or the effective version is a tombstone).
func reconstructDocAsOf(raw []byte, cutoff int64) (doc map[string]any, live bool, err error) {
	decoded, err := snappy.Decode(nil, raw)
	if err != nil {
		return nil, false, fmt.Errorf("bboltstore: decode history: %w", err)
	}
	var versions []docVersion
	if err := json.Unmarshal(decoded, &versions); err != nil {
		return nil, false, fmt.Errorf("bboltstore: decode history versions: %w", err)
	}
	// Pick the version with the greatest At not exceeding cutoff. On ties
	// (same nanosecond) the later-appended version wins — last write wins.
	var chosen *docVersion
	for i := range versions {
		if versions[i].At <= cutoff && (chosen == nil || versions[i].At >= chosen.At) {
			chosen = &versions[i]
		}
	}
	if chosen == nil || chosen.Deleted {
		return nil, false, nil
	}
	doc = map[string]any{}
	if len(chosen.Data) > 0 {
		// Tolerate opaque non-JSON blobs the same way Query does.
		_ = json.Unmarshal(chosen.Data, &doc)
	}
	return doc, true, nil
}
