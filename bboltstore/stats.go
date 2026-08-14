package bboltstore

import (
	"encoding/json"
	"fmt"
	"math"

	bolt "go.etcd.io/bbolt"
)

// Running statistics: incremental Welford aggregates per (entity, attr).
//
// Storage layout:
//
//	stat:{entity} — key: attr name (bytes)
//	                value: JSON statsValue
//
// Adds are incremental — Welford's online algorithm gives O(1) update
// per numeric leaf. Updates and Deletes set the Stale flag; the next
// read recomputes from the numeric range index. This trade-off is
// correct under arbitrary writes and fast in the append-mostly
// workload tln-db targets.

const statsPrefix = "stat:"

func statsBucketName(entityID string) []byte { return []byte(statsPrefix + entityID) }

type statsValue struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean"`
	M2    float64 `json:"m2"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Stale bool    `json:"stale,omitempty"`
}

// statsWelfordAdd applies one increment to the running aggregate.
func statsWelfordAdd(v *statsValue, x float64) {
	v.Count++
	delta := x - v.Mean
	v.Mean += delta / float64(v.Count)
	delta2 := x - v.Mean
	v.M2 += delta * delta2
	if v.Count == 1 || x < v.Min {
		v.Min = x
	}
	if v.Count == 1 || x > v.Max {
		v.Max = x
	}
}

func statsLoad(tx *bolt.Tx, entityID, attr string) (statsValue, bool, error) {
	b := tx.Bucket(statsBucketName(entityID))
	if b == nil {
		return statsValue{}, false, nil
	}
	raw := b.Get([]byte(attr))
	if raw == nil {
		return statsValue{}, false, nil
	}
	var v statsValue
	if err := json.Unmarshal(raw, &v); err != nil {
		return statsValue{}, false, fmt.Errorf("stats: decode: %w", err)
	}
	return v, true, nil
}

func statsStore(tx *bolt.Tx, entityID, attr string, v statsValue) error {
	b, err := tx.CreateBucketIfNotExists(statsBucketName(entityID))
	if err != nil {
		return err
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put([]byte(attr), raw)
}

func statsMarkStale(tx *bolt.Tx, entityID, attr string) error {
	v, exists, err := statsLoad(tx, entityID, attr)
	if err != nil {
		return err
	}
	if !exists {
		v = statsValue{Stale: true}
	}
	v.Stale = true
	return statsStore(tx, entityID, attr, v)
}

// statsAddFresh adds x to attr's running aggregate. Only safe when the
// caller knows the doc is brand new (no oldDoc had a numeric for this
// attr); otherwise use statsMarkStale.
func statsAddFresh(tx *bolt.Tx, entityID, attr string, x float64) error {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return nil
	}
	v, _, err := statsLoad(tx, entityID, attr)
	if err != nil {
		return err
	}
	if v.Stale {
		return statsStore(tx, entityID, attr, v) // leave stale; will be recomputed on read
	}
	statsWelfordAdd(&v, x)
	return statsStore(tx, entityID, attr, v)
}

// statsRecompute scans the numeric index for attr and rebuilds the
// aggregate from scratch. Runs in a writable transaction so it can
// persist the result and clear the stale flag.
func statsRecompute(tx *bolt.Tx, entityID, attr string) (statsValue, error) {
	b := tx.Bucket(numIndexBucketName(entityID, attr))
	if b == nil {
		return statsValue{}, nil
	}
	var v statsValue
	cursor := b.Cursor()
	for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
		if len(k) != 12 {
			continue
		}
		statsWelfordAdd(&v, decodeFloat(k[:8]))
	}
	if err := statsStore(tx, entityID, attr, v); err != nil {
		return statsValue{}, err
	}
	return v, nil
}

// statsRead returns the current aggregate, recomputing if stale.
func statsRead(db *bolt.DB, entityID, attr string) (statsValue, error) {
	var v statsValue
	if err := db.View(func(tx *bolt.Tx) error {
		var err error
		v, _, err = statsLoad(tx, entityID, attr)
		return err
	}); err != nil {
		return statsValue{}, err
	}
	if !v.Stale {
		return v, nil
	}
	// Recompute requires a writable tx.
	if err := db.Update(func(tx *bolt.Tx) error {
		var err error
		v, err = statsRecompute(tx, entityID, attr)
		return err
	}); err != nil {
		return statsValue{}, err
	}
	return v, nil
}

