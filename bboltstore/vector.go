package bboltstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/proto/tlndbpb"
	"github.com/opentalon/tln-db/vectorindex"

	bolt "go.etcd.io/bbolt"
)

// Vector storage layout:
//
//	vec_registry:{entity}  bucket
//	  key   = scope name
//	  value = JSON-encoded scopeMeta{Dim, Metric}
//
//	vec_data:{entity}:{scope}  bucket
//	  key   = vector id
//	  value = packed []float32 (little-endian IEEE-754)
//
// The bbolt side is authoritative; the in-memory index is rebuilt by
// Open() scanning these buckets and replaying every vector through
// vectorindex.Index. A SIGKILL between bbolt commit and in-memory
// update can never lose data — the in-memory state is regenerated
// from disk every restart.
const (
	vecRegistryBucketPrefix = "vec_registry:"
	vecDataBucketPrefix     = "vec_data:"
)

type scopeMeta struct {
	Dim    int                `json:"dim"`
	Metric vectorindex.Metric `json:"metric"`
}

// vectorIndex returns the lazily-initialised in-memory index, building
// it from bbolt on first access. Callers must hold s.vecOnce semantics
// — i.e. always go through this method, never touch s.vectors
// directly.
func (s *Store) vectorIndex() (*vectorindex.Index, error) {
	s.vecOnce.Do(func() {
		s.vectors = vectorindex.New()
		s.vecLoadErr = s.rebuildVectorIndex()
	})
	if s.vecLoadErr != nil {
		return nil, s.vecLoadErr
	}
	return s.vectors, nil
}

// rebuildVectorIndex walks every vec_registry / vec_data bucket and
// replays each (entity, scope, id, vec) tuple into the in-memory
// graph. Runs once per Open. Error during replay is sticky — every
// subsequent call returns the same error.
func (s *Store) rebuildVectorIndex() error {
	return s.db.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			n := string(name)
			if len(n) < len(vecRegistryBucketPrefix) || n[:len(vecRegistryBucketPrefix)] != vecRegistryBucketPrefix {
				return nil
			}
			entity := n[len(vecRegistryBucketPrefix):]
			return b.ForEach(func(k, v []byte) error {
				var meta scopeMeta
				if err := json.Unmarshal(v, &meta); err != nil {
					return fmt.Errorf("vectorindex rebuild: decode meta %q/%q: %w", entity, k, err)
				}
				scope := string(k)
				data := tx.Bucket([]byte(vecDataBucketPrefix + entity + ":" + scope))
				if data == nil {
					return nil // empty scope is fine; metadata stays for dim lock
				}
				return data.ForEach(func(id, raw []byte) error {
					vec, err := decodeFloat32Slice(raw, meta.Dim)
					if err != nil {
						return fmt.Errorf("vectorindex rebuild: decode vec %q/%q/%q: %w",
							entity, scope, id, err)
					}
					return s.vectors.Insert(entity, scope, string(id), vec, meta.Metric)
				})
			})
		})
	})
}

// VectorInsert writes the vector to bbolt and updates the in-memory
// HNSW graph in lockstep. Dimension is locked on first insert into a
// (entity, scope) pair; later inserts with a different length return
// vectorindex.ErrDimensionMismatch.
//
// Order matters: bbolt commit first, in-memory second. If the in-
// memory update fails we surface the error but bbolt already holds the
// vector — the next process restart will pick it up via rebuild.
func (s *Store) VectorInsert(ctx context.Context, entityID, scope, id string, vec []float32, metric vectorindex.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateVecArgs(entityID, scope, id); err != nil {
		return err
	}
	if len(vec) == 0 {
		return vectorindex.ErrEmptyVector
	}
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}

	// Persist + lock dim under bbolt.
	var finalMetric vectorindex.Metric
	if err := s.db.Update(func(tx *bolt.Tx) error {
		m, err := vectorInsertInTx(tx, entityID, scope, id, vec, metric)
		if err != nil {
			return err
		}
		finalMetric = m
		if s.replEnabled {
			if _, err := appendOpLog(tx, &tlndbpb.OpLogEntry{
				Kind:     tlndbpb.OpKind_OP_KIND_VEC_INSERT,
				EntityId: entityID,
				DocId:    id,
				Scope:    scope,
				Vector:   append([]float32(nil), vec...),
				Metric:   int32(m),
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if s.replEnabled {
		s.signalRepl()
	}

	// In-memory mirror. Failure here means the in-memory side is stale
	// but bbolt holds the truth; rebuild on next Open will reconcile.
	return idx.Insert(entityID, scope, id, vec, finalMetric)
}

// vectorInsertInTx writes the vector + scope metadata into bbolt and
// returns the effective metric (an existing scope's metric wins on
// overwrite). Shared by the leader write path and follower apply.
func vectorInsertInTx(tx *bolt.Tx, entityID, scope, id string, vec []float32, metric vectorindex.Metric) (vectorindex.Metric, error) {
	regBucket, err := tx.CreateBucketIfNotExists([]byte(vecRegistryBucketPrefix + entityID))
	if err != nil {
		return metric, err
	}
	if existing := regBucket.Get([]byte(scope)); existing != nil {
		var meta scopeMeta
		if err := json.Unmarshal(existing, &meta); err != nil {
			return metric, fmt.Errorf("decode existing meta: %w", err)
		}
		if meta.Dim != len(vec) {
			return metric, fmt.Errorf("%w: scope %q/%q expects dim %d, got %d",
				vectorindex.ErrDimensionMismatch, entityID, scope, meta.Dim, len(vec))
		}
		metric = meta.Metric // keep the original metric on overwrite
	} else {
		encoded, _ := json.Marshal(scopeMeta{Dim: len(vec), Metric: metric})
		if err := regBucket.Put([]byte(scope), encoded); err != nil {
			return metric, err
		}
	}
	dataBucket, err := tx.CreateBucketIfNotExists([]byte(vecDataBucketPrefix + entityID + ":" + scope))
	if err != nil {
		return metric, err
	}
	return metric, dataBucket.Put([]byte(id), encodeFloat32Slice(vec))
}

// VectorSearch reads only from the in-memory index. The bbolt layer
// guarantees the index reflects every committed Insert/Delete.
func (s *Store) VectorSearch(ctx context.Context, entityID, scope string, query []float32, k int) ([]vectorindex.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, err := s.vectorIndex()
	if err != nil {
		return nil, err
	}
	return idx.Search(entityID, scope, query, k)
}

// VectorDelete removes (entityID, scope, id) from both bbolt and the
// in-memory graph. Returns tlndb.ErrNotFound when the id never
// existed in the scope.
func (s *Store) VectorDelete(ctx context.Context, entityID, scope, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateVecArgs(entityID, scope, id); err != nil {
		return err
	}
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}
	var found bool
	err = s.db.Update(func(tx *bolt.Tx) error {
		f, err := vectorDeleteInTx(tx, entityID, scope, id)
		if err != nil {
			return err
		}
		found = f
		if f && s.replEnabled {
			if _, err := appendOpLog(tx, &tlndbpb.OpLogEntry{
				Kind:     tlndbpb.OpKind_OP_KIND_VEC_DELETE,
				EntityId: entityID,
				DocId:    id,
				Scope:    scope,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return tlndb.ErrNotFound
	}
	if s.replEnabled {
		s.signalRepl()
	}
	idx.Delete(entityID, scope, id)
	return nil
}

// vectorDeleteInTx removes a single vector; found reports whether it
// existed. Shared by the leader write path and follower apply.
func vectorDeleteInTx(tx *bolt.Tx, entityID, scope, id string) (bool, error) {
	data := tx.Bucket([]byte(vecDataBucketPrefix + entityID + ":" + scope))
	if data == nil || data.Get([]byte(id)) == nil {
		return false, nil
	}
	return true, data.Delete([]byte(id))
}

// VectorDropScope removes every vector under (entityID, scope), the
// data bucket, and the registry entry. Returns tlndb.ErrNotFound
// when the scope never existed.
func (s *Store) VectorDropScope(ctx context.Context, entityID, scope string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if entityID == "" || scope == "" {
		return errors.New("bboltstore: VectorDropScope requires non-empty entity + scope")
	}
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}
	var found bool
	err = s.db.Update(func(tx *bolt.Tx) error {
		f, err := vectorDropScopeInTx(tx, entityID, scope)
		if err != nil {
			return err
		}
		found = f
		if f && s.replEnabled {
			if _, err := appendOpLog(tx, &tlndbpb.OpLogEntry{
				Kind:     tlndbpb.OpKind_OP_KIND_VEC_DROP_SCOPE,
				EntityId: entityID,
				Scope:    scope,
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return tlndb.ErrNotFound
	}
	if s.replEnabled {
		s.signalRepl()
	}
	idx.DropScope(entityID, scope)
	return nil
}

// vectorDropScopeInTx removes a scope's registry entry and data bucket;
// found reports whether the scope existed. Shared by leader write and
// follower apply.
func vectorDropScopeInTx(tx *bolt.Tx, entityID, scope string) (bool, error) {
	var found bool
	reg := tx.Bucket([]byte(vecRegistryBucketPrefix + entityID))
	if reg != nil && reg.Get([]byte(scope)) != nil {
		found = true
		if err := reg.Delete([]byte(scope)); err != nil {
			return false, err
		}
	}
	if tx.Bucket([]byte(vecDataBucketPrefix+entityID+":"+scope)) != nil {
		if err := tx.DeleteBucket([]byte(vecDataBucketPrefix + entityID + ":" + scope)); err != nil {
			return false, err
		}
	}
	return found, nil
}

// VectorListScopes returns every scope under entityID. Reads from the
// in-memory index so Count is exact.
func (s *Store) VectorListScopes(ctx context.Context, entityID string) ([]vectorindex.ScopeInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	idx, err := s.vectorIndex()
	if err != nil {
		return nil, err
	}
	return idx.ListScopes(entityID), nil
}

func validateVecArgs(entity, scope, id string) error {
	if entity == "" {
		return errors.New("bboltstore: empty entity")
	}
	if scope == "" {
		return errors.New("bboltstore: empty scope")
	}
	if id == "" {
		return errors.New("bboltstore: empty id")
	}
	return nil
}

// encodeFloat32Slice packs a []float32 as little-endian IEEE-754 bytes.
// 4 bytes per element; total length stays implicit because the scope
// metadata records the dimension.
func encodeFloat32Slice(v []float32) []byte {
	out := make([]byte, 4*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(out[4*i:], math.Float32bits(f))
	}
	return out
}

func decodeFloat32Slice(raw []byte, want int) ([]float32, error) {
	if len(raw)%4 != 0 {
		return nil, fmt.Errorf("bboltstore: vector blob length %d not multiple of 4", len(raw))
	}
	if want > 0 && len(raw)/4 != want {
		return nil, fmt.Errorf("bboltstore: vector blob length %d, want %d floats", len(raw)/4, want)
	}
	out := make([]float32, len(raw)/4)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(raw[4*i:]))
	}
	return out, nil
}
