package bboltstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	talondb "github.com/opentalon/talon-db"
	"github.com/opentalon/talon-db/proto/talondbpb"
	"github.com/opentalon/talon-db/vectorindex"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

// Option configures a Store at Open time.
type Option func(*Store)

// WithReplication enables the durable replication op-log. Every mutation
// is appended to the log inside its write transaction so a follower can
// bootstrap via Snapshot and tail via Replicate. retention bounds the
// number of most-recent entries kept (0 = keep everything). Standalone
// stores (the default) never write the op-log.
func WithReplication(retention uint64) Option {
	return func(s *Store) {
		s.replEnabled = true
		s.replRetention = retention
	}
}

// ErrSnapshotRequired is re-exported from the root package; TailOpLog
// returns it when the requested seq is older than the retained min_seq.
var ErrSnapshotRequired = talondb.ErrSnapshotRequired

var errStopTail = errors.New("stop tail batch")

// ReplicationEnabled reports whether this store maintains the op-log.
func (s *Store) ReplicationEnabled() bool { return s.replEnabled }

// CurrentSeq returns the local maximum op-log seq (0 when empty).
func (s *Store) CurrentSeq() uint64 {
	var v uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		if n := readMetaUint64(tx, oplogMetaBucket, nextSeqKey); n > 0 {
			v = n - 1
		}
		return nil
	})
	return v
}

// MinSeq returns the earliest retained op-log seq (0 when empty).
func (s *Store) MinSeq() uint64 {
	var v uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		v = readMetaUint64(tx, oplogMetaBucket, minSeqKey)
		return nil
	})
	return v
}

// SetAppliedSeq records the last applied op-log seq. Used after a
// follower installs a bootstrap snapshot so streaming resumes at the
// snapshot point.
func (s *Store) SetAppliedSeq(seq uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		return writeMetaUint64(tx, replMetaBucket, appliedSeqKey, seq)
	})
}

// AppliedSeq returns the last op-log seq a follower has applied (0 on a
// leader or fresh store).
func (s *Store) AppliedSeq() uint64 {
	var v uint64
	_ = s.db.View(func(tx *bolt.Tx) error {
		v = readMetaUint64(tx, replMetaBucket, appliedSeqKey)
		return nil
	})
	return v
}

// signalRepl wakes any TailOpLog waiters after a committed write.
func (s *Store) signalRepl() {
	s.replMu.Lock()
	close(s.replNotify)
	s.replNotify = make(chan struct{})
	s.replMu.Unlock()
}

func (s *Store) replWaitCh() <-chan struct{} {
	s.replMu.Lock()
	defer s.replMu.Unlock()
	return s.replNotify
}

func (s *Store) trimLoop() {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-t.C:
			_ = s.trimOpLog(s.replRetention)
		}
	}
}

// ---------- shared in-tx write helpers (leader write + follower apply) ----------

func readDocInTx(tx *bolt.Tx, entityID, docID string) ([]byte, error) {
	b := tx.Bucket([]byte(docsBucketPrefix + entityID))
	if b == nil {
		return nil, nil
	}
	raw := b.Get([]byte(docID))
	if raw == nil {
		return nil, nil
	}
	return snappy.Decode(nil, raw)
}

// writeDocInTx persists a document with its authoritative meta and
// rebuilds all derived indexes + per-doc history. meta is written
// verbatim (no clock/version recompute) so leader and follower converge
// byte-for-byte.
func writeDocInTx(tx *bolt.Tx, entityID, docID string, oldDoc, doc []byte, m docMeta) error {
	docsBucket, err := tx.CreateBucketIfNotExists([]byte(docsBucketPrefix + entityID))
	if err != nil {
		return err
	}
	metaBucket, err := tx.CreateBucketIfNotExists([]byte(metaBucketPrefix + entityID))
	if err != nil {
		return err
	}
	metaBytes, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("bboltstore: encode meta for %q: %w", docID, err)
	}
	if err := metaBucket.Put([]byte(docID), metaBytes); err != nil {
		return err
	}
	if err := docsBucket.Put([]byte(docID), snappy.Encode(nil, doc)); err != nil {
		return err
	}
	if err := indexDocOnPut(tx, entityID, docID, oldDoc, doc); err != nil {
		return err
	}
	return appendDocHistory(tx, entityID, docID, docVersion{At: m.UpdatedAt, Data: append([]byte(nil), doc...)})
}

func deleteDocInTx(tx *bolt.Tx, entityID, docID string, oldDoc []byte, at int64) error {
	if b := tx.Bucket([]byte(docsBucketPrefix + entityID)); b != nil {
		if err := b.Delete([]byte(docID)); err != nil {
			return err
		}
	}
	if b := tx.Bucket([]byte(metaBucketPrefix + entityID)); b != nil {
		if err := b.Delete([]byte(docID)); err != nil {
			return err
		}
	}
	if err := indexDocOnDelete(tx, entityID, docID, oldDoc); err != nil {
		return err
	}
	return appendDocHistory(tx, entityID, docID, docVersion{At: at, Deleted: true})
}

// ---------- follower apply ----------

// ApplyEntry applies a single replicated op-log entry to the local
// store, atomically advancing applied_seq. Intended for followers.
func (s *Store) ApplyEntry(entry *talondbpb.OpLogEntry) error {
	switch entry.Kind {
	case talondbpb.OpKind_OP_KIND_DOC_ASSERT, talondbpb.OpKind_OP_KIND_DOC_CHANGE:
		return s.applyDocPut(entry)
	case talondbpb.OpKind_OP_KIND_DOC_RETRACT:
		return s.applyDocDelete(entry)
	case talondbpb.OpKind_OP_KIND_VEC_INSERT:
		return s.applyVecInsert(entry)
	case talondbpb.OpKind_OP_KIND_VEC_DELETE:
		return s.applyVecDelete(entry)
	case talondbpb.OpKind_OP_KIND_VEC_DROP_SCOPE:
		return s.applyVecDropScope(entry)
	default:
		return fmt.Errorf("bboltstore: unknown oplog kind %v (seq %d)", entry.Kind, entry.Seq)
	}
}

// commitEntryInTx records the entry in the local op-log at its seq and
// advances applied_seq — all inside the caller's write tx.
func (s *Store) commitEntryInTx(tx *bolt.Tx, entry *talondbpb.OpLogEntry) error {
	if err := putOpLogEntryAt(tx, entry); err != nil {
		return err
	}
	return writeMetaUint64(tx, replMetaBucket, appliedSeqKey, entry.Seq)
}

func (s *Store) applyDocPut(entry *talondbpb.OpLogEntry) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		oldDoc, err := readDocInTx(tx, entry.EntityId, entry.DocId)
		if err != nil {
			return err
		}
		m := docMeta{CreatedAt: entry.CreatedAt, UpdatedAt: entry.UpdatedAt, Version: entry.Version}
		if err := writeDocInTx(tx, entry.EntityId, entry.DocId, oldDoc, entry.NewDoc, m); err != nil {
			return err
		}
		return s.commitEntryInTx(tx, entry)
	})
	if err == nil {
		s.signalRepl()
	}
	return err
}

func (s *Store) applyDocDelete(entry *talondbpb.OpLogEntry) error {
	err := s.db.Update(func(tx *bolt.Tx) error {
		oldDoc, err := readDocInTx(tx, entry.EntityId, entry.DocId)
		if err != nil {
			return err
		}
		if oldDoc != nil {
			if err := deleteDocInTx(tx, entry.EntityId, entry.DocId, oldDoc, entry.AtUnixNanos); err != nil {
				return err
			}
		}
		return s.commitEntryInTx(tx, entry)
	})
	if err == nil {
		s.signalRepl()
	}
	return err
}

func (s *Store) applyVecInsert(entry *talondbpb.OpLogEntry) error {
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}
	metric := vectorindex.Metric(entry.Metric)
	err = s.db.Update(func(tx *bolt.Tx) error {
		if _, err := vectorInsertInTx(tx, entry.EntityId, entry.Scope, entry.DocId, entry.Vector, metric); err != nil {
			return err
		}
		return s.commitEntryInTx(tx, entry)
	})
	if err != nil {
		return err
	}
	s.signalRepl()
	return idx.Insert(entry.EntityId, entry.Scope, entry.DocId, entry.Vector, metric)
}

func (s *Store) applyVecDelete(entry *talondbpb.OpLogEntry) error {
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if _, err := vectorDeleteInTx(tx, entry.EntityId, entry.Scope, entry.DocId); err != nil {
			return err
		}
		return s.commitEntryInTx(tx, entry)
	})
	if err != nil {
		return err
	}
	s.signalRepl()
	idx.Delete(entry.EntityId, entry.Scope, entry.DocId)
	return nil
}

func (s *Store) applyVecDropScope(entry *talondbpb.OpLogEntry) error {
	idx, err := s.vectorIndex()
	if err != nil {
		return err
	}
	err = s.db.Update(func(tx *bolt.Tx) error {
		if _, err := vectorDropScopeInTx(tx, entry.EntityId, entry.Scope); err != nil {
			return err
		}
		return s.commitEntryInTx(tx, entry)
	})
	if err != nil {
		return err
	}
	s.signalRepl()
	idx.DropScope(entry.EntityId, entry.Scope)
	return nil
}

// ---------- snapshot + tail (leader serving) ----------

// WriteSnapshot streams a consistent copy of the database to w. onSeq,
// when non-nil, is invoked with the snapshot's op-log seq before any
// file bytes are written (both observed within the same read tx, so the
// seq matches the copy). Used to bootstrap a fresh follower.
func (s *Store) WriteSnapshot(w io.Writer, onSeq func(uint64) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		var seq uint64
		if n := readMetaUint64(tx, oplogMetaBucket, nextSeqKey); n > 0 {
			seq = n - 1
		}
		if onSeq != nil {
			if err := onSeq(seq); err != nil {
				return err
			}
		}
		_, err := tx.WriteTo(w)
		return err
	})
}

// ReadOpLog invokes fn for each op-log entry with seq >= fromSeq, in
// order, without following live. Handy for debugging and tests.
func (s *Store) ReadOpLog(fromSeq uint64, fn func(*talondbpb.OpLogEntry) error) error {
	return s.db.View(func(tx *bolt.Tx) error {
		_, err := readOpLogFrom(tx, fromSeq, fn)
		return err
	})
}

// TailOpLog streams op-log entries with seq >= fromSeq to fn, in order,
// then follows live until ctx is done. Entries are read in bounded
// batches so the read transaction is never held across a network send.
func (s *Store) TailOpLog(ctx context.Context, fromSeq uint64, fn func(*talondbpb.OpLogEntry) error) error {
	if min := s.MinSeq(); min > 0 && fromSeq < min {
		return ErrSnapshotRequired
	}
	from := fromSeq
	if from == 0 {
		from = 1
	}
	const batchSize = 512
	for {
		ch := s.replWaitCh()

		var batch []*talondbpb.OpLogEntry
		err := s.db.View(func(tx *bolt.Tx) error {
			_, e := readOpLogFrom(tx, from, func(entry *talondbpb.OpLogEntry) error {
				batch = append(batch, entry)
				if len(batch) >= batchSize {
					return errStopTail
				}
				return nil
			})
			if errors.Is(e, errStopTail) {
				return nil
			}
			return e
		})
		if err != nil {
			return err
		}

		for _, entry := range batch {
			if err := fn(entry); err != nil {
				return err
			}
			from = entry.Seq + 1
		}
		if len(batch) > 0 {
			continue // more may be available immediately
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-time.After(15 * time.Second):
		}
	}
}
