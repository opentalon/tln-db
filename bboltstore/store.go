// Package bboltstore provides the bbolt-backed implementation of
// tlndb.DocumentStore. Documents are snappy-compressed and stored in
// per-tenant buckets; metadata (created_at, updated_at, version) lives
// alongside in a sibling bucket and is updated in the same transaction.
package bboltstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/proto/tlndbpb"
	"github.com/opentalon/tln-db/vectorindex"

	"github.com/golang/snappy"
	bolt "go.etcd.io/bbolt"
)

const (
	docsBucketPrefix = "docs:"
	metaBucketPrefix = "meta:"
)

// Store is a bbolt-backed DocumentStore.
type Store struct {
	db     *bolt.DB
	now    func() time.Time
	events tlndb.EventEmitter

	// Vector index — lazily rebuilt from the vec_registry / vec_data
	// buckets on first access. vecOnce gates the rebuild; vecLoadErr
	// is sticky so callers see the failure on every subsequent call.
	vecOnce    sync.Once
	vectors    *vectorindex.Index
	vecLoadErr error

	// Replication op-log (enabled via WithReplication). replNotify is a
	// broadcast channel closed+recreated after every committed write so
	// TailOpLog waiters wake without a lost-wakeup race.
	replEnabled   bool
	replRetention uint64
	replMu        sync.Mutex
	replNotify    chan struct{}
	done          chan struct{}
	closeOnce     sync.Once
}

// Open opens (or creates) a bbolt database at path and returns a Store.
// Callers must call Close when done. Pass WithReplication to enable the
// durable replication op-log.
func Open(path string, opts ...Option) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("bboltstore: open %q: %w", path, err)
	}
	s := &Store{db: db, now: time.Now, replNotify: make(chan struct{})}
	for _, opt := range opts {
		opt(s)
	}
	if s.replEnabled && s.replRetention > 0 {
		s.done = make(chan struct{})
		go s.trimLoop()
	}
	return s, nil
}

// Events returns the mutation event emitter. Subscribers receive a
// tlndb.MutationEvent after every committed Put / Delete /
// BatchPut. Emission is post-commit and runs outside the bbolt write
// lock so subscribers may call back into the store.
func (s *Store) Events() *tlndb.EventEmitter {
	return &s.events
}

// SetClock overrides the clock used to stamp document created_at /
// updated_at times. Intended for tests that need deterministic
// timestamps; production leaves the default time.Now.
func (s *Store) SetClock(now func() time.Time) {
	if now == nil {
		now = time.Now
	}
	s.now = now
}

// LastWritten reports when the document at (entityID, docID) was last
// written, from its stored updated_at metadata. The bool is false when
// no such document exists. Granularity is per-document: every Put bumps
// updated_at (even a no-op overwrite), which is the "last asserted"
// semantics fact-freshness needs.
func (s *Store) LastWritten(ctx context.Context, entityID, docID string) (time.Time, bool, error) {
	if err := validateIDs(entityID, docID); err != nil {
		return time.Time{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return time.Time{}, false, err
	}
	var (
		out   time.Time
		found bool
	)
	err := s.db.View(func(tx *bolt.Tx) error {
		mb := tx.Bucket([]byte(metaBucketPrefix + entityID))
		if mb == nil {
			return nil
		}
		raw := mb.Get([]byte(docID))
		if raw == nil {
			return nil
		}
		var m docMeta
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("bboltstore: decode meta for %q: %w", docID, err)
		}
		out = time.Unix(0, m.UpdatedAt).UTC()
		found = true
		return nil
	})
	if err != nil {
		return time.Time{}, false, err
	}
	return out, found, nil
}

// Close closes the underlying bbolt database.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		if s.done != nil {
			close(s.done)
		}
	})
	return s.db.Close()
}

// docMeta is the metadata stored alongside each document.
type docMeta struct {
	CreatedAt int64 `json:"created_at"`
	UpdatedAt int64 `json:"updated_at"`
	Version   int64 `json:"version"`
}

// Put writes a document. It overwrites any existing document with the
// same (entityID, docID) and bumps the version counter. On successful
// commit, fires one MutationEvent (Assert for a fresh doc, Change for
// an overwrite).
func (s *Store) Put(ctx context.Context, entityID, docID string, doc []byte) error {
	if err := validateIDs(entityID, docID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var pending []tlndb.MutationEvent
	err := s.db.Update(func(tx *bolt.Tx) error {
		ev, err := s.putInTxEvents(tx, entityID, docID, doc)
		if err != nil {
			return err
		}
		if ev != nil {
			pending = append(pending, *ev)
		}
		return nil
	})
	if err == nil {
		for _, ev := range pending {
			s.events.Emit(ctx, ev)
		}
		if s.replEnabled {
			s.signalRepl()
		}
	}
	return err
}

// Get returns the document at (entityID, docID), decompressed.
func (s *Store) Get(ctx context.Context, entityID, docID string) ([]byte, error) {
	if err := validateIDs(entityID, docID); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		docs := tx.Bucket([]byte(docsBucketPrefix + entityID))
		if docs == nil {
			return tlndb.ErrNotFound
		}
		raw := docs.Get([]byte(docID))
		if raw == nil {
			return tlndb.ErrNotFound
		}
		decoded, err := snappy.Decode(nil, raw)
		if err != nil {
			return fmt.Errorf("bboltstore: snappy decode: %w", err)
		}
		out = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Delete removes the document at (entityID, docID). Deleting a missing
// document is not an error and emits no event. On successful commit
// of a real removal, fires one Retract MutationEvent.
func (s *Store) Delete(ctx context.Context, entityID, docID string) error {
	if err := validateIDs(entityID, docID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var pending *tlndb.MutationEvent
	err := s.db.Update(func(tx *bolt.Tx) error {
		oldDoc, err := readDocInTx(tx, entityID, docID)
		if err != nil {
			return fmt.Errorf("bboltstore: decode prior doc for %q: %w", docID, err)
		}
		if oldDoc == nil {
			return nil // deleting a missing document is a no-op
		}
		delAt := s.now().UnixNano()
		if err := deleteDocInTx(tx, entityID, docID, oldDoc, delAt); err != nil {
			return err
		}
		if s.replEnabled {
			if _, err := appendOpLog(tx, &tlndbpb.OpLogEntry{
				Kind:        tlndbpb.OpKind_OP_KIND_DOC_RETRACT,
				EntityId:    entityID,
				DocId:       docID,
				AtUnixNanos: delAt,
			}); err != nil {
				return err
			}
		}
		pending = &tlndb.MutationEvent{
			Kind:        tlndb.EventRetract,
			EntityID:    entityID,
			DocID:       docID,
			OldDoc:      oldDoc,
			AtUnixNanos: delAt,
		}
		return nil
	})
	if err == nil {
		if pending != nil {
			s.events.Emit(ctx, *pending)
		}
		if s.replEnabled {
			s.signalRepl()
		}
	}
	return err
}

// BatchPut writes multiple documents for a single entity in one atomic
// transaction. If any document is invalid or ctx is cancelled mid-batch,
// no documents are written and no events fire. On successful commit,
// one MutationEvent per doc is emitted in map-iteration order.
func (s *Store) BatchPut(ctx context.Context, entityID string, docs map[string][]byte) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	for docID := range docs {
		if err := validateDocID(docID); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var pending []tlndb.MutationEvent
	err := s.db.Update(func(tx *bolt.Tx) error {
		for docID, doc := range docs {
			if err := ctx.Err(); err != nil {
				return err
			}
			ev, err := s.putInTxEvents(tx, entityID, docID, doc)
			if err != nil {
				return err
			}
			if ev != nil {
				pending = append(pending, *ev)
			}
		}
		return nil
	})
	if err == nil {
		for _, ev := range pending {
			s.events.Emit(ctx, ev)
		}
		if s.replEnabled {
			s.signalRepl()
		}
	}
	return err
}

// Scan visits every document for entityID. Iteration halts when fn
// returns false. The byte slice passed to fn is only valid for the
// duration of the call.
func (s *Store) Scan(ctx context.Context, entityID string, fn func(docID string, doc []byte) bool) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		docs := tx.Bucket([]byte(docsBucketPrefix + entityID))
		if docs == nil {
			return nil
		}
		return docs.ForEach(func(k, v []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			decoded, err := snappy.Decode(nil, v)
			if err != nil {
				return fmt.Errorf("bboltstore: snappy decode at %q: %w", string(k), err)
			}
			if !fn(string(k), decoded) {
				return errStopScan
			}
			return nil
		})
	})
	if errors.Is(err, errStopScan) {
		return nil
	}
	return err
}

var errStopScan = errors.New("stop scan")

// putInTxEvents writes the document + meta + index updates and
// returns the MutationEvent that should be emitted post-commit (or
// nil when the write is a no-op, e.g. an identical re-Put — currently
// always non-nil because we always bump the version counter even on
// identical bytes; refine if we ever add idempotency).
func (s *Store) putInTxEvents(tx *bolt.Tx, entityID, docID string, doc []byte) (*tlndb.MutationEvent, error) {
	// Capture the prior doc bytes (if any) for index delta computation
	// and the OldDoc field of the MutationEvent.
	oldDoc, err := readDocInTx(tx, entityID, docID)
	if err != nil {
		return nil, fmt.Errorf("bboltstore: decode prior doc for %q: %w", docID, err)
	}

	now := s.now().UnixNano()
	m := docMeta{CreatedAt: now, UpdatedAt: now, Version: 1}
	if mb := tx.Bucket([]byte(metaBucketPrefix + entityID)); mb != nil {
		if existing := mb.Get([]byte(docID)); existing != nil {
			if err := json.Unmarshal(existing, &m); err != nil {
				return nil, fmt.Errorf("bboltstore: decode meta for %q: %w", docID, err)
			}
			m.UpdatedAt = now
			m.Version++
		}
	}

	if err := writeDocInTx(tx, entityID, docID, oldDoc, doc, m); err != nil {
		return nil, err
	}

	kind := tlndb.EventAssert
	opKind := tlndbpb.OpKind_OP_KIND_DOC_ASSERT
	if oldDoc != nil {
		kind = tlndb.EventChange
		opKind = tlndbpb.OpKind_OP_KIND_DOC_CHANGE
	}

	if s.replEnabled {
		if _, err := appendOpLog(tx, &tlndbpb.OpLogEntry{
			Kind:        opKind,
			EntityId:    entityID,
			DocId:       docID,
			NewDoc:      append([]byte(nil), doc...),
			CreatedAt:   m.CreatedAt,
			UpdatedAt:   m.UpdatedAt,
			Version:     m.Version,
			AtUnixNanos: now,
		}); err != nil {
			return nil, err
		}
	}

	return &tlndb.MutationEvent{
		Kind:        kind,
		EntityID:    entityID,
		DocID:       docID,
		OldDoc:      oldDoc,
		NewDoc:      append([]byte(nil), doc...),
		AtUnixNanos: now,
	}, nil
}

func validateIDs(entityID, docID string) error {
	if err := validateEntityID(entityID); err != nil {
		return err
	}
	return validateDocID(docID)
}

func validateEntityID(entityID string) error {
	if entityID == "" {
		return fmt.Errorf("%w: empty", tlndb.ErrInvalidEntityID)
	}
	if strings.Contains(entityID, ":") {
		return fmt.Errorf("%w: contains reserved character ':'", tlndb.ErrInvalidEntityID)
	}
	return nil
}

func validateDocID(docID string) error {
	if docID == "" {
		return errors.New("bboltstore: docID must not be empty")
	}
	return nil
}

var _ tlndb.DocumentStore = (*Store)(nil)
