package tlndb

import (
	"context"
	"errors"
	"time"
)

// ErrNotFound is returned by Get when no document exists for the given
// entityID and docID.
var ErrNotFound = errors.New("tlndb: document not found")

// ErrSnapshotRequired is returned when a replication follower requests a
// log position older than the leader's retained window; the follower
// must re-bootstrap from a fresh Snapshot.
var ErrSnapshotRequired = errors.New("tlndb: requested seq older than retained min_seq; snapshot required")

// ErrInvalidEntityID is returned when an entityID is empty or contains a
// reserved character. The colon (":") separates the bucket prefix from
// the tenant name in the on-disk layout, so it cannot appear inside an
// entityID.
var ErrInvalidEntityID = errors.New("tlndb: invalid entity id")

// DocumentStore is the storage primitive tln-db exposes to the rest of
// the system. It stores opaque byte blobs (in practice JSON-encoded
// documents) under a two-level key: entityID (tenant) and docID. Per-
// entity buckets keep tenants strictly isolated.
//
// Backends are expected to be safe for concurrent use. Writes for a
// single entity must be serialised; writes across different entities
// may run in parallel.
type DocumentStore interface {
	// Put writes a document. It overwrites any existing document with
	// the same (entityID, docID).
	Put(ctx context.Context, entityID, docID string, doc []byte) error

	// Get returns the document at (entityID, docID). It returns
	// ErrNotFound when no such document exists.
	Get(ctx context.Context, entityID, docID string) ([]byte, error)

	// Delete removes the document at (entityID, docID). Deleting a
	// missing document is not an error.
	Delete(ctx context.Context, entityID, docID string) error

	// BatchPut writes multiple documents for a single entity in one
	// atomic transaction. Either every document is written or none are.
	BatchPut(ctx context.Context, entityID string, docs map[string][]byte) error

	// Scan visits every document for entityID, calling fn for each one.
	// Iteration halts when fn returns false. The slice passed to fn is
	// only valid for the duration of that call; copy it if you need to
	// retain it.
	Scan(ctx context.Context, entityID string, fn func(docID string, doc []byte) bool) error

	// LastWritten reports when the document at (entityID, docID) was last
	// written, from its updated_at metadata. The bool is false when no
	// such document exists. Granularity is per-document (every Put bumps
	// updated_at), which is the "last asserted" semantics fact-freshness
	// consumers such as the enrich block rely on.
	LastWritten(ctx context.Context, entityID, docID string) (time.Time, bool, error)
}
