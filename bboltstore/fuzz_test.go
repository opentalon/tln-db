package bboltstore_test

// Fuzz tests for entityID / docID validation. The contract we protect:
//
//   1. Put must never panic, regardless of input.
//   2. Put must reject empty entityIDs and empty docIDs with an error.
//   3. For all *valid* inputs (non-empty entity without ':', non-empty
//      docID), the value written must be retrievable byte-for-byte by
//      a subsequent Get.
//   4. The bbolt file must remain usable across operations — no
//      transaction must leave the database in a state where the next
//      Open or Get fails.
//
// What this catches: encoding edge cases (NUL bytes, invalid UTF-8,
// long IDs, multi-byte runes, embedded newlines) that could trip up
// bbolt bucket-name handling or our colon-validation heuristic.
//
// Run with:
//   go test -fuzz=FuzzPutGetRoundtrip -fuzztime=30s -run=^$ ./bboltstore
//   go test -fuzz=FuzzValidationRejection -fuzztime=30s -run=^$ ./bboltstore
//
// Seed corpus is intentionally small; -fuzz expands it.

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/bboltstore"
)

func fuzzStore(f *testing.F) *bboltstore.Store {
	f.Helper()
	path := filepath.Join(f.TempDir(), "fuzz.db")
	s, err := bboltstore.Open(path)
	if err != nil {
		f.Fatalf("Open: %v", err)
	}
	f.Cleanup(func() { _ = s.Close() })
	return s
}

// FuzzPutGetRoundtrip explores valid inputs: non-empty entity without
// ':', non-empty docID. It asserts the write is durable across a
// snappy round-trip and visible from Get.
func FuzzPutGetRoundtrip(f *testing.F) {
	f.Add("tenant-a", "doc-1", []byte("{}"))
	f.Add("t", "d", []byte(""))
	f.Add("tenant with spaces", "doc/with/slashes", []byte("{\"k\":\"v\"}"))
	f.Add("emoji-🎯", "doc-α", []byte("payload"))
	f.Add(strings.Repeat("x", 1024), "long-id-"+strings.Repeat("z", 256), []byte("x"))

	s := fuzzStore(f)
	ctx := context.Background()

	f.Fuzz(func(t *testing.T, entityID, docID string, doc []byte) {
		if entityID == "" || strings.ContainsRune(entityID, ':') || docID == "" {
			t.Skip()
		}
		if err := s.Put(ctx, entityID, docID, doc); err != nil {
			t.Fatalf("Put rejected valid input (entity=%q doc=%q): %v", entityID, docID, err)
		}
		got, err := s.Get(ctx, entityID, docID)
		if err != nil {
			t.Fatalf("Get after Put (entity=%q doc=%q): %v", entityID, docID, err)
		}
		if !bytes.Equal(got, doc) {
			t.Fatalf("roundtrip mismatch: got %q, want %q", got, doc)
		}
	})
}

// FuzzValidationRejection explores invalid inputs: empty entity, empty
// docID, or colons in the entity. Every such call must return a
// non-nil error and must not panic. A subsequent Put with a valid
// entity must still succeed — i.e. failed validation must not corrupt
// the store.
func FuzzValidationRejection(f *testing.F) {
	f.Add("", "doc-1")
	f.Add("tenant-a", "")
	f.Add("a:b", "doc-1")
	f.Add(":", "doc-1")
	f.Add("a::b", "doc-1")
	f.Add("", "")

	s := fuzzStore(f)
	ctx := context.Background()

	f.Fuzz(func(t *testing.T, entityID, docID string) {
		expectErr := entityID == "" || strings.ContainsRune(entityID, ':') || docID == ""
		if !expectErr {
			t.Skip()
		}
		err := s.Put(ctx, entityID, docID, []byte("{}"))
		if err == nil {
			t.Fatalf("Put should have rejected (entity=%q doc=%q)", entityID, docID)
		}
		// The store must remain functional after a rejected call.
		if err := s.Put(ctx, "tenant-canary", "canary", []byte("ok")); err != nil {
			t.Fatalf("canary Put after rejected call failed: %v", err)
		}
		got, err := s.Get(ctx, "tenant-canary", "canary")
		if err != nil {
			t.Fatalf("canary Get after rejected call: %v", err)
		}
		if !bytes.Equal(got, []byte("ok")) {
			t.Fatalf("canary roundtrip: got %q, want %q", got, "ok")
		}
		// Sanity: the rejected entity is still not retrievable.
		if entityID != "" && !strings.ContainsRune(entityID, ':') && docID != "" {
			return
		}
		_, gerr := s.Get(ctx, entityID, docID)
		if gerr == nil {
			t.Fatalf("Get of invalid (entity=%q doc=%q) should have errored", entityID, docID)
		}
		if errors.Is(gerr, tlndb.ErrNotFound) && entityID != "" && !strings.ContainsRune(entityID, ':') {
			// ErrNotFound is acceptable for a valid-entity-but-empty-docID case
			// where validation might bottom out at the docID check.
			return
		}
	})
}
