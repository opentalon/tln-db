// Package tlndbtest provides a reusable conformance suite for any
// implementation of tlndb.DocumentStore. Each backend (bboltstore
// today, Pebble or in-memory tomorrow) wires the suite into its own
// _test.go with a factory and gets the full contract checked for free.
//
// Specifications enforced by the suite (in declaration order):
//
//  1. PutGetRoundtrip — bytes written by Put are returned by Get
//     unchanged for the same (entityID, docID).
//  2. GetMissingReturnsErrNotFound — Get for an absent (entity, doc)
//     returns tlndb.ErrNotFound via errors.Is, never a nil slice.
//  3. DeleteIdempotent — Delete on a missing doc is a no-op and never
//     errors; deleting twice in a row never errors.
//  4. BatchPutAtomic — every (key, value) in the input map is readable
//     after BatchPut returns nil.
//  5. BatchPutRollbackOnCancelledContext — a context cancelled before
//     BatchPut completes rolls the entire batch back; pre-existing docs
//     remain untouched and the call returns context.Canceled.
//  6. BatchPutRejectsInvalidDocID — an empty docID anywhere in the map
//     rejects the entire batch before any write; other keys in the same
//     batch must not appear after the call.
//  7. ScanVisitsAllAndStops — Scan visits every doc under the given
//     entity exactly once and halts iteration when the callback returns
//     false. The exact visitation order is NOT specified.
//  8. ScanEmptyEntity — Scan over an unknown entity is a no-op (no
//     error, callback not invoked).
//  9. TenantIsolation — writes under one entity are invisible to Get
//     and Scan under another; deleting under one entity does not affect
//     another.
// 10. EmptyEntityIDRejected — Put, Get, Delete, BatchPut, and Scan all
//     reject an empty entityID with a non-nil error.
// 11. EmptyDocIDRejected — Put, Get, Delete, and BatchPut reject an
//     empty docID with a non-nil error.
//
// Backend-specific quirks (snappy compression in bboltstore, the colon
// restriction on entityIDs, the version counter) are NOT part of the
// contract and are tested directly in each backend's package.
package tlndbtest

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	tlndb "github.com/opentalon/tln-db"
)

// Factory builds a fresh, empty DocumentStore for a single subtest. It
// must register any cleanup (closing the store, removing files) via
// t.Cleanup so subtests can run in parallel.
type Factory func(t *testing.T) tlndb.DocumentStore

// Suite runs every conformance test against the store produced by
// factory. Each subtest receives its own store.
func Suite(t *testing.T, factory Factory) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"PutGetRoundtrip", testPutGetRoundtrip},
		{"GetMissingReturnsErrNotFound", testGetMissing},
		{"DeleteIdempotent", testDeleteIdempotent},
		{"BatchPutAtomic", testBatchPutAtomic},
		{"BatchPutRollbackOnCancelledContext", testBatchPutRollback},
		{"BatchPutRejectsInvalidDocID", testBatchPutInvalid},
		{"ScanVisitsAllAndStops", testScan},
		{"ScanEmptyEntity", testScanEmpty},
		{"TenantIsolation", testTenantIsolation},
		{"EmptyEntityIDRejected", testEmptyEntity},
		{"EmptyDocIDRejected", testEmptyDocID},
		{"LastWrittenTracksPut", testLastWritten},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, factory)
		})
	}
}

func testPutGetRoundtrip(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	doc := []byte(`{"hello":"world","n":42}`)
	if err := s.Put(ctx, "tenant-a", "doc-1", doc); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := s.Get(ctx, "tenant-a", "doc-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, doc) {
		t.Fatalf("Get returned %q, want %q", got, doc)
	}
}

func testGetMissing(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	tests := []struct {
		name           string
		seedEntity     string
		seedDocID      string
		queryEntity    string
		queryDocID     string
	}{
		{"empty store", "", "", "tenant-a", "doc-1"},
		{"wrong entity", "tenant-a", "doc-1", "tenant-b", "doc-1"},
		{"wrong doc id", "tenant-a", "doc-1", "tenant-a", "doc-2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.seedEntity != "" {
				if err := s.Put(ctx, tc.seedEntity, tc.seedDocID, []byte("{}")); err != nil {
					t.Fatalf("Put seed: %v", err)
				}
			}
			_, err := s.Get(ctx, tc.queryEntity, tc.queryDocID)
			if !errors.Is(err, tlndb.ErrNotFound) {
				t.Fatalf("Get: got %v, want ErrNotFound", err)
			}
		})
	}
}

func testDeleteIdempotent(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete on empty store: %v", err)
	}
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte("{}")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete first: %v", err)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete second: %v", err)
	}
	if _, err := s.Get(ctx, "tenant-a", "doc-1"); !errors.Is(err, tlndb.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func testBatchPutAtomic(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	docs := map[string][]byte{
		"a": []byte(`{"v":"a"}`),
		"b": []byte(`{"v":"b"}`),
		"c": []byte(`{"v":"c"}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	for k, want := range docs {
		got, err := s.Get(ctx, "tenant-a", k)
		if err != nil {
			t.Fatalf("Get %q: %v", k, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get %q = %q, want %q", k, got, want)
		}
	}
}

func testBatchPutRollback(t *testing.T, factory Factory) {
	s := factory(t)
	if err := s.Put(context.Background(), "tenant-a", "existing", []byte(`{"keep":true}`)); err != nil {
		t.Fatalf("Put existing: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	docs := map[string][]byte{"x": []byte("{}"), "y": []byte("{}")}
	err := s.BatchPut(ctx, "tenant-a", docs)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("BatchPut: got %v, want context.Canceled", err)
	}
	for _, k := range []string{"x", "y"} {
		if _, err := s.Get(context.Background(), "tenant-a", k); !errors.Is(err, tlndb.ErrNotFound) {
			t.Fatalf("Get %q after rollback: got %v, want ErrNotFound", k, err)
		}
	}
	if _, err := s.Get(context.Background(), "tenant-a", "existing"); err != nil {
		t.Fatalf("Get existing after rollback: %v", err)
	}
}

func testBatchPutInvalid(t *testing.T, factory Factory) {
	s := factory(t)
	docs := map[string][]byte{"a": []byte("{}"), "": []byte("{}")}
	if err := s.BatchPut(context.Background(), "tenant-a", docs); err == nil {
		t.Fatal("BatchPut: expected error for empty docID")
	}
	if _, err := s.Get(context.Background(), "tenant-a", "a"); !errors.Is(err, tlndb.ErrNotFound) {
		t.Fatalf("'a' should not have been written: %v", err)
	}
}

func testScan(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	docs := map[string][]byte{
		"a": []byte(`{"v":1}`),
		"b": []byte(`{"v":2}`),
		"c": []byte(`{"v":3}`),
		"d": []byte(`{"v":4}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	var visited []string
	if err := s.Scan(ctx, "tenant-a", func(id string, _ []byte) bool {
		visited = append(visited, id)
		return true
	}); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	sort.Strings(visited)
	want := []string{"a", "b", "c", "d"}
	if !equalStrings(visited, want) {
		t.Fatalf("Scan visited %v, want %v", visited, want)
	}
	var stopped []string
	if err := s.Scan(ctx, "tenant-a", func(id string, _ []byte) bool {
		stopped = append(stopped, id)
		return len(stopped) < 2
	}); err != nil {
		t.Fatalf("Scan early-exit: %v", err)
	}
	if len(stopped) != 2 {
		t.Fatalf("Scan should have stopped after 2, visited %d", len(stopped))
	}
}

func testScanEmpty(t *testing.T, factory Factory) {
	s := factory(t)
	count := 0
	err := s.Scan(context.Background(), "nobody", func(string, []byte) bool {
		count++
		return true
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if count != 0 {
		t.Fatalf("Scan over empty entity visited %d docs", count)
	}
}

func testTenantIsolation(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`"a"`)); err != nil {
		t.Fatalf("Put tenant-a: %v", err)
	}
	if err := s.Put(ctx, "tenant-b", "doc-1", []byte(`"b"`)); err != nil {
		t.Fatalf("Put tenant-b: %v", err)
	}
	got, err := s.Get(ctx, "tenant-a", "doc-1")
	if err != nil {
		t.Fatalf("Get tenant-a: %v", err)
	}
	if string(got) != `"a"` {
		t.Fatalf("tenant-a saw %q, want %q", got, `"a"`)
	}
	var seenInA []string
	_ = s.Scan(ctx, "tenant-a", func(id string, _ []byte) bool {
		seenInA = append(seenInA, id)
		return true
	})
	if len(seenInA) != 1 || seenInA[0] != "doc-1" {
		t.Fatalf("tenant-a scan: %v, want [doc-1]", seenInA)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete tenant-a: %v", err)
	}
	if _, err := s.Get(ctx, "tenant-b", "doc-1"); err != nil {
		t.Fatalf("tenant-b's doc was affected by tenant-a delete: %v", err)
	}
}

func testEmptyEntity(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	if err := s.Put(ctx, "", "doc-1", []byte("{}")); err == nil {
		t.Fatal("Put: expected error for empty entityID")
	}
	if _, err := s.Get(ctx, "", "doc-1"); err == nil {
		t.Fatal("Get: expected error for empty entityID")
	}
	if err := s.Delete(ctx, "", "doc-1"); err == nil {
		t.Fatal("Delete: expected error for empty entityID")
	}
	if err := s.BatchPut(ctx, "", map[string][]byte{"a": []byte("{}")}); err == nil {
		t.Fatal("BatchPut: expected error for empty entityID")
	}
	if err := s.Scan(ctx, "", func(string, []byte) bool { return true }); err == nil {
		t.Fatal("Scan: expected error for empty entityID")
	}
}

func testEmptyDocID(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()
	if err := s.Put(ctx, "tenant-a", "", []byte("{}")); err == nil {
		t.Fatal("Put: expected error for empty docID")
	}
	if _, err := s.Get(ctx, "tenant-a", ""); err == nil {
		t.Fatal("Get: expected error for empty docID")
	}
	if err := s.Delete(ctx, "tenant-a", ""); err == nil {
		t.Fatal("Delete: expected error for empty docID")
	}
	if err := s.BatchPut(ctx, "tenant-a", map[string][]byte{"": []byte("{}")}); err == nil {
		t.Fatal("BatchPut: expected error for empty docID")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// testLastWritten verifies the Freshness contract: a written doc reports
// a last-written time in [before, after]; a missing doc reports found ==
// false; and a deleted doc no longer reports a time.
func testLastWritten(t *testing.T, factory Factory) {
	s := factory(t)
	ctx := context.Background()

	if _, ok, err := s.LastWritten(ctx, "tenant-a", "missing"); err != nil || ok {
		t.Fatalf("LastWritten(missing): got ok=%v err=%v, want ok=false, nil", ok, err)
	}

	before := time.Now().Add(-time.Second)
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"n":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	after := time.Now().Add(time.Second)

	got, ok, err := s.LastWritten(ctx, "tenant-a", "doc-1")
	if err != nil || !ok {
		t.Fatalf("LastWritten after Put: got ok=%v err=%v, want ok=true, nil", ok, err)
	}
	if got.Before(before) || got.After(after) {
		t.Errorf("LastWritten time %v outside [%v, %v]", got, before, after)
	}

	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, ok, _ := s.LastWritten(ctx, "tenant-a", "doc-1"); ok {
		t.Error("LastWritten after Delete: got ok=true, want false")
	}
}
