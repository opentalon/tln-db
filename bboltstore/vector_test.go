package bboltstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/bboltstore"
	"github.com/opentalon/tln-db/vectorindex"
)

func openStore(t *testing.T) *bboltstore.Store {
	t.Helper()
	s, err := bboltstore.Open(filepath.Join(t.TempDir(), "vec.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestVectorInsertSearchRoundtrip(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if err := s.VectorInsert(ctx, "t", "s", string(rune('a'+i)), v, vectorindex.Cosine); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	hits, err := s.VectorSearch(ctx, "t", "s", []float32{1, 0, 0}, 2)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(hits) != 2 || hits[0].ID != "a" {
		t.Fatalf("hits = %v", hits)
	}
}

func TestVectorPersistAcrossRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "persist.db")

	// First open: write a handful of vectors, close.
	s1, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	ctx := context.Background()
	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{1, 1, 0},
	} {
		if err := s1.VectorInsert(ctx, "t", "s", string(rune('a'+i)), v, vectorindex.Cosine); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Second open: rebuild-on-Open must replay every vector.
	s2, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open #2: %v", err)
	}
	defer func() { _ = s2.Close() }()

	scopes, err := s2.VectorListScopes(ctx, "t")
	if err != nil {
		t.Fatalf("ListScopes: %v", err)
	}
	if len(scopes) != 1 || scopes[0].Scope != "s" || scopes[0].Count != 4 || scopes[0].Dim != 3 {
		t.Fatalf("scopes after restart: %+v", scopes)
	}
	hits, err := s2.VectorSearch(ctx, "t", "s", []float32{1, 0, 0}, 1)
	if err != nil {
		t.Fatalf("Search after restart: %v", err)
	}
	if len(hits) != 1 || hits[0].ID != "a" {
		t.Fatalf("nearest after restart: %v", hits)
	}
}

func TestVectorDeleteRemovesFromBboltAndIndex(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	_ = s.VectorInsert(ctx, "t", "s", "a", []float32{1, 0, 0}, vectorindex.Cosine)
	_ = s.VectorInsert(ctx, "t", "s", "b", []float32{0, 1, 0}, vectorindex.Cosine)
	_ = s.VectorInsert(ctx, "t", "s", "c", []float32{0, 0, 1}, vectorindex.Cosine)

	if err := s.VectorDelete(ctx, "t", "s", "b"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// Deleting a missing id surfaces tlndb.ErrNotFound — distinct
	// from "scope doesn't exist" so callers can treat the second case
	// as a configuration error.
	if err := s.VectorDelete(ctx, "t", "s", "missing"); !errors.Is(err, tlndb.ErrNotFound) {
		t.Errorf("Delete missing: want ErrNotFound, got %v", err)
	}

	hits, _ := s.VectorSearch(ctx, "t", "s", []float32{0, 1, 0}, 5)
	for _, h := range hits {
		if h.ID == "b" {
			t.Fatalf("b should be gone, hits = %v", hits)
		}
	}
}

func TestVectorDropScopeClearsAllState(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "drop.db")
	s1, _ := bboltstore.Open(path)
	ctx := context.Background()

	_ = s1.VectorInsert(ctx, "t", "v3", "a", []float32{1, 0, 0}, vectorindex.Cosine)
	_ = s1.VectorInsert(ctx, "t", "v4", "x", []float32{1, 0, 0, 0}, vectorindex.Cosine)

	if err := s1.VectorDropScope(ctx, "t", "v3"); err != nil {
		t.Fatalf("DropScope: %v", err)
	}
	if err := s1.VectorDropScope(ctx, "t", "ghost"); !errors.Is(err, tlndb.ErrNotFound) {
		t.Errorf("DropScope ghost: want ErrNotFound, got %v", err)
	}
	_ = s1.Close()

	// After restart only v4 should remain, AND a new Insert into v3
	// with a different dim must succeed (the lock is gone).
	s2, _ := bboltstore.Open(path)
	defer func() { _ = s2.Close() }()
	scopes, _ := s2.VectorListScopes(ctx, "t")
	if len(scopes) != 1 || scopes[0].Scope != "v4" {
		t.Fatalf("after drop+restart: %+v", scopes)
	}
	if err := s2.VectorInsert(ctx, "t", "v3", "new", []float32{1, 0, 0, 0, 0}, vectorindex.Cosine); err != nil {
		t.Errorf("v3 reinsert with new dim: %v", err)
	}
	if d, _ := s2.VectorListScopes(ctx, "t"); len(d) != 2 || d[0].Scope != "v3" || d[0].Dim != 5 {
		t.Errorf("after reinsert: %+v", d)
	}
}

func TestVectorEuclideanMetricPersisted(t *testing.T) {
	// Scope metric set on first insert sticks across restarts.
	t.Parallel()
	path := filepath.Join(t.TempDir(), "metric.db")
	s1, _ := bboltstore.Open(path)
	ctx := context.Background()
	_ = s1.VectorInsert(ctx, "t", "s", "a", []float32{1, 0}, vectorindex.Euclidean)
	_ = s1.Close()

	s2, _ := bboltstore.Open(path)
	defer func() { _ = s2.Close() }()
	scopes, _ := s2.VectorListScopes(ctx, "t")
	if len(scopes) != 1 || scopes[0].Metric != vectorindex.Euclidean {
		t.Errorf("metric not preserved: %+v", scopes)
	}
}

func TestVectorDimensionMismatchOnRestart(t *testing.T) {
	// Restart preserves the dim lock — second insert with wrong dim
	// must be rejected even after the in-memory index was rebuilt.
	t.Parallel()
	path := filepath.Join(t.TempDir(), "lock.db")
	s1, _ := bboltstore.Open(path)
	ctx := context.Background()
	_ = s1.VectorInsert(ctx, "t", "s", "a", []float32{1, 0, 0}, vectorindex.Cosine)
	_ = s1.Close()

	s2, _ := bboltstore.Open(path)
	defer func() { _ = s2.Close() }()
	err := s2.VectorInsert(ctx, "t", "s", "b", []float32{1, 0}, vectorindex.Cosine)
	if !errors.Is(err, vectorindex.ErrDimensionMismatch) {
		t.Errorf("want ErrDimensionMismatch after restart, got %v", err)
	}
}
