package bboltstore_test

// Benchmarks establish a baseline for the document-store hot paths
// before the index engine (#27) layers indexing work onto every Put.
// They are deliberately simple — no realistic write skew, no contention
// — because the goal is to detect regressions, not to predict
// production throughput.
//
// Spec:
//
//   BenchmarkPut       — single Put, 256-byte doc, fresh store per
//                        benchmark. Measures the bbolt-Update +
//                        snappy-encode + meta-write cost per op.
//   BenchmarkGet       — single Get against a 1k-doc prepopulated
//                        store. Measures bbolt-View + snappy-decode.
//   BenchmarkBatchPut  — 100 docs per BatchPut call, 256-byte docs.
//                        Reports ns/op for the batch; divide by 100
//                        for per-doc cost. Compare to BenchmarkPut to
//                        see the batching speedup.
//   BenchmarkScan      — full Scan over a 1k-doc prepopulated store,
//                        callback is a no-op. Measures snappy-decode
//                        cost dominating bucket iteration.
//
// Run with:
//   go test -bench=. -benchmem -run=^$ ./bboltstore
//
// To keep CI fast these benchmarks are NOT part of `go test ./...`
// (Go runs benchmarks only with -bench). The CI workflow does not
// invoke them.

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/opentalon/tln-db/bboltstore"
)

func benchOpen(b *testing.B) *bboltstore.Store {
	b.Helper()
	path := filepath.Join(b.TempDir(), "bench.db")
	s, err := bboltstore.Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	b.Cleanup(func() { _ = s.Close() })
	return s
}

// fakeStore is an alias used by bench_index_test.go so the helper
// signature stays in lockstep with newStoreForBench.
type fakeStore = bboltstore.Store

func newStoreForBench(b *testing.B) *fakeStore { return benchOpen(b) }

func benchDoc() []byte {
	doc := make([]byte, 256)
	for i := range doc {
		doc[i] = byte('a' + (i % 26))
	}
	return doc
}

func BenchmarkPut(b *testing.B) {
	s := benchOpen(b)
	ctx := context.Background()
	doc := benchDoc()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docID := fmt.Sprintf("doc-%d", i)
		if err := s.Put(ctx, "tenant-a", docID, doc); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	s := benchOpen(b)
	ctx := context.Background()
	doc := benchDoc()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = s.Put(ctx, "tenant-a", fmt.Sprintf("doc-%d", i), doc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		docID := fmt.Sprintf("doc-%d", i%n)
		if _, err := s.Get(ctx, "tenant-a", docID); err != nil {
			b.Fatalf("Get: %v", err)
		}
	}
}

func BenchmarkBatchPut(b *testing.B) {
	s := benchOpen(b)
	ctx := context.Background()
	doc := benchDoc()
	const batchSize = 100
	batch := make(map[string][]byte, batchSize)
	for j := 0; j < batchSize; j++ {
		batch[fmt.Sprintf("doc-%d", j)] = doc
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entity := fmt.Sprintf("tenant-%d", i)
		if err := s.BatchPut(ctx, entity, batch); err != nil {
			b.Fatalf("BatchPut: %v", err)
		}
	}
}

func BenchmarkScan(b *testing.B) {
	s := benchOpen(b)
	ctx := context.Background()
	doc := benchDoc()
	const n = 1000
	for i := 0; i < n; i++ {
		_ = s.Put(ctx, "tenant-a", fmt.Sprintf("doc-%d", i), doc)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		count := 0
		if err := s.Scan(ctx, "tenant-a", func(string, []byte) bool {
			count++
			return true
		}); err != nil {
			b.Fatalf("Scan: %v", err)
		}
		if count != n {
			b.Fatalf("Scan visited %d, want %d", count, n)
		}
	}
}
