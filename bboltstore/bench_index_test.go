package bboltstore_test

// Benchmarks for every IndexedStore lookup path. These establish a
// baseline before #28 (query engine) starts composing them. Each
// benchmark prepopulates a store with `prepN` documents and then
// measures one lookup per iteration.
//
// Run with:
//   go test -bench=BenchmarkIndex -benchmem -run=^$ ./bboltstore
//
// Not part of `go test ./...` — Go runs benchmarks only with -bench.

import (
	"context"
	"fmt"
	"testing"

	tlndb "github.com/opentalon/tln-db"
)

const prepN = 1000

func prepIndexed(b *testing.B) *fakeStore {
	b.Helper()
	s := newStoreForBench(b)
	ctx := context.Background()
	for i := 0; i < prepN; i++ {
		doc := fmt.Sprintf(`{"item_id":"i%d","type":"inspect","at":%d,"km":%d,"status":"active","category":"truck"}`, i%50, i*1000, i*10)
		if err := s.Put(ctx, "tenant-a", fmt.Sprintf("d%d", i), []byte(doc)); err != nil {
			b.Fatalf("Put: %v", err)
		}
	}
	return s
}

func BenchmarkIndexLookup(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Lookup(ctx, "tenant-a", "status:active"); err != nil {
			b.Fatalf("Lookup: %v", err)
		}
	}
}

func BenchmarkIndexLookupPrefix(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.LookupPrefix(ctx, "tenant-a", "category:"); err != nil {
			b.Fatalf("LookupPrefix: %v", err)
		}
	}
}

func BenchmarkIndexLookupNumericRange(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.LookupNumericRange(ctx, "tenant-a", "km", 2000, 8000, tlndb.RangeOpts{}); err != nil {
			b.Fatalf("LookupNumericRange: %v", err)
		}
	}
}

func BenchmarkIndexWindowQuery(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.WindowQuery(ctx, "tenant-a", "i7", nil, 0); err != nil {
			b.Fatalf("WindowQuery: %v", err)
		}
	}
}

func BenchmarkIndexGroupCount(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.GroupCount(ctx, "tenant-a", "i7", "type", "inspect"); err != nil {
			b.Fatalf("GroupCount: %v", err)
		}
	}
}

func BenchmarkIndexStats(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Stats(ctx, "tenant-a", "km"); err != nil {
			b.Fatalf("Stats: %v", err)
		}
	}
}

func BenchmarkIndexLastSeen(b *testing.B) {
	s := prepIndexed(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := s.LastSeen(ctx, "tenant-a", "i7", "inspect"); err != nil {
			b.Fatalf("LastSeen: %v", err)
		}
	}
}

func BenchmarkIndexDescendants(b *testing.B) {
	s := newStoreForBench(b)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "root", []byte(`{}`))
	for i := 0; i < 50; i++ {
		_ = s.Put(ctx, "tenant-a", fmt.Sprintf("child%d", i), []byte(`{"parent":"root"}`))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := s.Descendants(ctx, "tenant-a", "root"); err != nil {
			b.Fatalf("Descendants: %v", err)
		}
	}
}
