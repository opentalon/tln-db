package vectorindex_test

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"

	"github.com/opentalon/tln-db/vectorindex"
)

// SIFT-1M recall conformance.
//
// To run this test, fetch the SIFT-1M dataset and point
// TLNDB_SIFT_PATH at the directory that contains:
//
//	sift_base.fvecs     1,000,000 × 128-dim float32 (~516 MB)
//	sift_query.fvecs       10,000 × 128-dim float32
//	sift_groundtruth.ivecs 10,000 × 100 int32 (top-100 ids per query)
//
// Source (mirror of the INRIA TEXMEX corpus):
//
//	ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz
//	https://lear.inrialpes.fr/~douze/data/sift.tar.gz
//
// Untar into one directory and pass that directory:
//
//	TLNDB_SIFT_PATH=/path/to/sift go test -run SIFT -count=1 ./vectorindex/
//
// Acceptance: recall@10 ≥ 0.9 over the first 100 queries. We cap the
// query count so the test runs in ~minutes (full 10k queries take an
// order of magnitude longer); set TLNDB_SIFT_QUERIES to override.
//
// On laptop-class hardware the per-insert cost (HNSW + bbolt commit
// in lockstep) makes the full 1M insert phase ~hours; the recall
// pipeline itself is fast. Set TLNDB_SIFT_BASE_LIMIT to a smaller
// integer to cap the base corpus — recall measurement is then only
// meaningful relative to that truncated subset (groundtruth ids
// outside the subset are dropped from the per-query top-K before
// scoring, so the metric stays well-defined).
//
// .fvecs / .ivecs format (TEXMEX): a sequence of <int32 dim><dim ×
// {float32|int32}> records, little-endian.

const (
	siftPathEnv       = "TLNDB_SIFT_PATH"
	siftQueriesEnv    = "TLNDB_SIFT_QUERIES"
	siftBaseLimitEnv  = "TLNDB_SIFT_BASE_LIMIT"
	siftRecallMinEnv  = "TLNDB_SIFT_RECALL_MIN"
	siftDim           = 128
	siftDefaultK      = 10
	siftDefaultQ      = 100
	siftDefaultRecall = 0.9 // production target from opentalon/tln-db#12
)

// Recall threshold for the SIFT test.
//
// The tln-db#12 acceptance criterion is `recall@10 ≥ 0.9`. We hit
// it by pinning coder/hnsw to a post-2026-06-22 main commit; v0.6.1
// shipped without three recall-relevant fixes (efSearch-bounded
// termination, replenish() honouring the configured metric, heap
// ordering) and capped around `recall@10 ≈ 0.30` on SIFT.
//
// Override via TLNDB_SIFT_RECALL_MIN to relax the bar during
// experiments with alternative HNSW backends.

func TestSIFTRecall(t *testing.T) {
	dir := os.Getenv(siftPathEnv)
	if dir == "" {
		t.Skipf("set %s to a directory containing sift_base.fvecs / sift_query.fvecs / sift_groundtruth.ivecs", siftPathEnv)
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIFT recall test sized for Unix dev hosts")
	}

	base, err := readFvecs(filepath.Join(dir, "sift_base.fvecs"))
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	if raw := os.Getenv(siftBaseLimitEnv); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 && n < len(base) {
			base = base[:n]
			t.Logf("base truncated to %d vectors via %s", n, siftBaseLimitEnv)
		}
	}
	queries, err := readFvecs(filepath.Join(dir, "sift_query.fvecs"))
	if err != nil {
		t.Fatalf("read query: %v", err)
	}
	// We deliberately *don't* use sift_groundtruth.ivecs here: that
	// file's top-100 ids are computed against the full 1M base, so on
	// a truncated corpus the "correct" neighbours are mostly absent.
	// Compute groundtruth against the actual loaded subset by
	// brute-force cosine — fast at the scale this test runs at
	// (~10k × 50 queries × 128-dim ≈ 64M ops per query).
	for _, v := range base {
		if len(v) != siftDim {
			t.Fatalf("base vector wrong dim: %d", len(v))
		}
	}
	if len(base) == 0 || len(queries) == 0 {
		t.Fatalf("empty SIFT data: base=%d queries=%d", len(base), len(queries))
	}

	// SIFT exercises HNSW recall, not bbolt durability — bboltstore's
	// own persistence + restart tests cover the disk round-trip. A
	// per-Insert bbolt commit makes the 1M-scale corpus unrealistic
	// here (each commit is fsync-bound, ~hours total), so the SIFT
	// pipeline talks to the in-memory index directly. We bump
	// EfSearch + M from coder/hnsw v0.6.1's tiny defaults — the
	// shipped values target tens-of-vectors workloads, not SIFT.
	// M=16 / EfSearch=200 hits the tln-db#12 0.9 recall target on
	// SIFT-5K with the post-2026-06-22 coder/hnsw main pin, and stays
	// fast enough that the test runs in tens of seconds rather than
	// minutes on laptop-class hardware.
	idx := vectorindex.NewWithOptions(vectorindex.Options{
		M:        16,
		EfSearch: 200,
	})
	// SIFT descriptors are unsigned 8-bit features cast to float32.
	// They are traditionally compared with L2 / Euclidean, not cosine
	// — cosine treats two parallel-but-different-magnitude vectors as
	// identical, which collapses the SIFT histogram structure.
	for i, v := range base {
		if err := idx.Insert("sift", "base", strconv.Itoa(i), v, vectorindex.Euclidean); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	qLimit := siftDefaultQ
	if raw := os.Getenv(siftQueriesEnv); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			qLimit = n
		}
	}
	if qLimit > len(queries) {
		qLimit = len(queries)
	}

	corpus := len(base)
	hit := 0
	for qi := 0; qi < qLimit; qi++ {
		hits, err := idx.Search("sift", "base", queries[qi], siftDefaultK)
		if err != nil {
			t.Fatalf("search %d: %v", qi, err)
		}
		want := bruteForceTopKEuclidean(base, queries[qi], siftDefaultK)
		for _, h := range hits {
			if want[h.ID] {
				hit++
			}
		}
	}
	recall := float64(hit) / float64(qLimit*siftDefaultK)
	threshold := siftDefaultRecall
	if raw := os.Getenv(siftRecallMinEnv); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v >= 0 {
			threshold = v
		}
	}
	t.Logf("SIFT recall@%d over %d queries (base=%d) = %.4f (threshold %.2f)",
		siftDefaultK, qLimit, corpus, recall, threshold)
	if recall < threshold {
		t.Errorf("recall@%d = %.4f, want ≥ %.2f", siftDefaultK, recall, threshold)
	}
}

// bruteForceTopKEuclidean returns the ids of the K nearest base
// vectors to query under squared-Euclidean distance. Side-channel
// groundtruth for the truncated-corpus SIFT path — the on-disk
// ivecs file is full-1M-only.
func bruteForceTopKEuclidean(base [][]float32, query []float32, k int) map[string]bool {
	type pair struct {
		id   int
		dist float64
	}
	top := make([]pair, 0, k)
	for i, v := range base {
		d := squaredL2(query, v)
		if len(top) < k {
			top = append(top, pair{id: i, dist: d})
			for j := len(top) - 1; j > 0 && top[j-1].dist > top[j].dist; j-- {
				top[j-1], top[j] = top[j], top[j-1]
			}
			continue
		}
		if d < top[k-1].dist {
			top[k-1] = pair{id: i, dist: d}
			for j := k - 1; j > 0 && top[j-1].dist > top[j].dist; j-- {
				top[j-1], top[j] = top[j], top[j-1]
			}
		}
	}
	out := make(map[string]bool, k)
	for _, p := range top {
		out[strconv.Itoa(p.id)] = true
	}
	return out
}

func squaredL2(a, b []float32) float64 {
	var s float64
	for i := range a {
		d := float64(a[i]) - float64(b[i])
		s += d * d
	}
	return s
}

// readFvecs parses TEXMEX .fvecs: repeated <int32 dim><dim × float32>.
func readFvecs(path string) ([][]float32, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out [][]float32
	for {
		var dim int32
		if err := binary.Read(f, binary.LittleEndian, &dim); err != nil {
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			return nil, err
		}
		if dim <= 0 || dim > 1<<16 {
			return nil, errors.New("vectorindex sift: implausible dim")
		}
		buf := make([]byte, 4*int(dim))
		if _, err := io.ReadFull(f, buf); err != nil {
			return nil, err
		}
		v := make([]float32, dim)
		for i := range v {
			v[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[4*i:]))
		}
		out = append(out, v)
	}
}

