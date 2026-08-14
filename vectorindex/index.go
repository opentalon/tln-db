// Package vectorindex is the in-memory HNSW layer behind tln-db's
// vector RPCs. One process-wide Index owns a map of (entity, scope) →
// HNSW graph. Dimension is locked on first insert into a scope —
// vectors only compare within the same model's embedding space, so
// silently coercing dimensions would break the recall contract.
//
// Persistence is delegated to the caller: this package never touches
// disk. The tln-db bboltstore wraps an Index alongside a raw-vector
// bucket so a process restart can rebuild graphs by replaying the
// stored vectors.
package vectorindex

import (
	"errors"
	"fmt"
	"sync"

	"github.com/coder/hnsw"
)

// Errors returned by Index.
var (
	// ErrDimensionMismatch is returned when an Insert provides a vector
	// whose length differs from the scope's locked dimension, or when a
	// Search query vector doesn't match. Dimension is locked on the
	// first insert into a scope and never changes.
	ErrDimensionMismatch = errors.New("vectorindex: dimension mismatch")

	// ErrScopeNotFound is returned when Search targets a (entity, scope)
	// that has never had an Insert.
	ErrScopeNotFound = errors.New("vectorindex: scope not found")

	// ErrEmptyVector rejects zero-length vectors. HNSW can't compute
	// distance on them and the lock-on-first-insert rule needs at least
	// one component to set the dimension.
	ErrEmptyVector = errors.New("vectorindex: empty vector")
)

// Result is one neighbour returned by Search, ordered nearest-first.
type Result struct {
	ID       string
	Distance float32
}

// Metric selects the HNSW distance function. Default is Cosine.
type Metric int

const (
	// Cosine is 1 - (a·b / (|a| |b|)). Matches the default of most
	// embedding models.
	Cosine Metric = iota
	// Euclidean is sqrt(Σ (a_i - b_i)²).
	Euclidean
)

func (m Metric) distanceFunc() hnsw.DistanceFunc {
	switch m {
	case Euclidean:
		return hnsw.EuclideanDistance
	default:
		return hnsw.CosineDistance
	}
}

// Index is the process-wide vector store. The zero value is unusable —
// callers go through New.
type Index struct {
	mu     sync.RWMutex
	scopes map[scopeKey]*scopeState
	opts   Options
}

type scopeKey struct {
	entity string
	scope  string
}

type scopeState struct {
	graph  *hnsw.Graph[string]
	dim    int
	metric Metric

	// Tombstones for deleted ids. coder/hnsw v0.6.1's own Delete
	// leaves the graph in a state where Search SEGVs on the remaining
	// nodes, so we don't touch the upstream graph on Delete; Search
	// post-filters tombstoned ids and returns the next neighbour
	// instead. Re-inserting a tombstoned id clears its entry.
	tombstones map[string]bool
}

// Options tune the HNSW graphs the Index spins up for each scope.
// Zero values fall back to coder/hnsw v0.6.1 defaults (M=16, Ml=0.25,
// EfSearch=20). Realistic recall at SIFT-class scale benefits from a
// larger EfSearch (≥ 100); the defaults are tuned for tiny corpora.
//
// All Options are applied to every scope created after New; existing
// scopes keep their original settings. Mutating Options on a returned
// Index has no effect.
type Options struct {
	M        int
	Ml       float64
	EfSearch int
}

// New returns an empty Index. Use NewWithOptions to tune the
// underlying HNSW graphs.
func New() *Index {
	return NewWithOptions(Options{})
}

// NewWithOptions returns an empty Index that creates each scope's
// HNSW graph with the given knobs. Zero values keep the upstream
// defaults.
func NewWithOptions(o Options) *Index {
	return &Index{scopes: map[scopeKey]*scopeState{}, opts: o}
}

// Insert adds (or replaces) a vector under (entity, scope, id). On the
// first call for a (entity, scope) pair the dimension and metric are
// locked; later calls into the same scope must match the dimension or
// receive ErrDimensionMismatch. The metric argument is ignored on
// subsequent calls — passing a different metric is silently accepted
// because the scope's existing graph keeps its original distance
// function.
func (i *Index) Insert(entity, scope, id string, vec []float32, metric Metric) error {
	if err := validateID(entity, scope, id); err != nil {
		return err
	}
	if len(vec) == 0 {
		return ErrEmptyVector
	}
	i.mu.Lock()
	defer i.mu.Unlock()

	key := scopeKey{entity, scope}
	st, ok := i.scopes[key]
	if !ok {
		st = &scopeState{graph: i.newScopeGraph(metric), dim: len(vec), metric: metric}
		i.scopes[key] = st
	}
	if st.tombstones != nil {
		// Re-inserting a previously tombstoned id un-buries it.
		delete(st.tombstones, id)
	}
	if ok && len(vec) != st.dim {
		return fmt.Errorf("%w: scope %q/%q expects dim %d, got %d",
			ErrDimensionMismatch, entity, scope, st.dim, len(vec))
	}
	// v0.6.1's Add panics when the key already exists (its post-add
	// length invariant fails because Add-with-replace doesn't change
	// Len). The workaround is Delete-then-Add — but Delete on the only
	// node leaves the graph in a degenerate state (layers exist with
	// nil entries) that crashes assertDims on the next Add. So when
	// replacing the sole node, rebuild the graph from scratch instead.
	if _, ok := st.graph.Lookup(id); ok {
		if st.graph.Len() == 1 {
			st.graph = i.newScopeGraph(st.metric)
		} else {
			st.graph.Delete(id)
		}
	}
	st.graph.Add(hnsw.MakeNode(id, append([]float32(nil), vec...)))
	return nil
}

// Search returns the k nearest neighbours of query under (entity,
// scope), ordered closest first. Returns ErrScopeNotFound if the scope
// has never been written to, and ErrDimensionMismatch if the query
// length doesn't match the scope's locked dimension.
//
// k is clamped to the scope's current cardinality — a Search with k
// larger than the population returns every neighbour without error.
// Tombstoned ids (those passed through Delete) are filtered out
// transparently; Search may over-fetch from the underlying graph to
// keep the response size at k after tombstone removal.
func (i *Index) Search(entity, scope string, query []float32, k int) ([]Result, error) {
	if len(query) == 0 {
		return nil, ErrEmptyVector
	}
	if k <= 0 {
		return nil, nil
	}
	i.mu.RLock()
	defer i.mu.RUnlock()

	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return nil, ErrScopeNotFound
	}
	if len(query) != st.dim {
		return nil, fmt.Errorf("%w: scope %q/%q expects dim %d, got %d",
			ErrDimensionMismatch, entity, scope, st.dim, len(query))
	}
	// Over-fetch by the number of tombstones so the post-filter still
	// has room to fill k.
	fetchK := k + len(st.tombstones)
	hits := st.graph.Search(query, fetchK)
	distFn := st.metric.distanceFunc()
	out := make([]Result, 0, k)
	for _, n := range hits {
		if st.tombstones[n.Key] {
			continue
		}
		out = append(out, Result{ID: n.Key, Distance: distFn(query, n.Value)})
		if len(out) == k {
			break
		}
	}
	return out, nil
}

// Delete tombstones the vector at (entity, scope, id). The upstream
// hnsw graph is left intact because v0.6.1's own Delete leaves the
// graph in a state where subsequent Search calls SEGV on isolated
// nodes. The tombstone is honoured at Search time. DropScope and the
// rebuild path purge tombstones by discarding the graph entirely.
//
// Returns true when the id existed (and wasn't already tombstoned).
func (i *Index) Delete(entity, scope, id string) bool {
	if entity == "" || scope == "" || id == "" {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return false
	}
	if _, present := st.graph.Lookup(id); !present {
		return false
	}
	if st.tombstones != nil && st.tombstones[id] {
		return false
	}
	if st.tombstones == nil {
		st.tombstones = map[string]bool{}
	}
	st.tombstones[id] = true
	return true
}

// DropScope removes everything under (entity, scope) — both the
// in-memory graph and the dimension lock. After DropScope the next
// Insert into the same (entity, scope) starts fresh and may use a
// different dimension. Returns true when the scope existed.
func (i *Index) DropScope(entity, scope string) bool {
	if entity == "" || scope == "" {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	key := scopeKey{entity, scope}
	if _, ok := i.scopes[key]; !ok {
		return false
	}
	delete(i.scopes, key)
	return true
}

// ScopeInfo describes one scope's metadata.
type ScopeInfo struct {
	Scope  string
	Dim    int
	Count  int
	Metric Metric
}

// ListScopes returns every scope under entity, sorted by name. Returns
// an empty slice when the tenant has never written a vector.
func (i *Index) ListScopes(entity string) []ScopeInfo {
	i.mu.RLock()
	defer i.mu.RUnlock()
	out := make([]ScopeInfo, 0)
	for k, st := range i.scopes {
		if k.entity != entity {
			continue
		}
		out = append(out, ScopeInfo{
			Scope:  k.scope,
			Dim:    st.dim,
			Count:  st.graph.Len() - len(st.tombstones),
			Metric: st.metric,
		})
	}
	sortScopeInfo(out)
	return out
}

// Dim returns the locked dimension for (entity, scope). Reports 0 +
// false when the scope is unknown.
func (i *Index) Dim(entity, scope string) (int, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return 0, false
	}
	return st.dim, true
}

// Len returns the number of live (non-tombstoned) vectors stored under
// (entity, scope). 0 when the scope is unknown.
func (i *Index) Len(entity, scope string) int {
	i.mu.RLock()
	defer i.mu.RUnlock()
	st, ok := i.scopes[scopeKey{entity, scope}]
	if !ok {
		return 0
	}
	return st.graph.Len() - len(st.tombstones)
}

// sortScopeInfo sorts in place by Scope name. Inlined rather than
// reaching for sort.Slice + a closure because the slice is tiny in
// practice (one tenant rarely has more than a few scopes).
func sortScopeInfo(s []ScopeInfo) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1].Scope > s[j].Scope; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func (i *Index) newScopeGraph(metric Metric) *hnsw.Graph[string] {
	g := hnsw.NewGraph[string]()
	g.Distance = metric.distanceFunc()
	if i.opts.M > 0 {
		g.M = i.opts.M
	}
	if i.opts.Ml > 0 {
		g.Ml = i.opts.Ml
	}
	if i.opts.EfSearch > 0 {
		g.EfSearch = i.opts.EfSearch
	}
	return g
}

func validateID(entity, scope, id string) error {
	if entity == "" {
		return errors.New("vectorindex: empty entity")
	}
	if scope == "" {
		return errors.New("vectorindex: empty scope")
	}
	if id == "" {
		return errors.New("vectorindex: empty id")
	}
	return nil
}
