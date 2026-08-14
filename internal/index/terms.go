// Package index provides backend-agnostic helpers for extracting
// indexable terms from JSON documents. It is consumed by per-backend
// indexers (e.g. internal/bboltstore) and never imported by the public
// tlndb surface.
package index

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
)

// Result is the term-extraction output for a single document.
//
//   - Terms is the de-duplicated, sorted list of strings to feed to
//     the inverted index. Each scalar leaf in the JSON tree contributes
//     up to two terms: the bare value and a `last_segment:value`
//     composite. See the package examples for the exact rule set.
//   - Numerics is the (path, float64) pairs that feed the numeric range
//     index. Paths use the JSON last-segment of where the value
//     appeared. Multiple values at the same path are preserved (array
//     of numbers).
//
// The extractor never panics on malformed JSON; it returns an error
// and a zero-valued Result.
type Result struct {
	Terms    []string
	Numerics []NumericField
}

// NumericField is one (path, value) pair extracted from a numeric leaf.
type NumericField struct {
	Path  string
	Value float64
}

// Extract walks the JSON document and emits indexable terms per the
// #27 spec. Input must be a JSON object at the top level; any other
// top-level shape returns an error.
func Extract(doc []byte) (Result, error) {
	if len(doc) == 0 {
		return Result{}, nil
	}
	var root any
	if err := json.Unmarshal(doc, &root); err != nil {
		return Result{}, fmt.Errorf("index: invalid JSON: %w", err)
	}
	obj, ok := root.(map[string]any)
	if !ok {
		return Result{}, fmt.Errorf("index: top-level must be object, got %T", root)
	}
	w := &walker{seen: make(map[string]struct{})}
	w.walkObject(nil, obj)
	sort.Strings(w.terms)
	return Result{Terms: w.terms, Numerics: w.numerics}, nil
}

type walker struct {
	terms    []string
	numerics []NumericField
	seen     map[string]struct{}
}

func (w *walker) emit(s string) {
	if s == "" {
		return
	}
	if _, dup := w.seen[s]; dup {
		return
	}
	w.seen[s] = struct{}{}
	w.terms = append(w.terms, s)
}

func (w *walker) emitLeaf(path []string, value string) {
	w.emit(value)
	if len(path) > 0 {
		last := path[len(path)-1]
		w.emit(last + ":" + value)
	}
}

func (w *walker) walkObject(path []string, obj map[string]any) {
	for k, v := range obj {
		w.walkValue(append(path, k), v)
	}
}

func (w *walker) walkValue(path []string, v any) {
	switch x := v.(type) {
	case nil:
		// skip
	case bool:
		if x {
			w.emitLeaf(path, "true")
		} else {
			w.emitLeaf(path, "false")
		}
	case string:
		w.emitLeaf(path, x)
	case float64:
		// encoding/json decodes all numbers as float64. Stringify with
		// the shortest representation that round-trips.
		s := strconv.FormatFloat(x, 'g', -1, 64)
		w.emitLeaf(path, s)
		// NaN / ±Inf cannot be indexed in the numeric range bucket.
		if !math.IsNaN(x) && !math.IsInf(x, 0) && len(path) > 0 {
			w.numerics = append(w.numerics, NumericField{
				Path:  path[len(path)-1],
				Value: x,
			})
		}
	case json.Number:
		s := string(x)
		w.emitLeaf(path, s)
		if f, err := x.Float64(); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) && len(path) > 0 {
			w.numerics = append(w.numerics, NumericField{
				Path:  path[len(path)-1],
				Value: f,
			})
		}
	case []any:
		for _, elem := range x {
			w.walkValue(path, elem)
		}
	case map[string]any:
		w.walkObject(path, x)
	default:
		// Unknown type — encoding/json should never produce these for
		// untyped destinations, but fall back to fmt.Sprint to stay
		// non-panicking.
		w.emitLeaf(path, fmt.Sprint(x))
	}
}
