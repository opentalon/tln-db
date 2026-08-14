package bboltstore

import (
	"fmt"
	"sort"
)

// runQueryAggregates is the server-side aggregator used by Store.Query
// when QueryRequest.Aggregates is non-empty. Mirrors the
// tln-language adapter's runAggregates (which in turn mirrors
// MemoryStore.runAggregates) — same input shape, same output shape,
// same group-key sort, so client-side and server-side composition
// return identical rows.
//
// Result layout: [GroupBy columns..., Aggregate columns...].
func runQueryAggregates(matches []map[string]any, groupBy []string, aggs []QueryAggregate) []QueryRow {
	if len(groupBy) == 0 {
		row := make(QueryRow, len(aggs))
		for i, a := range aggs {
			row[i] = computeQueryAggregate(a, matches)
		}
		return []QueryRow{row}
	}

	type bucket struct {
		key     []any
		members []map[string]any
	}
	buckets := map[string]*bucket{}
	var order []string
	for _, b := range matches {
		key := make([]any, len(groupBy))
		for i, v := range groupBy {
			key[i] = b[v]
		}
		k := aggGroupKeyString(key)
		if _, ok := buckets[k]; !ok {
			buckets[k] = &bucket{key: key}
			order = append(order, k)
		}
		buckets[k].members = append(buckets[k].members, b)
	}
	sort.Strings(order)

	rows := make([]QueryRow, 0, len(order))
	for _, k := range order {
		bk := buckets[k]
		row := make(QueryRow, 0, len(groupBy)+len(aggs))
		row = append(row, bk.key...)
		for _, a := range aggs {
			row = append(row, computeQueryAggregate(a, bk.members))
		}
		rows = append(rows, row)
	}
	return rows
}

func computeQueryAggregate(a QueryAggregate, members []map[string]any) any {
	switch a.Fn {
	case "count":
		return float64(len(members))
	case "sum", "total":
		s, _ := aggQuerySumOver(a.Over, members)
		return s
	case "avg":
		s, n := aggQuerySumOver(a.Over, members)
		if n == 0 {
			return float64(0)
		}
		return s / float64(n)
	case "min":
		return aggQueryMinOver(a.Over, members)
	case "max":
		return aggQueryMaxOver(a.Over, members)
	}
	return nil
}

func aggQuerySumOver(t QueryTerm, members []map[string]any) (float64, int) {
	if t.Var == "" {
		return 0, 0
	}
	var sum float64
	n := 0
	for _, b := range members {
		if f, ok := aggQueryFloat(b[t.Var]); ok {
			sum += f
			n++
		}
	}
	return sum, n
}

func aggQueryMinOver(t QueryTerm, members []map[string]any) any {
	if t.Var == "" {
		return nil
	}
	var best float64
	seen := false
	for _, b := range members {
		f, ok := aggQueryFloat(b[t.Var])
		if !ok {
			continue
		}
		if !seen || f < best {
			best = f
			seen = true
		}
	}
	if !seen {
		return nil
	}
	return best
}

func aggQueryMaxOver(t QueryTerm, members []map[string]any) any {
	if t.Var == "" {
		return nil
	}
	var best float64
	seen := false
	for _, b := range members {
		f, ok := aggQueryFloat(b[t.Var])
		if !ok {
			continue
		}
		if !seen || f > best {
			best = f
			seen = true
		}
	}
	if !seen {
		return nil
	}
	return best
}

func aggQueryFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	}
	return 0, false
}

func aggGroupKeyString(parts []any) string {
	var b []byte
	for i, p := range parts {
		if i > 0 {
			b = append(b, '|')
		}
		b = append(b, fmt.Sprintf("%v", p)...)
	}
	return string(b)
}
