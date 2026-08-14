package bboltstore

import "testing"

// String predicates against a list-valued attribute quantify existentially:
// the predicate holds when any element satisfies it (tln-language #158).
// The inverted index already gathers such documents as candidates; this is
// the verify step agreeing with it.
func TestEvalQueryPredicateQuantifiesOverList(t *testing.T) {
	t.Parallel()
	cases := []struct {
		op    string
		left  any
		right any
		want  bool
	}{
		{"contains", []any{"go.mod", "main.go"}, "go.mod", true},
		{"contains", []string{"go.mod", "main.go"}, "go.mod", true},
		{"contains", []any{"README.md"}, "go.mod", false},
		{"starts_with", []any{"main.go", "internal/x.go"}, "internal/", true},
		{"starts_with", []any{"main.go", "cmd/x.go"}, "internal/", false},
		{"ends_with", []any{"go.sum", "main.go"}, ".go", true},
		{"ends_with", []any{"go.sum", "README.md"}, ".go", false},

		// Unhappy paths.
		{"contains", []any{}, "go.mod", false},
		{"contains", []any{nil, 42.0}, "go.mod", false},
		{"contains", []any{42.0, "go.mod"}, "go.mod", true},
		{"contains", []any{"go.mod"}, 42.0, false},
		{"contains", 42.0, "go.mod", false},

		// Scalars unchanged.
		{"contains", "go.mod,main.go", "go.mod", true},
		{"contains", "main.go", "go.mod", false},

		// `==` stays strict equality against a list.
		{"==", []any{"go.mod"}, "go.mod", false},
		{"!=", []any{"go.mod"}, "go.mod", true},
	}
	for _, c := range cases {
		if got := evalQueryPredicate(c.op, c.left, c.right); got != c.want {
			t.Errorf("%s(%#v, %#v) = %v, want %v", c.op, c.left, c.right, got, c.want)
		}
	}
}

// Full text (`matches` / `matches_phrase`) scans list elements too.
func TestMatchQueryFullTextScansListElements(t *testing.T) {
	t.Parallel()
	attrs := map[string]any{
		":record/type":        "pr",
		":attr/changed_files": []any{"go.mod", "main.go"},
		":attr/title":         "fix parser",
	}
	cases := []struct {
		attribute string
		query     string
		want      bool
	}{
		{"", "main.go", true},
		{"", "parser", true},
		{"", "nowhere", false},
		{":attr/changed_files", "main.go", true},
		{":attr/changed_files", "parser", false}, // scoped: no leak to :attr/title
		{":attr/changed_files", "", false},       // empty query matches nothing
		{":attr/missing", "main.go", false},
	}
	for _, c := range cases {
		got := matchQueryFullText(&QueryFullText{
			Entity:    QueryTerm{Var: "?e"},
			Attribute: c.attribute,
			Query:     c.query,
		}, attrs)
		if got != c.want {
			t.Errorf("fulltext attr=%q query=%q = %v, want %v", c.attribute, c.query, got, c.want)
		}
	}
}

// A list holding no string elements matches nothing rather than falling
// through to some scalar interpretation.
func TestMatchQueryFullTextNonStringElements(t *testing.T) {
	t.Parallel()
	attrs := map[string]any{":attr/labels": []any{42.0, nil, true}}
	if matchQueryFullText(&QueryFullText{Query: "42"}, attrs) {
		t.Error("non-string list elements should not match")
	}
}
