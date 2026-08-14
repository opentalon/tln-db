package grpcserver_test

import (
	"context"
	"testing"

	"github.com/opentalon/tln-db/proto/tlndbpb"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

// putJSON puts a JSON doc keyed by docID into tenant-a.
func putJSON(t *testing.T, c tlndbpb.TlnDBServiceClient, docID, body string) {
	t.Helper()
	if _, err := c.Put(context.Background(), &tlndbpb.PutRequest{
		EntityId: "tenant-a", DocId: docID, Doc: []byte(body),
	}); err != nil {
		t.Fatalf("Put %s: %v", docID, err)
	}
}

// strTerm / numTerm / varTerm / boolTerm are tiny constructors that
// match the tln-language adapter's call shape; we use them
// throughout the test file to keep the query construction readable.
func strTerm(s string) *tlndbpb.Term {
	return &tlndbpb.Term{Literal: structpb.NewStringValue(s)}
}
func numTerm(n float64) *tlndbpb.Term {
	return &tlndbpb.Term{Literal: structpb.NewNumberValue(n)}
}
func varTerm(name string) *tlndbpb.Term {
	return &tlndbpb.Term{Var: name}
}

func TestGRPCQueryPatternOnly(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	putJSON(t, c, "501", `{":record/type":"item",":record/status":"active"}`)
	putJSON(t, c, "502", `{":record/type":"item",":record/status":"retired"}`)
	putJSON(t, c, "601", `{":record/type":"category"}`)

	resp, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity:    varTerm("?e"),
				Attribute: ":record/type",
				Value:     strTerm("item"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("got %d rows, want 2: %+v", len(resp.GetRows()), resp.GetRows())
	}
}

func TestGRPCQueryWithPredicate(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	putJSON(t, c, "501", `{":record/type":"item",":attr/km":45000}`)
	putJSON(t, c, "502", `{":record/type":"item",":attr/km":10000}`)
	putJSON(t, c, "503", `{":record/type":"item",":attr/km":99999}`)

	resp, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e", "?km"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":attr/km", Value: varTerm("?km"),
			}}},
			{Clause: &tlndbpb.Clause_Predicate{Predicate: &tlndbpb.Predicate{
				Op: ">", Left: varTerm("?km"), Right: numTerm(20000),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("got %d rows, want 2 (km > 20000): %+v", len(resp.GetRows()), resp.GetRows())
	}
}

func TestGRPCQueryOr(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	putJSON(t, c, "501", `{":record/type":"item",":record/status":"active"}`)
	putJSON(t, c, "502", `{":record/type":"item",":record/status":"scheduled"}`)
	putJSON(t, c, "503", `{":record/type":"item",":record/status":"retired"}`)

	statusActive := &tlndbpb.Pattern{
		Entity: varTerm("?e"), Attribute: ":record/status", Value: strTerm("active"),
	}
	statusScheduled := &tlndbpb.Pattern{
		Entity: varTerm("?e"), Attribute: ":record/status", Value: strTerm("scheduled"),
	}

	resp, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
			{Clause: &tlndbpb.Clause_Or{Or: &tlndbpb.Or{Branches: []*tlndbpb.ClauseList{
				{Clauses: []*tlndbpb.Clause{{Clause: &tlndbpb.Clause_Pattern{Pattern: statusActive}}}},
				{Clauses: []*tlndbpb.Clause{{Clause: &tlndbpb.Clause_Pattern{Pattern: statusScheduled}}}},
			}}}},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("got %d rows, want 2 (active + scheduled): %+v", len(resp.GetRows()), resp.GetRows())
	}
}

func TestGRPCQueryNot(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	putJSON(t, c, "501", `{":record/type":"item",":record/status":"active"}`)
	putJSON(t, c, "502", `{":record/type":"item",":record/status":"retired"}`)
	putJSON(t, c, "503", `{":record/type":"item",":record/status":"scheduled"}`)

	resp, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
			{Clause: &tlndbpb.Clause_Not{Not: &tlndbpb.Not{Body: []*tlndbpb.Clause{
				{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
					Entity: varTerm("?e"), Attribute: ":record/status", Value: strTerm("retired"),
				}}},
			}}}},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 2 {
		t.Fatalf("got %d rows, want 2 (active + scheduled): %+v", len(resp.GetRows()), resp.GetRows())
	}
}

func TestGRPCQueryFullText(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	putJSON(t, c, "601", `{":record/type":"category",":record/name":"Vehicles"}`)
	putJSON(t, c, "602", `{":record/type":"category",":record/name":"Buildings"}`)

	resp, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("category"),
			}}},
			{Clause: &tlndbpb.Clause_Fulltext{Fulltext: &tlndbpb.FullText{
				Entity: varTerm("?e"), Attribute: ":record/name", Query: "vehic",
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 1 {
		t.Fatalf("got %d rows, want 1 (Vehicles): %+v", len(resp.GetRows()), resp.GetRows())
	}
}

func TestGRPCQueryNoAnchorErrors(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	// Query has only a Predicate, no literal-anchor Pattern. Server
	// rejects since it can't narrow.
	_, err := c.Query(ctx, &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Predicate{Predicate: &tlndbpb.Predicate{
				Op: "==", Left: varTerm("?e"), Right: numTerm(1),
			}}},
		},
	})
	if err == nil {
		t.Fatal("expected error for query without anchor pattern")
	}
}
