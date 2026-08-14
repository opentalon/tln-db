package grpcserver_test

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/opentalon/tln-db/proto/tlndbpb"

	structpb "google.golang.org/protobuf/types/known/structpb"
)

// seedItemsForAgg writes N item docs through Put. Each doc carries
// :record/type=item, :record/status=<status>, :attr/km=<km> so the
// aggregate queries below can bind ?km from the JSON.
func seedItemsForAgg(t *testing.T, c tlndbpb.TlnDBServiceClient, items []struct {
	id     string
	status string
	km     float64
}) {
	t.Helper()
	for _, it := range items {
		doc := fmt.Sprintf(`{":record/type":"item",":record/status":%q,":attr/km":%g}`, it.status, it.km)
		if _, err := c.Put(context.Background(), &tlndbpb.PutRequest{
			EntityId: "tenant-a", DocId: it.id, Doc: []byte(doc),
		}); err != nil {
			t.Fatalf("Put %s: %v", it.id, err)
		}
	}
}

func TestGRPCQueryAggregateCount(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	seedItemsForAgg(t, c, []struct {
		id     string
		status string
		km     float64
	}{{"1", "active", 100}, {"2", "active", 200}, {"3", "retired", 300}})

	resp, err := c.Query(context.Background(), &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
		},
		Aggregates: []*tlndbpb.Aggregate{
			{Fn: "count", Over: varTerm("?e"), As: "n"},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(resp.GetRows()) != 1 {
		t.Fatalf("got %d rows, want 1", len(resp.GetRows()))
	}
	if n := resp.GetRows()[0].GetValues()[0].GetNumberValue(); n != 3 {
		t.Fatalf("count = %v, want 3", n)
	}
}

func TestGRPCQueryAggregateSumAvgMinMax(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	seedItemsForAgg(t, c, []struct {
		id     string
		status string
		km     float64
	}{{"1", "active", 10}, {"2", "active", 20}, {"3", "active", 30}, {"4", "active", 40}, {"5", "active", 50}})

	resp, err := c.Query(context.Background(), &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e", "?km"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":attr/km", Value: varTerm("?km"),
			}}},
		},
		Aggregates: []*tlndbpb.Aggregate{
			{Fn: "sum", Over: varTerm("?km")},
			{Fn: "avg", Over: varTerm("?km")},
			{Fn: "min", Over: varTerm("?km")},
			{Fn: "max", Over: varTerm("?km")},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	row := resp.GetRows()[0].GetValues()
	if row[0].GetNumberValue() != 150 {
		t.Errorf("sum = %v, want 150", row[0].GetNumberValue())
	}
	if row[1].GetNumberValue() != 30 {
		t.Errorf("avg = %v, want 30", row[1].GetNumberValue())
	}
	if row[2].GetNumberValue() != 10 {
		t.Errorf("min = %v, want 10", row[2].GetNumberValue())
	}
	if row[3].GetNumberValue() != 50 {
		t.Errorf("max = %v, want 50", row[3].GetNumberValue())
	}
}

func TestGRPCQueryAggregateGroupBy(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	seedItemsForAgg(t, c, []struct {
		id     string
		status string
		km     float64
	}{
		{"1", "active", 10}, {"2", "active", 20}, {"3", "active", 30},
		{"4", "retired", 100}, {"5", "retired", 200},
		{"6", "scheduled", 5},
	})

	resp, err := c.Query(context.Background(), &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?status"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/status", Value: varTerm("?status"),
			}}},
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":attr/km", Value: varTerm("?km"),
			}}},
		},
		GroupBy: []string{"?status"},
		Aggregates: []*tlndbpb.Aggregate{
			{Fn: "count", Over: varTerm("?e")},
			{Fn: "sum", Over: varTerm("?km")},
		},
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	rows := resp.GetRows()
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3: %+v", len(rows), rows)
	}

	// Server returns rows in lexicographic-key order, but pull values
	// into a sortable struct so the assertion isn't order-dependent
	// against any future tweak.
	type triple struct {
		status string
		count  float64
		total  float64
	}
	got := make([]triple, 0, len(rows))
	for _, r := range rows {
		vs := r.GetValues()
		got = append(got, triple{
			status: vs[0].GetStringValue(),
			count:  vs[1].GetNumberValue(),
			total:  vs[2].GetNumberValue(),
		})
	}
	sort.Slice(got, func(i, j int) bool { return got[i].status < got[j].status })
	want := []triple{
		{"active", 3, 60},
		{"retired", 2, 300},
		{"scheduled", 1, 5},
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("row[%d] = %+v, want %+v", i, got[i], w)
		}
	}
}

func TestGRPCQueryAggregateRejectsUnknownFn(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	_, err := c.Query(context.Background(), &tlndbpb.QueryRequest{
		EntityId: "tenant-a",
		Find:     []string{"?e"},
		Where: []*tlndbpb.Clause{
			{Clause: &tlndbpb.Clause_Pattern{Pattern: &tlndbpb.Pattern{
				Entity: varTerm("?e"), Attribute: ":record/type", Value: strTerm("item"),
			}}},
		},
		Aggregates: []*tlndbpb.Aggregate{
			{Fn: "stddev", Over: varTerm("?e")}, // not implemented
		},
	})
	if err == nil {
		t.Fatal("expected InvalidArgument for unknown aggregate function")
	}
}

// silence unused-import lint if the test file evolves and drops
// structpb usage.
var _ = structpb.NewNullValue
