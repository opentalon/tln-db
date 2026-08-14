package grpcserver_test

import (
	"context"
	"testing"

	"github.com/opentalon/tln-db/proto/tlndbpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCVectorInsertSearchRoundtrip(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if _, err := client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
			EntityId: "tenant-a",
			Scope:    "embed3",
			Id:       string(rune('a' + i)),
			Vector:   v,
			Metric:   tlndbpb.VectorMetric_VECTOR_METRIC_COSINE,
		}); err != nil {
			t.Fatalf("VectorInsert %d: %v", i, err)
		}
	}
	resp, err := client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a",
		Scope:    "embed3",
		Vector:   []float32{1, 0, 0},
		K:        2,
	})
	if err != nil {
		t.Fatalf("VectorSearch: %v", err)
	}
	if len(resp.GetHits()) != 2 {
		t.Fatalf("got %d hits, want 2", len(resp.GetHits()))
	}
	if resp.GetHits()[0].GetId() != "a" {
		t.Errorf("nearest = %q, want %q", resp.GetHits()[0].GetId(), "a")
	}
}

func TestGRPCVectorScopeIsolation(t *testing.T) {
	// Three distinct scopes with three different dimensions under the
	// same tenant. Each scope's own dim-matched query should land hits
	// only in that scope; a cross-dim query must come back as
	// InvalidArgument.
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	type vec struct {
		scope string
		id    string
		v     []float32
	}
	for _, w := range []vec{
		{"small", "s1", []float32{1, 0, 0}},
		{"small", "s2", []float32{0, 1, 0}},
		{"medium", "m1", []float32{1, 1, 1, 1, 0, 0, 0, 0}},
		{"medium", "m2", []float32{0, 0, 0, 0, 1, 1, 1, 1}},
		{"large", "l1", append(make([]float32, 15), 1)},
	} {
		if _, err := client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
			EntityId: "tenant-a", Scope: w.scope, Id: w.id, Vector: w.v,
		}); err != nil {
			t.Fatalf("insert %s/%s: %v", w.scope, w.id, err)
		}
	}

	// Search in each scope's own dim — exactly the right vector returns.
	res, err := client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "small", Vector: []float32{1, 0, 0}, K: 1,
	})
	if err != nil || len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "s1" {
		t.Errorf("small search: %v %v", err, res.GetHits())
	}
	res, err = client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "medium", Vector: []float32{0, 0, 0, 0, 1, 1, 1, 1}, K: 1,
	})
	if err != nil || len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "m2" {
		t.Errorf("medium search: %v %v", err, res.GetHits())
	}

	// Wrong-dimension query → InvalidArgument from the typed mapper.
	_, err = client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "small", Vector: []float32{1, 1, 1, 1, 0, 0, 0, 0}, K: 1,
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("cross-dim search code = %s, want InvalidArgument", got)
	}
}

func TestGRPCVectorUnknownScopeReturnsNotFound(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	_, err := client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "ghost", Vector: []float32{1, 2, 3}, K: 1,
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("code = %s, want NotFound", got)
	}
}

func TestGRPCVectorDeleteAndDropScope(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	for i, v := range [][]float32{
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
	} {
		if _, err := client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
			EntityId: "tenant-a", Scope: "s", Id: string(rune('a' + i)), Vector: v,
		}); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	if _, err := client.VectorDelete(ctx, &tlndbpb.VectorDeleteRequest{
		EntityId: "tenant-a", Scope: "s", Id: "b",
	}); err != nil {
		t.Fatalf("VectorDelete: %v", err)
	}
	res, err := client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "s", Vector: []float32{0, 1, 0}, K: 5,
	})
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	for _, h := range res.GetHits() {
		if h.GetId() == "b" {
			t.Fatalf("b should be tombstoned, hits = %v", res.GetHits())
		}
	}

	// Deleting an unknown id surfaces NotFound.
	_, err = client.VectorDelete(ctx, &tlndbpb.VectorDeleteRequest{
		EntityId: "tenant-a", Scope: "s", Id: "missing",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("delete missing: code = %s, want NotFound", got)
	}

	// DropScope removes everything; subsequent Search returns NotFound.
	if _, err := client.VectorDropScope(ctx, &tlndbpb.VectorDropScopeRequest{
		EntityId: "tenant-a", Scope: "s",
	}); err != nil {
		t.Fatalf("DropScope: %v", err)
	}
	_, err = client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-a", Scope: "s", Vector: []float32{1, 0, 0}, K: 1,
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("search after drop: code = %s, want NotFound", got)
	}

	// DropScope on a never-existed scope is NotFound, not silent.
	_, err = client.VectorDropScope(ctx, &tlndbpb.VectorDropScopeRequest{
		EntityId: "tenant-a", Scope: "ghost",
	})
	if got := status.Code(err); got != codes.NotFound {
		t.Errorf("drop ghost: code = %s, want NotFound", got)
	}
}

func TestGRPCVectorListScopes(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	_, _ = client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-a", Scope: "zebra", Id: "z", Vector: []float32{1, 0},
		Metric: tlndbpb.VectorMetric_VECTOR_METRIC_EUCLIDEAN,
	})
	_, _ = client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-a", Scope: "apple", Id: "a", Vector: []float32{1, 0, 0},
	})
	_, _ = client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-a", Scope: "apple", Id: "b", Vector: []float32{0, 1, 0},
	})
	_, _ = client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-b", Scope: "ghost", Id: "g", Vector: []float32{1},
	})

	res, err := client.VectorListScopes(ctx, &tlndbpb.VectorListScopesRequest{
		EntityId: "tenant-a",
	})
	if err != nil {
		t.Fatalf("ListScopes: %v", err)
	}
	if len(res.GetScopes()) != 2 {
		t.Fatalf("got %d scopes, want 2", len(res.GetScopes()))
	}
	if res.GetScopes()[0].GetScope() != "apple" || res.GetScopes()[0].GetCount() != 2 || res.GetScopes()[0].GetDim() != 3 {
		t.Errorf("scopes[0] = %+v", res.GetScopes()[0])
	}
	if res.GetScopes()[1].GetScope() != "zebra" || res.GetScopes()[1].GetMetric() != tlndbpb.VectorMetric_VECTOR_METRIC_EUCLIDEAN {
		t.Errorf("scopes[1] = %+v", res.GetScopes()[1])
	}
}

func TestGRPCVectorTenantIsolation(t *testing.T) {
	// Two tenants, same scope name, different dimensions. Tenant b's
	// 4-dim search must NOT see tenant a's 3-dim vectors.
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-a", Scope: "s", Id: "a1", Vector: []float32{1, 0, 0},
	}); err != nil {
		t.Fatalf("a: %v", err)
	}
	if _, err := client.VectorInsert(ctx, &tlndbpb.VectorInsertRequest{
		EntityId: "tenant-b", Scope: "s", Id: "b1", Vector: []float32{1, 0, 0, 0},
	}); err != nil {
		t.Fatalf("b: %v", err)
	}
	res, err := client.VectorSearch(ctx, &tlndbpb.VectorSearchRequest{
		EntityId: "tenant-b", Scope: "s", Vector: []float32{1, 0, 0, 0}, K: 5,
	})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(res.GetHits()) != 1 || res.GetHits()[0].GetId() != "b1" {
		t.Errorf("tenant b leak: hits = %v", res.GetHits())
	}
}
