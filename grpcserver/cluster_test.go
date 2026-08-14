package grpcserver_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/opentalon/tln-db/proto/tlndbpb"
)

// putEvent writes a temporal-shaped doc to the test server.
func putEvent(t *testing.T, c tlndbpb.TlnDBServiceClient, docID, itemID, recordType string, at time.Time) {
	t.Helper()
	doc := fmt.Sprintf(`{"item_id":%q,"type":%q,"at":%d}`, itemID, recordType, at.UnixNano())
	if _, err := c.Put(context.Background(), &tlndbpb.PutRequest{
		EntityId: "tenant-a",
		DocId:    docID,
		Doc:      []byte(doc),
	}); err != nil {
		t.Fatalf("Put %s: %v", docID, err)
	}
}

func TestGRPCClusterQueryDetectsThreeFailuresWithin90Days(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	day := 24 * time.Hour
	base := time.Unix(0, 0)

	// 3 failures within 30 days, then a long quiet gap, then 2 more
	// (not enough on their own).
	putEvent(t, c, "e1", "truck-7", "failure", base)
	putEvent(t, c, "e2", "truck-7", "failure", base.Add(5*day))
	putEvent(t, c, "e3", "truck-7", "failure", base.Add(25*day))
	putEvent(t, c, "e4", "truck-7", "failure", base.Add(200*day))
	putEvent(t, c, "e5", "truck-7", "failure", base.Add(205*day))

	resp, err := c.ClusterQuery(ctx, &tlndbpb.ClusterQueryRequest{
		EntityId:    "tenant-a",
		ItemId:      "truck-7",
		Types:       []string{"failure"},
		WindowNanos: int64(90 * day),
		MinSize:     3,
	})
	if err != nil {
		t.Fatalf("ClusterQuery: %v", err)
	}
	if len(resp.GetClusters()) != 1 {
		t.Fatalf("got %d clusters, want 1: %+v", len(resp.GetClusters()), resp.GetClusters())
	}
	c0 := resp.GetClusters()[0]
	if len(c0.GetEvents()) != 3 {
		t.Fatalf("cluster size %d, want 3", len(c0.GetEvents()))
	}
	if c0.GetFirstUnixNanos() != base.UnixNano() {
		t.Errorf("FirstUnixNanos = %d, want %d", c0.GetFirstUnixNanos(), base.UnixNano())
	}
	if c0.GetLastUnixNanos() != base.Add(25*day).UnixNano() {
		t.Errorf("LastUnixNanos = %d, want %d", c0.GetLastUnixNanos(), base.Add(25*day).UnixNano())
	}
}

func TestGRPCClusterQueryRespectsTypeFilter(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	day := 24 * time.Hour
	base := time.Unix(0, 0)

	putEvent(t, c, "e1", "truck-7", "failure", base)
	putEvent(t, c, "e2", "truck-7", "inspection", base.Add(1*day))
	putEvent(t, c, "e3", "truck-7", "failure", base.Add(2*day))
	putEvent(t, c, "e4", "truck-7", "inspection", base.Add(3*day))
	putEvent(t, c, "e5", "truck-7", "failure", base.Add(4*day))

	// Filter to failures only — 3 of them within 90 days.
	resp, err := c.ClusterQuery(ctx, &tlndbpb.ClusterQueryRequest{
		EntityId:    "tenant-a",
		ItemId:      "truck-7",
		Types:       []string{"failure"},
		WindowNanos: int64(90 * day),
		MinSize:     3,
	})
	if err != nil {
		t.Fatalf("ClusterQuery: %v", err)
	}
	if len(resp.GetClusters()) != 1 || len(resp.GetClusters()[0].GetEvents()) != 3 {
		t.Fatalf("failure cluster wrong: %+v", resp.GetClusters())
	}
	for _, ev := range resp.GetClusters()[0].GetEvents() {
		if ev.GetType() != "failure" {
			t.Errorf("non-failure event in failure-filtered cluster: %v", ev)
		}
	}
}

func TestGRPCClusterQueryEmptyWhenItemAbsent(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := c.ClusterQuery(ctx, &tlndbpb.ClusterQueryRequest{
		EntityId:    "tenant-a",
		ItemId:      "nobody",
		WindowNanos: int64(24 * time.Hour),
		MinSize:     3,
	})
	if err != nil {
		t.Fatalf("ClusterQuery: %v", err)
	}
	if len(resp.GetClusters()) != 0 {
		t.Fatalf("expected empty result, got %+v", resp.GetClusters())
	}
}

func TestGRPCClusterQueryWindowTooNarrow(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	day := 24 * time.Hour
	base := time.Unix(0, 0)
	putEvent(t, c, "e1", "truck-7", "failure", base)
	putEvent(t, c, "e2", "truck-7", "failure", base.Add(10*day))
	putEvent(t, c, "e3", "truck-7", "failure", base.Add(20*day))

	resp, err := c.ClusterQuery(ctx, &tlndbpb.ClusterQueryRequest{
		EntityId:    "tenant-a",
		ItemId:      "truck-7",
		WindowNanos: int64(5 * day), // any pair is > 5 days apart
		MinSize:     3,
	})
	if err != nil {
		t.Fatalf("ClusterQuery: %v", err)
	}
	if len(resp.GetClusters()) != 0 {
		t.Fatalf("expected no clusters with narrow window, got %+v", resp.GetClusters())
	}
}
