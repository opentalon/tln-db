package grpcserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/opentalon/tln-db/proto/tlndbpb"
)

func TestGRPCSequenceJoinDetectsInOrder(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	day := 24 * time.Hour
	base := time.Unix(0, 0)
	putEvent(t, c, "e1", "truck-1", "inspect", base)
	putEvent(t, c, "e2", "truck-1", "fault", base.Add(10*day))
	putEvent(t, c, "e3", "truck-2", "fault", base) // wrong order
	putEvent(t, c, "e4", "truck-2", "inspect", base.Add(5*day))

	resp, err := c.SequenceJoin(ctx, &tlndbpb.SequenceJoinRequest{
		EntityId:    "tenant-a",
		Steps:       []string{"inspect", "fault"},
		WindowNanos: int64(30 * day),
	})
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if len(resp.GetMatches()) != 1 {
		t.Fatalf("got %d matches, want 1 (truck-1 only): %+v", len(resp.GetMatches()), resp.GetMatches())
	}
	m := resp.GetMatches()[0]
	if m.GetItemId() != "truck-1" {
		t.Fatalf("matched item = %q, want truck-1", m.GetItemId())
	}
	if len(m.GetEvents()) != 2 {
		t.Fatalf("match events = %d, want 2", len(m.GetEvents()))
	}
}

func TestGRPCSequenceJoinWindowFilters(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	day := 24 * time.Hour
	base := time.Unix(0, 0)
	// inspect → fault but 200 days apart; window is 30.
	putEvent(t, c, "e1", "truck-1", "inspect", base)
	putEvent(t, c, "e2", "truck-1", "fault", base.Add(200*day))

	resp, err := c.SequenceJoin(ctx, &tlndbpb.SequenceJoinRequest{
		EntityId:    "tenant-a",
		Steps:       []string{"inspect", "fault"},
		WindowNanos: int64(30 * day),
	})
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if len(resp.GetMatches()) != 0 {
		t.Fatalf("expected 0 matches (outside window), got %+v", resp.GetMatches())
	}
}

func TestGRPCSequenceJoinItemFilter(t *testing.T) {
	t.Parallel()
	c, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	base := time.Unix(0, 0)
	putEvent(t, c, "e1", "truck-1", "inspect", base)
	putEvent(t, c, "e2", "truck-1", "fault", base.Add(time.Hour))
	putEvent(t, c, "e3", "truck-2", "inspect", base)
	putEvent(t, c, "e4", "truck-2", "fault", base.Add(time.Hour))

	// Restrict to truck-2 only.
	resp, err := c.SequenceJoin(ctx, &tlndbpb.SequenceJoinRequest{
		EntityId: "tenant-a",
		ItemIds:  []string{"truck-2"},
		Steps:    []string{"inspect", "fault"},
	})
	if err != nil {
		t.Fatalf("SequenceJoin: %v", err)
	}
	if len(resp.GetMatches()) != 1 || resp.GetMatches()[0].GetItemId() != "truck-2" {
		t.Fatalf("expected truck-2 only, got %+v", resp.GetMatches())
	}
}
