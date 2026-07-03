package grpcserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/opentalon/talon-db/proto/talondbpb"
)

// TestGRPCQueryAsOf drives the time-travel RPC end-to-end: a record that
// was active in the past but retired now matches an as-of query at the
// earlier instant only. Uses dialSub to control the store clock.
func TestGRPCQueryAsOf(t *testing.T) {
	c, store, cleanup := dialSub(t)
	defer cleanup()
	ctx := context.Background()

	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	store.SetClock(func() time.Time { return t0 })
	putJSON(t, c, "501", `{":record/type":"item",":record/status":"active"}`)

	store.SetClock(func() time.Time { return t1 })
	putJSON(t, c, "501", `{":record/type":"item",":record/status":"retired"}`)

	activeAsOf := func(at time.Time) int {
		resp, err := c.QueryAsOf(ctx, &talondbpb.QueryAsOfRequest{
			EntityId: "tenant-a",
			Find:     []string{"?e"},
			Where: []*talondbpb.Clause{
				{Clause: &talondbpb.Clause_Pattern{Pattern: &talondbpb.Pattern{
					Entity:    varTerm("?e"),
					Attribute: ":record/status",
					Value:     strTerm("active"),
				}}},
			},
			AtUnixNanos: at.UnixNano(),
		})
		if err != nil {
			t.Fatalf("QueryAsOf: %v", err)
		}
		return len(resp.GetRows())
	}

	mid := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	if n := activeAsOf(mid); n != 1 {
		t.Fatalf("active as-of mid = %d rows, want 1", n)
	}
	if n := activeAsOf(after); n != 0 {
		t.Fatalf("active as-of after = %d rows, want 0", n)
	}
}
