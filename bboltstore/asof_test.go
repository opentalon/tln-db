package bboltstore_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/opentalon/talon-db/bboltstore"
)

// putJSON marshals fields to JSON and Puts them as the document.
func putJSON(t *testing.T, s *bboltstore.Store, entityID, docID string, fields map[string]any) {
	t.Helper()
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := s.Put(context.Background(), entityID, docID, raw); err != nil {
		t.Fatalf("Put %s: %v", docID, err)
	}
}

// statusQuery finds entity IDs whose :attr/status equals want.
func statusQuery(want string) bboltstore.QueryRequest {
	return bboltstore.QueryRequest{
		EntityID: "tenant",
		Find:     []string{"?e"},
		Where: []bboltstore.QueryClause{{Pattern: &bboltstore.QueryPattern{
			Attribute: ":attr/status",
			Value:     bboltstore.QueryTerm{Literal: want},
		}}},
	}
}

func asOfIDs(t *testing.T, s *bboltstore.Store, req bboltstore.QueryRequest, at time.Time) []float64 {
	t.Helper()
	rows, err := s.QueryAsOf(context.Background(), req, at)
	if err != nil {
		t.Fatalf("QueryAsOf: %v", err)
	}
	out := make([]float64, 0, len(rows))
	for _, r := range rows {
		if len(r) > 0 {
			if f, ok := r[0].(float64); ok {
				out = append(out, f)
			}
		}
	}
	return out
}

// TestQueryAsOfReconstructsPastValue proves a value change is time-travelled:
// a record certified in the past but defective now matches the certified
// query only at the earlier instant.
func TestQueryAsOfReconstructsPastValue(t *testing.T) {
	s := newStore(t)

	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	s.SetClock(func() time.Time { return t0 })
	putJSON(t, s, "tenant", "1", map[string]any{":record/type": "machine", ":attr/status": "certified"})

	s.SetClock(func() time.Time { return t1 })
	putJSON(t, s, "tenant", "1", map[string]any{":record/type": "machine", ":attr/status": "defective"})

	mid := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	// As of mid: was certified.
	if got := asOfIDs(t, s, statusQuery("certified"), mid); len(got) != 1 || got[0] != 1 {
		t.Fatalf("certified as-of mid = %v, want [1]", got)
	}
	if got := asOfIDs(t, s, statusQuery("defective"), mid); len(got) != 0 {
		t.Fatalf("defective as-of mid = %v, want []", got)
	}

	// As of after the second write: now defective, no longer certified.
	if got := asOfIDs(t, s, statusQuery("certified"), after); len(got) != 0 {
		t.Fatalf("certified as-of after = %v, want []", got)
	}
	if got := asOfIDs(t, s, statusQuery("defective"), after); len(got) != 1 || got[0] != 1 {
		t.Fatalf("defective as-of after = %v, want [1]", got)
	}
}

// TestQueryAsOfBeforeCreation returns nothing for a record that did not
// exist yet at the target instant.
func TestQueryAsOfBeforeCreation(t *testing.T) {
	s := newStore(t)
	t0 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	s.SetClock(func() time.Time { return t0 })
	putJSON(t, s, "tenant", "1", map[string]any{":attr/status": "certified"})

	before := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if got := asOfIDs(t, s, statusQuery("certified"), before); len(got) != 0 {
		t.Fatalf("as-of before creation = %v, want []", got)
	}
}

// TestQueryAsOfDeletedTombstone hides a document that was deleted before
// the target instant but surfaces it beforehand.
func TestQueryAsOfDeletedTombstone(t *testing.T) {
	s := newStore(t)
	t0 := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	t1 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	s.SetClock(func() time.Time { return t0 })
	putJSON(t, s, "tenant", "1", map[string]any{":attr/status": "certified"})

	s.SetClock(func() time.Time { return t1 })
	if err := s.Delete(context.Background(), "tenant", "1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	mid := time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)

	if got := asOfIDs(t, s, statusQuery("certified"), mid); len(got) != 1 || got[0] != 1 {
		t.Fatalf("as-of before delete = %v, want [1]", got)
	}
	if got := asOfIDs(t, s, statusQuery("certified"), after); len(got) != 0 {
		t.Fatalf("as-of after delete = %v, want []", got)
	}
}
