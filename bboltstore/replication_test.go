package bboltstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	talondb "github.com/opentalon/talon-db"
	"github.com/opentalon/talon-db/proto/talondbpb"
	"github.com/opentalon/talon-db/vectorindex"
)

func openRepl(t *testing.T, retention uint64) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "db.bbolt"), WithReplication(retention))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// applyAll copies every op-log entry from leader into follower.
func applyAll(t *testing.T, leader, follower *Store, from uint64) {
	t.Helper()
	if err := leader.ReadOpLog(from, func(e *talondbpb.OpLogEntry) error {
		return follower.ApplyEntry(e)
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
}

func TestOpLogSeqMonotonic(t *testing.T) {
	ctx := context.Background()
	s := openRepl(t, 0)

	if err := s.Put(ctx, "e", "a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.BatchPut(ctx, "e", map[string][]byte{"b": []byte(`{"n":2}`)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(ctx, "e", "a", []byte(`{"n":3}`)); err != nil { // change
		t.Fatal(err)
	}
	if err := s.Delete(ctx, "e", "b"); err != nil {
		t.Fatal(err)
	}

	var seqs []uint64
	var kinds []talondbpb.OpKind
	if err := s.ReadOpLog(1, func(e *talondbpb.OpLogEntry) error {
		seqs = append(seqs, e.Seq)
		kinds = append(kinds, e.Kind)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 3, 4}
	if len(seqs) != len(want) {
		t.Fatalf("got %d entries, want %d (%v)", len(seqs), len(want), seqs)
	}
	for i := range want {
		if seqs[i] != want[i] {
			t.Fatalf("seq[%d]=%d want %d", i, seqs[i], want[i])
		}
	}
	if kinds[0] != talondbpb.OpKind_OP_KIND_DOC_ASSERT || kinds[2] != talondbpb.OpKind_OP_KIND_DOC_CHANGE || kinds[3] != talondbpb.OpKind_OP_KIND_DOC_RETRACT {
		t.Fatalf("unexpected kinds: %v", kinds)
	}
	if s.CurrentSeq() != 4 {
		t.Fatalf("CurrentSeq=%d want 4", s.CurrentSeq())
	}
}

func TestApplyReproducesLeaderState(t *testing.T) {
	ctx := context.Background()
	leader := openRepl(t, 0)
	follower := openRepl(t, 0)

	// Mixed document workload.
	docs := map[string][]byte{
		"u1": []byte(`{"kind":"user","age":30,"name":"ann"}`),
		"u2": []byte(`{"kind":"user","age":41,"name":"bob"}`),
		"u3": []byte(`{"kind":"user","age":41,"name":"cy"}`),
	}
	if err := leader.BatchPut(ctx, "t", docs); err != nil {
		t.Fatal(err)
	}
	if err := leader.Put(ctx, "t", "u1", []byte(`{"kind":"user","age":31,"name":"ann"}`)); err != nil {
		t.Fatal(err)
	}
	if err := leader.Delete(ctx, "t", "u2"); err != nil {
		t.Fatal(err)
	}
	// Vector workload.
	if err := leader.VectorInsert(ctx, "t", "emb", "v1", []float32{1, 0, 0}, vectorindex.Cosine); err != nil {
		t.Fatal(err)
	}
	if err := leader.VectorInsert(ctx, "t", "emb", "v2", []float32{0, 1, 0}, vectorindex.Cosine); err != nil {
		t.Fatal(err)
	}

	applyAll(t, leader, follower, 1)

	// Documents: byte-identical Get + identical updated_at (meta).
	for _, id := range []string{"u1", "u3"} {
		lg, err := leader.Get(ctx, "t", id)
		if err != nil {
			t.Fatalf("leader get %s: %v", id, err)
		}
		fg, err := follower.Get(ctx, "t", id)
		if err != nil {
			t.Fatalf("follower get %s: %v", id, err)
		}
		if !bytes.Equal(lg, fg) {
			t.Fatalf("doc %s mismatch: %s vs %s", id, lg, fg)
		}
		lw, _, _ := leader.LastWritten(ctx, "t", id)
		fw, _, _ := follower.LastWritten(ctx, "t", id)
		if lw.UnixNano() != fw.UnixNano() {
			t.Fatalf("doc %s updated_at mismatch: %d vs %d", id, lw.UnixNano(), fw.UnixNano())
		}
	}
	// Deleted doc absent on follower.
	if _, err := follower.Get(ctx, "t", "u2"); !errors.Is(err, talondb.ErrNotFound) {
		t.Fatalf("u2 should be deleted on follower, got %v", err)
	}

	// Derived index parity: Lookup on the inverted index.
	lset, err := leader.Lookup(ctx, "t", "name:ann")
	if err != nil {
		t.Fatal(err)
	}
	fset, err := follower.Lookup(ctx, "t", "name:ann")
	if err != nil {
		t.Fatal(err)
	}
	if lset.Len() != fset.Len() {
		t.Fatalf("Lookup name:ann parity: leader %d follower %d", lset.Len(), fset.Len())
	}

	// Vector parity: nearest neighbour to v1's direction.
	lhits, err := leader.VectorSearch(ctx, "t", "emb", []float32{1, 0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	fhits, err := follower.VectorSearch(ctx, "t", "emb", []float32{1, 0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(lhits) != 1 || len(fhits) != 1 || lhits[0].ID != fhits[0].ID {
		t.Fatalf("vector search parity: leader %+v follower %+v", lhits, fhits)
	}

	if follower.AppliedSeq() != leader.CurrentSeq() {
		t.Fatalf("follower applied %d, leader current %d", follower.AppliedSeq(), leader.CurrentSeq())
	}
}

func TestResumeFromAppliedSeq(t *testing.T) {
	ctx := context.Background()
	leader := openRepl(t, 0)
	follower := openRepl(t, 0)

	if err := leader.Put(ctx, "t", "a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := leader.Put(ctx, "t", "b", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}
	// Apply only the first entry.
	applyAll(t, leader, follower, 1)
	// (applyAll drained all; simulate partial by re-applying from applied+1
	// after more writes.)
	if err := leader.Put(ctx, "t", "c", []byte(`{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	before := follower.AppliedSeq()
	applyAll(t, leader, follower, before+1)

	got, err := follower.Get(ctx, "t", "c")
	if err != nil {
		t.Fatalf("follower get c: %v", err)
	}
	if string(got) != `{"n":3}` {
		t.Fatalf("c = %s", got)
	}
	if follower.AppliedSeq() != leader.CurrentSeq() {
		t.Fatalf("applied %d current %d", follower.AppliedSeq(), leader.CurrentSeq())
	}
}

func TestSnapshotBootstrap(t *testing.T) {
	ctx := context.Background()
	leader := openRepl(t, 0)
	for i, doc := range [][]byte{[]byte(`{"n":1}`), []byte(`{"n":2}`)} {
		if err := leader.Put(ctx, "t", string(rune('a'+i)), doc); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot to a file.
	dir := t.TempDir()
	snapPath := filepath.Join(dir, "follower.bbolt")
	f, err := os.Create(snapPath)
	if err != nil {
		t.Fatal(err)
	}
	var seq uint64
	if err := leader.WriteSnapshot(f, func(s uint64) error { seq = s; return nil }); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if seq != leader.CurrentSeq() {
		t.Fatalf("snapshot seq %d, leader current %d", seq, leader.CurrentSeq())
	}

	// Open the follower from the snapshot; data should be present.
	follower, err := Open(snapPath, WithReplication(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = follower.Close() }()
	if err := follower.SetAppliedSeq(seq); err != nil {
		t.Fatal(err)
	}

	got, err := follower.Get(ctx, "t", "a")
	if err != nil {
		t.Fatalf("get a from snapshot: %v", err)
	}
	if string(got) != `{"n":1}` {
		t.Fatalf("a = %s", got)
	}
	if follower.AppliedSeq() != seq {
		t.Fatalf("applied %d want %d", follower.AppliedSeq(), seq)
	}

	// New leader writes then apply onto the bootstrapped follower.
	if err := leader.Put(ctx, "t", "c", []byte(`{"n":3}`)); err != nil {
		t.Fatal(err)
	}
	applyAll(t, leader, follower, follower.AppliedSeq()+1)
	if got, _ := follower.Get(ctx, "t", "c"); string(got) != `{"n":3}` {
		t.Fatalf("post-bootstrap c = %s", got)
	}
}

func TestTailSnapshotRequiredAfterTrim(t *testing.T) {
	ctx := context.Background()
	s := openRepl(t, 2) // keep only ~2 most-recent entries

	for i := 0; i < 10; i++ {
		if err := s.Put(ctx, "t", string(rune('a'+i)), []byte(`{"n":1}`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.trimOpLog(2); err != nil {
		t.Fatal(err)
	}
	if s.MinSeq() <= 1 {
		t.Fatalf("expected min_seq to advance, got %d", s.MinSeq())
	}
	// Requesting a trimmed-away seq must signal snapshot-required.
	err := s.TailOpLog(ctx, 1, func(*talondbpb.OpLogEntry) error { return nil })
	if !errors.Is(err, talondb.ErrSnapshotRequired) {
		t.Fatalf("want ErrSnapshotRequired, got %v", err)
	}
}

func TestStandaloneWritesNoOpLog(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := Open(filepath.Join(dir, "db.bbolt")) // no replication
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Put(ctx, "t", "a", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if s.CurrentSeq() != 0 {
		t.Fatalf("standalone should not write op-log, CurrentSeq=%d", s.CurrentSeq())
	}
}
