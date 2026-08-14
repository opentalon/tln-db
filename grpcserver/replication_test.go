package grpcserver_test

import (
	"context"
	"net"
	"path/filepath"
	"testing"

	"github.com/opentalon/tln-db/bboltstore"
	"github.com/opentalon/tln-db/grpcserver"
	"github.com/opentalon/tln-db/proto/tlndbpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dialRepl starts a Server over the given store with the given options
// and returns a client + cleanup.
func dialRepl(t *testing.T, store *bboltstore.Store, opts ...grpcserver.Option) (tlndbpb.TlnDBServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	tlndbpb.RegisterTlnDBServiceServer(srv, grpcserver.New(store, store.Events(), "test", opts...))
	go func() { _ = srv.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return tlndbpb.NewTlnDBServiceClient(conn), func() {
		_ = conn.Close()
		srv.GracefulStop()
	}
}

func TestReadOnlyRejectsWrites(t *testing.T) {
	store, err := bboltstore.Open(filepath.Join(t.TempDir(), "ro.db"), bboltstore.WithReplication(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	client, cleanup := dialRepl(t, store, grpcserver.WithRole("follower"), grpcserver.WithReadOnly())
	defer cleanup()
	ctx := context.Background()

	_, err = client.Put(ctx, &tlndbpb.PutRequest{EntityId: "t", DocId: "a", Doc: []byte(`{}`)})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Put on read-only: want FailedPrecondition, got %v", err)
	}

	h, err := client.Health(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h.GetRole() != "follower" {
		t.Fatalf("Health role = %q, want follower", h.GetRole())
	}
}

func TestReplicateRPC(t *testing.T) {
	ctx := context.Background()

	leaderStore, err := bboltstore.Open(filepath.Join(t.TempDir(), "leader.db"), bboltstore.WithReplication(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = leaderStore.Close() }()

	followerStore, err := bboltstore.Open(filepath.Join(t.TempDir(), "follower.db"), bboltstore.WithReplication(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = followerStore.Close() }()

	// Leader writes.
	for _, kv := range []struct{ id, doc string }{
		{"a", `{"n":1}`}, {"b", `{"n":2}`}, {"a", `{"n":3}`},
	} {
		if err := leaderStore.Put(ctx, "t", kv.id, []byte(kv.doc)); err != nil {
			t.Fatal(err)
		}
	}

	client, cleanup := dialRepl(t, leaderStore, grpcserver.WithRole("leader"))
	defer cleanup()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.Replicate(streamCtx, &tlndbpb.ReplicateRequest{FromSeq: 1})
	if err != nil {
		t.Fatal(err)
	}

	target := leaderStore.CurrentSeq()
	var last uint64
	for last < target {
		entry, err := stream.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if err := followerStore.ApplyEntry(entry); err != nil {
			t.Fatalf("apply seq %d: %v", entry.GetSeq(), err)
		}
		last = entry.GetSeq()
	}
	cancel()

	got, err := followerStore.Get(ctx, "t", "a")
	if err != nil {
		t.Fatalf("follower get a: %v", err)
	}
	if string(got) != `{"n":3}` {
		t.Fatalf("follower a = %s, want {\"n\":3}", got)
	}
	if followerStore.AppliedSeq() != target {
		t.Fatalf("follower applied %d, want %d", followerStore.AppliedSeq(), target)
	}
}
