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
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

// dial spins up a Server, registers it on an in-memory bufconn
// listener, and returns a ready-to-use client + cleanup func.
func dial(t *testing.T) (tlndbpb.TlnDBServiceClient, func()) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "test.db")
	store, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	tlndbpb.RegisterTlnDBServiceServer(srv, grpcserver.New(store, store.Events(), "test"))
	go func() { _ = srv.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return tlndbpb.NewTlnDBServiceClient(conn), func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = store.Close()
	}
}

func TestGRPCPutGetDelete(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Put(ctx, &tlndbpb.PutRequest{
		EntityId: "tenant-a", DocId: "doc-1",
		Doc: []byte(`{"hello":"world"}`),
	}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := client.Get(ctx, &tlndbpb.GetRequest{EntityId: "tenant-a", DocId: "doc-1"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !got.GetFound() || string(got.GetDoc()) != `{"hello":"world"}` {
		t.Fatalf("Get: found=%v doc=%q", got.GetFound(), got.GetDoc())
	}

	if _, err := client.Delete(ctx, &tlndbpb.DeleteRequest{EntityId: "tenant-a", DocId: "doc-1"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, _ = client.Get(ctx, &tlndbpb.GetRequest{EntityId: "tenant-a", DocId: "doc-1"})
	if got.GetFound() {
		t.Fatal("Get after Delete: still found")
	}
}

func TestGRPCLookupRoundtrip(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	_, _ = client.Put(ctx, &tlndbpb.PutRequest{
		EntityId: "tenant-a", DocId: "v1",
		Doc: []byte(`{"status":"active","km":45000}`),
	})
	_, _ = client.Put(ctx, &tlndbpb.PutRequest{
		EntityId: "tenant-a", DocId: "v2",
		Doc: []byte(`{"status":"retired","km":99999}`),
	})

	list, err := client.Lookup(ctx, &tlndbpb.LookupRequest{
		EntityId: "tenant-a", Term: "status:active",
	})
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(list.GetDocIds()) != 1 || list.GetDocIds()[0] != "v1" {
		t.Fatalf("Lookup: %v", list.GetDocIds())
	}
}

func TestGRPCNumericRange(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		body := []byte(`{"km":` + itoaTest(i*10) + `}`)
		_, _ = client.Put(ctx, &tlndbpb.PutRequest{EntityId: "tenant-a", DocId: docName(i), Doc: body})
	}
	list, err := client.LookupNumericRange(ctx, &tlndbpb.NumericRangeRequest{
		EntityId: "tenant-a", Attr: "km", Min: 20, Max: 40,
	})
	if err != nil {
		t.Fatalf("LookupNumericRange: %v", err)
	}
	if len(list.GetDocIds()) != 3 {
		t.Fatalf("got %v, want 3 entries", list.GetDocIds())
	}
}

func TestGRPCStats(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		_, _ = client.Put(ctx, &tlndbpb.PutRequest{
			EntityId: "tenant-a", DocId: docName(i),
			Doc: []byte(`{"km":` + itoaTest(i*10) + `}`),
		})
	}
	resp, err := client.Stats(ctx, &tlndbpb.StatsRequest{EntityId: "tenant-a", Attr: "km"})
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if resp.GetCount() != 5 || resp.GetMean() != 30 {
		t.Fatalf("Stats: %+v", resp)
	}
}

func TestGRPCHealth(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	resp, err := client.Health(context.Background(), nil)
	if err != nil {
		t.Fatalf("Health: %v", err)
	}
	if resp.GetStatus() != "ok" {
		t.Fatalf("Status = %q", resp.GetStatus())
	}
}

func TestGRPCGetMissingReturnsFound0(t *testing.T) {
	t.Parallel()
	client, cleanup := dial(t)
	defer cleanup()
	resp, err := client.Get(context.Background(), &tlndbpb.GetRequest{
		EntityId: "tenant-a", DocId: "no-such-doc",
	})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetFound() {
		t.Fatalf("missing doc returned found=true")
	}
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

func docName(i int) string { return "d" + itoaTest(i) }
