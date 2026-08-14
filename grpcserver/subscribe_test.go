package grpcserver_test

import (
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/bboltstore"
	"github.com/opentalon/tln-db/grpcserver"
	"github.com/opentalon/tln-db/proto/tlndbpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const subBufSize = 1 << 20

// dialSub spins up a real bboltstore + grpcserver behind a bufconn,
// returns a connected client + the store (for direct mutation) +
// cleanup func.
func dialSub(t *testing.T) (tlndbpb.TlnDBServiceClient, *bboltstore.Store, func()) {
	t.Helper()

	path := filepath.Join(t.TempDir(), "sub.db")
	store, err := bboltstore.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	lis := bufconn.Listen(subBufSize)
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
	return tlndbpb.NewTlnDBServiceClient(conn), store, func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = store.Close()
	}
}

// receiveN reads up to `want` events from the stream with a per-call
// timeout. Returns the events received and any error.
func receiveN(t *testing.T, stream tlndbpb.TlnDBService_SubscribeClient, want int, perEventTimeout time.Duration) []*tlndbpb.MutationEvent {
	t.Helper()
	out := make([]*tlndbpb.MutationEvent, 0, want)
	for i := 0; i < want; i++ {
		type recvResult struct {
			ev  *tlndbpb.MutationEvent
			err error
		}
		ch := make(chan recvResult, 1)
		go func() {
			ev, err := stream.Recv()
			ch <- recvResult{ev, err}
		}()
		select {
		case r := <-ch:
			if r.err != nil {
				if errors.Is(r.err, io.EOF) {
					return out
				}
				t.Fatalf("Recv[%d]: %v", i, r.err)
			}
			out = append(out, r.ev)
		case <-time.After(perEventTimeout):
			t.Fatalf("Recv[%d]: timeout after %s; got %d so far", i, perEventTimeout, len(out))
		}
	}
	return out
}

func TestSubscribeAssertChangeRetract(t *testing.T) {
	t.Parallel()
	client, store, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// Give the subscription a moment to register before we start writing.
	time.Sleep(50 * time.Millisecond)

	ctxw := context.Background()
	if err := store.Put(ctxw, "tenant-a", "doc-1", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("Put 1: %v", err)
	}
	if err := store.Put(ctxw, "tenant-a", "doc-1", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("Put 2: %v", err)
	}
	if err := store.Delete(ctxw, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got := receiveN(t, stream, 3, 2*time.Second)
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3", len(got))
	}
	want := []tlndbpb.MutationEventKind{
		tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_ASSERT,
		tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_CHANGE,
		tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_RETRACT,
	}
	for i, w := range want {
		if got[i].GetKind() != w {
			t.Errorf("event[%d] kind = %v, want %v", i, got[i].GetKind(), w)
		}
		if got[i].GetEntityId() != "tenant-a" || got[i].GetDocId() != "doc-1" {
			t.Errorf("event[%d] addr = %s/%s", i, got[i].GetEntityId(), got[i].GetDocId())
		}
	}
	if string(got[0].GetNewDoc()) != `{"v":1}` || len(got[0].GetOldDoc()) != 0 {
		t.Errorf("assert event payload wrong: old=%q new=%q", got[0].GetOldDoc(), got[0].GetNewDoc())
	}
	if string(got[1].GetOldDoc()) != `{"v":1}` || string(got[1].GetNewDoc()) != `{"v":2}` {
		t.Errorf("change event payload wrong: old=%q new=%q", got[1].GetOldDoc(), got[1].GetNewDoc())
	}
	if string(got[2].GetOldDoc()) != `{"v":2}` || len(got[2].GetNewDoc()) != 0 {
		t.Errorf("retract event payload wrong: old=%q new=%q", got[2].GetOldDoc(), got[2].GetNewDoc())
	}
}

func TestSubscribeEntityFilter(t *testing.T) {
	t.Parallel()
	client, store, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{EntityId: "tenant-a"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctxw := context.Background()
	if err := store.Put(ctxw, "tenant-a", "doc-1", []byte(`{}`)); err != nil {
		t.Fatalf("Put A: %v", err)
	}
	if err := store.Put(ctxw, "tenant-b", "doc-2", []byte(`{}`)); err != nil {
		t.Fatalf("Put B: %v", err)
	}
	if err := store.Put(ctxw, "tenant-a", "doc-3", []byte(`{}`)); err != nil {
		t.Fatalf("Put C: %v", err)
	}

	got := receiveN(t, stream, 2, 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2 (tenant-a only)", len(got))
	}
	for _, ev := range got {
		if ev.GetEntityId() != "tenant-a" {
			t.Errorf("leaked event from %q", ev.GetEntityId())
		}
	}
}

func TestSubscribePrefixFilter(t *testing.T) {
	t.Parallel()
	client, store, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{DocIdPrefix: "ticket-"})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctxw := context.Background()
	_ = store.Put(ctxw, "t", "ticket-1", []byte(`{}`))
	_ = store.Put(ctxw, "t", "user-1", []byte(`{}`))
	_ = store.Put(ctxw, "t", "ticket-2", []byte(`{}`))

	got := receiveN(t, stream, 2, 2*time.Second)
	if len(got) != 2 {
		t.Fatalf("got %d, want 2", len(got))
	}
	for _, ev := range got {
		if ev.GetDocId() != "ticket-1" && ev.GetDocId() != "ticket-2" {
			t.Errorf("prefix filter leaked %q", ev.GetDocId())
		}
	}
}

func TestSubscribeMultipleSubscribersEachGetCopy(t *testing.T) {
	t.Parallel()
	client, store, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 3
	streams := make([]tlndbpb.TlnDBService_SubscribeClient, n)
	for i := range streams {
		s, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{})
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		streams[i] = s
	}
	time.Sleep(100 * time.Millisecond)

	if err := store.Put(context.Background(), "t", "doc-1", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for i, s := range streams {
		go func(i int, s tlndbpb.TlnDBService_SubscribeClient) {
			defer wg.Done()
			got := receiveN(t, s, 1, 2*time.Second)
			if len(got) != 1 || got[0].GetDocId() != "doc-1" {
				t.Errorf("subscriber %d got %v", i, got)
			}
		}(i, s)
	}
	wg.Wait()
}

func TestSubscribeStreamEndsOnCancel(t *testing.T) {
	t.Parallel()
	client, _, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	cancel()

	// Recv should return promptly with a non-nil error (context cancelled
	// surfaces as transport error on the client side).
	deadline := time.After(2 * time.Second)
	done := make(chan struct{})
	go func() {
		_, _ = stream.Recv()
		close(done)
	}()
	select {
	case <-done:
		// ok
	case <-deadline:
		t.Fatal("Recv did not return after context cancel")
	}
}

func TestSubscribeDoesNotBlockCommits(t *testing.T) {
	t.Parallel()
	client, store, cleanup := dialSub(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Subscribe(ctx, &tlndbpb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	_ = stream

	// Write a few docs in quick succession; commits must not stall on
	// the subscriber. We don't drain the stream — we only care that
	// the writes complete in bounded time.
	deadline := time.Now().Add(2 * time.Second)
	for i := 0; i < 50; i++ {
		if time.Now().After(deadline) {
			t.Fatalf("Puts didn't finish in 2s (stalled on subscriber)")
		}
		docID := "doc-" + intToStr(i)
		if err := store.Put(context.Background(), "t", docID, []byte(`{}`)); err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
	}
}

// intToStr avoids importing strconv just for one use.
func intToStr(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// keep tlndb import live in case of future test additions
var _ tlndb.EventKind
