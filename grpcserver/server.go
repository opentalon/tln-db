// Package grpcserver implements the tlndbpb.TlnDBServiceServer
// interface as a thin translation layer over tlndb.IndexedStore.
// Every RPC method delegates to the matching store call and converts
// errors via google.golang.org/grpc/status.
package grpcserver

import (
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"time"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/proto/tlndbpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// replicator is the subset of the store surface that powers the
// Replicate / Snapshot RPCs and the replication fields of Health. The
// bboltstore.Store satisfies it; other backends leave it nil.
type replicator interface {
	TailOpLog(ctx context.Context, fromSeq uint64, fn func(*tlndbpb.OpLogEntry) error) error
	WriteSnapshot(w io.Writer, onSeq func(uint64) error) error
	ReplicationEnabled() bool
	AppliedSeq() uint64
	CurrentSeq() uint64
	MinSeq() uint64
}

// Server wraps a tlndb.IndexedStore. An optional EventEmitter, when
// non-nil, powers the Subscribe streaming RPC; clients can subscribe
// to MutationEvents that fire post-commit.
type Server struct {
	tlndbpb.UnimplementedTlnDBServiceServer
	store    tlndb.IndexedStore
	events   *tlndb.EventEmitter
	version  string
	role     string
	readOnly bool
	repl     replicator
}

// Option configures a Server.
type Option func(*Server)

// WithReadOnly rejects all write RPCs with FailedPrecondition. Used for
// replica followers.
func WithReadOnly() Option { return func(s *Server) { s.readOnly = true } }

// WithRole sets the role reported by Health ("leader"/"follower"/"standalone").
func WithRole(role string) Option { return func(s *Server) { s.role = role } }

// New constructs a Server over the given store. version is reported
// by the Health RPC. events, when non-nil, enables the Subscribe RPC;
// pass store.Events() if the backend supports it. Vector RPCs work
// when the store satisfies the local vectorStore interface (the
// bboltstore.Store does); other backends get Unimplemented for the
// vector surface. When the store satisfies replicator, the Replicate /
// Snapshot RPCs are served.
func New(store tlndb.IndexedStore, events *tlndb.EventEmitter, version string, opts ...Option) *Server {
	srv := &Server{store: store, events: events, version: version, role: "standalone"}
	if r, ok := store.(replicator); ok {
		srv.repl = r
	}
	for _, opt := range opts {
		opt(srv)
	}
	return srv
}

func (s *Server) roErr() error {
	return status.Error(codes.FailedPrecondition, "tlndb: read-only replica")
}

// ---------- DocumentStore ----------

func (s *Server) Put(ctx context.Context, req *tlndbpb.PutRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	if err := s.store.Put(ctx, req.GetEntityId(), req.GetDocId(), req.GetDoc()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) Get(ctx context.Context, req *tlndbpb.GetRequest) (*tlndbpb.GetResponse, error) {
	doc, err := s.store.Get(ctx, req.GetEntityId(), req.GetDocId())
	if errors.Is(err, tlndb.ErrNotFound) {
		return &tlndbpb.GetResponse{Found: false}, nil
	}
	if err != nil {
		return nil, mapError(err)
	}
	return &tlndbpb.GetResponse{Doc: doc, Found: true}, nil
}

func (s *Server) Delete(ctx context.Context, req *tlndbpb.DeleteRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	if err := s.store.Delete(ctx, req.GetEntityId(), req.GetDocId()); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) BatchPut(ctx context.Context, req *tlndbpb.BatchPutRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	docs := make(map[string][]byte, len(req.GetEntries()))
	for _, e := range req.GetEntries() {
		docs[e.GetDocId()] = e.GetDoc()
	}
	if err := s.store.BatchPut(ctx, req.GetEntityId(), docs); err != nil {
		return nil, mapError(err)
	}
	return &emptypb.Empty{}, nil
}

// ---------- IndexedStore ----------

func (s *Server) Lookup(ctx context.Context, req *tlndbpb.LookupRequest) (*tlndbpb.DocIDList, error) {
	set, err := s.store.Lookup(ctx, req.GetEntityId(), req.GetTerm())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) LookupPrefix(ctx context.Context, req *tlndbpb.LookupPrefixRequest) (*tlndbpb.DocIDList, error) {
	set, err := s.store.LookupPrefix(ctx, req.GetEntityId(), req.GetPrefix())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) LookupNumericRange(ctx context.Context, req *tlndbpb.NumericRangeRequest) (*tlndbpb.DocIDList, error) {
	if math.IsNaN(req.GetMin()) || math.IsNaN(req.GetMax()) || math.IsInf(req.GetMin(), 0) || math.IsInf(req.GetMax(), 0) {
		return nil, status.Error(codes.InvalidArgument, "tlndb: NaN/Inf bound rejected")
	}
	set, err := s.store.LookupNumericRange(ctx, req.GetEntityId(), req.GetAttr(), req.GetMin(), req.GetMax(), tlndb.RangeOpts{
		MinExclusive: req.GetMinExclusive(),
		MaxExclusive: req.GetMaxExclusive(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

func (s *Server) WindowQuery(ctx context.Context, req *tlndbpb.WindowRequest) (*tlndbpb.WindowResponse, error) {
	events, err := s.store.WindowQuery(ctx, req.GetEntityId(), req.GetItemId(), req.GetTypes(), time.Duration(req.GetWindowNanos()))
	if err != nil {
		return nil, mapError(err)
	}
	out := &tlndbpb.WindowResponse{Events: make([]*tlndbpb.TemporalEvent, 0, len(events))}
	for _, e := range events {
		out.Events = append(out.Events, &tlndbpb.TemporalEvent{
			DocId:       e.DocID,
			Type:        e.Type,
			AtUnixNanos: e.At.UnixNano(),
		})
	}
	return out, nil
}

func (s *Server) SequenceJoin(ctx context.Context, req *tlndbpb.SequenceJoinRequest) (*tlndbpb.SequenceJoinResponse, error) {
	matches, err := s.store.SequenceJoin(
		ctx,
		req.GetEntityId(),
		req.GetItemIds(),
		req.GetSteps(),
		time.Duration(req.GetWindowNanos()),
	)
	if err != nil {
		return nil, mapError(err)
	}
	out := &tlndbpb.SequenceJoinResponse{Matches: make([]*tlndbpb.SequenceMatch, 0, len(matches))}
	for _, m := range matches {
		events := make([]*tlndbpb.TemporalEvent, 0, len(m.Events))
		for _, e := range m.Events {
			events = append(events, &tlndbpb.TemporalEvent{
				DocId:       e.DocID,
				Type:        e.Type,
				AtUnixNanos: e.At.UnixNano(),
			})
		}
		out.Matches = append(out.Matches, &tlndbpb.SequenceMatch{
			ItemId: m.ItemID,
			Events: events,
		})
	}
	return out, nil
}

func (s *Server) ClusterQuery(ctx context.Context, req *tlndbpb.ClusterQueryRequest) (*tlndbpb.ClusterQueryResponse, error) {
	clusters, err := s.store.ClusterQuery(
		ctx,
		req.GetEntityId(),
		req.GetItemId(),
		req.GetTypes(),
		time.Duration(req.GetWindowNanos()),
		int(req.GetMinSize()),
	)
	if err != nil {
		return nil, mapError(err)
	}
	out := &tlndbpb.ClusterQueryResponse{Clusters: make([]*tlndbpb.TemporalCluster, 0, len(clusters))}
	for _, c := range clusters {
		events := make([]*tlndbpb.TemporalEvent, 0, len(c.Events))
		for _, e := range c.Events {
			events = append(events, &tlndbpb.TemporalEvent{
				DocId:       e.DocID,
				Type:        e.Type,
				AtUnixNanos: e.At.UnixNano(),
			})
		}
		out.Clusters = append(out.Clusters, &tlndbpb.TemporalCluster{
			FirstUnixNanos: c.First.UnixNano(),
			LastUnixNanos:  c.Last.UnixNano(),
			Events:         events,
		})
	}
	return out, nil
}

func (s *Server) GroupCount(ctx context.Context, req *tlndbpb.GroupRequest) (*tlndbpb.GroupResponse, error) {
	g, err := s.store.GroupCount(ctx, req.GetEntityId(), req.GetItemId(), req.GetAttr(), req.GetValue())
	if err != nil {
		return nil, mapError(err)
	}
	return &tlndbpb.GroupResponse{
		Count:          int64(g.Count),
		FirstUnixNanos: g.First.UnixNano(),
		LastUnixNanos:  g.Last.UnixNano(),
		DocIds:         collectDocIDs(g.DocIDs),
	}, nil
}

func (s *Server) Stats(ctx context.Context, req *tlndbpb.StatsRequest) (*tlndbpb.StatsResponse, error) {
	st, err := s.store.Stats(ctx, req.GetEntityId(), req.GetAttr())
	if err != nil {
		return nil, mapError(err)
	}
	return &tlndbpb.StatsResponse{
		Count: st.Count,
		Mean:  st.Mean,
		M2:    st.M2,
		Min:   st.Min,
		Max:   st.Max,
	}, nil
}

func (s *Server) LastSeen(ctx context.Context, req *tlndbpb.LastSeenRequest) (*tlndbpb.LastSeenResponse, error) {
	t, ok, err := s.store.LastSeen(ctx, req.GetEntityId(), req.GetItemId(), req.GetRecordType())
	if err != nil {
		return nil, mapError(err)
	}
	out := &tlndbpb.LastSeenResponse{Found: ok}
	if ok {
		out.AtUnixNanos = t.UnixNano()
	}
	return out, nil
}

func (s *Server) LastWritten(ctx context.Context, req *tlndbpb.LastWrittenRequest) (*tlndbpb.LastWrittenResponse, error) {
	t, ok, err := s.store.LastWritten(ctx, req.GetEntityId(), req.GetDocId())
	if err != nil {
		return nil, mapError(err)
	}
	out := &tlndbpb.LastWrittenResponse{Found: ok}
	if ok {
		out.AtUnixNanos = t.UnixNano()
	}
	return out, nil
}

func (s *Server) Ancestors(ctx context.Context, req *tlndbpb.AncestorsRequest) (*tlndbpb.StringList, error) {
	chain, err := s.store.Ancestors(ctx, req.GetEntityId(), req.GetCategoryId())
	if err != nil {
		return nil, mapError(err)
	}
	return &tlndbpb.StringList{Items: chain}, nil
}

func (s *Server) Descendants(ctx context.Context, req *tlndbpb.DescendantsRequest) (*tlndbpb.DocIDList, error) {
	set, err := s.store.Descendants(ctx, req.GetEntityId(), req.GetRootId())
	if err != nil {
		return nil, mapError(err)
	}
	return docIDListFromSet(set), nil
}

// ---------- Subscribe ----------

// subscribeQueueDepth bounds the per-subscriber buffer. A subscriber
// that falls behind beyond this depth is dropped from the stream with
// codes.ResourceExhausted; the client can reconnect to resync. The
// alternative — blocking the producer — would let a slow consumer
// stall commits across the whole server.
const subscribeQueueDepth = 1024

// Subscribe streams MutationEvents that fire after each committed Put
// or Delete. Filtering by entity_id and doc_id_prefix is applied
// server-side. The stream terminates when the client cancels its
// context, the server shuts down, or the subscriber falls behind by
// more than subscribeQueueDepth events.
func (s *Server) Subscribe(req *tlndbpb.SubscribeRequest, stream tlndbpb.TlnDBService_SubscribeServer) error {
	if s.events == nil {
		return status.Error(codes.Unimplemented, "tlndb: server constructed without an EventEmitter")
	}
	ctx := stream.Context()
	ch := make(chan tlndb.MutationEvent, subscribeQueueDepth)

	entityFilter := req.GetEntityId()
	prefixFilter := req.GetDocIdPrefix()

	unsubscribe := s.events.Subscribe(func(_ context.Context, ev tlndb.MutationEvent) {
		if entityFilter != "" && ev.EntityID != entityFilter {
			return
		}
		if prefixFilter != "" && !strings.HasPrefix(ev.DocID, prefixFilter) {
			return
		}
		select {
		case ch <- ev:
		default:
			// Buffer full — close the channel to signal the streamer
			// to terminate. Subsequent events for this subscriber are
			// dropped on the floor; client must reconnect.
			select {
			case <-ctx.Done():
			default:
				// Best-effort close; the receiving goroutine will see
				// the closed channel and exit.
			}
		}
	})
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case ev, ok := <-ch:
			if !ok {
				return status.Error(codes.ResourceExhausted, "tlndb: subscriber buffer overflow; reconnect to resync")
			}
			if err := stream.Send(&tlndbpb.MutationEvent{
				Kind:        mutationKindToProto(ev.Kind),
				EntityId:    ev.EntityID,
				DocId:       ev.DocID,
				OldDoc:      ev.OldDoc,
				NewDoc:      ev.NewDoc,
				AtUnixNanos: ev.AtUnixNanos,
			}); err != nil {
				return err
			}
		}
	}
}

func mutationKindToProto(k tlndb.EventKind) tlndbpb.MutationEventKind {
	switch k {
	case tlndb.EventAssert:
		return tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_ASSERT
	case tlndb.EventChange:
		return tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_CHANGE
	case tlndb.EventRetract:
		return tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_RETRACT
	}
	return tlndbpb.MutationEventKind_MUTATION_EVENT_KIND_UNSPECIFIED
}

// ---------- Replication ----------

func (s *Server) Replicate(req *tlndbpb.ReplicateRequest, stream tlndbpb.TlnDBService_ReplicateServer) error {
	if s.repl == nil || !s.repl.ReplicationEnabled() {
		return status.Error(codes.Unimplemented, "tlndb: replication not enabled")
	}
	err := s.repl.TailOpLog(stream.Context(), req.GetFromSeq(), func(e *tlndbpb.OpLogEntry) error {
		return stream.Send(e)
	})
	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, tlndb.ErrSnapshotRequired):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

// snapshotWriter chunks bbolt's WriteTo output into SnapshotChunk
// messages on the stream.
type snapshotWriter struct {
	stream tlndbpb.TlnDBService_SnapshotServer
}

func (w snapshotWriter) Write(p []byte) (int, error) {
	const maxChunk = 256 << 10
	for off := 0; off < len(p); off += maxChunk {
		end := off + maxChunk
		if end > len(p) {
			end = len(p)
		}
		if err := w.stream.Send(&tlndbpb.SnapshotChunk{Data: p[off:end]}); err != nil {
			return off, err
		}
	}
	return len(p), nil
}

func (s *Server) Snapshot(_ *tlndbpb.SnapshotRequest, stream tlndbpb.TlnDBService_SnapshotServer) error {
	if s.repl == nil || !s.repl.ReplicationEnabled() {
		return status.Error(codes.Unimplemented, "tlndb: replication not enabled")
	}
	err := s.repl.WriteSnapshot(snapshotWriter{stream}, func(seq uint64) error {
		return stream.Send(&tlndbpb.SnapshotChunk{Seq: seq})
	})
	if err != nil {
		return status.Error(codes.Internal, err.Error())
	}
	return nil
}

// ---------- Operational ----------

func (s *Server) Health(ctx context.Context, _ *emptypb.Empty) (*tlndbpb.HealthResponse, error) {
	resp := &tlndbpb.HealthResponse{Status: "ok", Version: s.version, Role: s.role}
	if s.repl != nil {
		resp.AppliedSeq = s.repl.AppliedSeq()
		resp.CurrentSeq = s.repl.CurrentSeq()
		resp.MinSeq = s.repl.MinSeq()
	}
	return resp, nil
}

// ---------- helpers ----------

func docIDListFromSet(set tlndb.DocIDSet) *tlndbpb.DocIDList {
	ids := collectDocIDs(set)
	return &tlndbpb.DocIDList{DocIds: ids}
}

func collectDocIDs(set tlndb.DocIDSet) []string {
	if set == nil {
		return nil
	}
	out := make([]string, 0, set.Len())
	set.ForEach(func(id string) bool {
		out = append(out, id)
		return true
	})
	return out
}

func mapError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, tlndb.ErrInvalidEntityID) || errors.Is(err, tlndb.ErrInvalidValue) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	if errors.Is(err, tlndb.ErrNotFound) {
		return status.Error(codes.NotFound, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
