package grpcserver

import (
	"context"
	"errors"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/proto/tlndbpb"
	"github.com/opentalon/tln-db/vectorindex"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// vectorStore is the slice of bboltstore.Store the vector RPCs need.
// Defined locally so backends are free to implement only the parts
// they support; if a store doesn't satisfy this interface the vector
// RPCs return Unimplemented.
type vectorStore interface {
	VectorInsert(ctx context.Context, entityID, scope, id string, vec []float32, metric vectorindex.Metric) error
	VectorSearch(ctx context.Context, entityID, scope string, query []float32, k int) ([]vectorindex.Result, error)
	VectorDelete(ctx context.Context, entityID, scope, id string) error
	VectorDropScope(ctx context.Context, entityID, scope string) error
	VectorListScopes(ctx context.Context, entityID string) ([]vectorindex.ScopeInfo, error)
}

func (s *Server) vectorBackend() (vectorStore, error) {
	if v, ok := s.store.(vectorStore); ok {
		return v, nil
	}
	return nil, status.Error(codes.Unimplemented, "vector RPCs require a vector-aware store")
}

// VectorInsert routes the wire metric enum to the vectorindex.Metric
// the in-memory index uses. The metric argument is only honoured on
// the first insert into a (entity, scope) pair; later inserts keep the
// scope's original metric.
func (s *Server) VectorInsert(ctx context.Context, req *tlndbpb.VectorInsertRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	v, err := s.vectorBackend()
	if err != nil {
		return nil, err
	}
	if err := v.VectorInsert(ctx,
		req.GetEntityId(),
		req.GetScope(),
		req.GetId(),
		req.GetVector(),
		metricFromProto(req.GetMetric()),
	); err != nil {
		return nil, mapVectorError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) VectorSearch(ctx context.Context, req *tlndbpb.VectorSearchRequest) (*tlndbpb.VectorSearchResponse, error) {
	v, err := s.vectorBackend()
	if err != nil {
		return nil, err
	}
	hits, err := v.VectorSearch(ctx,
		req.GetEntityId(),
		req.GetScope(),
		req.GetVector(),
		int(req.GetK()),
	)
	if err != nil {
		return nil, mapVectorError(err)
	}
	out := &tlndbpb.VectorSearchResponse{Hits: make([]*tlndbpb.VectorHit, 0, len(hits))}
	for _, h := range hits {
		out.Hits = append(out.Hits, &tlndbpb.VectorHit{Id: h.ID, Distance: h.Distance})
	}
	return out, nil
}

func (s *Server) VectorDelete(ctx context.Context, req *tlndbpb.VectorDeleteRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	v, err := s.vectorBackend()
	if err != nil {
		return nil, err
	}
	if err := v.VectorDelete(ctx, req.GetEntityId(), req.GetScope(), req.GetId()); err != nil {
		return nil, mapVectorError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) VectorDropScope(ctx context.Context, req *tlndbpb.VectorDropScopeRequest) (*emptypb.Empty, error) {
	if s.readOnly {
		return nil, s.roErr()
	}
	v, err := s.vectorBackend()
	if err != nil {
		return nil, err
	}
	if err := v.VectorDropScope(ctx, req.GetEntityId(), req.GetScope()); err != nil {
		return nil, mapVectorError(err)
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) VectorListScopes(ctx context.Context, req *tlndbpb.VectorListScopesRequest) (*tlndbpb.VectorListScopesResponse, error) {
	v, err := s.vectorBackend()
	if err != nil {
		return nil, err
	}
	scopes, err := v.VectorListScopes(ctx, req.GetEntityId())
	if err != nil {
		return nil, mapVectorError(err)
	}
	out := &tlndbpb.VectorListScopesResponse{Scopes: make([]*tlndbpb.VectorScope, 0, len(scopes))}
	for _, s := range scopes {
		out.Scopes = append(out.Scopes, &tlndbpb.VectorScope{
			Scope:  s.Scope,
			Dim:    int32(s.Dim),
			Count:  int32(s.Count),
			Metric: metricToProto(s.Metric),
		})
	}
	return out, nil
}

func metricFromProto(m tlndbpb.VectorMetric) vectorindex.Metric {
	switch m {
	case tlndbpb.VectorMetric_VECTOR_METRIC_EUCLIDEAN:
		return vectorindex.Euclidean
	default:
		// UNSPECIFIED + COSINE both fall through to Cosine — Cosine is
		// the right default for embeddings the typical caller carries.
		return vectorindex.Cosine
	}
}

func metricToProto(m vectorindex.Metric) tlndbpb.VectorMetric {
	switch m {
	case vectorindex.Euclidean:
		return tlndbpb.VectorMetric_VECTOR_METRIC_EUCLIDEAN
	default:
		return tlndbpb.VectorMetric_VECTOR_METRIC_COSINE
	}
}

// mapVectorError converts the vectorindex sentinels + tlndb.ErrNotFound
// to gRPC status codes. Dimension mismatch + empty-vector are caller
// bugs (InvalidArgument); ScopeNotFound / tlndb.ErrNotFound are
// NotFound; anything else is surfaced as Internal so it shows up
// loudly in logs.
func mapVectorError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, vectorindex.ErrDimensionMismatch),
		errors.Is(err, vectorindex.ErrEmptyVector):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, vectorindex.ErrScopeNotFound),
		errors.Is(err, tlndb.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
