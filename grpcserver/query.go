package grpcserver

import (
	"context"
	"fmt"
	"time"

	"github.com/opentalon/tln-db/bboltstore"
	"github.com/opentalon/tln-db/proto/tlndbpb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

// Query handles the structured-query RPC by translating proto clauses
// into the bboltstore composer's input types, running the composer,
// and encoding result rows back to google.protobuf.Value.
//
// The Server takes any tlndb.IndexedStore in its constructor, but
// the structured Query composer lives on bboltstore.Store specifically
// (it needs the bbolt-level Lookup + index access for narrowing). If
// the configured store isn't a bboltstore.Store, Query returns
// codes.Unimplemented — non-bbolt backends would need to ship their
// own composer.
func (s *Server) Query(ctx context.Context, req *tlndbpb.QueryRequest) (*tlndbpb.QueryResponse, error) {
	bbolt, ok := s.store.(*bboltstore.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "tlndb: structured Query requires a bboltstore backend")
	}
	clauses, err := decodeQueryClauses(req.GetWhere())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	aggs, err := decodeAggregates(req.GetAggregates())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rows, err := bbolt.Query(ctx, bboltstore.QueryRequest{
		EntityID:   req.GetEntityId(),
		Find:       req.GetFind(),
		Where:      clauses,
		Aggregates: aggs,
		GroupBy:    req.GetGroupBy(),
	})
	if err != nil {
		return nil, mapError(err)
	}
	return encodeQueryResponse(rows)
}

// QueryAsOf handles the time-travel query RPC. It shares Query's clause
// decoding and row encoding, delegating to the store's history-backed
// composer at the requested instant.
func (s *Server) QueryAsOf(ctx context.Context, req *tlndbpb.QueryAsOfRequest) (*tlndbpb.QueryResponse, error) {
	bbolt, ok := s.store.(*bboltstore.Store)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "tlndb: structured QueryAsOf requires a bboltstore backend")
	}
	clauses, err := decodeQueryClauses(req.GetWhere())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	aggs, err := decodeAggregates(req.GetAggregates())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	rows, err := bbolt.QueryAsOf(ctx, bboltstore.QueryRequest{
		EntityID:   req.GetEntityId(),
		Find:       req.GetFind(),
		Where:      clauses,
		Aggregates: aggs,
		GroupBy:    req.GetGroupBy(),
	}, time.Unix(0, req.GetAtUnixNanos()))
	if err != nil {
		return nil, mapError(err)
	}
	return encodeQueryResponse(rows)
}

// encodeQueryResponse projects composer rows into the proto response.
func encodeQueryResponse(rows []bboltstore.QueryRow) (*tlndbpb.QueryResponse, error) {
	out := &tlndbpb.QueryResponse{Rows: make([]*tlndbpb.QueryRow, 0, len(rows))}
	for _, row := range rows {
		values := make([]*structpb.Value, 0, len(row))
		for _, v := range row {
			pv, err := encodeQueryValue(v)
			if err != nil {
				return nil, status.Errorf(codes.Internal, "encode result value: %v", err)
			}
			values = append(values, pv)
		}
		out.Rows = append(out.Rows, &tlndbpb.QueryRow{Values: values})
	}
	return out, nil
}

func decodeQueryClauses(in []*tlndbpb.Clause) ([]bboltstore.QueryClause, error) {
	out := make([]bboltstore.QueryClause, 0, len(in))
	for _, c := range in {
		decoded, err := decodeQueryClause(c)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func decodeQueryClause(c *tlndbpb.Clause) (bboltstore.QueryClause, error) {
	if c == nil {
		return bboltstore.QueryClause{}, fmt.Errorf("nil clause")
	}
	switch x := c.GetClause().(type) {
	case *tlndbpb.Clause_Pattern:
		return bboltstore.QueryClause{Pattern: &bboltstore.QueryPattern{
			Entity:    decodeTerm(x.Pattern.GetEntity()),
			Attribute: x.Pattern.GetAttribute(),
			Value:     decodeTerm(x.Pattern.GetValue()),
		}}, nil
	case *tlndbpb.Clause_Predicate:
		return bboltstore.QueryClause{Predicate: &bboltstore.QueryPredicate{
			Op:    x.Predicate.GetOp(),
			Left:  decodeTerm(x.Predicate.GetLeft()),
			Right: decodeTerm(x.Predicate.GetRight()),
		}}, nil
	case *tlndbpb.Clause_Or:
		branches := make([][]bboltstore.QueryClause, 0, len(x.Or.GetBranches()))
		for _, b := range x.Or.GetBranches() {
			decoded, err := decodeQueryClauses(b.GetClauses())
			if err != nil {
				return bboltstore.QueryClause{}, err
			}
			branches = append(branches, decoded)
		}
		return bboltstore.QueryClause{Or: &bboltstore.QueryOr{Branches: branches}}, nil
	case *tlndbpb.Clause_Not:
		body, err := decodeQueryClauses(x.Not.GetBody())
		if err != nil {
			return bboltstore.QueryClause{}, err
		}
		return bboltstore.QueryClause{Not: &bboltstore.QueryNot{Body: body}}, nil
	case *tlndbpb.Clause_Fulltext:
		return bboltstore.QueryClause{FullText: &bboltstore.QueryFullText{
			Entity:    decodeTerm(x.Fulltext.GetEntity()),
			Query:     x.Fulltext.GetQuery(),
			Attribute: x.Fulltext.GetAttribute(),
		}}, nil
	}
	return bboltstore.QueryClause{}, fmt.Errorf("unknown clause variant")
}

func decodeAggregates(in []*tlndbpb.Aggregate) ([]bboltstore.QueryAggregate, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]bboltstore.QueryAggregate, 0, len(in))
	for _, a := range in {
		switch a.GetFn() {
		case "count", "sum", "total", "avg", "min", "max":
		default:
			return nil, fmt.Errorf("unknown aggregate function %q", a.GetFn())
		}
		out = append(out, bboltstore.QueryAggregate{
			Fn:   a.GetFn(),
			Over: decodeTerm(a.GetOver()),
			As:   a.GetAs(),
		})
	}
	return out, nil
}

func decodeTerm(t *tlndbpb.Term) bboltstore.QueryTerm {
	if t == nil {
		return bboltstore.QueryTerm{}
	}
	out := bboltstore.QueryTerm{Var: t.GetVar()}
	if t.GetLiteral() != nil {
		out.Literal = decodeQueryValue(t.GetLiteral())
	}
	return out
}

func decodeQueryValue(v *structpb.Value) any {
	if v == nil {
		return nil
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_NullValue:
		return nil
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		return k.NumberValue
	case *structpb.Value_BoolValue:
		return k.BoolValue
	case *structpb.Value_ListValue:
		list := k.ListValue.GetValues()
		out := make([]any, 0, len(list))
		for _, e := range list {
			out = append(out, decodeQueryValue(e))
		}
		return out
	case *structpb.Value_StructValue:
		m := map[string]any{}
		for kk, vv := range k.StructValue.GetFields() {
			m[kk] = decodeQueryValue(vv)
		}
		return m
	}
	return nil
}

func encodeQueryValue(v any) (*structpb.Value, error) {
	switch x := v.(type) {
	case nil:
		return structpb.NewNullValue(), nil
	case bool:
		return structpb.NewBoolValue(x), nil
	case string:
		return structpb.NewStringValue(x), nil
	case float64:
		return structpb.NewNumberValue(x), nil
	case float32:
		return structpb.NewNumberValue(float64(x)), nil
	case int:
		return structpb.NewNumberValue(float64(x)), nil
	case int32:
		return structpb.NewNumberValue(float64(x)), nil
	case int64:
		return structpb.NewNumberValue(float64(x)), nil
	}
	// Fallback: try structpb's reflection-based conversion. Handles
	// slices and maps the explicit branches above don't.
	return structpb.NewValue(v)
}
