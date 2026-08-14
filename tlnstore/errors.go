package tlnstore

import (
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Typed error sentinels exposed by the adapter. Callers use
// errors.Is to distinguish failure categories without scraping
// strings or coupling to grpc/codes:
//
//	if errors.Is(err, tlndb.ErrNotFound) { ... }
//	if errors.Is(err, tlndb.ErrUnavailable) { /* retry */ }
//
// The map below covers every gRPC code tlndb-server emits today.
// Unknown codes fall through to ErrInternal so callers can blanket-
// log them without worrying about gaps.
var (
	// ErrNotFound is returned when a server RPC reports the target
	// document, attribute, or entity doesn't exist. Distinct from
	// "doc with that ID not yet written" — the server's Get RPC uses
	// the (Doc, Found) response shape for the latter; ErrNotFound
	// only surfaces when the server itself signals NotFound.
	ErrNotFound = errors.New("tlndb: not found")

	// ErrInvalidArgument is returned for client-side mistakes the
	// server rejected: malformed term, unknown aggregate fn, NaN/Inf
	// bounds on numeric range, etc. Callers should fix the request
	// rather than retry.
	ErrInvalidArgument = errors.New("tlndb: invalid argument")

	// ErrUnavailable is the transport-level retry signal. Server is
	// down, network blip, or backpressure (Subscribe stream overflow).
	// Callers may retry with backoff.
	ErrUnavailable = errors.New("tlndb: unavailable")

	// ErrUnauthenticated covers permission failures once auth lands.
	// Today no server RPC emits this, but having the sentinel reserved
	// lets callers' switch statements stay exhaustive.
	ErrUnauthenticated = errors.New("tlndb: unauthenticated")

	// ErrPermissionDenied is the per-tenant authorization signal once
	// auth lands. Reserved like ErrUnauthenticated.
	ErrPermissionDenied = errors.New("tlndb: permission denied")

	// ErrDeadlineExceeded surfaces when ctx times out mid-RPC or the
	// server itself reports a deadline.
	ErrDeadlineExceeded = errors.New("tlndb: deadline exceeded")

	// ErrCanceled surfaces when ctx is cancelled mid-RPC. Distinct
	// from ErrDeadlineExceeded because the cause is the caller, not
	// the elapsed deadline.
	ErrCanceled = errors.New("tlndb: canceled")

	// ErrResourceExhausted covers backpressure signals: subscribe-buffer
	// overflow on the server side is the headline source today. Treat
	// as Unavailable's bigger sibling — clients should reconnect /
	// resync rather than blindly retry the same call.
	ErrResourceExhausted = errors.New("tlndb: resource exhausted")

	// ErrInternal is the catch-all for non-actionable failures
	// (server bug, malformed proto, anything the client can't decide
	// on). Logged and propagated.
	ErrInternal = errors.New("tlndb: internal")
)

// mapStatusError converts a gRPC error into a typed tlndbError so
// callers can use errors.Is against the sentinels above, AND
// status.FromError keeps returning the original gRPC status (the
// custom Unwrap walks to the underlying error, which grpc-go's
// FromError follows).
//
// Non-gRPC errors flow through unchanged.
func mapStatusError(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	sentinel := sentinelForCode(st.Code())
	if sentinel == nil {
		return err
	}
	return &tlndbError{sentinel: sentinel, cause: err}
}

// wrapStatusErrorf prepends a contextual prefix to the message while
// preserving the typed sentinel + the original gRPC status. Analogue
// of fmt.Errorf("...: %w", err) but typed.
func wrapStatusErrorf(err error, format string, args ...any) error {
	if err == nil {
		return nil
	}
	prefix := fmt.Sprintf(format, args...)
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	sentinel := sentinelForCode(st.Code())
	if sentinel == nil {
		return fmt.Errorf("%s: %w", prefix, err)
	}
	return &tlndbError{sentinel: sentinel, cause: err, prefix: prefix}
}

// tlndbError carries a typed sentinel alongside the original gRPC
// error. errors.Is finds the sentinel via the Is method; errors.As
// (and grpc-go's status.FromError) reaches the gRPC error via the
// single-arg Unwrap.
type tlndbError struct {
	sentinel error
	cause    error
	prefix   string
}

func (e *tlndbError) Error() string {
	if e.prefix == "" {
		return fmt.Sprintf("%s: %s", e.sentinel.Error(), e.cause.Error())
	}
	return fmt.Sprintf("%s: %s: %s", e.prefix, e.sentinel.Error(), e.cause.Error())
}

func (e *tlndbError) Unwrap() error { return e.cause }

func (e *tlndbError) Is(target error) bool {
	return target == e.sentinel
}

// GRPCStatus lets status.FromError reach the underlying gRPC status
// without walking the Unwrap chain. The chain works too in principle,
// but having the type implement GRPCStatus directly bypasses any
// errors.As edge cases in grpc-go's FromError implementation.
func (e *tlndbError) GRPCStatus() *status.Status {
	st, _ := status.FromError(e.cause)
	return st
}

func sentinelForCode(c codes.Code) error {
	switch c {
	case codes.OK:
		return nil
	case codes.NotFound:
		return ErrNotFound
	case codes.InvalidArgument:
		return ErrInvalidArgument
	case codes.Unavailable:
		return ErrUnavailable
	case codes.Unauthenticated:
		return ErrUnauthenticated
	case codes.PermissionDenied:
		return ErrPermissionDenied
	case codes.DeadlineExceeded:
		return ErrDeadlineExceeded
	case codes.Canceled:
		return ErrCanceled
	case codes.ResourceExhausted:
		return ErrResourceExhausted
	}
	return ErrInternal
}
