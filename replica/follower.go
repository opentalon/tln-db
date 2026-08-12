// Package replica implements the follower side of talon-db's async
// streaming read-replication. A follower bootstraps from a leader's
// Snapshot, then tails the leader's op-log via Replicate, applying each
// entry to its local store. It serves reads only; writes are rejected by
// the read-only grpcserver.
package replica

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/opentalon/talon-db/bboltstore"
	"github.com/opentalon/talon-db/proto/talondbpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// bootstrapMarker is created next to the db file when the follower must
// re-bootstrap on next start (leader could not serve the requested seq).
func bootstrapMarker(dbPath string) string { return dbPath + ".bootstrap" }

// NeedsBootstrap reports whether the follower must fetch a fresh
// snapshot before it can stream: either the db file is missing (fresh
// PVC) or a re-bootstrap marker was left by a prior run.
func NeedsBootstrap(dbPath string) bool {
	if _, err := os.Stat(bootstrapMarker(dbPath)); err == nil {
		return true
	}
	_, err := os.Stat(dbPath)
	return errors.Is(err, os.ErrNotExist)
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

// Bootstrap fetches a consistent snapshot from the leader and installs
// it at dbPath, returning the snapshot's op-log seq. Any existing db
// file and re-bootstrap marker are replaced.
func Bootstrap(ctx context.Context, leaderAddr, dbPath string) (uint64, error) {
	conn, err := dial(leaderAddr)
	if err != nil {
		return 0, fmt.Errorf("replica: dial leader %q: %w", leaderAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := talondbpb.NewTalonDBServiceClient(conn)
	stream, err := client.Snapshot(ctx, &talondbpb.SnapshotRequest{})
	if err != nil {
		return 0, fmt.Errorf("replica: open snapshot stream: %w", err)
	}

	// First message is the header carrying the snapshot seq.
	header, err := stream.Recv()
	if err != nil {
		return 0, fmt.Errorf("replica: snapshot header: %w", err)
	}
	seq := header.GetSeq()

	tmp := dbPath + ".bootstrap.tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return 0, fmt.Errorf("replica: create %q: %w", tmp, err)
	}
	if len(header.GetData()) > 0 {
		if _, err := f.Write(header.GetData()); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return 0, err
		}
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return 0, fmt.Errorf("replica: recv snapshot chunk: %w", err)
		}
		if _, err := f.Write(chunk.GetData()); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return 0, err
		}
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}
	if err := os.Rename(tmp, dbPath); err != nil {
		return 0, fmt.Errorf("replica: install snapshot: %w", err)
	}
	_ = os.Remove(bootstrapMarker(dbPath))
	return seq, nil
}

// Follower streams the leader's op-log into a local store.
type Follower struct {
	Store      *bboltstore.Store
	LeaderAddr string
	DBPath     string
}

// Run tails the leader until ctx is cancelled. On a transient error it
// reconnects with capped backoff, resuming from the last applied seq.
// If the leader can no longer serve the requested seq (its op-log has
// been trimmed past it), Run drops a re-bootstrap marker and returns a
// fatal error so a restart re-snapshots.
func (f *Follower) Run(ctx context.Context) error {
	backoff := time.Second
	const maxBackoff = 30 * time.Second
	for {
		applied, err := f.stream(ctx)
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		if status.Code(err) == codes.FailedPrecondition {
			_ = os.WriteFile(bootstrapMarker(f.DBPath), nil, 0o600)
			return fmt.Errorf("replica: leader requires re-bootstrap (%w); restart to re-snapshot", err)
		}
		if applied > 0 {
			backoff = time.Second // made progress; reset backoff
		}
		log.Printf("talondb-replica: stream error (retrying in %s): %v", backoff, err)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
}

// stream opens one Replicate stream from the last applied seq and
// applies entries until it errors. Returns the number of entries applied
// on this connection.
func (f *Follower) stream(ctx context.Context) (int, error) {
	conn, err := dial(f.LeaderAddr)
	if err != nil {
		return 0, err
	}
	defer func() { _ = conn.Close() }()

	client := talondbpb.NewTalonDBServiceClient(conn)
	from := f.Store.AppliedSeq() + 1
	stream, err := client.Replicate(ctx, &talondbpb.ReplicateRequest{FromSeq: from})
	if err != nil {
		return 0, err
	}

	applied := 0
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return applied, nil
		}
		if err != nil {
			return applied, err
		}
		if err := f.Store.ApplyEntry(entry); err != nil {
			return applied, fmt.Errorf("replica: apply seq %d: %w", entry.GetSeq(), err)
		}
		applied++
	}
}
