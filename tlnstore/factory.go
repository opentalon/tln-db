package tlnstore

import (
	"context"
	"fmt"

	"github.com/opentalon/tln-language/pkg/tln"
)

// Factory dials the tlndb-server named by the store config and returns the
// FactStore adapter, so tln-db can be selected as "the store" from a project's
// config/store.tln (ADR 0013):
//
//	store db { target env "TLNDB_ADDR" }   // e.g. unix:///tmp/tlndb.sock
//
// It matches core's StoreFactory shape (func(tln.ConnectorSpec) (tln.FactStore,
// error)), so `tln bundle` wires it via tln.WithStorePlugin. tln-db is a
// sidecar: `tlndb-server` runs separately; this builds only the gRPC client.
func Factory(spec tln.ConnectorSpec) (tln.FactStore, error) {
	target := spec.Config["target"]
	if target == "" {
		return nil, fmt.Errorf("tln-db: store %q requires a `target` (e.g. unix:///tmp/tlndb.sock)", spec.Name)
	}
	cli, err := NewClient(context.Background(), target)
	if err != nil {
		return nil, fmt.Errorf("tln-db: dial %s: %w", target, err)
	}
	return New(cli), nil
}
