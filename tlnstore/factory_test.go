package tlnstore_test

import (
	"testing"

	"github.com/opentalon/tln-db/tlnstore"
	"github.com/opentalon/tln-language/pkg/tln"
)

// TestFactory_RequiresTarget: a store config with no target is rejected before
// any dial.
func TestFactory_RequiresTarget(t *testing.T) {
	if _, err := tlnstore.Factory(tln.ConnectorSpec{Name: "db", Plugin: "db"}); err == nil {
		t.Fatal("expected an error when target is missing")
	}
}

// TestFactory_BuildsStore: a target yields a FactStore (the gRPC client is
// lazy, so no server is needed to construct it).
func TestFactory_BuildsStore(t *testing.T) {
	store, err := tlnstore.Factory(tln.ConnectorSpec{
		Name: "db", Config: map[string]string{"target": "unix:///tmp/tlndb-test.sock"},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	if store == nil {
		t.Fatal("factory returned a nil store")
	}
}
