package bboltstore_test

import (
	"testing"

	tlndb "github.com/opentalon/tln-db"
	"github.com/opentalon/tln-db/tlndbtest"
)

// TestIndexedConformance wires the bbolt-backed Store into the
// backend-agnostic IndexedSuite. Specs are documented in
// tlndbtest/indexed.go.
func TestIndexedConformance(t *testing.T) {
	tlndbtest.IndexedSuite(t, func(t *testing.T) tlndb.IndexedStore {
		return newStore(t)
	})
}
