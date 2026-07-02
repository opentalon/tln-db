# talon-db

Go-native fact database for the [Talon language](https://github.com/opentalon/talon-language). Ships as a Go library for embedding and as a gRPC/HTTP sidecar (`talondb-server`) for sharing one store across multiple processes.

**Status:** Phase 3a. Document store + full index engine + sidecar server + composite Query/SequenceJoin/ClusterQuery/Subscribe RPCs + per-scope HNSW vector index shipped. Mutation engine, script VM, and RETE incremental matcher are next.

## What this is

talon-db is the Phase-3 storage backend behind the `FactStore` interface in `talon-language`. The pre-3 baseline is Datalevin (JVM); talon-db swaps that for a Go-native, bbolt-backed engine consumed either as a library (in-process) or via gRPC (Postgres-style local-socket sidecar).

It is built on four external Go primitives — `bbolt` (B+ tree storage), `RoaringBitmap` (compressed bitmap set ops), `vellum` (FST term dictionary; reserved for future prefix-accelerated lookups), and `snappy` (compression) — plus `google.golang.org/grpc` and `protobuf` for the wire protocol. Everything else is custom Go.

## Layers

```
talon-db
  ├── Document store   ✅ bbolt + snappy + per-tenant buckets, ACID, SIGKILL-durable
  ├── Index engine     ✅ inverted (roaring), numeric range, temporal, group-by,
  │                       closure table, Welford running stats, absence
  ├── Public API       ✅ talondb.IndexedStore (Lookup, LookupPrefix,
  │                       LookupNumericRange, WindowQuery, GroupCount, Stats,
  │                       LastSeen, LastWritten, Ancestors, Descendants)
  ├── Composite RPCs   ✅ structured Query (Pattern/Predicate/Or/Not/FullText +
  │                       Aggregates + GroupBy), SequenceJoin, ClusterQuery
  ├── Streaming        ✅ Subscribe — server-streamed MutationEvents for
  │                       reactive consumers
  ├── Vector index     ✅ per-(entity, scope) HNSW with dimension locked on
  │                       first insert; cosine + Euclidean; bbolt-backed
  │                       persistence + rebuild-on-Open
  ├── Sidecar          ✅ talondb-server: gRPC over Unix socket / TCP, HTTP/JSON
  ├── Mutation engine  ⏳ pre-commit hooks, reactive triggers, transactions (#29)
  ├── Script engine    ⏳ bytecode VM, cache, registry (#30)
  └── RETE engine      ⏳ incremental match for reactive blocks (#89)
```

## Quick start — library

Embed talon-db directly in your Go program. Open the store, then use the `IndexedStore` surface; closing flushes to disk.

```go
import (
    talondb "github.com/opentalon/talon-db"
    "github.com/opentalon/talon-db/bboltstore"
)

store, err := bboltstore.Open("talon.db")
if err != nil { log.Fatal(err) }
defer store.Close()

var _ talondb.IndexedStore = store  // type-check the surface

ctx := context.Background()
_ = store.Put(ctx, "tenant-a", "vehicle-1",
    []byte(`{"status":"active","category":"truck","km":45000}`))

// Term lookup against the inverted index
got, _ := store.Lookup(ctx, "tenant-a", ":status:active")
got.ForEach(func(id string) bool { fmt.Println(id); return true })
```

## Quick start — sidecar

Run `talondb-server` once and connect from multiple talon processes. Same flow Postgres uses: separate daemon, local socket by default.

```bash
# Build (or `go install ./cmd/talondb-server`)
go build -o /tmp/talondb-server ./cmd/talondb-server

# Run with a Unix socket (default), TCP, and HTTP all at once
/tmp/talondb-server \
  --db /var/lib/talondb.bbolt \
  --socket /var/run/talondb.sock \
  --tcp :9899 \
  --http :8080
```

On startup the server prints one handshake line per listener:

```
talondb-server ready unix:///var/run/talondb.sock
talondb-server ready tcp://0.0.0.0:9899
talondb-server ready http://0.0.0.0:8080
```

SIGINT / SIGTERM trigger graceful shutdown.

### gRPC client (from Go)

```go
import (
    "context"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    pb "github.com/opentalon/talon-db/proto/talondbpb"
)

conn, _ := grpc.NewClient("unix:///var/run/talondb.sock",
    grpc.WithTransportCredentials(insecure.NewCredentials()))
defer conn.Close()
svc := pb.NewTalonDBServiceClient(conn)

svc.Put(ctx, &pb.PutRequest{
    EntityId: "tenant-a", DocId: "vehicle-1",
    Doc: []byte(`{"status":"active"}`),
})
```

For the `talon-language` consumer, this is wrapped in `internal/talondb` — `talon run --store talon-db --talondb unix:///path/to.sock` is all that's needed at the call site.

### HTTP / curl

Useful for debugging or non-Go clients. Method names map 1:1 to `POST /v1/<Method>`; request body is protojson, response is protojson.

```bash
curl -s -X POST http://localhost:8080/v1/Put \
  -H 'Content-Type: application/json' \
  -d '{"entity_id":"tenant-a","doc_id":"vehicle-1","doc":"eyJzdGF0dXMiOiJhY3RpdmUifQ=="}'

curl -s -X POST http://localhost:8080/v1/Lookup \
  -H 'Content-Type: application/json' \
  -d '{"entity_id":"tenant-a","term":":status:active"}'

curl -s http://localhost:8080/v1/health
```

(`doc` is base64-encoded because protojson encodes `bytes` fields that way.)

## RPC surface

| Group | Methods |
|---|---|
| DocumentStore | `Put`, `Get`, `Delete`, `BatchPut`, `LastWritten` (doc `updated_at`) |
| Inverted lookup | `Lookup`, `LookupPrefix` |
| Numeric range | `LookupNumericRange` |
| Temporal | `WindowQuery` |
| Group-by | `GroupCount` |
| Statistics | `Stats` (Welford count / mean / m2 / min / max) |
| Absence | `LastSeen` |
| Closure | `Ancestors`, `Descendants` |
| Composite | `Query` (Pattern + Predicate + Or + Not + FullText + Aggregates + GroupBy), `SequenceJoin` (`A` followed_by `B`), `ClusterQuery` (N events within window) |
| Streaming | `Subscribe` — server-streamed `MutationEvent`s for reactive consumers |
| Vector index | `VectorInsert`, `VectorSearch`, `VectorDelete`, `VectorDropScope`, `VectorListScopes` — per-(entity, scope) HNSW with cosine or Euclidean distance |
| Ops | `Health` |

Schema lives in [`proto/talondb.proto`](proto/talondb.proto). Generated Go bindings are committed at [`proto/talondbpb/`](proto/talondbpb/).

## Vector index

Each `(entity, scope)` pair owns its own HNSW graph; the dimension is locked on the first insert into a scope so vectors from different embedding models never collide. Cosine is the default metric; Euclidean is opt-in.

```go
import (
    "github.com/opentalon/talon-db/bboltstore"
    "github.com/opentalon/talon-db/vectorindex"
)

store, _ := bboltstore.Open("talon.db")
defer store.Close()

ctx := context.Background()
_ = store.VectorInsert(ctx, "tenant-a", "embed3", "doc-1",
    []float32{0.1, 0.2, 0.3}, vectorindex.Cosine)
_ = store.VectorInsert(ctx, "tenant-a", "embed3", "doc-2",
    []float32{0.2, 0.1, 0.4}, vectorindex.Cosine)

hits, _ := store.VectorSearch(ctx, "tenant-a", "embed3",
    []float32{0.1, 0.2, 0.3}, 5)
for _, h := range hits {
    fmt.Println(h.ID, h.Distance)
}
```

The bbolt layer is authoritative: raw vector blobs live in `vec_data:{entity}:{scope}` and per-scope metadata in `vec_registry:{entity}`. On `Open` the in-memory HNSW is rebuilt by replaying every vector — a SIGKILL between commit and in-memory update can't lose data.

Same surface over the wire via gRPC: `VectorInsert`, `VectorSearch`, `VectorDelete`, `VectorDropScope`, `VectorListScopes`. The wrapper in talon-language [`internal/talondb`](https://github.com/opentalon/talon-language/tree/master/internal/talondb) exposes these to `find similar ... using vector scope "X"` rules.

### Tuning

The defaults (`M=16`, `EfSearch=20`) are tuned for tiny corpora. For SIFT-class workloads, bump them through `vectorindex.NewWithOptions`:

```go
idx := vectorindex.NewWithOptions(vectorindex.Options{
    M:        16,   // edges per node, per layer
    EfSearch: 200,  // beam width for both insert and search
})
```

The pipeline hits `recall@10 = 0.998` on SIFT-5K with these settings (see `vectorindex/sift_test.go`). Conformance against the full SIFT-1M corpus is env-gated behind `TALONDB_SIFT_PATH`; `TALONDB_SIFT_BASE_LIMIT` truncates the corpus for laptop-tractable runs.

## Testing

```bash
go test -race ./...                      # full suite
go test -bench=. ./bboltstore/           # benchmarks
go test -fuzz=FuzzPutGetRoundtrip ./bboltstore/   # fuzz
```

The conformance suite in [`talondbtest`](talondbtest/) (`talondbtest.IndexedSuite`) is the same set future backends (Pebble, in-memory, etc.) will be run against; the bbolt backend passes it today.

## References

- [opentalon/talon-language](https://github.com/opentalon/talon-language) — the consumer
- [#25 — RFC: talon-db umbrella](https://github.com/opentalon/talon-language/issues/25)
- [#26 — Document store](https://github.com/opentalon/talon-language/issues/26) (closed)
- [#27 — Index engine](https://github.com/opentalon/talon-language/issues/27) (closed)
- [#28 — Query engine composition](https://github.com/opentalon/talon-language/issues/28) (primitives shipped; sequence patterns + script integration remain)
- [#29 — Mutation engine](https://github.com/opentalon/talon-language/issues/29)
- [#30 — Script engine](https://github.com/opentalon/talon-language/issues/30)
- [#89 — RETE incremental match engine](https://github.com/opentalon/talon-language/issues/89)

## License

Apache 2.0. See [LICENSE](LICENSE).
