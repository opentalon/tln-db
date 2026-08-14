// Package tlndb is the Go-native embedded fact database for the Tln
// language. It is the planned Phase-3 backend behind the FactStore
// abstraction in github.com/opentalon/tln-language, replacing the
// Datalevin (JVM) backend after production access patterns are understood.
//
// Status: Phase 3a, pre-alpha. The current scope is the document store
// layer specified in tln-language issue #26: snappy-compressed JSON
// blobs in a bbolt B+ tree with per-tenant bucket isolation and ACID
// transactions. Index engine (#27), query engine (#28), mutation engine
// (#29), and script engine (#30) follow as separate milestones.
//
// The eventual headline feature is a RETE-based incremental match engine
// (#89) that consumes token deltas from tln-language's reactive
// dispatcher instead of rescanning the FactStore on every event. RETE's
// index requirements (alpha-memory lookup keyed on attribute,
// beta-memory hash joins) shape the storage design and are why the
// engine lives here rather than being retrofitted onto Datalevin.
//
// See the RFCs in tln-language for the full design: #15 (storage
// engine strategy), #25 (umbrella), #26–#30 (sub-components), #89
// (RETE engine).
package tlndb
