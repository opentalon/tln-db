// talondb-server is the standalone gRPC daemon that fronts a bbolt-
// backed talon-db. It exposes the TalonDBService over a Unix socket
// (primary), an optional TCP port (for remote callers), and an
// optional HTTP/JSON endpoint (for curl / non-Go clients) that
// translates JSON requests into in-process gRPC calls.
//
// Usage:
//
//	talondb-server --db ./talondb.bbolt
//	talondb-server --db ./talondb.bbolt --socket /tmp/talondb.sock --tcp :9899
//	talondb-server --db ./talondb.bbolt --http :8080
//	talondb-server --config /etc/talondb/config.yaml --metrics :9090
//
// Configuration may also come from a YAML file (--config / TALONDB_CONFIG)
// and TALONDB_* environment variables (TALONDB_DB, TALONDB_SOCKET,
// TALONDB_TCP, TALONDB_HTTP, TALONDB_METRICS). Precedence, low to high:
// built-in defaults < config file < env vars < explicit flags. This lets a
// Kubernetes ConfigMap or Secret drive the server. When --metrics is set,
// Prometheus metrics are served at GET /metrics on that address.
//
// On successful startup the server prints exactly one handshake line
// to stdout per listener — e.g.
//
//	talondb-server ready unix:///tmp/talondb.sock
//	talondb-server ready tcp://0.0.0.0:9899
//	talondb-server ready http://0.0.0.0:8080
//
// — so wrapper scripts can grep stdout for "ready" before issuing
// client calls. SIGINT and SIGTERM trigger a graceful shutdown.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/opentalon/talon-db/bboltstore"
	"github.com/opentalon/talon-db/grpcserver"
	"github.com/opentalon/talon-db/proto/talondbpb"
	"github.com/opentalon/talon-db/replica"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

// version is overwritten via -ldflags during release builds; the
// in-tree default is reported by the Health RPC.
var version = "dev"

func main() {
	var (
		dbPath         = flag.String("db", "talondb.bbolt", "path to the bbolt data file")
		socketPath     = flag.String("socket", "", "Unix-socket path for gRPC (empty = no Unix socket)")
		tcpAddr        = flag.String("tcp", "", "TCP address for gRPC (empty = no TCP listener), e.g. :9899")
		httpAddr       = flag.String("http", "", "TCP address for HTTP/JSON (empty = no HTTP listener), e.g. :8080")
		metricsAddr    = flag.String("metrics", "", "TCP address for Prometheus /metrics (empty = disabled), e.g. :9090")
		configPath     = flag.String("config", "", "path to a YAML config file (also TALONDB_CONFIG)")
		roleFlag       = flag.String("role", "standalone", "replication role: standalone | leader | follower")
		replicateFrom  = flag.String("replicate-from", "", "follower: leader gRPC address, e.g. leader:9899")
		oplogRetention = flag.String("oplog-retention", "", "leader/follower: max op-log entries kept (default 100000; 0 = keep all)")
	)
	flag.Parse()

	// Resolve configuration: defaults < config file < TALONDB_* env < flags.
	setFlags := map[string]bool{}
	flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })
	cfg, err := resolveConfig(
		serverConfig{
			DB: *dbPath, Socket: *socketPath, TCP: *tcpAddr, HTTP: *httpAddr, Metrics: *metricsAddr,
			Role: *roleFlag, ReplicateFrom: *replicateFrom, OplogRetention: *oplogRetention,
		},
		setFlags,
		*configPath,
		os.Getenv,
	)
	if err != nil {
		log.Fatalf("talondb-server: %v", err)
	}

	switch cfg.Role {
	case "standalone", "leader", "follower":
	default:
		log.Fatalf("talondb-server: invalid role %q (want standalone|leader|follower)", cfg.Role)
	}
	if cfg.Role == "follower" && cfg.ReplicateFrom == "" {
		log.Fatalf("talondb-server: --replicate-from (TALONDB_REPLICATE_FROM) is required for role=follower")
	}

	retention := uint64(100000)
	if cfg.OplogRetention != "" {
		n, err := strconv.ParseUint(cfg.OplogRetention, 10, 64)
		if err != nil {
			log.Fatalf("talondb-server: invalid oplog-retention %q: %v", cfg.OplogRetention, err)
		}
		retention = n
	}

	if cfg.Socket == "" && cfg.TCP == "" && cfg.HTTP == "" {
		// Default behaviour: a Unix socket next to the data file.
		cfg.Socket = filepath.Join(filepath.Dir(cfg.DB), "talondb.sock")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// A fresh follower bootstraps from the leader's Snapshot before it
	// opens the store for serving.
	var bootstrapSeq uint64
	didBootstrap := false
	if cfg.Role == "follower" && replica.NeedsBootstrap(cfg.DB) {
		log.Printf("talondb-server: bootstrapping follower from leader %s", cfg.ReplicateFrom)
		seq, err := replica.Bootstrap(ctx, cfg.ReplicateFrom, cfg.DB)
		if err != nil {
			log.Fatalf("talondb-server: bootstrap: %v", err)
		}
		bootstrapSeq = seq
		didBootstrap = true
		log.Printf("talondb-server: bootstrap installed at seq %d", seq)
	}

	var storeOpts []bboltstore.Option
	if cfg.Role != "standalone" {
		storeOpts = append(storeOpts, bboltstore.WithReplication(retention))
	}
	store, err := bboltstore.Open(cfg.DB, storeOpts...)
	if err != nil {
		log.Fatalf("talondb-server: open %q: %v", cfg.DB, err)
	}
	defer func() { _ = store.Close() }()

	if didBootstrap {
		if err := store.SetAppliedSeq(bootstrapSeq); err != nil {
			log.Fatalf("talondb-server: set applied seq: %v", err)
		}
	}

	srvOpts := []grpcserver.Option{grpcserver.WithRole(cfg.Role)}
	if cfg.Role == "follower" {
		srvOpts = append(srvOpts, grpcserver.WithReadOnly())
	}
	svc := grpcserver.New(store, store.Events(), version, srvOpts...)
	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(grpc_prometheus.StreamServerInterceptor),
	)
	talondbpb.RegisterTalonDBServiceServer(grpcSrv, svc)
	grpc_prometheus.Register(grpcSrv)

	// Follower: stream the leader's op-log in the background.
	if cfg.Role == "follower" {
		f := &replica.Follower{Store: store, LeaderAddr: cfg.ReplicateFrom, DBPath: cfg.DB}
		go func() {
			if err := f.Run(ctx); err != nil {
				log.Fatalf("talondb-server: follower: %v", err)
			}
		}()
	}

	var wg sync.WaitGroup
	httpSrv := (*http.Server)(nil)
	metricsSrv := (*http.Server)(nil)

	if cfg.Socket != "" {
		ln, err := listenUnix(cfg.Socket)
		if err != nil {
			log.Fatalf("talondb-server: %v", err)
		}
		defer func() { _ = os.Remove(cfg.Socket) }()
		announce("unix://" + cfg.Socket)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := grpcSrv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				log.Printf("talondb-server: unix serve: %v", err)
			}
		}()
	}

	if cfg.TCP != "" {
		ln, err := net.Listen("tcp", cfg.TCP)
		if err != nil {
			log.Fatalf("talondb-server: tcp listen %q: %v", cfg.TCP, err)
		}
		announce("tcp://" + ln.Addr().String())
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := grpcSrv.Serve(ln); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				log.Printf("talondb-server: tcp serve: %v", err)
			}
		}()
	}

	if cfg.HTTP != "" {
		httpSrv = startHTTPServer(cfg.HTTP, svc, &wg)
	}

	if cfg.Metrics != "" {
		metricsSrv = startMetricsServer(cfg.Metrics, &wg)
	}

	<-ctx.Done()
	log.Printf("talondb-server: shutdown signal received")

	grpcSrv.GracefulStop()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(shutdownCtx)
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(shutdownCtx)
	}
	wg.Wait()
}

// startMetricsServer exposes Prometheus metrics at GET /metrics — gRPC
// server RPC metrics (registered via grpc_prometheus) plus the default Go
// runtime and process collectors. Kubernetes scrapes this via a
// ServiceMonitor when metrics are enabled.
func startMetricsServer(addr string, wg *sync.WaitGroup) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	announce("http://" + addr + "/metrics")
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("talondb-server: metrics: %v", err)
		}
	}()
	return srv
}

func listenUnix(path string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("mkdir socket dir: %w", err)
	}
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen unix %q: %w", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = ln.Close()
		return nil, fmt.Errorf("chmod socket: %w", err)
	}
	return ln, nil
}

func announce(target string) {
	fmt.Printf("talondb-server ready %s\n", target)
}

// startHTTPServer exposes a JSON-over-HTTP surface that mirrors the
// gRPC method set. Each method is one POST: /v1/<method> with the
// request body as protojson. Useful for curl debugging and polyglot
// clients; production traffic should go via gRPC for performance.
func startHTTPServer(addr string, svc *grpcserver.Server, wg *sync.WaitGroup) *http.Server {
	mux := http.NewServeMux()
	register(mux, "Put", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.PutRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Put(ctx, req)
	})
	register(mux, "Get", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.GetRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Get(ctx, req)
	})
	register(mux, "Delete", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.DeleteRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Delete(ctx, req)
	})
	register(mux, "BatchPut", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.BatchPutRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.BatchPut(ctx, req)
	})
	register(mux, "Lookup", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.LookupRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Lookup(ctx, req)
	})
	register(mux, "LookupPrefix", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.LookupPrefixRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.LookupPrefix(ctx, req)
	})
	register(mux, "LookupNumericRange", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.NumericRangeRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.LookupNumericRange(ctx, req)
	})
	register(mux, "WindowQuery", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.WindowRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.WindowQuery(ctx, req)
	})
	register(mux, "GroupCount", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.GroupRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.GroupCount(ctx, req)
	})
	register(mux, "Stats", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.StatsRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Stats(ctx, req)
	})
	register(mux, "LastSeen", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.LastSeenRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.LastSeen(ctx, req)
	})
	register(mux, "Ancestors", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.AncestorsRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Ancestors(ctx, req)
	})
	register(mux, "Descendants", func(ctx context.Context, b []byte) (proto.Message, error) {
		req := &talondbpb.DescendantsRequest{}
		if err := protojson.Unmarshal(b, req); err != nil {
			return nil, err
		}
		return svc.Descendants(ctx, req)
	})
	mux.HandleFunc("GET /v1/health", func(w http.ResponseWriter, r *http.Request) {
		resp, err := svc.Health(r.Context(), &emptypb.Empty{})
		writeJSON(w, resp, err)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	announce("http://" + addr)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("talondb-server: http: %v", err)
		}
	}()
	return srv
}

func register(mux *http.ServeMux, method string, fn func(context.Context, []byte) (proto.Message, error)) {
	path := "/v1/" + method
	mux.HandleFunc("POST "+path, func(w http.ResponseWriter, r *http.Request) {
		body, err := readBody(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp, err := fn(r.Context(), body)
		writeJSON(w, resp, err)
	})
}

func readBody(r *http.Request) ([]byte, error) {
	const max = 16 << 20
	if r.ContentLength > max {
		return nil, fmt.Errorf("request body too large")
	}
	defer func() { _ = r.Body.Close() }()
	buf := make([]byte, 0, max)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if len(buf) > max {
			return nil, fmt.Errorf("request body too large")
		}
		if err != nil {
			break
		}
	}
	return buf, nil
}

func writeJSON(w http.ResponseWriter, msg proto.Message, err error) {
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
		return
	}
	if msg == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	out, marshalErr := protojson.Marshal(msg)
	if marshalErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": marshalErr.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(out)
}
