package main

import (
	"os"
	"path/filepath"
	"testing"
)

// envMap returns a getenv func backed by a map, for hermetic tests.
func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveConfig_Defaults(t *testing.T) {
	cfg, err := resolveConfig(serverConfig{}, nil, "", envMap(nil))
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if want := defaultConfig(); cfg != want {
		t.Fatalf("defaults: got %+v want %+v", cfg, want)
	}
}

func TestResolveConfig_FileOverlaysDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("db: /data/tlndb.bbolt\ntcp: \":9899\"\nhttp: \":8080\"\nmetrics: \":9090\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveConfig(serverConfig{}, nil, path, envMap(nil))
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.DB != "/data/tlndb.bbolt" || cfg.TCP != ":9899" || cfg.HTTP != ":8080" || cfg.Metrics != ":9090" {
		t.Fatalf("file overlay: got %+v", cfg)
	}
	if cfg.Socket != "" {
		t.Fatalf("socket should stay default empty, got %q", cfg.Socket)
	}
}

func TestResolveConfig_EnvBeatsFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("tcp: \":9899\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveConfig(serverConfig{}, nil, path, envMap(map[string]string{"TLNDB_TCP": ":7000"}))
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.TCP != ":7000" {
		t.Fatalf("env should beat file: got %q want :7000", cfg.TCP)
	}
}

func TestResolveConfig_FlagBeatsEnvAndFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("tcp: \":9899\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveConfig(
		serverConfig{TCP: ":7001"},
		map[string]bool{"tcp": true},
		path,
		envMap(map[string]string{"TLNDB_TCP": ":7000"}),
	)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.TCP != ":7001" {
		t.Fatalf("flag should beat env+file: got %q want :7001", cfg.TCP)
	}
}

func TestResolveConfig_UnsetFlagDoesNotClobber(t *testing.T) {
	// The flag value is present (parsed to its default "") but was NOT
	// explicitly set, so it must not overwrite the env value.
	cfg, err := resolveConfig(
		serverConfig{TCP: ""},
		map[string]bool{}, // nothing explicitly set
		"",
		envMap(map[string]string{"TLNDB_TCP": ":7000"}),
	)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.TCP != ":7000" {
		t.Fatalf("unset flag clobbered env: got %q want :7000", cfg.TCP)
	}
}

func TestResolveConfig_ExplicitEmptyFlagOverrides(t *testing.T) {
	// Explicitly passing --socket "" must blank out a file/env value.
	cfg, err := resolveConfig(
		serverConfig{Socket: ""},
		map[string]bool{"socket": true},
		"",
		envMap(map[string]string{"TLNDB_SOCKET": "/tmp/x.sock"}),
	)
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.Socket != "" {
		t.Fatalf("explicit empty flag should override: got %q", cfg.Socket)
	}
}

func TestResolveConfig_ConfigPathFromEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("http: \":8080\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := resolveConfig(serverConfig{}, nil, "", envMap(map[string]string{"TLNDB_CONFIG": path}))
	if err != nil {
		t.Fatalf("resolveConfig: %v", err)
	}
	if cfg.HTTP != ":8080" {
		t.Fatalf("TLNDB_CONFIG not honored: got %q", cfg.HTTP)
	}
}

func TestResolveConfig_MissingFileErrors(t *testing.T) {
	_, err := resolveConfig(serverConfig{}, nil, "/no/such/config.yaml", envMap(nil))
	if err == nil {
		t.Fatal("expected error for missing config file")
	}
}
