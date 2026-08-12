package main

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// serverConfig holds the resolved listener/storage settings for
// talondb-server. Values are merged from four sources, in increasing
// order of precedence:
//
//	built-in defaults → config file (--config) → TALONDB_* env vars →
//	explicitly-set command-line flags
//
// This lets a Kubernetes ConfigMap (mounted config file) or Secret/
// ConfigMap-derived environment variables drive the server while still
// allowing a flag to override any single value on the command line.
type serverConfig struct {
	DB      string `yaml:"db"`
	Socket  string `yaml:"socket"`
	TCP     string `yaml:"tcp"`
	HTTP    string `yaml:"http"`
	Metrics string `yaml:"metrics"`

	// Replication.
	Role           string `yaml:"role"`            // standalone | leader | follower
	ReplicateFrom  string `yaml:"replicate_from"`  // follower: leader gRPC address
	OplogRetention string `yaml:"oplog_retention"` // leader/follower: max op-log entries kept
}

// defaultConfig returns the built-in defaults. These match the historical
// flag defaults so existing invocations keep behaving identically.
func defaultConfig() serverConfig {
	return serverConfig{
		DB:             "talondb.bbolt",
		Socket:         "",
		TCP:            "",
		HTTP:           "",
		Metrics:        "",
		Role:           "standalone",
		ReplicateFrom:  "",
		OplogRetention: "",
	}
}

// overlay copies every non-empty field of o onto c. Empty strings mean
// "unset — defer to a lower-precedence source", so a config file or env
// var can never blank out a value; only an explicit flag can (handled in
// resolveConfig).
func (c *serverConfig) overlay(o serverConfig) {
	if o.DB != "" {
		c.DB = o.DB
	}
	if o.Socket != "" {
		c.Socket = o.Socket
	}
	if o.TCP != "" {
		c.TCP = o.TCP
	}
	if o.HTTP != "" {
		c.HTTP = o.HTTP
	}
	if o.Metrics != "" {
		c.Metrics = o.Metrics
	}
	if o.Role != "" {
		c.Role = o.Role
	}
	if o.ReplicateFrom != "" {
		c.ReplicateFrom = o.ReplicateFrom
	}
	if o.OplogRetention != "" {
		c.OplogRetention = o.OplogRetention
	}
}

// loadConfigFile reads and parses a YAML config file.
func loadConfigFile(path string) (serverConfig, error) {
	var cfg serverConfig
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, fmt.Errorf("read config %q: %w", path, err)
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parse config %q: %w", path, err)
	}
	return cfg, nil
}

// resolveConfig merges configuration from all sources. flags carries the
// values parsed from the command line and setFlags records which flags the
// user actually passed (via flag.Visit), so a flag left at its default does
// not clobber an env/file value. configPath is the --config value; if empty
// the TALONDB_CONFIG env var is consulted. getenv is injected for testing.
func resolveConfig(flags serverConfig, setFlags map[string]bool, configPath string, getenv func(string) string) (serverConfig, error) {
	cfg := defaultConfig()

	// Config file (--config, else TALONDB_CONFIG).
	if configPath == "" {
		configPath = getenv("TALONDB_CONFIG")
	}
	if configPath != "" {
		fileCfg, err := loadConfigFile(configPath)
		if err != nil {
			return cfg, err
		}
		cfg.overlay(fileCfg)
	}

	// Environment variables.
	cfg.overlay(serverConfig{
		DB:             getenv("TALONDB_DB"),
		Socket:         getenv("TALONDB_SOCKET"),
		TCP:            getenv("TALONDB_TCP"),
		HTTP:           getenv("TALONDB_HTTP"),
		Metrics:        getenv("TALONDB_METRICS"),
		Role:           getenv("TALONDB_ROLE"),
		ReplicateFrom:  getenv("TALONDB_REPLICATE_FROM"),
		OplogRetention: getenv("TALONDB_OPLOG_RETENTION"),
	})

	// Explicitly-set flags win — including setting a value back to empty.
	if setFlags["db"] {
		cfg.DB = flags.DB
	}
	if setFlags["socket"] {
		cfg.Socket = flags.Socket
	}
	if setFlags["tcp"] {
		cfg.TCP = flags.TCP
	}
	if setFlags["http"] {
		cfg.HTTP = flags.HTTP
	}
	if setFlags["metrics"] {
		cfg.Metrics = flags.Metrics
	}
	if setFlags["role"] {
		cfg.Role = flags.Role
	}
	if setFlags["replicate-from"] {
		cfg.ReplicateFrom = flags.ReplicateFrom
	}
	if setFlags["oplog-retention"] {
		cfg.OplogRetention = flags.OplogRetention
	}

	return cfg, nil
}
