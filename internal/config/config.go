// Package config loads couchgres.yaml (bind, Postgres URL, log level, seed admins).
// Runtime CouchDB settings are stored in couchgres (/_node/_local/_config).
package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Bind       string            `yaml:"bind"`
	Port       int               `yaml:"port"`
	Postgres   Postgres          `yaml:"postgres"`
	HTTP       HTTP              `yaml:"http"`
	Replicator Replicator        `yaml:"replicator"`
	Admins     map[string]string `yaml:"admins"`
	Log        string            `yaml:"log"`
}

type Postgres struct {
	URL      string `yaml:"url"`
	PoolSize int32  `yaml:"pool_size"`
}

// Duration is a human-readable YAML duration such as "10s" or "5m".
type Duration time.Duration

func (d *Duration) UnmarshalText(text []byte) error {
	value, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(value)
	return nil
}

func (d Duration) Std() time.Duration {
	return time.Duration(d)
}

// HTTP contains listener-level resource limits. These are startup settings
// because net/http fixes them when the server starts accepting connections.
type HTTP struct {
	ReadHeaderTimeout Duration `yaml:"read_header_timeout"`
	ReadTimeout       Duration `yaml:"read_timeout"`
	WriteTimeout      Duration `yaml:"write_timeout"`
	IdleTimeout       Duration `yaml:"idle_timeout"`
	MaxHeaderBytes    int      `yaml:"max_header_bytes"`
}

type Replicator struct {
	Enabled             bool `yaml:"enabled"`
	AllowPublicNetworks bool `yaml:"allow_public_networks"`
}

func defaults() Config {
	return Config{
		Bind: "127.0.0.1",
		Port: 5984,
		Postgres: Postgres{
			URL:      "postgres://localhost/couchgres",
			PoolSize: 16,
		},
		HTTP: HTTP{
			ReadHeaderTimeout: Duration(10 * time.Second),
			ReadTimeout:       Duration(60 * time.Second),
			WriteTimeout:      Duration(5 * time.Minute),
			IdleTimeout:       Duration(2 * time.Minute),
			MaxHeaderBytes:    64 << 10,
		},
		Replicator: Replicator{
			Enabled: true,
		},
		Log: "info",
	}
}

// Load reads the given path (or COUCHGRES_CONFIG, or ./couchgres.yaml when
// present), then applies env overrides. A missing default file is fine.
func Load(path string) (Config, error) {
	cfg := defaults()

	if path == "" {
		path = os.Getenv("COUCHGRES_CONFIG")
	}
	optional := false
	if path == "" {
		path = "couchgres.yaml"
		optional = true
	}
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		dec := yaml.NewDecoder(bytes.NewReader(data))
		dec.KnownFields(true)
		if err := dec.Decode(&cfg); err != nil {
			return cfg, fmt.Errorf("parsing %s: %w", path, err)
		}
	case optional && os.IsNotExist(err):
	default:
		return cfg, fmt.Errorf("reading config %s: %w", path, err)
	}

	if v := os.Getenv("COUCHGRES_BIND"); v != "" {
		cfg.Bind = v
	}
	if v := os.Getenv("COUCHGRES_PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("COUCHGRES_PORT must be a port number: %q", v)
		}
		cfg.Port = port
	}
	if v := os.Getenv("COUCHGRES_PG_URL"); v != "" {
		cfg.Postgres.URL = v
	}
	for name, target := range map[string]*Duration{
		"COUCHGRES_HTTP_READ_HEADER_TIMEOUT": &cfg.HTTP.ReadHeaderTimeout,
		"COUCHGRES_HTTP_READ_TIMEOUT":        &cfg.HTTP.ReadTimeout,
		"COUCHGRES_HTTP_WRITE_TIMEOUT":       &cfg.HTTP.WriteTimeout,
		"COUCHGRES_HTTP_IDLE_TIMEOUT":        &cfg.HTTP.IdleTimeout,
	} {
		if value := os.Getenv(name); value != "" {
			if err := target.UnmarshalText([]byte(value)); err != nil {
				return cfg, fmt.Errorf("%s must be a duration: %q", name, value)
			}
		}
	}
	if value := os.Getenv("COUCHGRES_HTTP_MAX_HEADER_BYTES"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil {
			return cfg, fmt.Errorf(
				"COUCHGRES_HTTP_MAX_HEADER_BYTES must be an integer: %q", value)
		}
		cfg.HTTP.MaxHeaderBytes = n
	}
	if v := os.Getenv("COUCHGRES_REPLICATOR_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return cfg, fmt.Errorf("COUCHGRES_REPLICATOR_ENABLED must be a boolean: %q", v)
		}
		cfg.Replicator.Enabled = enabled
	}
	if v := os.Getenv("COUCHGRES_REPLICATOR_ALLOW_PUBLIC_NETWORKS"); v != "" {
		allow, err := strconv.ParseBool(v)
		if err != nil {
			return cfg, fmt.Errorf(
				"COUCHGRES_REPLICATOR_ALLOW_PUBLIC_NETWORKS must be a boolean: %q", v)
		}
		cfg.Replicator.AllowPublicNetworks = allow
	}
	// COUCHGRES_ADMIN=name:password adds a server admin without a config
	// file (used by CI and quick starts).
	if v := os.Getenv("COUCHGRES_ADMIN"); v != "" {
		name, password, ok := strings.Cut(v, ":")
		if !ok || name == "" {
			return cfg, fmt.Errorf("COUCHGRES_ADMIN must be name:password")
		}
		if cfg.Admins == nil {
			cfg.Admins = map[string]string{}
		}
		cfg.Admins[name] = password
	}
	if err := validateHTTP(cfg.HTTP); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func validateHTTP(cfg HTTP) error {
	for name, value := range map[string]Duration{
		"read_header_timeout": cfg.ReadHeaderTimeout,
		"read_timeout":        cfg.ReadTimeout,
		"write_timeout":       cfg.WriteTimeout,
		"idle_timeout":        cfg.IdleTimeout,
	} {
		if value <= 0 {
			return fmt.Errorf("http.%s must be greater than zero", name)
		}
	}
	if cfg.ReadTimeout < cfg.ReadHeaderTimeout {
		return fmt.Errorf("http.read_timeout must not be shorter than http.read_header_timeout")
	}
	if cfg.MaxHeaderBytes < 1024 || cfg.MaxHeaderBytes > 1<<20 {
		return fmt.Errorf("http.max_header_bytes must be between 1024 and 1048576")
	}
	return nil
}
