// Package config loads couchgres.yaml (bind, Postgres URL, log level, seed admins).
// Runtime CouchDB settings are stored in couchgres (/_node/_local/_config).
package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Bind       string            `yaml:"bind"`
	Port       int               `yaml:"port"`
	Postgres   Postgres          `yaml:"postgres"`
	Replicator Replicator        `yaml:"replicator"`
	Admins     map[string]string `yaml:"admins"`
	Log        string            `yaml:"log"`
}

type Postgres struct {
	URL      string `yaml:"url"`
	PoolSize int32  `yaml:"pool_size"`
}

type Replicator struct {
	Enabled bool `yaml:"enabled"`
}

func defaults() Config {
	return Config{
		Bind: "127.0.0.1",
		Port: 5984,
		Postgres: Postgres{
			URL:      "postgres://localhost/couchgres",
			PoolSize: 16,
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
	if v := os.Getenv("COUCHGRES_REPLICATOR_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return cfg, fmt.Errorf("COUCHGRES_REPLICATOR_ENABLED must be a boolean: %q", v)
		}
		cfg.Replicator.Enabled = enabled
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
	return cfg, nil
}
