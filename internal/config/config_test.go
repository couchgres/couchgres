package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "couchgres.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReplicatorEnabledDefaultsTrue(t *testing.T) {
	t.Setenv("COUCHGRES_REPLICATOR_ENABLED", "")
	t.Setenv("COUCHGRES_REPLICATOR_ALLOW_PRIVATE_NETWORKS", "")
	cfg, err := Load(writeConfig(t, "{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Replicator.Enabled {
		t.Fatal("replicator.enabled defaulted to false")
	}
	if cfg.Replicator.AllowPrivateNetworks {
		t.Fatal("replicator.allow_private_networks defaulted to true")
	}
}

func TestReplicatorEnabledFromYAML(t *testing.T) {
	t.Setenv("COUCHGRES_REPLICATOR_ENABLED", "")
	cfg, err := Load(writeConfig(t, "replicator:\n  enabled: false\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Replicator.Enabled {
		t.Fatal("replicator.enabled YAML value was ignored")
	}
}

func TestReplicatorEnabledEnvOverride(t *testing.T) {
	t.Setenv("COUCHGRES_REPLICATOR_ENABLED", "false")
	cfg, err := Load(writeConfig(t, "replicator:\n  enabled: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Replicator.Enabled {
		t.Fatal("COUCHGRES_REPLICATOR_ENABLED override was ignored")
	}
}

func TestReplicatorEnabledEnvInvalid(t *testing.T) {
	t.Setenv("COUCHGRES_REPLICATOR_ENABLED", "sometimes")
	_, err := Load(writeConfig(t, "{}\n"))
	if err == nil || !strings.Contains(err.Error(), "COUCHGRES_REPLICATOR_ENABLED") {
		t.Fatalf("invalid env error: %v", err)
	}
}

func TestReplicatorPrivateNetworksConfiguration(t *testing.T) {
	t.Setenv("COUCHGRES_REPLICATOR_ENABLED", "")
	t.Setenv("COUCHGRES_REPLICATOR_ALLOW_PRIVATE_NETWORKS", "")
	cfg, err := Load(writeConfig(t, "replicator:\n  allow_private_networks: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Replicator.AllowPrivateNetworks {
		t.Fatal("replicator.allow_private_networks YAML value was ignored")
	}

	t.Setenv("COUCHGRES_REPLICATOR_ALLOW_PRIVATE_NETWORKS", "false")
	cfg, err = Load(writeConfig(t, "replicator:\n  allow_private_networks: true\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Replicator.AllowPrivateNetworks {
		t.Fatal("private network environment override was ignored")
	}

	t.Setenv("COUCHGRES_REPLICATOR_ALLOW_PRIVATE_NETWORKS", "sometimes")
	if _, err := Load(writeConfig(t, "{}\n")); err == nil ||
		!strings.Contains(err.Error(), "COUCHGRES_REPLICATOR_ALLOW_PRIVATE_NETWORKS") {
		t.Fatalf("invalid private network environment error: %v", err)
	}
}

func TestHTTPDefaults(t *testing.T) {
	for _, name := range []string{
		"COUCHGRES_HTTP_READ_HEADER_TIMEOUT",
		"COUCHGRES_HTTP_READ_TIMEOUT",
		"COUCHGRES_HTTP_WRITE_TIMEOUT",
		"COUCHGRES_HTTP_IDLE_TIMEOUT",
		"COUCHGRES_HTTP_MAX_HEADER_BYTES",
	} {
		t.Setenv(name, "")
	}
	cfg, err := Load(writeConfig(t, "{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.ReadHeaderTimeout.Std() != 10*time.Second ||
		cfg.HTTP.ReadTimeout.Std() != time.Minute ||
		cfg.HTTP.WriteTimeout.Std() != 5*time.Minute ||
		cfg.HTTP.IdleTimeout.Std() != 2*time.Minute ||
		cfg.HTTP.MaxHeaderBytes != 64<<10 {
		t.Fatalf("HTTP defaults: %+v", cfg.HTTP)
	}
}

func TestHTTPYAMLAndEnvironment(t *testing.T) {
	t.Setenv("COUCHGRES_HTTP_READ_TIMEOUT", "45s")
	t.Setenv("COUCHGRES_HTTP_MAX_HEADER_BYTES", "32768")
	cfg, err := Load(writeConfig(t, `
http:
  read_header_timeout: 3s
  write_timeout: 2m
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTP.ReadHeaderTimeout.Std() != 3*time.Second ||
		cfg.HTTP.ReadTimeout.Std() != 45*time.Second ||
		cfg.HTTP.WriteTimeout.Std() != 2*time.Minute ||
		cfg.HTTP.IdleTimeout.Std() != 2*time.Minute ||
		cfg.HTTP.MaxHeaderBytes != 32768 {
		t.Fatalf("HTTP config: %+v", cfg.HTTP)
	}
}

func TestHTTPConfigRejectsUnsafeValues(t *testing.T) {
	t.Run("zero_timeout", func(t *testing.T) {
		_, err := Load(writeConfig(t, "http:\n  read_timeout: 0s\n"))
		if err == nil || !strings.Contains(err.Error(), "read_timeout") {
			t.Fatalf("error: %v", err)
		}
	})
	t.Run("read_before_headers", func(t *testing.T) {
		_, err := Load(writeConfig(t, `
http:
  read_header_timeout: 20s
  read_timeout: 10s
`))
		if err == nil || !strings.Contains(err.Error(), "must not be shorter") {
			t.Fatalf("error: %v", err)
		}
	})
	t.Run("oversized_headers", func(t *testing.T) {
		_, err := Load(writeConfig(t, "http:\n  max_header_bytes: 1048577\n"))
		if err == nil || !strings.Contains(err.Error(), "max_header_bytes") {
			t.Fatalf("error: %v", err)
		}
	})
	t.Run("invalid_environment", func(t *testing.T) {
		t.Setenv("COUCHGRES_HTTP_IDLE_TIMEOUT", "forever")
		_, err := Load(writeConfig(t, "{}\n"))
		if err == nil || !strings.Contains(err.Error(), "COUCHGRES_HTTP_IDLE_TIMEOUT") {
			t.Fatalf("error: %v", err)
		}
	})
}
