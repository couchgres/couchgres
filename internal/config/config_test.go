package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
	cfg, err := Load(writeConfig(t, "{}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Replicator.Enabled {
		t.Fatal("replicator.enabled defaulted to false")
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
