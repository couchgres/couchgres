package main

import (
	"net/http"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/config"
)

func TestNewHTTPServerAppliesResourceLimits(t *testing.T) {
	cfg := config.HTTP{
		ReadHeaderTimeout: config.Duration(2 * time.Second),
		ReadTimeout:       config.Duration(3 * time.Second),
		WriteTimeout:      config.Duration(4 * time.Second),
		IdleTimeout:       config.Duration(5 * time.Second),
		MaxHeaderBytes:    8192,
	}
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer("127.0.0.1:0", handler, cfg)
	if server.ReadHeaderTimeout != 2*time.Second ||
		server.ReadTimeout != 3*time.Second ||
		server.WriteTimeout != 4*time.Second ||
		server.IdleTimeout != 5*time.Second ||
		server.MaxHeaderBytes != 8192 || server.Handler == nil {
		t.Fatalf("server limits: %+v", server)
	}
}
