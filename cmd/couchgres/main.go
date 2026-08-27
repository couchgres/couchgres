package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/couchgres/couchgres/internal/config"
	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/httpapi"
	"github.com/couchgres/couchgres/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("couchgres exiting", "error", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := ""
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	setupLogging(cfg.Log)

	ctx, stop := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.Postgres.URL, cfg.Postgres.PoolSize)
	if err != nil {
		return err
	}
	defer st.Close()

	serverUUID, err := st.Bootstrap(ctx)
	if err != nil {
		return fmt.Errorf("bootstrapping: %w", err)
	}
	if err := seedAdmins(ctx, st, cfg.Admins); err != nil {
		return err
	}

	api, err := httpapi.New(ctx, st, serverUUID)
	if err != nil {
		return fmt.Errorf("initializing API: %w", err)
	}
	// The API owns the JavaScript pool. Stop background work, drain every HTTP
	// handler, and release the pool before the deferred store close.
	defer func() {
		stop()
		api.Close()
	}()
	addr := net.JoinHostPort(cfg.Bind, strconv.Itoa(cfg.Port))
	// Local replication endpoints loop back over HTTP; wildcard binds
	// loop back via localhost.
	selfHost := cfg.Bind
	if selfHost == "0.0.0.0" || selfHost == "::" || selfHost == "" {
		selfHost = "127.0.0.1"
	}
	api.SetReplicatorAllowPrivateNetworks(cfg.Replicator.AllowPrivateNetworks)
	api.SetSelfURL("http://" + net.JoinHostPort(selfHost, strconv.Itoa(cfg.Port)))
	api.SetStreamWriteTimeout(cfg.HTTP.WriteTimeout.Std())
	server := newHTTPServer(addr, api, cfg.HTTP)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		slog.Info("couchgres listening", "addr", "http://"+addr)
		errCh <- server.Serve(listener)
	}()
	if cfg.Replicator.Enabled {
		api.StartReplicatorWorker(ctx)
	}

	select {
	case err := <-errCh:
		// Serve closes its listeners before returning, but accepted connections
		// can still have active handlers. Cancel those before API cleanup waits
		// for them and releases the JavaScript pool.
		if closeErr := server.Close(); closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	case <-ctx.Done():
		slog.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				slog.Warn("graceful shutdown timed out; closing active connections")
			}
			if closeErr := server.Close(); closeErr != nil {
				return errors.Join(err, closeErr)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				return err
			}
		}
		return nil
	}
}

func newHTTPServer(addr string, handler http.Handler, cfg config.HTTP) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout.Std(),
		ReadTimeout:       cfg.ReadTimeout.Std(),
		WriteTimeout:      cfg.WriteTimeout.Std(),
		IdleTimeout:       cfg.IdleTimeout.Std(),
		MaxHeaderBytes:    cfg.MaxHeaderBytes,
	}
}

// seedAdmins hashes any not-yet-stored admins from the config file into the
// config tree, then refuses to run without at least one admin (CouchDB 3.x
// abolished admin party).
func seedAdmins(ctx context.Context, st *store.Store, admins map[string]string) error {
	iterations := couch.DefaultIterations
	if v, ok, err := st.ConfigGet(ctx, "couch_httpd_auth", "iterations"); err != nil {
		return err
	} else if ok {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			iterations = n
		}
	}
	for name, password := range admins {
		if _, exists, err := st.ConfigGet(ctx, "admins", name); err != nil {
			return err
		} else if exists {
			continue
		}
		hashed := couch.HashAdminPassword(password, iterations)
		if _, err := st.ConfigSet(ctx, "admins", name, hashed); err != nil {
			return err
		}
		slog.Info("stored admin", "name", name)
	}

	entries, err := st.ConfigAll(ctx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Section == "admins" {
			return nil
		}
	}
	return fmt.Errorf(
		"no server admins configured: add at least one under `admins:` in couchgres.yaml " +
			"or set COUCHGRES_ADMIN=name:password")
}

func setupLogging(level string) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl = slog.LevelInfo
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr,
		&slog.HandlerOptions{Level: lvl})))
}
