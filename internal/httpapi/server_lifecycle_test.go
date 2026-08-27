package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/couchgres/couchgres/internal/jsengine"
)

func TestServerCloseDrainsHandlersBeforeJavaScriptPool(t *testing.T) {
	pool := jsengine.NewPool(1, 2*time.Second)
	server := &Server{js: pool}
	started := make(chan struct{})
	release := make(chan struct{})
	handlerErr := make(chan error, 1)
	server.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		_, err := pool.MapDocs(r.Context(), "during-shutdown",
			[]string{`function(doc) { emit(1, null); }`}, nil,
			[]json.RawMessage{json.RawMessage(`{}`)})
		handlerErr <- err
		w.WriteHeader(http.StatusNoContent)
	})

	requestDone := make(chan struct{})
	go func() {
		server.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
		close(requestDone)
	}()
	<-started

	closeDone := make(chan struct{})
	go func() {
		server.Close()
		close(closeDone)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		server.serveMu.Lock()
		closing := server.closing
		server.serveMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not enter closing state")
		}
		time.Sleep(time.Millisecond)
	}

	late := httptest.NewRecorder()
	server.ServeHTTP(late, httptest.NewRequest("GET", "/", nil))
	if late.Code != http.StatusServiceUnavailable {
		t.Fatalf("request after shutdown started: status %d body %s", late.Code, late.Body)
	}
	select {
	case <-closeDone:
		t.Fatal("Server.Close returned before the active handler drained")
	default:
	}

	close(release)
	if err := <-handlerErr; err != nil {
		t.Fatalf("handler lost JavaScript pool during shutdown: %v", err)
	}
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("active handler did not return")
	}
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Server.Close did not return after handler drained")
	}

	_, err := pool.MapDocs(context.Background(), "after-shutdown",
		[]string{`function(doc) { emit(1, null); }`}, nil,
		[]json.RawMessage{json.RawMessage(`{}`)})
	if !errors.Is(err, jsengine.ErrClosed) {
		t.Fatalf("pool remained open after Server.Close: %v", err)
	}
	server.Close() // idempotent
}
