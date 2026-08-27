package httpapi

import (
	"strings"
	"testing"
)

func TestEndpointForDisplayRedactsPrivateURLCredentials(t *testing.T) {
	got, _ := endpointForDisplay("http://admin:secret@127.0.0.1:5984/db").(string)
	if got != "http://127.0.0.1:5984/db/" {
		t.Fatalf("redacted endpoint: %q", got)
	}
	if strings.Contains(got, "admin") || strings.Contains(got, "secret") {
		t.Fatalf("endpoint exposed credentials: %q", got)
	}
}
