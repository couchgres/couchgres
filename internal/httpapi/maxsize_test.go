package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMaxHTTPRequestSize(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()

	resp := send(t, h, "PUT", "/_node/_local/_config/chttpd/max_http_request_size",
		decode(t, `"64"`), testAdminAuth, admin)
	if resp.status != 200 {
		t.Fatalf("config put: %+v", resp)
	}
	t.Cleanup(func() {
		send(t, h, "DELETE", "/_node/_local/_config/chttpd/max_http_request_size", nil,
			testAdminAuth, admin)
	})

	send(t, h, "DELETE", "/maxsize_db", nil, testAdminAuth, admin)
	if resp := send(t, h, "PUT", "/maxsize_db", nil, testAdminAuth, admin); resp.status != 201 {
		t.Fatalf("create db: %+v", resp)
	}
	defer send(t, h, "DELETE", "/maxsize_db", nil, testAdminAuth, admin)

	big := `{"x":"` + strings.Repeat("a", 200) + `"}`

	t.Run("content_length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/maxsize_db/cl", strings.NewReader(big))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(testAdminAuth, admin)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assertTooLarge(t, rec)
	})

	t.Run("unknown_length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/maxsize_db/ul", strings.NewReader(big))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(testAdminAuth, admin)
		req.ContentLength = -1
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assertTooLarge(t, rec)
	})

	t.Run("understated_length", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/maxsize_db/us", strings.NewReader(big))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set(testAdminAuth, admin)
		req.ContentLength = 10
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		assertTooLarge(t, rec)
	})
}

func TestRequestSizeConfigRejectsUnsafeValues(t *testing.T) {
	h := testHandler(t)
	admin := adminAuth()
	for _, path := range []string{
		"/_node/_local/_config/chttpd/max_http_request_size",
		"/_node/_local/_config/couchgres_httpd/max_attachment_request_size",
	} {
		for _, value := range []string{"0", "-1", "268435457", "invalid"} {
			resp := send(t, h, "PUT", path, value, testAdminAuth, admin)
			if resp.status != http.StatusBadRequest {
				t.Errorf("%s=%s: status=%d body=%+v", path, value, resp.status, resp.body)
			}
		}
	}
}

func assertTooLarge(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != 413 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body["error"] != "too_large" {
		t.Fatalf("body: %+v", body)
	}
}
