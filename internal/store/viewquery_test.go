package store

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuiltinReduceSumNonFiniteVectorReturnsError(t *testing.T) {
	_, err := builtinReduce("_sum", []reduceRowGroup{{
		values: []json.RawMessage{json.RawMessage(`[1e999]`)},
	}})
	if err == nil {
		t.Fatal("expected non-finite reduce value to return an error")
	}
	if !strings.Contains(err.Error(), "marshal view JSON") {
		t.Fatalf("expected marshal error, got %v", err)
	}
}
