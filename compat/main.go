// Command compat replays YAML test cases against two
// CouchDB-speaking servers (real CouchDB and couchgres), normalizes the
// responses, and diffs them.
//
// Usage:
//
//	COUCHGRES_URL=http://localhost:5984 COUCH_URL=http://localhost:5985 \
//	  go run ./compat [cases-dir]
//
// When COUCH_URL is unset, the test cases run against couchgres alone and only
// each case's expect_status is checked.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

type caseFile struct {
	Cases []testCase `yaml:"cases"`
}

type testCase struct {
	Name    string            `yaml:"name"`
	Method  string            `yaml:"method"`
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
	Body    any               `yaml:"body"`
	// ExpectStatus is what couchgres must return (checked in both modes).
	ExpectStatus int `yaml:"expect_status"`
	// SkipDiff skips the response comparison, keeping only the status check.
	SkipDiff bool `yaml:"skip_diff"`
	// Ignore lists response members to blank before diffing, for values that
	// are implementation-defined (e.g. multi-key view offsets, which come
	// out of CouchDB's shard merge).
	Ignore []string `yaml:"ignore"`
}

type target struct {
	label string
	base  string
}

func (t *target) run(c *testCase) (int, any, error) {
	var body io.Reader
	if c.Body != nil {
		raw, err := json.Marshal(c.Body)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(c.Method, t.base+c.Path, body)
	if err != nil {
		return 0, nil, err
	}
	if c.Body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s: %s %s: %w", t.label, c.Method, c.Path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, err
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		v = string(raw)
	}
	return resp.StatusCode, v, nil
}

// blank replaces the named members with a placeholder anywhere they appear
// (per-case `ignore` list).
func blank(v any, names []string) any {
	if len(names) == 0 {
		return v
	}
	switch t := v.(type) {
	case []any:
		for i, item := range t {
			t[i] = blank(item, names)
		}
	case map[string]any:
		for key, item := range t {
			ignored := false
			for _, name := range names {
				if key == name {
					ignored = true
					break
				}
			}
			if ignored {
				t[key] = "<ignored>"
			} else {
				t[key] = blank(item, names)
			}
		}
	}
	return v
}

// normalize replaces values that legitimately differ between implementations
// (revs, seqs, uuids, sizes, timestamps) with stable placeholders.
func normalize(v any) any {
	switch t := v.(type) {
	case string:
		if isRev(t) {
			num, _, _ := strings.Cut(t, "-")
			return num + "-<hash>"
		}
		if len(t) == 32 && isHex(t) {
			return "<uuid>"
		}
		return t
	case []any:
		for i, item := range t {
			t[i] = normalize(item)
		}
		return t
	case map[string]any:
		for key, item := range t {
			switch key {
			case "update_seq", "purge_seq", "instance_start_time", "uuid",
				"git_sha", "vendor", "version", "features",
				"seq", "last_seq", "pending",
				"bookmark", "execution_time_ms":
				t[key] = "<varies>"
			case "sizes", "other", "data_size", "disk_size":
				t[key] = "<size>"
			case "results":
				// _changes row order is shard-merge order in clustered
				// CouchDB. The order is implementation-defined, so compare the set.
				t[key] = sortRowsByID(normalize(item))
			default:
				t[key] = normalize(item)
			}
		}
		return t
	default:
		return v
	}
}

// sortRowsByID orders an array of objects by their "id" member when every
// element has one. Anything else passes through unchanged.
func sortRowsByID(v any) any {
	rows, ok := v.([]any)
	if !ok {
		return v
	}
	for _, raw := range rows {
		obj, ok := raw.(map[string]any)
		if !ok {
			return v
		}
		if _, ok := obj["id"].(string); !ok {
			return v
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		iID, _ := rows[i].(map[string]any)["id"].(string)
		jID, _ := rows[j].(map[string]any)["id"].(string)
		return iID < jID
	})
	return rows
}

func isRev(s string) bool {
	num, hash, ok := strings.Cut(s, "-")
	if !ok || len(hash) < 32 {
		return false
	}
	if _, err := strconv.ParseUint(num, 10, 64); err != nil {
		return false
	}
	for _, c := range hash {
		if !isAlnumOrDash(c) {
			return false
		}
	}
	return true
}

func isAlnumOrDash(c rune) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
		(c >= '0' && c <= '9') || c == '_' || c == '-'
}

func isHex(s string) bool {
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}

func main() {
	casesDir := "compat/cases"
	if len(os.Args) > 1 {
		casesDir = os.Args[1]
	}
	couchgresURL := os.Getenv("COUCHGRES_URL")
	if couchgresURL == "" {
		couchgresURL = "http://localhost:5984"
	}
	couchgres := &target{label: "couchgres", base: couchgresURL}
	var couchdb *target
	if url := os.Getenv("COUCH_URL"); url != "" {
		couchdb = &target{label: "couchdb", base: url}
	} else {
		fmt.Fprintln(os.Stderr,
			"COUCH_URL not set: checking expected status codes only")
	}

	files, err := filepath.Glob(filepath.Join(casesDir, "*.yaml"))
	if err != nil || len(files) == 0 {
		fmt.Fprintf(os.Stderr, "no test case files in %s\n", casesDir)
		os.Exit(1)
	}
	sort.Strings(files)

	total, failures := 0, 0
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		var c caseFile
		if err := yaml.Unmarshal(data, &c); err != nil {
			fmt.Fprintf(os.Stderr, "parsing %s: %v\n", file, err)
			os.Exit(1)
		}
		fmt.Printf("== %s\n", filepath.Base(file))

		for i := range c.Cases {
			tc := &c.Cases[i]
			total++
			status, body, err := couchgres.run(tc)
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				os.Exit(1)
			}
			failed := false
			if tc.ExpectStatus != 0 && status != tc.ExpectStatus {
				fmt.Printf("FAIL %s: couchgres status %d, expected %d\n",
					tc.Name, status, tc.ExpectStatus)
				failed = true
			}
			if couchdb != nil && !failed {
				couchStatus, couchBody, err := couchdb.run(tc)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				if status != couchStatus {
					fmt.Printf("FAIL %s: status couchgres=%d couchdb=%d\n",
						tc.Name, status, couchStatus)
					failed = true
				} else if !tc.SkipDiff {
					mine, theirs := normalize(blank(body, tc.Ignore)), normalize(blank(couchBody, tc.Ignore))
					if !reflect.DeepEqual(mine, theirs) {
						mineJSON, _ := json.Marshal(mine)
						theirsJSON, _ := json.Marshal(theirs)
						fmt.Printf("FAIL %s: body mismatch\n  couchgres: %s\n  couchdb:   %s\n",
							tc.Name, mineJSON, theirsJSON)
						failed = true
					}
				}
			}
			if failed {
				failures++
			} else {
				fmt.Printf("ok   %s\n", tc.Name)
			}
		}
	}

	fmt.Printf("\n%d/%d cases passed\n", total-failures, total)
	if failures > 0 {
		os.Exit(1)
	}
}
