package couch

import (
	"strconv"
	"strings"
)

// SystemDBs are the databases allowed to begin with an underscore.
var SystemDBs = []string{"_users", "_replicator"}

func IsSystemDB(name string) bool {
	for _, s := range SystemDBs {
		if name == s {
			return true
		}
	}
	return false
}

// ValidateDBName checks a database name against CouchDB's rules.
// ^[a-z][a-z0-9_$()+/-]*$, or one of the system database names.
func ValidateDBName(name string) error {
	if IsSystemDB(name) {
		return nil
	}
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		return IllegalDatabaseName(name)
	}
	for _, c := range name[1:] {
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case strings.ContainsRune("_$()+/-", c):
		default:
			return IllegalDatabaseName(name)
		}
	}
	return nil
}

// ValidateDocID rejects reserved ids beginning with an underscore other
// than _design/ and _local/ documents.
func ValidateDocID(id string) error {
	if id == "" {
		return BadRequest("Document id must not be empty")
	}
	if id[0] == '_' && !strings.HasPrefix(id, "_design/") && !strings.HasPrefix(id, "_local/") {
		return NewError(400, "illegal_docid",
			"Only reserved document ids may start with underscore.")
	}
	return nil
}

// ResolveBuiltinReduce canonicalizes an underscore reduce name the way
// CouchDB 3.5 does. Builtin names match by prefix. Trailing characters,
// including "_sumorama", are disregarded. _top_N/_bottom_N rank
// reducers validate their rank. The second return is the CouchDB-shaped
// rejection for names that resolve to nothing.
func ResolveBuiltinReduce(name string) (string, *Error) {
	for _, prefix := range []string{"_top_", "_bottom_"} {
		if rest, ok := strings.CutPrefix(name, prefix); ok {
			n := 0
			ok := rest != ""
			for _, c := range rest {
				if c < '0' || c > '9' {
					ok = false
					break
				}
				n = n*10 + int(c-'0')
			}
			if !ok {
				return "", NewError(400, "invalid_design_doc", "invalid rank reducer")
			}
			if n < 1 || n > 100 {
				return "", NewError(400, "invalid_design_doc",
					"rank value must be between 1 and 100")
			}
			return prefix + strconv.Itoa(n), nil
		}
	}
	for _, builtin := range []string{
		"_approx_count_distinct", "_stats", "_sum", "_count", "_first", "_last",
	} {
		if strings.HasPrefix(name, builtin) {
			return builtin, nil
		}
	}
	return "", NewError(400, "invalid_design_doc",
		"`"+name+"` is not a supported reduce function.")
}
