package couch

import (
	"encoding/json"
	"math"
	"strings"
)

// UserDocPrefix is the mandatory _users document id prefix.
const UserDocPrefix = "org.couchdb.user:"

// UserRecord is what authentication needs from a _users document.
type UserRecord struct {
	Name     string
	Roles    []string
	Salt     string
	Password *HashedPassword
}

// UserRecordFromDoc extracts credentials from a _users doc body.
func UserRecordFromDoc(body map[string]any) (*UserRecord, bool) {
	name, ok := body["name"].(string)
	if !ok {
		return nil, false
	}
	record := &UserRecord{Name: name}
	if roles, ok := body["roles"].([]any); ok {
		for _, role := range roles {
			if s, ok := role.(string); ok {
				record.Roles = append(record.Roles, s)
			}
		}
	}
	if password, present := passwordHashFromDoc(body); present &&
		password.Validate(MinPasswordIterations, MaxPasswordIterations) == nil {
		record.Salt = password.Salt
		record.Password = password
	}
	return record, true
}

// ValidateUserDoc is CouchDB's built-in auth-ddoc validate_doc_update,
// natively. old is the current body for updates. Deleting marks a deletion.
// The exact reason strings matter to clients.
func ValidateUserDoc(
	docid string,
	doc, old map[string]any,
	deleting bool,
	u *UserCtx,
	minIterations, maxIterations int,
) error {
	isAdmin := u.IsServerAdmin()

	if deleting {
		if isAdmin {
			return nil
		}
		oldName, _ := old["name"].(string)
		if oldName != "" && oldName == u.Name {
			return nil
		}
		return Forbidden("Only admins may delete other user docs.")
	}

	if docType, _ := doc["type"].(string); docType != "user" {
		return Forbidden("doc.type must be user")
	}
	name, _ := doc["name"].(string)
	if name == "" {
		return Forbidden("doc.name is required")
	}
	if docid != UserDocPrefix+name {
		return Forbidden("Doc ID must be of the form org.couchdb.user:name")
	}
	roles, err := stringRoles(doc)
	if err != nil {
		return err
	}
	for _, role := range roles {
		if strings.HasPrefix(role, "_") {
			return Forbidden("No system roles (starting with underscore) in users db.")
		}
	}
	if err := validateUserPassword(doc, minIterations, maxIterations); err != nil {
		return Forbidden("Invalid password hash parameters.")
	}

	if !isAdmin {
		if u.Name != name {
			return Forbidden("You may only update your own user document.")
		}
		if old == nil {
			if len(roles) > 0 {
				return Forbidden("Only _admin may set roles")
			}
		} else {
			oldRoles, _ := stringRoles(old)
			if !equalStrings(roles, oldRoles) {
				return Forbidden("Only _admin may edit roles")
			}
		}
	}
	return nil
}

// PrepareUserDoc is CouchDB's before_doc_update for _users: a plaintext
// "password" member is replaced with pbkdf2 fields before storage.
func PrepareUserDoc(body map[string]any, iterations int) error {
	password, ok := body["password"].(string)
	if !ok {
		return nil
	}
	h, err := HashUserPasswordChecked(password, iterations)
	if err != nil {
		return err
	}
	delete(body, "password")
	body["password_scheme"] = h.Scheme
	body["derived_key"] = h.DerivedKey
	body["salt"] = h.Salt
	body["iterations"] = h.Iterations
	return nil
}

// validateUserPassword allows passwordless user records, or a plaintext
// password that PrepareUserDoc will replace. Pre-hashed fields must be complete
// and valid before the document can be stored.
func validateUserPassword(body map[string]any, minIterations, maxIterations int) error {
	if plaintext, present := body["password"]; present {
		if _, ok := plaintext.(string); !ok {
			return ErrInvalidPasswordHash
		}
		return ValidatePasswordIterationRange(minIterations, maxIterations)
	}
	h, present := passwordHashFromDoc(body)
	if !present {
		return nil
	}
	return h.Validate(minIterations, maxIterations)
}

// passwordHashFromDoc returns present=true when any stored password-hash field
// exists. A nil hash with present=true means the fields were incomplete or had
// invalid JSON types.
func passwordHashFromDoc(body map[string]any) (*HashedPassword, bool) {
	schemeValue, hasScheme := body["password_scheme"]
	derivedValue, hasDerived := body["derived_key"]
	saltValue, hasSalt := body["salt"]
	_, hasIterations := body["iterations"]
	present := hasScheme || hasDerived || hasSalt || hasIterations
	if !present {
		return nil, false
	}
	scheme, schemeOK := schemeValue.(string)
	derived, derivedOK := derivedValue.(string)
	salt, saltOK := saltValue.(string)
	iterations, iterationsOK := intField(body, "iterations")
	if !schemeOK || !derivedOK || !saltOK || !iterationsOK {
		return nil, true
	}
	return &HashedPassword{
		Scheme:     scheme,
		DerivedKey: derived,
		Salt:       salt,
		Iterations: iterations,
	}, true
}

func stringRoles(doc map[string]any) ([]string, error) {
	if doc == nil {
		return nil, nil
	}
	raw, present := doc["roles"]
	if !present {
		return nil, Forbidden("doc.roles must exist")
	}
	items, ok := raw.([]any)
	if !ok {
		return nil, Forbidden("doc.roles must be an array of strings")
	}
	roles := make([]string, 0, len(items))
	for _, item := range items {
		s, ok := item.(string)
		if !ok {
			return nil, Forbidden("doc.roles must be an array of strings")
		}
		roles = append(roles, s)
	}
	return roles, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func intField(body map[string]any, key string) (int, bool) {
	switch v := body[key].(type) {
	case int:
		return v, true
	case int64:
		if v < math.MinInt || v > math.MaxInt {
			return 0, false
		}
		return int(v), true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || math.Trunc(v) != v ||
			v < math.MinInt || v > math.MaxInt {
			return 0, false
		}
		return int(v), true
	case json.Number:
		n, err := v.Int64()
		if err != nil || n < math.MinInt || n > math.MaxInt {
			return 0, false
		}
		return int(n), true
	default:
		if num, ok := v.(interface{ Int64() (int64, error) }); ok {
			if n, err := num.Int64(); err == nil && n >= math.MinInt && n <= math.MaxInt {
				return int(n), true
			}
		}
		return 0, false
	}
}
