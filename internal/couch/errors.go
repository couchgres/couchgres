package couch

import "fmt"

// Error is a CouchDB-shaped HTTP error: a status code plus the canonical
// {"error": ..., "reason": ...} body. Clients pattern-match on the error and
// reason strings, so the constructors below reproduce CouchDB's exact wording.
type Error struct {
	Status int    `json:"-"`
	Err    string `json:"error"`
	Reason string `json:"reason"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("%d %s: %s", e.Status, e.Err, e.Reason)
}

func NewError(status int, err, reason string) *Error {
	return &Error{Status: status, Err: err, Reason: reason}
}

func BadRequest(reason string) *Error {
	return NewError(400, "bad_request", reason)
}

func InvalidJSON() *Error {
	return BadRequest("invalid UTF-8 JSON")
}

func QueryParseError(param, value string) *Error {
	return NewError(400, "query_parse_error",
		fmt.Sprintf("Invalid value for %s: %s", param, value))
}

func IllegalDatabaseName(name string) *Error {
	return NewError(400, "illegal_database_name", fmt.Sprintf(
		"Name: '%s'. Only lowercase characters (a-z), digits (0-9), and any of"+
			" the characters _, $, (, ), +, -, and / are allowed."+
			" Must begin with a letter.", name))
}

func DocValidation(reason string) *Error {
	return NewError(400, "doc_validation", reason)
}

func Unauthorized(reason string) *Error {
	return NewError(401, "unauthorized", reason)
}

func Forbidden(reason string) *Error {
	return NewError(403, "forbidden", reason)
}

func DBNotFound() *Error {
	return NewError(404, "not_found", "Database does not exist.")
}

func DocMissing() *Error {
	return NewError(404, "not_found", "missing")
}

func DocDeleted() *Error {
	return NewError(404, "not_found", "deleted")
}

func NotFound() *Error {
	return DocMissing()
}

func MethodNotAllowed(allowed string) *Error {
	return NewError(405, "method_not_allowed", "Only "+allowed+" allowed")
}

func Conflict() *Error {
	return NewError(409, "conflict", "Document update conflict.")
}

func DBExists() *Error {
	return NewError(412, "file_exists",
		"The database could not be created, the file already exists.")
}

func BadContentType(reason string) *Error {
	return NewError(415, "bad_content_type", reason)
}

func NotImplemented(reason string) *Error {
	return NewError(501, "not_implemented", reason)
}
