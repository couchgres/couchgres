package couch

// UserCtx is the authenticated principal for a request, mirroring CouchDB's
// userCtx object. An empty Name means anonymous.
type UserCtx struct {
	Name  string
	Roles []string
	// How the user authenticated. Values are "cookie", "default" (basic), "jwt",
	// "proxy", or "" when anonymous.
	Authenticated string
}

func Anonymous() *UserCtx {
	return &UserCtx{}
}

func (u *UserCtx) IsAnonymous() bool {
	return u.Name == ""
}

func (u *UserCtx) IsServerAdmin() bool {
	for _, role := range u.Roles {
		if role == "_admin" {
			return true
		}
	}
	return false
}

// NameJSON is the userCtx "name" member. It is null for anonymous users.
func (u *UserCtx) NameJSON() any {
	if u.Name == "" {
		return nil
	}
	return u.Name
}

// RolesJSON never marshals as null.
func (u *UserCtx) RolesJSON() []string {
	if u.Roles == nil {
		return []string{}
	}
	return u.Roles
}

// SecurityObj is a parsed /{db}/_security object. Missing members are empty.
type SecurityObj struct {
	AdminNames  []string
	AdminRoles  []string
	MemberNames []string
	MemberRoles []string
}

func ParseSecurity(obj map[string]any) *SecurityObj {
	list := func(group, field string) []string {
		g, _ := obj[group].(map[string]any)
		items, _ := g[field].([]any)
		out := make([]string, 0, len(items))
		for _, item := range items {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	}
	return &SecurityObj{
		AdminNames:  list("admins", "names"),
		AdminRoles:  list("admins", "roles"),
		MemberNames: list("members", "names"),
		MemberRoles: list("members", "roles"),
	}
}

// ValidateSecurity checks a client-supplied _security body. admins/members,
// when present, must be objects of string arrays.
func ValidateSecurity(v any) error {
	obj, ok := v.(map[string]any)
	if !ok {
		return BadRequest("Request body must be a JSON object")
	}
	for _, group := range []string{"admins", "members"} {
		g, present := obj[group]
		if !present {
			continue
		}
		gm, ok := g.(map[string]any)
		if !ok {
			return BadRequest(group + " must be an object")
		}
		for _, field := range []string{"names", "roles"} {
			f, present := gm[field]
			if !present {
				continue
			}
			// Non-string entries crash CouchDB's internal validation into
			// a 500 no_majority. Clients test for exactly that.
			items, ok := f.([]any)
			if !ok {
				return NewError(500, "error", "no_majority")
			}
			for _, item := range items {
				if _, ok := item.(string); !ok {
					return NewError(500, "error", "no_majority")
				}
			}
		}
	}
	return nil
}

func (s *SecurityObj) IsDBAdmin(u *UserCtx) bool {
	if u.IsServerAdmin() {
		return true
	}
	if u.Name != "" && contains(s.AdminNames, u.Name) {
		return true
	}
	return intersects(s.AdminRoles, u.Roles)
}

func (s *SecurityObj) IsDBMember(u *UserCtx) bool {
	if s.IsDBAdmin(u) {
		return true
	}
	// No members configured = public database.
	if len(s.MemberNames) == 0 && len(s.MemberRoles) == 0 {
		return true
	}
	if u.Name != "" && contains(s.MemberNames, u.Name) {
		return true
	}
	return intersects(s.MemberRoles, u.Roles)
}

// Authorization failures, with CouchDB's split between "who are you"
// (401 for anonymous) and "you can't" (403 for named users).

func MemberRequired(u *UserCtx) *Error {
	if u.IsAnonymous() {
		return Unauthorized("You are not authorized to access this db.")
	}
	return Forbidden("You are not allowed to access this db.")
}

func DBAdminRequired(u *UserCtx) *Error {
	if u.IsAnonymous() {
		return Unauthorized("You are not a db or server admin.")
	}
	return Forbidden("You are not a db or server admin.")
}

func ServerAdminRequired() *Error {
	return Unauthorized("You are not a server admin.")
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func intersects(a, b []string) bool {
	for _, s := range b {
		if contains(a, s) {
			return true
		}
	}
	return false
}
