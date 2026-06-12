package http

import "testing"

func TestProjectScopeRequirement(t *testing.T) {
	cases := []struct {
		name, method, path string
		wantPID, wantRole  string
		wantApplies        bool
	}{
		{"list (no pid)", "GET", "/projects", "", "", false},
		{"create (no pid)", "POST", "/projects", "", "", false},
		{"non-project path", "GET", "/auth/me", "", "", false},
		{"get project → viewer", "GET", "/projects/p1", "p1", "viewer", true},
		{"delete project → owner", "DELETE", "/projects/p1", "p1", "owner", true},
		{"patch project → editor", "PATCH", "/projects/p1", "p1", "editor", true},
		{"members list → owner", "GET", "/projects/p1/members", "p1", "owner", true},
		{"members put → owner", "PUT", "/projects/p1/members/u1", "p1", "owner", true},
		{"sub read → viewer", "GET", "/projects/p1/datasets", "p1", "viewer", true},
		{"sub write → editor", "POST", "/projects/p1/datasets/d1/versions/v1/analyze", "p1", "editor", true},
		{"sub delete → editor", "DELETE", "/projects/p1/datasets/d1", "p1", "editor", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pid, role, applies := projectScopeRequirement(c.method, c.path)
			if applies != c.wantApplies || pid != c.wantPID || role != c.wantRole {
				t.Fatalf("got (%q,%q,%v) want (%q,%q,%v)", pid, role, applies, c.wantPID, c.wantRole, c.wantApplies)
			}
		})
	}
}
