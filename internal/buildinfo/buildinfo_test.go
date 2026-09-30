package buildinfo

import (
	"runtime/debug"
	"testing"
)

func TestBuildIdentity(t *testing.T) {
	const revision = "0123456789abcdef0123456789abcdef01234567"
	for _, tc := range []struct {
		name     string
		version  string
		describe string
		commit   string
		module   string
		revision string
		dirty    bool
		want     Info
	}{
		{name: "no metadata", version: "dev", want: Info{"dev", "unknown"}},
		{name: "empty override", want: Info{"dev", "unknown"}},
		{name: "source archive release", version: "v0.1.0", want: Info{"v0.1.0", "unknown"}},
		{name: "module release", version: "dev", module: "v0.2.0", want: Info{"v0.2.0", "unknown"}},
		{name: "module dirty suffix", version: "dev", module: "v0.2.0+dirty", revision: revision, dirty: true,
			want: Info{"v0.2.0+dirty", revision}},
		{name: "checkout", version: "dev", module: "(devel)", revision: revision,
			want: Info{"dev+0123456789ab", revision}},
		{name: "modified checkout", version: "dev", module: "(devel)", revision: revision, dirty: true,
			want: Info{"dev+0123456789ab.dirty", revision}},
		{name: "release override", version: "v0.3.0", module: "v0.2.0", revision: revision,
			want: Info{"v0.3.0", revision}},
		{name: "modified release", version: "v0.3.0", revision: revision, dirty: true,
			want: Info{"v0.3.0+dirty", revision}},
		{name: "exact beta tag", describe: "1.0.1-beta-0-g0123456", revision: revision,
			want: Info{"1.0.1-beta", revision}},
		{name: "modified beta tag", describe: "1.0.1-beta-0-g0123456", revision: revision, dirty: true,
			want: Info{"1.0.1-beta+dirty", revision}},
		{name: "exact rc tag", describe: "v1.0.1-rc.1-0-g0123456", revision: revision,
			want: Info{"v1.0.1-rc.1", revision}},
		{name: "commits after beta tag", describe: "1.0.1-beta-2-g0123456", revision: revision,
			want: Info{"1.0.1-beta+2.g0123456", revision}},
		{name: "modified commits after rc tag", describe: "v1.0.1-rc.1-2-g0123456", revision: revision, dirty: true,
			want: Info{"v1.0.1-rc.1+2.g0123456.dirty", revision}},
		{name: "existing tag metadata", describe: "1.0.1-beta+linux-2-g0123456", revision: revision, dirty: true,
			want: Info{"1.0.1-beta+linux.2.g0123456.dirty", revision}},
		{name: "tag with git-like suffix", describe: "1.0.1-beta-2-gabcdef0-0-g0123456", revision: revision,
			want: Info{"1.0.1-beta-2-gabcdef0", revision}},
		{name: "tag ending in dirty", describe: "1.0.1-beta-dirty-0-g0123456", revision: revision, dirty: true,
			want: Info{"1.0.1-beta-dirty+dirty", revision}},
		{name: "explicit override preserves hyphens", version: "1.0.1-beta-2-gabcdef0", describe: "v1.0.0-2-g0123456", revision: revision,
			want: Info{"1.0.1-beta-2-gabcdef0", revision}},
		{name: "explicit override with metadata", version: "1.0.1-rc.1+linux", describe: "v1.0.0-2-g0123456", revision: revision, dirty: true,
			want: Info{"1.0.1-rc.1+linux.dirty", revision}},
		{name: "existing dirty metadata", version: "1.0.1-beta+dirty.linux", revision: revision, dirty: true,
			want: Info{"1.0.1-beta+dirty.linux", revision}},
		{name: "untagged checkout", describe: "0123456", revision: revision, dirty: true,
			want: Info{"dev+0123456.dirty", revision}},
		{name: "stamped commit without Go VCS metadata", describe: "1.0.1-beta-0-g0123456", commit: revision,
			want: Info{"1.0.1-beta", revision}},
		{name: "stamped dirty tag without Go VCS metadata", describe: "1.0.1-beta-0-g0123456-dirty", commit: revision,
			want: Info{"1.0.1-beta+dirty", revision}},
		{name: "stamped dirty override without Go VCS metadata", version: "1.0.1-rc.1", describe: "1.0.1-beta-0-g0123456-dirty", commit: revision,
			want: Info{"1.0.1-rc.1+dirty", revision}},
		{name: "stamped dirty and Go dirty", describe: "1.0.1-beta-0-g0123456-dirty", commit: revision, revision: revision, dirty: true,
			want: Info{"1.0.1-beta+dirty", revision}},
		{name: "stamped commit takes precedence", describe: "1.0.1-beta-0-g0123456", commit: revision, revision: "abcdef0",
			want: Info{"1.0.1-beta", revision}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &debug.BuildInfo{Main: debug.Module{Version: tc.module}}
			if tc.revision != "" {
				info.Settings = append(info.Settings, debug.BuildSetting{Key: "vcs.revision", Value: tc.revision})
			}
			if tc.dirty {
				info.Settings = append(info.Settings, debug.BuildSetting{Key: "vcs.modified", Value: "true"})
			}
			if got := fromBuildInfo(tc.version, tc.describe, tc.commit, info); got != tc.want {
				t.Fatalf("build identity: got %+v, want %+v", got, tc.want)
			}
		})
	}
	if got := fromBuildInfo("dev", "", "", nil); got != (Info{"dev", "unknown"}) {
		t.Fatalf("missing build info: %+v", got)
	}
	if got := fromBuildInfo("dev", "1.0.1-beta-0-g0123456-dirty", revision, nil); got != (Info{"1.0.1-beta+dirty", revision}) {
		t.Fatalf("stamped identity without build info: %+v", got)
	}
}
