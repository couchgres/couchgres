// Package buildinfo identifies the running Couchgres binary independently of
// the CouchDB API version it implements.
package buildinfo

import (
	"regexp"
	"runtime/debug"
	"strings"
)

// Version may be set explicitly at build time with -ldflags -X. Otherwise,
// builds use GitDescribe or the module and VCS information embedded by Go.
var Version = "dev"

// GitDescribe is stamped by Make using git describe --tags --long --always --dirty.
// --long keeps Git's suffix distinguishable from hyphens inside a tag.
var GitDescribe string

// GitCommit is the full SHA stamped by Make even when Go omits VCS metadata.
var GitCommit string

var describeSuffix = regexp.MustCompile(`^(.+)-([0-9]+)-g([0-9a-f]+)$`)

type Info struct {
	Version string
	GitSHA  string
}

func Current() Info {
	info, _ := debug.ReadBuildInfo()
	return fromBuildInfo(Version, GitDescribe, GitCommit, info)
}

func fromBuildInfo(version, describe, commit string, info *debug.BuildInfo) Info {
	build := Info{Version: version, GitSHA: commit}
	if build.GitSHA == "" {
		build.GitSHA = "unknown"
	}
	if build.Version == "" {
		build.Version = "dev"
	}
	describe, dirty := strings.CutSuffix(describe, "-dirty")
	if build.Version == "dev" && describe != "" {
		build.Version = describeVersion(describe)
	}
	if info != nil {
		if build.Version == "dev" && info.Main.Version != "" && info.Main.Version != "(devel)" {
			build.Version = info.Main.Version
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if build.GitSHA == "unknown" && setting.Value != "" {
					build.GitSHA = setting.Value
				}
			case "vcs.modified":
				dirty = dirty || setting.Value == "true"
			}
		}
	}
	if build.Version == "dev" && build.GitSHA != "unknown" {
		build.Version += "+" + build.GitSHA[:min(12, len(build.GitSHA))]
	}
	if dirty {
		_, metadata, _ := strings.Cut(build.Version, "+")
		if !strings.Contains("."+metadata+".", ".dirty.") {
			build.Version = addMetadata(build.Version, "dirty")
		}
	}
	return build
}

func describeVersion(describe string) string {
	parts := describeSuffix.FindStringSubmatch(describe)
	if parts == nil {
		// With --always, an untagged checkout is just an abbreviated hash.
		return "dev+" + describe
	}
	tag, count, revision := parts[1], parts[2], parts[3]
	if count == "0" {
		return tag
	}
	return addMetadata(tag, count+".g"+revision)
}

func addMetadata(version, metadata string) string {
	if strings.Contains(version, "+") {
		return version + "." + metadata
	}
	return version + "+" + metadata
}
