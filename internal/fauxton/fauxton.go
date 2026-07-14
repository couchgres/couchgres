// Package fauxton embeds a prebuilt Fauxton dashboard (Apache-2.0, see
// LICENSE) so /_utils works without a CouchDB installation.
//
// dist/ is not in git. Run `make fauxton` (or go generate) to fetch
// share/www from a pinned Apache CouchDB source release before building.
package fauxton

import (
	"embed"
	"io/fs"
)

//go:generate ./update.sh

//go:embed all:dist
var files embed.FS

// FS is the embedded Fauxton build, rooted at its index.html.
func FS() fs.FS {
	sub, err := fs.Sub(files, "dist")
	if err != nil {
		panic(err) // The embedded path cannot fail at runtime.
	}
	return sub
}
