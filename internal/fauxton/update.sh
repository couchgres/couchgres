#!/bin/sh
# Fetch the Fauxton build used by //go:embed (internal/fauxton/dist).
#
# To move to a newer CouchDB release:
#  bump VERSION and SHA256 below
#  run make fauxton
set -eu

# CouchDB version and SHA256 checksum for the source tarball.
VERSION=3.5.2
SHA256=e561102aaadfdda1e499e6e9e12d2473433291b608bcd390bcbcf590bbb6cf68
TARBALL="apache-couchdb-${VERSION}.tar.gz"
URL="https://downloads.apache.org/couchdb/source/${VERSION}/${TARBALL}"

cd "$(dirname "$0")"
tmp="${TMPDIR:-/tmp}/couchgres-fauxton-$$"
mkdir "$tmp"
trap 'rm -rf "$tmp"' EXIT

# Fetch the CouchDB source tarball.
curl -sSL "$URL" -o "$tmp/$TARBALL"

if command -v sha256sum >/dev/null 2>&1; then
	got=$(sha256sum "$tmp/$TARBALL" | cut -d' ' -f1)
elif command -v shasum >/dev/null 2>&1; then
	got=$(shasum -a 256 "$tmp/$TARBALL" | cut -d' ' -f1)
else
	echo "need sha256sum or shasum" >&2
	exit 1
fi
if [ "$got" != "$SHA256" ]; then
	echo "sha256 mismatch: got $got, pinned $SHA256" >&2
	exit 1
fi

# Extract Fauxton bundle and LICENSE.
tar xzf "$tmp/$TARBALL" -C "$tmp" \
	"apache-couchdb-${VERSION}/share/www" \
	"apache-couchdb-${VERSION}/src/fauxton/LICENSE"

# Copy the Fauxton bundle to dist/.
rm -rf dist
cp -R "$tmp/apache-couchdb-${VERSION}/share/www" dist

rm -rf dist/docs

# Copy the LICENSE to the root directory.
cp "$tmp/apache-couchdb-${VERSION}/src/fauxton/LICENSE" LICENSE

echo "Fetched Fauxton from CouchDB ${VERSION}!"
