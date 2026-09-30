#!/usr/bin/env bash
# Replicate between a real CouchDB and couchgres in four directions, then
# compare every doc, rev, conflict, and attachment.
#
# Both servers need admin:secret. COUCH must use a private network address
# reachable from couchgres: remote replication rejects loopback addresses.
#   COUCH=http://admin:secret@10.254.0.1:5985 \
#   CG=http://admin:secret@127.0.0.1:5984 compat/replication-check.sh
set -euo pipefail

COUCH=${COUCH:-http://admin:secret@127.0.0.1:5985}
CG=${CG:-http://admin:secret@127.0.0.1:5984}
JSON='Content-Type: application/json'

say() { printf '\n== %s\n' "$*"; }

# Python helper shared by the blocks below (urllib ignores URL credentials,
# so requests carry an explicit Authorization header).
PYHELPER='
import base64, json, urllib.parse, urllib.request

def split_auth(url):
    p = urllib.parse.urlsplit(url)
    host = p.hostname + (":" + str(p.port) if p.port else "")
    clean = urllib.parse.urlunsplit((p.scheme, host, p.path, p.query, p.fragment))
    auth = None
    if p.username:
        cred = (p.username + ":" + (p.password or "")).encode()
        auth = "Basic " + base64.b64encode(cred).decode()
    return clean, auth

def request(base, path, method="GET", data=None, ctype=None):
    clean, auth = split_auth(base + path)
    headers = {"Accept": "application/json"}
    if auth:
        headers["Authorization"] = auth
    if ctype:
        headers["Content-Type"] = ctype
    req = urllib.request.Request(clean, method=method, data=data, headers=headers)
    return urllib.request.urlopen(req)

def get(base, path):
    with request(base, path) as resp:
        body = resp.read()
        if resp.headers.get_content_type() == "application/json":
            return json.loads(body)
        return body
'

replicate() {
	# Capture the response before parsing it: curl -f discards HTTP error
	# bodies, masking the replication error with a JSONDecodeError.
	local response status
	response=$(curl -sS --fail-with-body -X POST -H "$JSON" "$1/_replicate" -d "$2") || {
		status=$?
		printf 'Replication request failed (curl exit %s):\n%s\n' "$status" "$response" >&2
		return "$status"
	}
	python3 -c '
import json, sys
body = sys.stdin.read().strip()
try:
    r = json.loads(body)
except json.JSONDecodeError:
    sys.exit("Replication returned invalid JSON: " + (body or "<empty response>"))
assert r.get("ok") is True, r
h = r["history"][0]
print("   wrote {} docs, {} failures".format(
    h["docs_written"], h["doc_write_failures"]))
assert h["doc_write_failures"] == 0, r
assert h["docs_written"] == int(sys.argv[1]), r' "$3" <<< "$response"
}

# --- Seed the CouchDB source database -------------------------------------
say "seed couchdb source db"
curl -sf -X DELETE "$COUCH/repl_src" > /dev/null || true
curl -sf -X DELETE "$CG/repl_src" > /dev/null || true
curl -sf -X PUT "$COUCH/repl_src" > /dev/null

rev1=$(curl -sf -X PUT -H "$JSON" -d '{"v":1}' "$COUCH/repl_src/updated" | python3 -c 'import json,sys;print(json.load(sys.stdin)["rev"])')
curl -sf -X PUT -H "$JSON" -d '{"v":2}' "$COUCH/repl_src/updated?rev=$rev1" > /dev/null

drev=$(curl -sf -X PUT -H "$JSON" -d '{"gone":true}' "$COUCH/repl_src/deleted-doc" | python3 -c 'import json,sys;print(json.load(sys.stdin)["rev"])')
curl -sf -X DELETE "$COUCH/repl_src/deleted-doc?rev=$drev" > /dev/null

# Conflict: a sibling branch pushed with new_edits=false.
hash1=${rev1#1-}
curl -sf -X POST -H "$JSON" "$COUCH/repl_src/_bulk_docs" -d '{
  "new_edits": false,
  "docs": [{"_id": "updated", "_rev": "2-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "_revisions": {"start": 2, "ids": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "'"$hash1"'"]},
            "branch": true}]}' > /dev/null

# Attachments: one small inline (couchdb stores text gzipped and replicates
# it encoded), one 200KB (forces the replicator's multipart PUT path).
python3 - "$COUCH" <<EOF
import sys
$PYHELPER
couch = sys.argv[1]
big = base64.b64encode(bytes(range(256)) * 800).decode()  # ~200KB
doc = {"kind": "attachments",
       "_attachments": {
           "small.txt": {"content_type": "text/plain",
                          "data": base64.b64encode(b"tiny").decode()},
           "big.bin": {"content_type": "application/octet-stream", "data": big}}}
request(couch, "/repl_src/with-atts", method="PUT",
        data=json.dumps(doc).encode(), ctype="application/json")
EOF

# --- Replicate CouchDB -> couchgres ----------------------------------------
say "replicate couchdb -> couchgres"
replicate "$COUCH" '{
  "source": "'"$COUCH"'/repl_src",
  "target": "'"$CG"'/repl_src",
  "create_target": true}' 4

# --- Compare every doc, rev, conflict, attachment --------------------------
say "compare repl_src (couchdb vs couchgres)"
python3 - "$COUCH" "$CG" repl_src <<EOF
import sys
$PYHELPER
couch, cg, db = sys.argv[1], sys.argv[2], sys.argv[3]

# Same doc ids (deleted docs excluded from _all_docs on both sides).
a = get(couch, "/" + db + "/_all_docs")
b = get(cg, "/" + db + "/_all_docs")
ids_a = [r["id"] for r in a["rows"]]
ids_b = [r["id"] for r in b["rows"]]
assert ids_a == ids_b, (ids_a, ids_b)
revs_a = [r["value"]["rev"] for r in a["rows"]]
revs_b = [r["value"]["rev"] for r in b["rows"]]
assert revs_a == revs_b, (revs_a, revs_b)  # replication copies revs verbatim

# Every leaf (conflicts included) matches exactly, ancestry included.
for docid in ids_a + ["deleted-doc"]:
    path = "/" + db + "/" + docid + "?open_revs=all&revs=true"
    ka = sorted(json.dumps(e, sort_keys=True) for e in get(couch, path))
    kb = sorted(json.dumps(e, sort_keys=True) for e in get(cg, path))
    assert ka == kb, docid + ":\n" + str(ka) + "\n" + str(kb)

# Conflict view matches.
ca = get(couch, "/" + db + "/updated?conflicts=true")
cb = get(cg, "/" + db + "/updated?conflicts=true")
assert ca.get("_conflicts") == cb.get("_conflicts") != None, (ca, cb)

# Attachment bytes match.
for name in ("small.txt", "big.bin"):
    aa = get(couch, "/" + db + "/with-atts/" + name)
    ab = get(cg, "/" + db + "/with-atts/" + name)
    assert aa == ab, "attachment {} differs ({} vs {} bytes)".format(name, len(aa), len(ab))

print("   {} live docs + tombstone + conflict + 2 attachments identical".format(len(ids_a)))
EOF

# --- Reverse: couchgres -> CouchDB ------------------------------------------
say "seed couchgres source db"
curl -sf -X DELETE "$CG/repl_back" > /dev/null || true
curl -sf -X DELETE "$COUCH/repl_back" > /dev/null || true
curl -sf -X PUT "$CG/repl_back" > /dev/null
brev=$(curl -sf -X PUT -H "$JSON" -d '{"from":"couchgres"}' "$CG/repl_back/home" | python3 -c 'import json,sys;print(json.load(sys.stdin)["rev"])')
curl -sf -X PUT -H "$JSON" -d '{"from":"couchgres","v":2}' "$CG/repl_back/home?rev=$brev" > /dev/null
currev=$(curl -sf "$CG/repl_back/home" | python3 -c 'import json,sys;print(json.load(sys.stdin)["_rev"])')
curl -sf -X PUT -H 'Content-Type: text/plain' -d 'reverse attachment' "$CG/repl_back/home/note.txt?rev=$currev" > /dev/null

say "replicate couchgres -> couchdb"
replicate "$COUCH" '{
  "source": "'"$CG"'/repl_back",
  "target": "'"$COUCH"'/repl_back",
  "create_target": true}' 1

say "compare repl_back (couchgres vs couchdb)"
python3 - "$CG" "$COUCH" repl_back <<EOF
import sys
$PYHELPER
src, dst, db = sys.argv[1], sys.argv[2], sys.argv[3]

a = get(src, "/" + db + "/home?revs=true")
b = get(dst, "/" + db + "/home?revs=true")
assert a["_rev"] == b["_rev"], (a, b)
assert a["_revisions"] == b["_revisions"], (a, b)
att_a = get(src, "/" + db + "/home/note.txt")
att_b = get(dst, "/" + db + "/home/note.txt")
assert att_a == att_b, (att_a, att_b)
print("   doc, ancestry, and attachment identical on the couchdb side")
EOF

# --- couchgres as the replicator ------------------------------------------
say "couchgres pulls from couchdb (its own _replicate)"
curl -sf -X DELETE "$CG/repl_pull" > /dev/null || true
replicate "$CG" '{
  "source": "'"$COUCH"'/repl_src",
  "target": "repl_pull",
  "create_target": true}' 4

say "compare repl_pull (couchdb vs couchgres-pulled copy)"
python3 - "$COUCH" "$CG" <<EOF
import sys
$PYHELPER
couch, cg = sys.argv[1], sys.argv[2]
for docid in ("updated", "with-atts", "deleted-doc"):
    path_a = "/repl_src/" + docid + "?open_revs=all&revs=true"
    path_b = "/repl_pull/" + docid + "?open_revs=all&revs=true"
    ka = sorted(json.dumps(e, sort_keys=True) for e in get(couch, path_a))
    kb = sorted(json.dumps(e, sort_keys=True) for e in get(cg, path_b))
    assert ka == kb, docid + ":\n" + str(ka) + "\n" + str(kb)
for name in ("small.txt", "big.bin"):
    aa = get(couch, "/repl_src/with-atts/" + name)
    ab = get(cg, "/repl_pull/with-atts/" + name)
    assert aa == ab, "attachment " + name + " differs"
print("   pulled copy identical (docs, conflict, tombstone, attachments)")
EOF

say "couchgres pushes to couchdb (its own _replicate)"
curl -sf -X DELETE "$COUCH/repl_push" > /dev/null || true
replicate "$CG" '{
  "source": "repl_back",
  "target": "'"$COUCH"'/repl_push",
  "create_target": true}' 1

python3 - "$CG" "$COUCH" <<EOF
import sys
$PYHELPER
cg, couch = sys.argv[1], sys.argv[2]
a = get(cg, "/repl_back/home?revs=true")
b = get(couch, "/repl_push/home?revs=true")
assert a["_rev"] == b["_rev"] and a["_revisions"] == b["_revisions"], (a, b)
att_a = get(cg, "/repl_back/home/note.txt")
att_b = get(couch, "/repl_push/home/note.txt")
assert att_a == att_b, (att_a, att_b)
print("   pushed copy identical on the couchdb side")
EOF

say "clean up replication test databases"
for db in repl_src repl_back repl_pull repl_push; do
	curl -sf -X DELETE "$COUCH/$db" > /dev/null 2>&1 || true
	curl -sf -X DELETE "$CG/$db" > /dev/null 2>&1 || true
done

say "replication check passed"
