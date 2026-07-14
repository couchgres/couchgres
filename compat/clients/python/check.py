#!/usr/bin/env python3
"""Exercise couchgres with couchdb-python.

    COUCHGRES_URL=http://admin:secret@127.0.0.1:5984 python3 check.py
"""

import os
import sys

import couchdb

URL = os.environ.get("COUCHGRES_URL", "http://admin:secret@127.0.0.1:5984")
DB = "client_python"

failures = 0


def step(name):
    def wrap(fn):
        global failures
        try:
            fn()
            print(f"ok   {name}")
        except Exception as exc:  # noqa: BLE001 - report and keep going
            failures += 1
            print(f"FAIL {name}: {exc}", file=sys.stderr)

    return wrap


server = couchdb.Server(URL)
if DB in server:
    del server[DB]

db = None


@step("server version + create database")
def _():
    global db
    assert server.version().startswith("3.")
    db = server.create(DB)


@step("doc save / load / update / delete")
def _():
    doc = {"_id": "apple", "kind": "fruit", "count": 3}
    doc_id, rev1 = db.save(doc)
    assert doc_id == "apple" and rev1.startswith("1-")
    loaded = db["apple"]
    loaded["count"] = 4
    _, rev2 = db.save(loaded)
    assert rev2.startswith("2-")
    db.delete(db["apple"])
    assert "apple" not in db


@step("bulk update + iteration")
def _():
    docs = [{"_id": f"doc{i:03d}", "n": i} for i in range(30)]
    results = db.update(docs)
    assert all(ok for ok, _, _ in results)
    assert len(db) == 30
    ids = [row.id for row in db.view("_all_docs", startkey="doc010", endkey="doc012")]
    assert ids == ["doc010", "doc011", "doc012"]


@step("temporary-free view via design doc")
def _():
    db["_design/client"] = {
        "views": {
            "by_n": {
                "map": "function(doc){ if(doc.n !== undefined) emit(doc.n, null); }",
                "reduce": "_count",
            }
        }
    }
    rows = list(db.view("client/by_n", reduce=False, descending=True, limit=3))
    assert [r.key for r in rows] == [29, 28, 27]
    total = list(db.view("client/by_n"))
    assert total[0].value == 30


@step("mango find")
def _():
    hits = list(db.find({"selector": {"n": {"$lt": 5}}}))
    assert len(hits) == 5


@step("attachments")
def _():
    doc_id, _ = db.save({"_id": "with-att"})
    doc = db["with-att"]
    db.put_attachment(doc, b"hello from python\n", "note.txt", "text/plain")
    att = db.get_attachment("with-att", "note.txt")
    assert att.read() == b"hello from python\n"


@step("changes feed")
def _():
    changes = db.changes(since=0)
    ids = {c["id"] for c in changes["results"]}
    assert "doc000" in ids and "_design/client" in ids


@step("delete database")
def _():
    del server[DB]


if failures:
    print(f"python: {failures} failure(s)", file=sys.stderr)
    sys.exit(1)
print("python: all passed")
