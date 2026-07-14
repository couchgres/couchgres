// PouchDB replication against couchgres: _changes, _revs_diff, _bulk_docs
// (new_edits=false), and _local checkpoints.
//
//   COUCHGRES_URL=http://admin:secret@127.0.0.1:5984 npm run check
import PouchDB from "pouchdb";
import memory from "pouchdb-adapter-memory";
import assert from "node:assert/strict";

PouchDB.plugin(memory);

const url = process.env.COUCHGRES_URL || "http://admin:secret@127.0.0.1:5984";
const remoteName = `${url.replace(/\/+$/, "")}/client_pouch`;

let failures = 0;
async function step(name, fn) {
  try {
    await fn();
    console.log(`ok   ${name}`);
  } catch (err) {
    failures++;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

// Start clean even after a crashed previous run.
await new PouchDB(remoteName).destroy().catch(() => {});

const local = new PouchDB("client-local", { adapter: "memory" });
const remote = new PouchDB(remoteName);

await step("seed local db (docs, updates, a deletion, an attachment)", async () => {
  const docs = [];
  for (let i = 0; i < 40; i++) {
    docs.push({ _id: `p${String(i).padStart(3, "0")}`, n: i, tag: i % 2 ? "odd" : "even" });
  }
  await local.bulkDocs(docs);
  // A doc with edit history, so more than one rev crosses the wire.
  const d = await local.get("p001");
  d.n = 100;
  await local.put(d);
  // A deletion, so a tombstone replicates.
  const gone = await local.get("p002");
  await local.remove(gone);
  // An attachment.
  await local.put({
    _id: "with-att",
    _attachments: {
      "hello.txt": { content_type: "text/plain", data: Buffer.from("hello pouch\n").toString("base64") },
    },
  });
});

await step("push replicate local -> couchgres", async () => {
  const res = await local.replicate.to(remote);
  assert.equal(res.ok, true);
  assert.ok(res.docs_written >= 41, `docs_written=${res.docs_written}`);
});

await step("remote state matches after push", async () => {
  const info = await remote.info();
  assert.equal(info.doc_count, 40); // 41 written, 1 deleted
  const d = await remote.get("p001");
  assert.equal(d.n, 100);
  assert.ok(d._rev.startsWith("2-"));
  await assert.rejects(remote.get("p002"), (e) => e.status === 404);
  const att = await remote.getAttachment("with-att", "hello.txt");
  assert.equal(Buffer.from(att).toString(), "hello pouch\n");
});

await step("pull replicate couchgres -> fresh local db", async () => {
  const mirror = new PouchDB("client-mirror", { adapter: "memory" });
  const res = await mirror.replicate.from(remote);
  assert.equal(res.ok, true);
  const info = await mirror.info();
  assert.equal(info.doc_count, 40);
  const d = await mirror.get("p001", { revs: true });
  assert.equal(d._revisions.ids.length, 2); // full history came back
  await mirror.destroy();
});

await step("conflict created on couchgres survives sync", async () => {
  // Both sides edit p003 while disconnected, then sync: one branch must
  // win deterministically and the other must be visible as a conflict.
  const fork = new PouchDB("client-fork", { adapter: "memory" });
  await fork.replicate.from(remote);
  const a = await local.get("p003");
  a.side = "local";
  await local.put(a);
  const b = await fork.get("p003");
  b.side = "fork";
  await fork.put(b);
  await local.replicate.to(remote);
  await fork.replicate.to(remote);
  const merged = await remote.get("p003", { conflicts: true });
  assert.ok(merged.side === "local" || merged.side === "fork");
  assert.equal(merged._conflicts.length, 1);
  await fork.destroy();
});

await step("live sync applies an incoming change", async () => {
  const sync = local.sync(remote, { live: true, retry: false });
  const seen = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("change did not arrive within 10s")), 10000);
    local.changes({ since: "now", live: true }).on("change", (c) => {
      if (c.id === "from-remote") {
        clearTimeout(timer);
        resolve();
      }
    });
  });
  await remote.put({ _id: "from-remote", via: "continuous" });
  await seen;
  sync.cancel();
  const doc = await local.get("from-remote");
  assert.equal(doc.via, "continuous");
});

await step("destroy remote db", async () => {
  await remote.destroy();
});

await local.destroy();

if (failures > 0) {
  console.error(`pouchdb: ${failures} failure(s)`);
  process.exit(1);
}
console.log("pouchdb: all passed");
