// Exercise couchgres through nano: db lifecycle, CRUD, bulk, _all_docs,
// views, mango, attachments, changes.
//
//   COUCHGRES_URL=http://admin:secret@127.0.0.1:5984 npm run check
import Nano from "nano";
import assert from "node:assert/strict";

const url = process.env.COUCHGRES_URL || "http://admin:secret@127.0.0.1:5984";
const nano = Nano(url);
const dbName = "client_nano";

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
await nano.db.destroy(dbName).catch(() => {});

await step("server info", async () => {
  const info = await nano.info();
  assert.equal(info.couchdb, "Welcome");
});

await step("create database", async () => {
  const res = await nano.db.create(dbName);
  assert.equal(res.ok, true);
});

const db = nano.db.use(dbName);

await step("insert / get / update / delete doc", async () => {
  const ins = await db.insert({ kind: "fruit", name: "apple", count: 3 }, "apple");
  assert.equal(ins.ok, true);
  const doc = await db.get("apple");
  assert.equal(doc.name, "apple");
  doc.count = 4;
  const upd = await db.insert(doc);
  assert.ok(upd.rev.startsWith("2-"));
  const del = await db.destroy("apple", upd.rev);
  assert.equal(del.ok, true);
  await assert.rejects(db.get("apple"), (e) => e.statusCode === 404);
});

await step("bulk insert + _all_docs", async () => {
  const docs = [];
  for (let i = 0; i < 50; i++) {
    docs.push({ _id: `doc${String(i).padStart(3, "0")}`, kind: "bulk", n: i });
  }
  const res = await db.bulk({ docs });
  assert.equal(res.length, 50);
  assert.ok(res.every((r) => r.ok));
  const all = await db.list({ include_docs: true, startkey: "doc010", endkey: "doc019" });
  assert.equal(all.rows.length, 10);
  assert.equal(all.rows[0].doc.n, 10);
});

await step("view: map + reduce + group", async () => {
  await db.insert({
    _id: "_design/client",
    views: {
      by_n: {
        map: "function(doc){ if(doc.kind==='bulk') emit(doc.n % 5, doc.n); }",
        reduce: "_sum",
      },
    },
  });
  const mapped = await db.view("client", "by_n", { reduce: false, key: 3 });
  assert.equal(mapped.rows.length, 10);
  const reduced = await db.view("client", "by_n", { group: true });
  assert.equal(reduced.rows.length, 5);
  const total = await db.view("client", "by_n");
  assert.equal(total.rows[0].value, (49 * 50) / 2);
});

await step("mango: createIndex + find", async () => {
  const idx = await db.createIndex({ index: { fields: ["n"] }, name: "n-idx" });
  assert.ok(["created", "exists"].includes(idx.result));
  const found = await db.find({ selector: { n: { $gte: 45 } }, sort: [{ n: "desc" }] });
  assert.equal(found.docs.length, 5);
  assert.equal(found.docs[0].n, 49);
});

await step("attachments: insert + fetch", async () => {
  const body = Buffer.from("hello from nano\n");
  const ins = await db.attachment.insert("with-att", "note.txt", body, "text/plain");
  assert.equal(ins.ok, true);
  const got = await db.attachment.get("with-att", "note.txt");
  assert.equal(got.toString(), body.toString());
  const doc = await db.get("with-att");
  assert.equal(doc._attachments["note.txt"].content_type, "text/plain");
});

await step("changes feed reflects writes", async () => {
  const changes = await db.changes({ since: 0 });
  const ids = changes.results.map((r) => r.id);
  assert.ok(ids.includes("doc000"));
  assert.ok(ids.includes("_design/client"));
});

await step("destroy database", async () => {
  const res = await nano.db.destroy(dbName);
  assert.equal(res.ok, true);
});

if (failures > 0) {
  console.error(`nano: ${failures} failure(s)`);
  process.exit(1);
}
console.log("nano: all passed");
