package jsengine

// CouchDB view-server JavaScript installed in each QuickJS VM. It provides emit/sum/log
// globals, require() over the design doc, and entry points for map/reduce/
// filter/validate/show/list/update/rewrite. Payloads cross the Go/JS boundary
// as one JSON string per batch, not per document.
const runtimeJS = `
'use strict';
(function() {
var __views = [];
var __lib = {};
var __fnCache = {};
var __emitted = null;
var __emitCount = 0;
var __maxEmits = 100000;
var __mapActive = false;
var __outputLimitMarker = '` + outputLimitMarker + `';

function __isResourceLimit(e) {
	// QuickJS reports allocation failure as a null exception when it cannot
	// reserve enough heap to construct an InternalError.
	if (e === null) return true;
	var msg = String(e);
	return msg.indexOf(__outputLimitMarker) !== -1 ||
		msg.toLowerCase().indexOf('out of memory') !== -1 ||
		msg.toLowerCase().indexOf('string too long') !== -1;
}

// CouchDB view-server globals available to user functions.
function emit(key, value) {
	if (__emitCount >= __maxEmits) {
		throw new Error(__outputLimitMarker + ': emitted-row limit reached');
	}
	__emitCount++;
	__emitted.push([key === undefined ? null : key, value === undefined ? null : value]);
}
function sum(values) {
	var total = 0;
	for (var i = 0; i < values.length; i++) total += values[i];
	return total;
}
function log(msg) {} // accepted, discarded (couchgres logs map errors itself)
function toJSON(v) { return JSON.stringify(v); }
var isArray = Array.isArray;

// CommonJS require over the design doc (CouchDB resolves paths like
// 'views/lib/name' against the ddoc object tree).
function require(path, parent) {
	var start = (parent && parent.id) ? parent.id.split('/').slice(0, -1) : [];
	var segments = path.split('/');
	var resolved = path.charAt(0) === '.' ? start : [];
	for (var i = 0; i < segments.length; i++) {
		var seg = segments[i];
		if (seg === '.' || seg === '') continue;
		else if (seg === '..') resolved.pop();
		else resolved.push(seg);
	}
	var id = resolved.join('/');
	if (__fnCache['require:' + id]) return __fnCache['require:' + id].exports;
	var src = __lib;
	for (var j = 0; j < resolved.length; j++) {
		if (src === null || typeof src !== 'object') { src = undefined; break; }
		src = src[resolved[j]];
	}
	if (typeof src !== 'string') throw new Error('invalid require path: ' + path);
	var module = { id: id, exports: {} };
	__fnCache['require:' + id] = module;
	var wrapper = __compileSource('(function(module, exports, require){' + src + '\n})');
	wrapper(module, module.exports, function(p) { return require(p, module); });
	return module.exports;
}

function __compileSource(src) {
	// Indirect eval: compiles in global scope where emit/require live.
	return (0, eval)(src);
}
function __compile(src) {
	// Trailing semicolons are common in the wild ("function(){...};") and
	// CouchDB accepts them. They would break the parenthesized wrapper.
	if (!__fnCache[src]) __fnCache[src] = __compileSource('(' + String(src).replace(/[;\s]+$/, '') + ')');
	return __fnCache[src];
}

// __install: one-time per-context setup of the ddoc's functions.
function __install(__in) {
	var p = JSON.parse(__in);
	__lib = p.lib || {};
	__views = [];
	for (var i = 0; i < p.views.length; i++) __views.push(__compile(p.views[i]));
	return 'ok';
}

// __map: [doc, ...] -> per doc, per view fn, the emitted [key, value] rows.
// A throwing map function skips that doc for that view (CouchDB behavior).
function __map(__in) {
	if (__mapActive) {
		throw new Error(__outputLimitMarker + ': nested map invocation');
	}
	__mapActive = true;
	var docs = JSON.parse(__in);
	var out = [];
	__emitCount = 0;
	try {
		for (var d = 0; d < docs.length; d++) {
			var perView = [];
			// Each view fn gets its own copy of the doc: one map function
			// mutating doc must not leak into the next (COUCHDB-925).
			var raw = __views.length > 1 ? JSON.stringify(docs[d]) : null;
			for (var v = 0; v < __views.length; v++) {
				var doc = v === 0 ? docs[d] : JSON.parse(raw);
				__emitted = [];
				try { __views[v](doc); } catch (e) {
					if (__isResourceLimit(e)) throw e;
					__emitted = [];
				}
				perView.push(__emitted);
			}
			out.push(perView);
		}
		return JSON.stringify(out);
	} finally {
		__emitted = null;
		__mapActive = false;
	}
}

// __reduce: {src, groups: [{keys, values}], rereduce} -> one value per group.
function __reduce(__in) {
	var p = JSON.parse(__in);
	var f = __compile(p.src);
	var out = [];
	for (var i = 0; i < p.groups.length; i++) {
		var g = p.groups[i];
		var r = f(g.keys, g.values, p.rereduce);
		out.push(r === undefined ? null : r);
	}
	return JSON.stringify(out);
}

// __setDDoc points require() at the full design doc for zoo calls. View
// contexts only carry the views subtree. The ddoc doubles as 'this' in
// the user function, like CouchDB's render server.
function __setDDoc(p) {
	if (p.ddoc) __lib = p.ddoc;
	return p.ddoc || null;
}

// __filter: {src, docs, req, ddoc} -> booleans. A throwing filter drops the doc.
function __filter(__in) {
	var p = JSON.parse(__in);
	var self = __setDDoc(p);
	var f = __compile(p.src);
	var out = [];
	for (var i = 0; i < p.docs.length; i++) {
		var pass;
		try { pass = !!f.call(self, p.docs[i], p.req); } catch (e) {
			if (__isResourceLimit(e)) throw e;
			pass = false;
		}
		out.push(pass);
	}
	return JSON.stringify(out);
}

// __ddocfn: {src, args, ddoc} -> the function's return value (shows, updates,
// function-form rewrites, or anything with plain call semantics).
function __ddocfn(__in) {
	var p = JSON.parse(__in);
	var self = __setDDoc(p);
	var f = __compile(p.src);
	var r = f.apply(self, p.args);
	return JSON.stringify(r === undefined ? null : r);
}

// ---- render machinery shared by shows and lists ------------------------
// send/start/provides/registerType are the CouchDB render API. Chunks from
// send(), the function's return value, then the chosen provider's chunks
// and return value concatenate in that order (CouchDB's semantics).
var __defaultMimes = {
	all: ['*/*'],
	text: ['text/plain; charset=utf-8', 'txt'],
	html: ['text/html; charset=utf-8'],
	xhtml: ['application/xhtml+xml', 'xhtml'],
	xml: ['application/xml', 'text/xml', 'application/x-xml'],
	js: ['text/javascript', 'application/javascript', 'application/x-javascript'],
	css: ['text/css'],
	ics: ['text/calendar'],
	csv: ['text/csv'],
	rss: ['application/rss+xml'],
	atom: ['application/atom+xml'],
	yaml: ['application/x-yaml', 'text/yaml'],
	multipart_form: ['multipart/form-data'],
	url_encoded_form: ['application/x-www-form-urlencoded'],
	json: ['application/json', 'text/x-json']
};
var __mimes = null, __providers = null, __chunks = null, __resp = null;
var __getRow = null;
function getRow() { return __getRow ? __getRow() : null; }

function registerType(key) {
	var mimes = [];
	for (var i = 1; i < arguments.length; i++) mimes.push(arguments[i]);
	__mimes[key] = mimes;
}
function provides(key, fn) { __providers.push({key: key, fn: fn}); }
function send(chunk) { __chunks.push(String(chunk)); }
function start(resp) { __mergeResp(resp); }

function __renderReset() {
	__mimes = {};
	for (var k in __defaultMimes) __mimes[k] = __defaultMimes[k];
	__providers = [];
	__chunks = [];
	__resp = null;
}

// __mergeResp folds a response object (or returned string) into the
// accumulated response: body chunks append, headers merge, other members
// (code, json, base64, stop) overwrite.
function __mergeResp(r) {
	if (r === undefined || r === null) return;
	if (typeof r !== 'object') { __chunks.push(String(r)); return; }
	if (!__resp) __resp = {};
	for (var k in r) {
		if (k === 'body') __chunks.push(String(r.body));
		else if (k === 'headers') {
			__resp.headers = __resp.headers || {};
			for (var h in r.headers) __resp.headers[h] = r.headers[h];
		} else if (k === 'code' || k === 'json' || k === 'base64' || k === 'stop') {
			__resp[k] = r[k];
		} else {
			// Unknown top-level members are headers (the legacy list()
			// API: start({"X-Header": "value"})).
			__resp.headers = __resp.headers || {};
			__resp.headers[k] = r[k];
		}
	}
}

// __chooseProvider picks the registered provider for the request: the
// ?format= override wins, then Accept-header matching (q-value, then
// specificity, then registration order), then the first provider.
function __chooseProvider(req) {
	if (!__providers.length) return null;
	var format = req && req.query && req.query.format;
	if (format) {
		for (var i = 0; i < __providers.length; i++) {
			if (__providers[i].key === format) {
				return {fn: __providers[i].fn, mime: (__mimes[format] || ['text/html'])[0]};
			}
		}
		throw {code: 500, error: 'render_error',
			reason: 'the format option is set to ' + format + ', but no provider exists for that format.'};
	}
	var accept = req && req.headers && (req.headers.Accept || req.headers.accept);
	if (!accept) {
		return {fn: __providers[0].fn, mime: (__mimes[__providers[0].key] || ['text/html'])[0]};
	}
	var ranges = [];
	var parts = accept.split(',');
	for (var a = 0; a < parts.length; a++) {
		var seg = parts[a].split(';');
		var type = seg[0].trim().toLowerCase().split('/');
		var q = 1.0;
		for (var s = 1; s < seg.length; s++) {
			var kv = seg[s].split('=');
			if (kv[0].trim().toLowerCase() === 'q') {
				var f = parseFloat(kv[1]);
				if (!isNaN(f)) q = f;
			}
		}
		ranges.push({type: type[0] || '*', subtype: type[1] || '*', q: q});
	}
	var best = null, bestQ = 0, bestSpec = -1;
	for (var p = 0; p < __providers.length; p++) {
		var mimes = __mimes[__providers[p].key] || [];
		for (var m = 0; m < mimes.length; m++) {
			var mt = mimes[m].split(';')[0].trim().toLowerCase().split('/');
			for (var rI = 0; rI < ranges.length; rI++) {
				var rg = ranges[rI];
				if (rg.q <= 0) continue;
				if (rg.type !== '*' && mt[0] !== '*' && rg.type !== mt[0]) continue;
				if (rg.subtype !== '*' && mt[1] !== '*' && rg.subtype !== mt[1]) continue;
				var spec = (rg.type !== '*' ? 2 : 0) + (rg.subtype !== '*' ? 1 : 0);
				if (rg.q > bestQ || (rg.q === bestQ && spec > bestSpec)) {
					best = {fn: __providers[p].fn, mime: mimes[m]};
					bestQ = rg.q;
					bestSpec = spec;
				}
			}
		}
	}
	if (!best) {
		throw {code: 406, error: 'not_acceptable',
			reason: 'Content-Type ' + accept + ' not supported'};
	}
	return best;
}

// __render runs a show/list body and assembles {resp, ct}. Thrown
// {code, error, reason} objects come back as {err}.
function __render(fn, self, args, req) {
	__renderReset();
	try {
		__mergeResp(fn.apply(self, args));
		var prov = __chooseProvider(req);
		var ct = null;
		if (prov) {
			__mergeResp(prov.fn());
			ct = prov.mime;
		}
		var resp = __resp || {};
		// body members were already folded into the chunk stream.
		if (resp.json === undefined && resp.base64 === undefined) {
			resp.body = __chunks.join('');
		}
		delete resp.stop;
		return {resp: resp, ct: ct};
	} catch (e) {
		if (e && e.error && e.reason) {
			return {err: {code: e.code || 500, error: String(e.error), reason: String(e.reason)}};
		}
		throw e;
	}
}

// __show: {src, doc, req, ddoc} -> {resp, ct} | {err}.
function __show(__in) {
	var p = JSON.parse(__in);
	var self = __setDDoc(p);
	var f = __compile(p.src);
	return JSON.stringify(__render(f, self, [p.doc, p.req], p.req));
}

// __list: {src, head, rows, req, ddoc} -> {resp, ct} | {err}. getRow walks
// the pre-materialized rows (the Go/JS boundary stays per-call, not per-row).
function __list(__in) {
	var p = JSON.parse(__in);
	var self = __setDDoc(p);
	var f = __compile(p.src);
	var i = 0;
	__getRow = function() { return i < p.rows.length ? p.rows[i++] : null; };
	try {
		return JSON.stringify(__render(f, self, [p.head, p.req], p.req));
	} finally {
		__getRow = null;
	}
}

// __validate: {src, newDoc, oldDoc, userCtx, secObj, ddoc} -> null or the
// forbidden/unauthorized object the function threw.
function __validate(__in) {
	var p = JSON.parse(__in);
	var self = __setDDoc(p);
	var f = __compile(p.src);
	try {
		f.call(self, p.newDoc, p.oldDoc, p.userCtx, p.secObj);
		return 'null';
	} catch (e) {
		if (__isResourceLimit(e)) throw e;
		if (e && e.forbidden !== undefined) return JSON.stringify({forbidden: String(e.forbidden)});
		if (e && e.unauthorized !== undefined) return JSON.stringify({unauthorized: String(e.unauthorized)});
		return JSON.stringify({forbidden: String(e)});
	}
}

function __expose(name, value) {
	Object.defineProperty(globalThis, name, {
		value: value, writable: false, configurable: false, enumerable: false
	});
}

// User functions compile in the global realm, so expose only their supported
// CouchDB API and the entry points called by Go. Mutable counters and budgets
// stay in this closure and cannot be reset by design-document code.
__expose('emit', emit);
__expose('sum', sum);
__expose('log', log);
__expose('toJSON', toJSON);
__expose('isArray', isArray);
__expose('require', require);
__expose('registerType', registerType);
__expose('provides', provides);
__expose('send', send);
__expose('start', start);
__expose('getRow', getRow);
__expose('__install', __install);
__expose('__map', __map);
__expose('__reduce', __reduce);
__expose('__filter', __filter);
__expose('__ddocfn', __ddocfn);
__expose('__show', __show);
__expose('__list', __list);
__expose('__validate', __validate);
Object.defineProperty(globalThis, '__configureCouchgresRuntime', {
	value: function(maxEmits) { __maxEmits = maxEmits; },
	writable: false, configurable: true, enumerable: false
});
})();
`
