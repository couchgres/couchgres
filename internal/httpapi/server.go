// Package httpapi serves the CouchDB-compatible HTTP API.
package httpapi

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/couchgres/couchgres/internal/couch"
	"github.com/couchgres/couchgres/internal/jsengine"
	"github.com/couchgres/couchgres/internal/replicate"
	"github.com/couchgres/couchgres/internal/store"
)

const couchDBVersion = "3.5.0"

type Server struct {
	store              *store.Store
	config             *configCache
	broker             *store.Broker
	scheduler          *replicate.Scheduler
	lifetime           context.Context
	handler            http.Handler
	router             http.Handler
	serverUUID         string
	maxUUIDCount       int
	credentialCache    *credentialCache
	passwordAuth       *passwordAuthenticator
	bodyLimiter        *requestBodyLimiter
	js                 *jsengine.Pool
	mapper             jsMapper
	reducer            jsReducer
	vdu                *vduCache
	viewCache          *viewRespCache
	startedAt          time.Time
	streamWriteTimeout time.Duration
	replicatorOnce     sync.Once

	// UUID generation state (config uuids/algorithm).
	uuidMu         sync.Mutex
	uuidPrefix     string
	uuidSeq        int
	uuidLastMicros int64
}

// ServeHTTP makes *Server the http.Handler main and tests mount.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.handler.ServeHTTP(w, r)
}

// SetSelfURL wires the server's own base URL so replication endpoints given
// as bare database names resolve over loopback (authenticated with a
// self-minted admin session).
func (s *Server) SetSelfURL(base string) {
	s.scheduler.SetSelf(base, s.mintAdminCookie)
}

// SetStreamWriteTimeout keeps active streaming responses on a rolling write
// deadline while preserving the listener's slow-reader protection.
func (s *Server) SetStreamWriteTimeout(timeout time.Duration) {
	if timeout > 0 {
		s.streamWriteTimeout = timeout
	}
}

// mintAdminCookie issues a session for the first configured admin.
func (s *Server) mintAdminCookie() string {
	admins := s.config.section("admins")
	names := make([]string, 0, len(admins))
	for name := range admins {
		names = append(names, name)
	}
	if len(names) == 0 {
		return ""
	}
	sort.Strings(names)
	name := names[0]
	salt := ""
	if h, ok := couch.ParseAdminPassword(admins[name]); ok {
		salt = h.Salt
	}
	return couch.EncodeSessionCookie(s.config.cookieSecret(), salt, name, time.Now().Unix())
}

// New builds the API server. ctx bounds background work started during
// construction. StartReplicatorWorker starts the durable _replicator watcher
// after the caller has wired the server's own URL.
func New(ctx context.Context, st *store.Store, serverUUID string) (*Server, error) {
	config, err := loadConfigCache(ctx, st)
	if err != nil {
		return nil, err
	}
	s := &Server{
		store:              st,
		config:             config,
		broker:             st.StartBroker(ctx),
		lifetime:           ctx,
		serverUUID:         serverUUID,
		maxUUIDCount:       1000,
		credentialCache:    newCredentialCache(),
		passwordAuth:       newPasswordAuthenticator(),
		bodyLimiter:        newRequestBodyLimiter(),
		js:                 jsengine.NewPool(0, 5*time.Second),
		vdu:                newVDUCache(),
		viewCache:          newViewRespCache(),
		startedAt:          time.Now(),
		streamWriteTimeout: 5 * time.Minute,
	}
	s.mapper = jsMapper{pool: s.js}
	s.reducer = jsReducer{pool: s.js}
	s.applyStoreConfig()
	go s.watchInvalidations(ctx)
	go func() {
		<-ctx.Done()
		s.js.Close()
	}()
	s.scheduler = replicate.NewScheduler(st, s.broker)

	mux := http.NewServeMux()

	mux.Handle("GET /{$}", h(s.welcome))
	mux.Handle("/{$}", h(s.serverMethodNotAllowed))
	mux.Handle("GET /favicon.ico", h(s.favicon))
	mux.Handle("GET /_up", h(s.up))
	mux.Handle("GET /_uuids", h(s.uuids))
	// A method-less /_uuids route would cross ServeMux's method dimension with
	// GET /{db}. Spell the rejected methods out instead.
	for _, method := range []string{"POST", "PUT", "DELETE"} {
		mux.Handle(method+" /_uuids", h(func(http.ResponseWriter, *http.Request) error {
			return couch.MethodNotAllowed("GET,HEAD")
		}))
	}
	mux.Handle("GET /_all_dbs", h(s.allDBs))
	mux.Handle("GET /_dbs_info", h(s.dbsInfoAll))
	mux.Handle("POST /_dbs_info", h(s.dbsInfo))

	mux.Handle("GET /_session", h(s.sessionGet))
	mux.Handle("POST /_session", h(s.sessionPost))
	mux.Handle("DELETE /_session", h(s.sessionDelete))

	mux.Handle("POST /_replicate", h(s.replicateNow))
	mux.Handle("GET /_scheduler/jobs", h(s.schedulerJobs))
	mux.Handle("GET /_scheduler/docs", h(s.schedulerDocs))
	mux.Handle("GET /_active_tasks", h(s.activeTasks))
	mux.Handle("GET /_db_updates", h(s.dbUpdates))

	mux.Handle("GET /_membership", h(s.membership))
	mux.Handle("GET /_cluster_setup", h(s.clusterSetupGet))
	mux.Handle("POST /_cluster_setup", h(s.clusterSetupPost))
	mux.Handle("GET /_reshard", h(s.reshard))
	mux.Handle("GET /_reshard/state", h(s.reshardState))

	// A wildcard {node} would be ambiguous with /{db}/_design/{ddoc}, so the
	// two accepted node names are registered literally. Anything else returns 404.
	for _, node := range []string{"_local", "nonode@nohost"} {
		prefix := "/_node/" + node + "/_config"
		mux.Handle("GET "+prefix, h(s.configAll))
		mux.Handle("GET "+prefix+"/{section}", h(s.configSection))
		mux.Handle("GET "+prefix+"/{section}/{key}", h(s.configGet))
		mux.Handle("PUT "+prefix+"/{section}/{key}", h(s.configPut))
		mux.Handle("DELETE "+prefix+"/{section}/{key}", h(s.configDelete))
		mux.Handle("GET /_node/"+node+"/_versions", h(s.nodeVersions))
		mux.Handle("GET /_node/"+node+"/_system", h(s.nodeSystem))
		mux.Handle("GET /_node/"+node+"/_stats", h(s.nodeStats))
	}

	mux.Handle("GET /{db}", h(s.dbGet))
	mux.Handle("PUT /{db}", h(s.dbPut))
	mux.Handle("DELETE /{db}", h(s.dbDelete))
	mux.Handle("POST /{db}", h(s.docPost))
	mux.Handle("/{db}", h(s.dbMethodNotAllowed))

	mux.Handle("GET /{db}/_all_docs", h(s.allDocsGet))
	mux.Handle("POST /{db}/_all_docs", h(s.allDocsPost))
	mux.Handle("POST /{db}/_all_docs/queries", h(s.allDocsQueries))
	mux.Handle("GET /{db}/_design_docs", h(s.designDocsGet))
	mux.Handle("POST /{db}/_design_docs", h(s.designDocsPost))
	mux.Handle("POST /{db}/_find", h(s.mangoFind))
	mux.Handle("POST /{db}/_explain", h(s.mangoExplain))
	mux.Handle("POST /{db}/_index", h(s.mangoIndexPost))
	mux.Handle("GET /{db}/_index", h(s.mangoIndexList))
	mux.Handle("DELETE /{db}/_index/_design/{ddoc}/json/{name}", h(s.mangoIndexDelete))
	mux.Handle("DELETE /{db}/_index/{ddoc}/json/{name}", h(s.mangoIndexDelete))
	mux.Handle("POST /{db}/_compact", h(s.compactDB))
	mux.Handle("POST /{db}/_compact/{ddoc}", h(s.compactView))
	mux.Handle("POST /{db}/_view_cleanup", h(s.viewCleanup))
	// Temporary views left CouchDB in 2.x. Answer like 3.x does.
	tempViewGone := h(func(w http.ResponseWriter, r *http.Request) error {
		return couch.NewError(410, "gone", "Temporary views are not supported in CouchDB")
	})
	mux.Handle("GET /{db}/_temp_view", tempViewGone)
	mux.Handle("POST /{db}/_temp_view", tempViewGone)
	mux.Handle("GET /{db}/_revs_limit", h(s.revsLimitGet))
	mux.Handle("PUT /{db}/_revs_limit", h(s.revsLimitPut))
	mux.Handle("GET /{db}/_security", h(s.securityGet))
	mux.Handle("PUT /{db}/_security", h(s.securityPut))

	mux.Handle("POST /{db}/_purge", h(s.purge))
	mux.Handle("GET /{db}/_purged_infos_limit", h(s.purgedInfosLimitGet))
	mux.Handle("PUT /{db}/_purged_infos_limit", h(s.purgedInfosLimitPut))

	mux.Handle("GET /{db}/_partition/{partition}", h(s.partitionInfo))
	mux.Handle("GET /{db}/_partition/{partition}/_all_docs", h(s.partitionAllDocsGet))
	mux.Handle("POST /{db}/_partition/{partition}/_all_docs", h(s.partitionAllDocsPost))
	mux.Handle("GET /{db}/_partition/{partition}/_design/{ddoc}/_view/{view}", h(s.partitionViewGet))
	mux.Handle("POST /{db}/_partition/{partition}/_design/{ddoc}/_view/{view}", h(s.partitionViewPost))
	mux.Handle("POST /{db}/_partition/{partition}/_find", h(s.mangoFind))
	mux.Handle("POST /{db}/_partition/{partition}/_explain", h(s.mangoExplain))

	mux.Handle("GET /{db}/_changes", h(s.changesGet))
	mux.Handle("POST /{db}/_changes", h(s.changesPost))
	mux.Handle("POST /{db}/_bulk_docs", h(s.bulkDocs))
	mux.Handle("POST /{db}/_bulk_get", h(s.bulkGet))
	mux.Handle("POST /{db}/_revs_diff", h(s.revsDiff))
	mux.Handle("POST /{db}/_missing_revs", h(s.missingRevs))
	mux.Handle("POST /{db}/_ensure_full_commit", h(s.ensureFullCommit))

	mux.Handle("GET /{db}/_local/{docid}", h(s.localGet))
	mux.Handle("PUT /{db}/_local/{docid}", h(s.localPut))
	mux.Handle("DELETE /{db}/_local/{docid}", h(s.localDelete))
	mux.Handle("PUT /{db}/_local/{docid}/{rest...}", h(func(w http.ResponseWriter, r *http.Request) error {
		return couch.BadRequest("_local documents do not accept attachments.")
	}))
	mux.Handle("GET /{db}/_local_docs", h(s.localDocs))
	mux.Handle("POST /{db}/_local_docs", h(s.localDocs))

	mux.Handle("GET /{db}/_design/{ddoc}", h(s.designGet))
	mux.Handle("PUT /{db}/_design/{ddoc}", h(s.designPut))
	mux.Handle("DELETE /{db}/_design/{ddoc}", h(s.designDelete))
	mux.Handle("/{db}/_design/{ddoc}", h(s.designCopyOr405))
	mux.Handle("GET /{db}/_design/{ddoc}/_view/{view}", h(s.viewGet))
	mux.Handle("POST /{db}/_design/{ddoc}/_view/{view}", h(s.viewPost))
	mux.Handle("POST /{db}/_design/{ddoc}/_view/{view}/queries", h(s.viewQueries))
	mux.Handle("GET /{db}/_design/{ddoc}/_info", h(s.designInfo))
	// The zoo routes have no method because dispatch happens internally. They
	// do not cross the method dimension with the attachment fallback.
	mux.Handle("/{db}/_design/{ddoc}/_show", h(invalidZooPath))
	mux.Handle("/{db}/_design/{ddoc}/_show/{fn}", h(s.showHandler))
	mux.Handle("/{db}/_design/{ddoc}/_show/{fn}/{docid...}", h(s.showHandler))
	mux.Handle("/{db}/_design/{ddoc}/_update", h(invalidZooPath))
	mux.Handle("/{db}/_design/{ddoc}/_update/{fn}", h(s.updateHandler))
	// update docids may contain slashes (COUCHDB-1229).
	mux.Handle("/{db}/_design/{ddoc}/_update/{fn}/{docid...}", h(s.updateHandler))
	mux.Handle("/{db}/_design/{ddoc}/_list", h(invalidZooPath))
	mux.Handle("/{db}/_design/{ddoc}/_list/{fn}/{view}", h(s.listHandler))
	mux.Handle("/{db}/_design/{ddoc}/_list/{fn}/{viewddoc}/{view}", h(s.listHandler))
	mux.Handle("/{db}/_design/{ddoc}/_rewrite", h(s.rewriteHandler))
	mux.Handle("/{db}/_design/{ddoc}/_rewrite/{path...}", h(s.rewriteHandler))
	mux.Handle("/{db}/_design/{ddoc}/_search/{index}", h(s.searchUnavailable))
	mux.Handle("/{db}/_design/{ddoc}/_nouveau/{index}", h(s.notFound))

	mux.Handle("GET /{db}/_shards", h(s.shards))
	mux.Handle("GET /{db}/_shards/{docid}", h(s.shardsDoc))
	mux.Handle("POST /{db}/_sync_shards", h(s.syncShards))
	// Attachment routes dispatch on method internally. Method-specific
	// patterns here would conflict with the document fallbacks.
	mux.Handle("/{db}/_design/{ddoc}/{attname...}", h(s.designAttachmentDispatch))

	mux.Handle("GET /{db}/{docid}", h(s.docGet))
	mux.Handle("PUT /{db}/{docid}", h(s.docPut))
	mux.Handle("DELETE /{db}/{docid}", h(s.docDelete))
	mux.Handle("/{db}/{docid}", h(s.docCopyOr405))
	mux.Handle("/{db}/{docid}/{attname...}", h(s.attachmentDispatch))

	mux.Handle("/", h(s.notFound))

	// /_utils serves a static file tree of arbitrary depth, which ServeMux
	// patterns can't express next to /{db} routes. Branch before the mux.
	root := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.decodePlusToSpace(r)
		// X-HTTP-Method-Override lets broken clients fake PUT/DELETE
		// through POST (never any other original method).
		if r.Method == http.MethodPost {
			switch override := r.Header.Get("X-HTTP-Method-Override"); strings.ToUpper(override) {
			case http.MethodPut, http.MethodDelete:
				r.Method = strings.ToUpper(override)
			}
			r.Header.Del("X-HTTP-Method-Override")
		}
		_, pattern := mux.Handler(r)
		release, ok := s.protectRequestBody(w, r, pattern)
		if !ok {
			return
		}
		defer release()
		if r.URL.Path == "/_utils" || strings.HasPrefix(r.URL.Path, "/_utils/") {
			h(s.utils).ServeHTTP(w, r)
			return
		}
		// An empty partition segment would hit ServeMux's clean-path
		// redirect. Answer like CouchDB instead.
		if strings.Contains(r.URL.Path, "/_partition//") {
			writeError(w, couch.NewError(400, "illegal_partition",
				"Partition must not start with an underscore"))
			return
		}
		if cb := r.URL.Query().Get("callback"); cb != "" &&
			s.config.getBool("chttpd", "allow_jsonp", false) {
			if !validJSONPCallback(cb) {
				writeError(w, couch.BadRequest("invalid_callback"))
				return
			}
			jw := &jsonpWriter{ResponseWriter: w, callback: cb}
			mux.ServeHTTP(jw, r)
			jw.finish()
			return
		}
		mux.ServeHTTP(w, r)
	})
	s.router = root
	s.handler = couchHeaders(s.authenticate(root))
	return s, nil
}

// StartReplicatorWorker starts the durable _replicator watcher.
func (s *Server) StartReplicatorWorker(ctx context.Context) {
	s.replicatorOnce.Do(func() {
		go s.scheduler.WatchSingleton(ctx)
	})
}

// h adapts an error-returning handler to http.Handler. Errors become
// CouchDB-shaped JSON responses.
type handlerFunc func(http.ResponseWriter, *http.Request) error

func h(fn handlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			writeError(w, err)
		}
	})
}
