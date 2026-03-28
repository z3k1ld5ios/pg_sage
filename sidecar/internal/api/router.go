package api

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/pg-sage/sidecar/internal/config"
	"github.com/pg-sage/sidecar/internal/fleet"
)

// NewRouter creates the API + dashboard HTTP handler.
// AuthFn and rateFn wrap /api/v1/* routes only; static
// dashboard assets are served without auth.
func NewRouter(
	mgr *fleet.DatabaseManager,
	cfg *config.Config,
	middlewares ...func(http.Handler) http.Handler,
) http.Handler {
	apiMux := http.NewServeMux()
	registerAPIRoutes(apiMux, mgr, cfg)

	// Stack middlewares onto API routes only.
	var apiHandler http.Handler = apiMux
	for i := len(middlewares) - 1; i >= 0; i-- {
		apiHandler = middlewares[i](apiHandler)
	}
	// Always apply body size limit and CORS to API routes.
	apiHandler = maxBodyMiddleware(apiHandler)
	apiHandler = corsMiddleware(apiHandler)

	// Top-level mux: API routes get auth, static does not.
	root := http.NewServeMux()
	root.Handle("/api/v1/", apiHandler)

	// Embedded dashboard (SPA fallback).
	staticSub, _ := fs.Sub(staticFiles, "dist")
	fileServer := http.FileServer(http.FS(staticSub))
	root.HandleFunc("/", func(
		w http.ResponseWriter, r *http.Request,
	) {
		path := r.URL.Path
		if path == "/" || !strings.Contains(path, ".") {
			r.URL.Path = "/"
		}
		fileServer.ServeHTTP(w, r)
	})

	return root
}

func registerAPIRoutes(
	mux *http.ServeMux,
	mgr *fleet.DatabaseManager,
	cfg *config.Config,
) {
	mux.HandleFunc(
		"GET /api/v1/databases", databasesHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/findings", findingsListHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/findings/{id}",
		findingDetailHandler(mgr))
	mux.HandleFunc(
		"POST /api/v1/findings/{id}/suppress",
		suppressHandler(mgr))
	mux.HandleFunc(
		"POST /api/v1/findings/{id}/unsuppress",
		unsuppressHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/actions", actionsListHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/actions/{id}",
		actionDetailHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/forecasts", forecastsHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/query-hints", queryHintsHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/alert-log", alertLogHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/snapshots/latest",
		snapshotLatestHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/snapshots/history",
		snapshotHistoryHandler(mgr))
	mux.HandleFunc(
		"GET /api/v1/config", configGetHandler(mgr, cfg))
	mux.HandleFunc(
		"PUT /api/v1/config",
		configUpdateHandler(mgr, cfg))
	mux.HandleFunc(
		"GET /api/v1/metrics", metricsHandler(mgr))
	mux.HandleFunc(
		"POST /api/v1/emergency-stop",
		emergencyStopHandler(mgr))
	mux.HandleFunc(
		"POST /api/v1/resume", resumeHandler(mgr))
}
