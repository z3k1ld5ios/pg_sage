package api

import (
	"net/http"

	"github.com/pg-sage/sidecar/internal/config"
	"github.com/pg-sage/sidecar/internal/llm"
)

// listModelsHandler returns available LLM models from the
// configured provider. Results are cached in the llm package.
func listModelsHandler(
	cfg *config.LLMConfig,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if cfg.Endpoint == "" || cfg.APIKey == "" {
			jsonError(w,
				"LLM not configured (missing endpoint or API key)",
				http.StatusServiceUnavailable)
			return
		}

		models, err := llm.ListModels(
			r.Context(), cfg.Endpoint, cfg.APIKey)
		if err != nil {
			jsonError(w, err.Error(),
				http.StatusBadGateway)
			return
		}

		jsonResponse(w, map[string]any{
			"models":  models,
			"current": cfg.Model,
		})
	}
}
