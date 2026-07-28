// Copyright 2018 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
)

const swaggerSpecDir = "/opt/kfp/swagger"

var swaggerSpecs = map[string]string{
	"v2beta1": "kfp_api_v2beta1.swagger.json",
}

// SwaggerHandler returns an HTTP handler that serves the Swagger UI
func SwaggerHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handle swagger root
		if r.URL.Path == "/swagger" {
			http.Redirect(w, r, "/swagger/", http.StatusFound)
			return
		}

		// Keep swagger.json as a backwards-compatible alias for the current API.
		if r.URL.Path == "/swagger/swagger.json" {
			serveSwaggerJSON(w, "v2beta1")
			return
		}
		for version := range swaggerSpecs {
			if r.URL.Path == "/swagger/"+version+"/swagger.json" {
				serveSwaggerJSON(w, version)
				return
			}
		}

		// Serve index.html for swagger UI
		if r.URL.Path == "/swagger/" || r.URL.Path == "/swagger/index.html" {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write([]byte(swaggerHTML))
			return
		}

		// Return 404 for other paths
		http.NotFound(w, r)
	})
}

// serveSwaggerJSON serves the auto-generated swagger specification
func serveSwaggerJSON(w http.ResponseWriter, version string) {
	filename, ok := swaggerSpecs[version]
	if !ok {
		http.Error(w, "Unsupported API version", http.StatusNotFound)
		return
	}
	data, err := os.ReadFile(filepath.Join(swaggerSpecDir, filename))
	if err != nil {
		http.Error(w, "Swagger specification not found. Make sure to build the API with swagger spec generation.", http.StatusNotFound)
		return
	}

	// Verify it's valid JSON
	var js interface{}
	if err := json.Unmarshal(data, &js); err != nil {
		http.Error(w, "Invalid swagger specification", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(data)
}

// swaggerHTML is the HTML for the Swagger UI
const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kubeflow Pipelines API - Swagger UI</title>
    <link rel="stylesheet" type="text/css" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.10.5/swagger-ui.css">
    <style>
        html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
        *, *:before, *:after { box-sizing: inherit; }
        body { margin: 0; padding: 0; }
        .topbar { background-color: #1f77b4; padding: 10px 20px; }
        .topbar .link { color: white; font-weight: bold; text-decoration: none; }
    </style>
</head>
<body>
    <div class="topbar">
        <a class="link" href="/">Kubeflow Pipelines API Documentation</a>
    </div>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.10.5/swagger-ui-bundle.js" charset="UTF-8"></script>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5.10.5/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
    <script>
    window.onload = function() {
        const ui = SwaggerUIBundle({
            urls: [
                {url: "v2beta1/swagger.json", name: "KFP API v2beta1"}
            ],
            "urls.primaryName": "KFP API v2beta1",
            dom_id: '#swagger-ui',
            deepLinking: true,
            presets: [
                SwaggerUIBundle.presets.apis,
                SwaggerUIStandalonePreset
            ],
            plugins: [
                SwaggerUIBundle.plugins.DownloadUrl
            ],
            layout: "StandaloneLayout",
            defaultModelsExpandDepth: 1,
            defaultModelExpandDepth: 1,
            docExpansion: "list",
            filter: true,
            tryItOutEnabled: true,
            persistAuthorization: true,
        });
        window.ui = ui;
    };
    </script>
</body>
</html>
`
