// Package web holds the static playground UI.
package web

import "embed"

//go:embed index.html style.css app.js
var FS embed.FS
