package migrations

import "embed"

// FS contains the embedded database migrations.
//
//go:embed *.sql
var FS embed.FS
