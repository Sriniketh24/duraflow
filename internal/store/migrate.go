package store

import (
	"context"
	_ "embed"
	"fmt"
)

// schemaSQL is the embedded copy of migrations/0001_init.sql. go:embed cannot
// reach outside the package directory, so this file is kept byte-for-byte
// identical to the root migrations/0001_init.sql (see that file for the
// authoritative, human-edited copy). The schema is idempotent (CREATE TABLE/
// INDEX IF NOT EXISTS), so re-running Migrate on every boot is safe.
//
//go:embed schema.sql
var schemaSQL string

// Migrate applies the embedded schema. It is safe to call on every process
// start: every statement is guarded with IF NOT EXISTS.
func (p *Postgres) Migrate(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, schemaSQL)
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}
