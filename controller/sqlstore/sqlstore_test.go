package sqlstore

import (
	"database/sql"
	"testing"

	"github.com/battlesnakeio/engine/controller/testsuite"
	"github.com/stretchr/testify/require"
)

func mustExec(db *sql.DB, sq string) {
	if _, err := db.Exec(sq); err != nil {
		panic(err)
	}
}

func TestSQLStore(t *testing.T) {
	s, err := NewSQLStore(
		"postgres://postgres@127.0.0.1:5433/postgres?sslmode=disable",
	)
	require.NoError(t, err)

	testsuite.Suite(t, s, func() {
		mustExec(s.db, "TRUNCATE locks")
		mustExec(s.db, "TRUNCATE games")
		mustExec(s.db, "TRUNCATE game_frames")
	})
}
