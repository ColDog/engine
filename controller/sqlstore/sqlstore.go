package sqlstore

import (
	"context"
	"database/sql"
	"time"

	_ "github.com/lib/pq" // Import pq driver.

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
)

const migrations = `
CREATE TABLE IF NOT EXISTS locks (
	key VARCHAR(255) PRIMARY KEY,
	token VARCHAR(255) NOT NULL,
	expiry TIMESTAMP NOT NULL
);
CREATE TABLE IF NOT EXISTS games (
	id VARCHAR(255) PRIMARY KEY,
	value BYTEA
);
CREATE TABLE IF NOT EXISTS game_frames (
	id VARCHAR(255),
	turn INTEGER,
	value BYTEA,
	PRIMARY KEY (id, turn)
);
`

// NewSQLStore returns a new store using a postgres database.
func NewSQLStore(url string) (*Store, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err = db.PingContext(ctx); err != nil {
		return nil, err
	}

	_, err = db.ExecContext(ctx, migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Store represents an SQL store.
type Store struct {
	db *sql.DB
}

// transact is a transaction wrapper, helps avoid failed to close connections.
func (s *Store) transact(
	ctx context.Context, txFunc func(*sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	err = txFunc(tx)
	return err
}

// Lock will lock a specific game, returning a token that must be used to
// write frames to the game.
func (s *Store) Lock(ctx context.Context, key, token string) (string, error) {
	now := time.Now()
	expiry := now.Add(controller.LockExpiry)

	if token == "" {
		token = uuid.NewV4().String()
	}

	var inserted string
	if err := s.transact(ctx, func(tx *sql.Tx) error {
		// Do a conditional update or insert.
		// - If `key` already exists and `token` matches the current token, bump.
		// - If `key` doesn't exist insert expiry.
		// - If `key` exists and `token` doesn't match return ErrIsLocked.
		if _, err := tx.ExecContext(ctx, `
		INSERT INTO locks (key, token, expiry) VALUES ($1, $2, $3)
		ON CONFLICT (key)
		DO UPDATE SET token=$2, expiry=$3
		WHERE locks.token=$2 OR locks.expiry < $4`,
			key, token, expiry, now,
		); err != nil {
			return err
		}
		r := tx.QueryRowContext(ctx, "SELECT token FROM locks WHERE key=$1", key)
		if err := r.Scan(&inserted); err != nil {
			if err != sql.ErrNoRows {
				return err
			}
		}
		return nil
	}); err != nil {
		return "", err
	}

	if inserted == token {
		return token, nil
	}
	return "", controller.ErrIsLocked
}

// Unlock will unlock a game if it is locked and the token used to lock it
// is correct.
func (s *Store) Unlock(ctx context.Context, key, token string) error {
	now := time.Now()
	return s.transact(ctx, func(tx *sql.Tx) error {
		r := tx.QueryRowContext(ctx,
			`SELECT token FROM locks WHERE key=$1 AND expiry > $2`, key, now)

		var curToken string
		if err := r.Scan(&curToken); err != nil {
			if err == sql.ErrNoRows {
				return nil
			}
			return err
		}
		if curToken != "" && curToken != token {
			return controller.ErrIsLocked
		}

		_, err := tx.ExecContext(
			ctx, `DELETE FROM locks WHERE key=$1 AND token=$2`, key, token)
		return err
	})
}

// PopGameID returns a new game that is unlocked and running. Workers call
// this method through the controller to find games to process.
func (s *Store) PopGameID(ctx context.Context) (string, error) {
	now := time.Now()
	r := s.db.QueryRowContext(ctx, `
		SELECT id FROM games
		LEFT JOIN locks ON locks.key = games.id AND locks.expiry > $1
		WHERE locks.key IS NULL
		LIMIT 1
	`, now)

	var id string
	if err := r.Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return "", controller.ErrNotFound
		}
		return "", err
	}
	return id, nil
}

// SetGameStatus is used to set a specific game status. This operation
// should be atomic.
func (s *Store) SetGameStatus(
	ctx context.Context, id string, status rules.GameStatus) error {
	return s.transact(ctx, func(tx *sql.Tx) error {
		r := tx.QueryRowContext(
			ctx, "SELECT value FROM games WHERE id=$1 FOR UPDATE", id)
		var data []byte
		if err := r.Scan(&data); err != nil {
			return err
		}

		g := &pb.Game{}
		if err := proto.Unmarshal(data, g); err != nil {
			return err
		}
		g.Status = string(status)

		{
			var err error
			data, err = proto.Marshal(g)
			if err != nil {
				return err
			}
		}

		_, err := tx.ExecContext(
			ctx, "UPDATE games SET value=$1 WHERE id=$2", data, id)
		return err
	})
}

// CreateGame will insert a game with the default game frames.
func (s *Store) CreateGame(
	ctx context.Context, g *pb.Game, frames []*pb.GameFrame) error {
	return s.transact(ctx, func(tx *sql.Tx) error {
		var data []byte
		{
			var err error
			data, err = proto.Marshal(g)
			if err != nil {
				return err
			}
		}
		// Upsert games.
		if _, err := tx.ExecContext(ctx, `
		INSERT INTO games (id, value) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET value=$2`,
			g.ID, data,
		); err != nil {
			return err
		}
		return s.pushFrames(ctx, tx, g.ID, frames...)
	})
}

func (s *Store) pushFrames(
	ctx context.Context, tx *sql.Tx, id string, frames ...*pb.GameFrame) error {
	r := tx.QueryRowContext(
		ctx, "SELECT MAX(turn) FROM game_frames where id=$1", id)

	var last *int
	var i int
	if err := r.Scan(&last); err != nil {
		if err != sql.ErrNoRows {
			return err
		}
	}
	if last == nil {
		i = -1 // Nothing exists.
	} else {
		i = *last
	}
	for _, f := range frames {
		i++
		if i != int(f.Turn) {
			return controller.ErrInvalidSequence
		}
	}

	for _, frame := range frames {
		frameData, err := proto.Marshal(frame)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(
			ctx, `INSERT INTO game_frames (id, turn, value) VALUES ($1, $2, $3)`,
			id, frame.Turn, frameData,
		); err != nil {
			return err
		}
	}
	return nil
}

// PushGameFrame will push a game frame onto the list of frames.
func (s *Store) PushGameFrame(
	ctx context.Context, id string, t *pb.GameFrame) error {
	return s.transact(ctx, func(tx *sql.Tx) error {
		return s.pushFrames(ctx, tx, id, t)
	})
}

// ListGameFrames will list frames by an offset and limit, it supports
// negative offset.
func (s *Store) ListGameFrames(
	c context.Context, id string, limit, offset int) ([]*pb.GameFrame, error) {
	if _, err := s.GetGame(c, id); err != nil {
		return nil, err
	}

	order := "ASC"
	if offset < 0 {
		order = "DESC"
		offset = -offset
	}

	rows, err := s.db.QueryContext(c,
		`SELECT value FROM game_frames WHERE id=$1 ORDER BY turn `+
			order+` LIMIT $2 OFFSET $3`,
		id, limit, offset,
	)
	if err != nil {
		return nil, err
	}

	var frames []*pb.GameFrame
	defer rows.Close()
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}

		frame := &pb.GameFrame{}
		if err := proto.Unmarshal(data, frame); err != nil {
			return nil, err
		}

		frames = append(frames, frame)
	}

	return frames, nil
}

// GetGame will fetch the game.
func (s *Store) GetGame(c context.Context, id string) (*pb.Game, error) {
	r := s.db.QueryRowContext(c, "SELECT value FROM games WHERE id=$1", id)

	var data []byte
	if err := r.Scan(&data); err != nil {
		if err == sql.ErrNoRows {
			return nil, controller.ErrNotFound
		}
		return nil, err
	}

	g := &pb.Game{}
	if err := proto.Unmarshal(data, g); err != nil {
		return nil, err
	}
	return g, nil
}
