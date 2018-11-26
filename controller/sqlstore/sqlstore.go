package sqlstore

import (
	"context"
	"database/sql"
	"fmt"
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

	_, err = db.Exec(migrations)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Store represents an SQL store.
type Store struct {
	db *sql.DB
}

// Lock will lock a specific game, returning a token that must be used to
// write frames to the game.
func (s *Store) Lock(ctx context.Context, key, token string) (string, error) {
	now := time.Now()
	expiry := now.Add(controller.LockExpiry)

	if token == "" {
		token = uuid.NewV4().String()
	}

	// Do a conditional update or insert.
	// - If `key` already exists and `token` matches the current token, bump.
	// - If `key` doesn't exist insert expiry.
	// - If `key` exists and `token` doesn't match return ErrIsLocked.
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO locks (key, token, expiry) VALUES ($1, $2, $3)
		ON CONFLICT (key)
		DO UPDATE SET token=$2, expiry=$3
		WHERE locks.token=$2 OR locks.expiry < $4`,
		key, token, expiry, now,
	); err != nil {
		return "", err
	}

	r := s.db.QueryRowContext(ctx, "SELECT token FROM locks WHERE key=$1", key)

	var inserted string
	if err := r.Scan(&inserted); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
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
	r := s.db.QueryRowContext(ctx,
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

	_, err := s.db.ExecContext(
		ctx, `DELETE FROM locks WHERE key=$1 AND token=$2`, key, token)
	return err
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
	c context.Context, id string, status rules.GameStatus) error {

	tx, err := s.db.BeginTx(c, nil)
	if err != nil {
		return err
	}

	r := tx.QueryRowContext(c, "SELECT value FROM games WHERE id=$1", id)

	var data []byte
	if serr := r.Scan(&data); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			return rerr
		}
		return serr
	}

	g := &pb.Game{}
	if perr := proto.Unmarshal(data, g); perr != nil {
		return perr
	}
	g.Status = string(status)

	newData, err := proto.Marshal(g)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(
		c, "UPDATE games SET value=$1 WHERE id=$2", newData, id)
	return err
}

// CreateGame will insert a game with the default game frames.
func (s *Store) CreateGame(
	c context.Context, g *pb.Game, frames []*pb.GameFrame) error {
	tx, err := s.db.BeginTx(c, &sql.TxOptions{})
	if err != nil {
		return err
	}

	gameData, err := proto.Marshal(g)
	if err != nil {
		return err
	}

	if _, eerr := tx.ExecContext(
		c, `
		INSERT INTO games (id, value) VALUES ($1, $2)
		ON CONFLICT (id) DO UPDATE SET value=$2`,
		g.ID, gameData,
	); eerr != nil {
		return eerr
	}

	if err = s.pushFrames(c, tx, g.ID, frames...); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			return rerr
		}
		return err
	}
	return tx.Commit()
}

func (s *Store) pushFrames(
	c context.Context, tx *sql.Tx, id string, frames ...*pb.GameFrame) error {
	r := tx.QueryRowContext(
		c, "SELECT MAX(turn) FROM game_frames where id=$1", id)

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
			c, `INSERT INTO game_frames (id, turn, value) VALUES ($1, $2, $3)`,
			id, frame.Turn, frameData,
		); err != nil {
			return err
		}
	}
	return nil
}

// PushGameFrame will push a game frame onto the list of frames.
func (s *Store) PushGameFrame(
	c context.Context, id string, t *pb.GameFrame) error {
	tx, err := s.db.BeginTx(c, &sql.TxOptions{})
	if err != nil {
		return err
	}
	if err = s.pushFrames(c, tx, id, t); err != nil {
		if rerr := tx.Rollback(); rerr != nil {
			return rerr
		}
		return err
	}
	return tx.Commit()
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

	rows, err := s.db.QueryContext(
		c,
		fmt.Sprintf(
			`SELECT value FROM game_frames WHERE id=$1 ORDER BY turn %s LIMIT $2 OFFSET $3`,
			order,
		),
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
	if perr := proto.Unmarshal(data, g); perr != nil {
		return nil, perr
	}

	return g, nil
}
