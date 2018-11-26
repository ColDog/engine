package controller

import (
	"context"
	"sync"
	"time"

	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LockExpiry is the time after which a lock will expire.
var LockExpiry = 1 * time.Second

var (
	// ErrNotFound is thrown when a game is not found.
	ErrNotFound = status.Error(codes.NotFound, "controller: game not found")
	// ErrIsLocked is returned when a game is locked.
	ErrIsLocked = status.Error(codes.ResourceExhausted, "controller: game is locked")
	// ErrInvalidSequence is returned when a game tick is written with an
	// invalid sequence.
	ErrInvalidSequence = status.Error(codes.ResourceExhausted, "controller: invalid game tick sequence")
)

// Store is the interface to the game store. It implements locking for workers
// that are processing games and implements logic for distributing games to
// specific workers that need it.
type Store interface {
	// Lock will lock a specific game, returning a token that must be used to
	// write frames to the game.
	Lock(ctx context.Context, key, token string) (string, error)
	// Unlock will unlock a game if it is locked and the token used to lock it
	// is correct.
	Unlock(ctx context.Context, key, token string) error
	// PopGameID returns a new game that is unlocked and running. Workers call
	// this method through the controller to find games to process.
	PopGameID(context.Context) (string, error)
	// SetGameStatus is used to set a specific game status. This operation
	// should be atomic.
	SetGameStatus(c context.Context, id string, status rules.GameStatus) error
	// CreateGame will insert a game with the default game frames.
	CreateGame(context.Context, *pb.Game, []*pb.GameFrame) error
	// PushGameFrame will push a game frame onto the list of frames.
	PushGameFrame(c context.Context, id string, t *pb.GameFrame) error
	// ListGameFrames will list frames by an offset and limit, it supports
	// negative offset.
	ListGameFrames(c context.Context, id string, limit, offset int) ([]*pb.GameFrame, error)
	// GetGame will fetch the game.
	GetGame(context.Context, string) (*pb.Game, error)
}

// InMemStore returns an in memory implementation of the Store interface.
func InMemStore() Store {
	return &inmem{
		games:  map[string]*pb.Game{},
		frames: map[string][]*pb.GameFrame{},
		locks:  map[string]*lock{},
	}
}

type lock struct {
	token   string
	expires time.Time
}

type inmem struct {
	games  map[string]*pb.Game
	frames map[string][]*pb.GameFrame
	locks  map[string]*lock
	lock   sync.Mutex
}

func (in *inmem) Clear() {
	in.games = map[string]*pb.Game{}
	in.frames = map[string][]*pb.GameFrame{}
	in.locks = map[string]*lock{}
}

func (in *inmem) Lock(ctx context.Context, key, token string) (string, error) {
	in.lock.Lock()
	defer in.lock.Unlock()

	now := time.Now()

	l, ok := in.locks[key]
	if ok {
		// We have a lock token, if it's expired just delete it and continue as
		// if nothing happened.
		if l.expires.Before(now) {
			delete(in.locks, key)
		} else {
			// If the token is not expired and matched our active token, let's
			// just bump the expiration.
			if l.token == token {
				l.expires = time.Now().Add(LockExpiry)
				return l.token, nil
			}
			// If it's not our token, we should throw an error.
			return "", ErrIsLocked
		}
	}
	if token == "" {
		token = uuid.NewV4().String()
	}
	// Lock was expired or non-existant, create a new token.
	l = &lock{
		token:   token,
		expires: now.Add(LockExpiry),
	}
	in.locks[key] = l
	return l.token, nil
}

func (in *inmem) isLocked(key string) bool {
	l, ok := in.locks[key]
	return ok && l.expires.After(time.Now())
}

func (in *inmem) Unlock(ctx context.Context, key, token string) error {
	in.lock.Lock()
	defer in.lock.Unlock()

	now := time.Now()

	l, ok := in.locks[key]
	// No lock? Don't care.
	if !ok {
		return nil
	}
	// We have a lock that matches our token, even if it's expired we are safe
	// to remove it. If it's expired, remove it as well.
	if l.expires.Before(now) || l.token == token {
		delete(in.locks, key)
		return nil
	}
	// The token is valid and doesn't match our lock.
	return ErrIsLocked
}

func (in *inmem) PopGameID(ctx context.Context) (string, error) {
	in.lock.Lock()
	defer in.lock.Unlock()

	// For every game we need to make sure it's active and is not locked before
	// returning it. We get randomness due to go's built in random map.
	for id, g := range in.games {
		if !in.isLocked(id) && g.Status == string(rules.GameStatusRunning) {
			return id, nil
		}
	}
	return "", ErrNotFound
}

func (in *inmem) CreateGame(ctx context.Context, g *pb.Game, frames []*pb.GameFrame) error {
	in.lock.Lock()
	defer in.lock.Unlock()
	in.games[g.ID] = g
	in.frames[g.ID] = frames
	return nil
}

func (in *inmem) SetGameStatus(ctx context.Context, id string, status rules.GameStatus) error {
	in.lock.Lock()
	defer in.lock.Unlock()
	if g, ok := in.games[id]; ok {
		g.Status = string(status)
		return nil
	}
	return ErrNotFound
}

func (in *inmem) PushGameFrame(ctx context.Context, id string, g *pb.GameFrame) error {
	in.lock.Lock()
	defer in.lock.Unlock()
	frames := in.frames[id]
	if len(frames) > 0 {
		last := frames[len(frames)-1]
		if last.Turn+1 != g.Turn {
			return ErrInvalidSequence
		}
	} else {
		if g.Turn != 0 {
			return ErrInvalidSequence
		}
	}
	in.frames[id] = append(in.frames[id], g)
	return nil
}

func (in *inmem) ListGameFrames(ctx context.Context, id string, limit, offset int) ([]*pb.GameFrame, error) {
	in.lock.Lock()
	defer in.lock.Unlock()
	if _, ok := in.games[id]; !ok {
		return nil, ErrNotFound
	}
	frames := in.frames[id]
	if len(frames) == 0 {
		return nil, nil
	}
	if offset < 0 {
		offset = len(frames) + offset
	}
	if offset >= len(frames) {
		return nil, nil
	}
	if offset+limit >= len(frames) {
		limit = len(frames) - offset
	}
	return frames[offset : offset+limit], nil
}

func (in *inmem) GetGame(ctx context.Context, id string) (*pb.Game, error) {
	in.lock.Lock()
	defer in.lock.Unlock()

	if g, ok := in.games[id]; ok {
		// Clone the game, since this could be modified after this is returned
		// and upset internal state inside the store.
		clone := proto.Clone(g).(*pb.Game)
		return clone, nil
	}
	return nil, ErrNotFound
}
