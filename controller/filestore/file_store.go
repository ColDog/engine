package filestore

import (
	"context"
	"os/user"
	"path"
	"sync"
	"time"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/gogo/protobuf/proto"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

func defaultDir() string {
	return path.Join(homeDir(), ".battlesnake/games")
}

func homeDir() string {
	usr, err := user.Current()
	if err != nil {
		return "."
	}
	return usr.HomeDir
}

// NewFileStore returns a file based store implementation (1 file per game).
func NewFileStore(directory string) controller.Store {
	if directory == "" {
		directory = defaultDir()
	}

	return &fileStore{
		games:     map[string]*pb.Game{},
		frames:    map[string][]*pb.GameFrame{},
		writers:   map[string]writer{},
		locks:     map[string]*lock{},
		directory: directory,
	}
}

type lock struct {
	token   string
	expires time.Time
}

type fileStore struct {
	games     map[string]*pb.Game
	frames    map[string][]*pb.GameFrame
	writers   map[string]writer
	locks     map[string]*lock
	lock      sync.Mutex
	directory string
}

// closeGame removes the game from in-memory cache and closes the handle to its
// file. Should be called when game is complete.
func (fs *fileStore) closeGame(id string) {
	if w, ok := fs.writers[id]; ok {
		err := w.Close()
		if err != nil {
			log.WithError(err).Error("Error while closing file writer")
		}
	}
	delete(fs.games, id)
	delete(fs.frames, id)
	delete(fs.writers, id)
}

func (fs *fileStore) Lock(ctx context.Context, key, token string, expires time.Duration) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	now := time.Now()

	l, ok := fs.locks[key]
	if ok {
		// We have a lock token, if it's expired just delete it and continue as
		// if nothing happened.
		if l.expires.Before(now) {
			delete(fs.locks, key)
		} else {
			// If the token is not expired and matched our active token, let's
			// just bump the expiration.
			if l.token == token {
				l.expires = time.Now().Add(expires)
				return l.token, nil
			}
			// If it's not our token, we should throw an error.
			return "", controller.ErrIsLocked
		}
	}
	if token == "" {
		token = uuid.NewV4().String()
	}
	// Lock was expired or non-existant, create a new token.
	l = &lock{
		token:   token,
		expires: now.Add(expires),
	}
	fs.locks[key] = l
	return l.token, nil
}

func (fs *fileStore) isLocked(key string) bool {
	l, ok := fs.locks[key]
	return ok && l.expires.After(time.Now())
}

func (fs *fileStore) Unlock(ctx context.Context, key, token string) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	now := time.Now()

	l, ok := fs.locks[key]
	// No lock? Don't care.
	if !ok {
		return nil
	}
	// We have a lock that matches our token, even if it's expired we are safe
	// to remove it. If it's expired, remove it as well.
	if l.expires.Before(now) || l.token == token {
		delete(fs.locks, key)
		return nil
	}
	// The token is valid and doesn't match our lock.
	return controller.ErrIsLocked
}

// PopGameID gives the next running game. Since running games should always be
// cached in memory it is not necessary to scan file system.
func (fs *fileStore) PopGameID(ctx context.Context) (string, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	// For every game we need to make sure it's active and is not locked before
	// returning it. We get randomness due to go's built in random map.
	for id, g := range fs.games {
		if !fs.isLocked(id) && g.Status == string(rules.GameStatusRunning) {
			return id, nil
		}
	}
	return "", controller.ErrNotFound
}

func (fs *fileStore) CreateGame(ctx context.Context, g *pb.Game, frames []*pb.GameFrame) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	fs.games[g.ID] = g
	if len(frames) == 0 {
		fs.frames[g.ID] = []*pb.GameFrame{}
		return nil
	}
	return fs.appendFrames(g.ID, frames)
}

func (fs *fileStore) SetGameStatus(ctx context.Context, id string, status rules.GameStatus) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	game, err := fs.requireGame(id)
	if err != nil {
		return err
	}

	game.Status = string(status)
	if status != rules.GameStatusRunning {
		fs.closeGame(id)
	}
	return nil
}

func (fs *fileStore) PushGameFrame(ctx context.Context, id string, g *pb.GameFrame) error {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	return fs.appendFrame(id, g)
}

func (fs *fileStore) ListGameFrames(ctx context.Context, id string, limit, offset int) ([]*pb.GameFrame, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	if _, err := fs.requireGame(id); err != nil {
		return nil, err
	}
	frames, err := fs.requireFrames(id)
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = len(frames) + offset
	}

	if len(frames) == 0 || offset >= len(frames) {
		return nil, nil
	}
	if offset+limit >= len(frames) {
		limit = len(frames) - offset
	}
	return frames[offset : offset+limit], nil
}

func (fs *fileStore) GetGame(ctx context.Context, id string) (*pb.Game, error) {
	fs.lock.Lock()
	defer fs.lock.Unlock()

	g, err := fs.requireGame(id)
	if err != nil {
		return nil, err
	}

	// Clone the game, since this could be modified after this is returned
	// and upset internal state inside the store.
	clone := proto.Clone(g).(*pb.Game)
	return clone, nil
}

func (fs *fileStore) requireHandle(id string, mustBeNew bool) (writer, error) {
	if w, ok := fs.writers[id]; ok {
		return w, nil
	}

	handle, err := openFileWriter(fs.directory, id, mustBeNew)
	if err != nil {
		return nil, err
	}

	fs.writers[id] = handle
	return handle, nil
}

func (fs *fileStore) requireGame(id string) (*pb.Game, error) {
	// Do nothing if game already loaded.
	if g, ok := fs.games[id]; ok {
		return g, nil
	}

	// Load frames from file.
	g, err := ReadGameInfo(fs.directory, id)
	if err != nil {
		return nil, err
	}

	fs.games[id] = g
	return g, nil
}

func (fs *fileStore) requireFrames(id string) ([]*pb.GameFrame, error) {
	// Do nothing if frames already loaded.
	if frames, ok := fs.frames[id]; ok {
		return frames, nil
	}

	// Load frames from file.
	frames, err := ReadGameFrames(fs.directory, id)
	if err != nil {
		return nil, err
	}

	fs.frames[id] = frames
	return frames, nil
}

func (fs *fileStore) appendFrame(id string, f *pb.GameFrame) error {
	game, err := fs.requireGame(id)
	if err != nil {
		return err
	}

	alreadyHasFrames := fs.hasAnyFrames(id)

	handle, err := fs.requireHandle(id, !alreadyHasFrames)
	if err != nil {
		return err
	}

	// If this is the first frame, then first write the game info header.
	if !alreadyHasFrames {
		err := writeGameInfo(handle, game, f.Snakes)
		if err != nil {
			return err
		}
	}

	// Add frame to in-memory cache
	fs.frames[id] = append(fs.frames[id], f)

	// Add frame to archive file
	return writeFrame(handle, f)
}

func (fs *fileStore) appendFrames(gameID string, frames []*pb.GameFrame) error {
	for _, f := range frames {
		if err := fs.appendFrame(gameID, f); err != nil {
			return err
		}
	}
	return nil
}

func (fs *fileStore) hasAnyFrames(gameID string) bool {
	frames, ok := fs.frames[gameID]
	return ok && len(frames) > 0
}

type gameArchive struct {
	game   *pb.Game
	frames []*pb.GameFrame
}

func getFilePath(directory string, id string) string {
	return path.Join(directory, id) + ".bs"
}
