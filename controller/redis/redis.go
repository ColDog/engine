package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

// Store is an implementation of the controller.Store interface
type Store struct {
	client  *redis.Client
	dataTTL time.Duration
}

// DefaultDataTTL is how long data will be kept before redis evicts it
const DefaultDataTTL = time.Hour * 24 * 30

// NewStore will create a new instance of an underlying redis client, so it should not be re-created across "threads"
// - connectURL see: github.com/go-redis/redis/options.go for URL specifics
// The underlying redis client will be immediately tested for connectivity, so don't call this until you know redis can connect.
// Returns a new instance OR an error if unable (meaning an issue connecting to your redis URL)
func NewStore(connectURL string) (*Store, error) {
	o, err := redis.ParseURL(connectURL)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse redis URL")
	}

	client := redis.NewClient(o)

	// Validate it's connected
	err = client.Ping().Err()
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect ")
	}

	return &Store{client: client, dataTTL: DefaultDataTTL}, nil
}

// Close closes the underlying redis client. see: github.com/go-redis/redis/Client.go
func (rs *Store) Close() error {
	return rs.client.Close()
}

// Lock will lock a specific game, returning a token that must be used to
// write frames to the game.
func (rs *Store) Lock(ctx context.Context, key, token string, expires time.Duration) (string, error) {
	// Generate a token if the one passed is empty
	if token == "" {
		token = uuid.NewV4().String()
	}

	// Acquire or match the lock token
	pipe := rs.client.TxPipeline()
	newLock := pipe.SetNX(gameLockKey(key), token, expires)
	lockTkn := pipe.Get(gameLockKey(key))
	_, err := pipe.Exec()
	if err != nil {
		return "", errors.Wrap(err, "unexpected redis error during tx pipeline")
	}

	// Either we got a new lock or we have the same token for this to succeed
	if newLock.Val() || token == lockTkn.Val() {
		return lockTkn.Val(), nil
	}

	// Default pessimistically to no lock acquired
	return "", controller.ErrIsLocked
}

// Unlock will unlock a game if it is locked and the token used to lock it
// is correct.
func (rs *Store) Unlock(ctx context.Context, key, token string) error {
	// Short-circuit empty-string, we won't allow that
	if token == "" {
		return controller.ErrNotFound
	}

	r, err := unlockCmd.Run(rs.client, []string{gameLockKey(key)}, token).Result()
	if err != nil {
		return errors.Wrap(err, "unexpected redis error during unlock")
	}

	// UnlockCmd returns a 1 if key was found
	if r.(int64) != 1 {
		return controller.ErrNotFound
	}

	return nil
}

// PopGameID returns a new game that is unlocked and running. Workers call
// this method through the controller to find games to process.
func (rs *Store) PopGameID(c context.Context) (string, error) {
	r, err := findUnlockedGameCmd.Run(rs.client, []string{}).Result()
	if err != nil {
		return "", errors.Wrap(err, "unexpected redis exception while popping game")
	}

	gameID := fmt.Sprint(r)
	if len(gameID) == 0 {
		return "", controller.ErrNotFound
	}

	return fmt.Sprint(r), nil
}

// SetGameStatus is used to set a specific game status. This operation
// should be atomic.
func (rs *Store) SetGameStatus(c context.Context, id string, status rules.GameStatus) error {
	key := gameKey(id)
	err := rs.client.HSet(key, "status", string(status)).Err()
	if err != nil {
		return errors.Wrap(err, "unexpected redis error when setting game status")
	}

	return nil
}

// CreateGame will insert a game with the default game frames.
func (rs *Store) CreateGame(c context.Context, game *pb.Game, frames []*pb.GameFrame) error {
	if game.ID == "" {
		return fmt.Errorf("game must have a non-zero ID")
	}

	// Marshal the game
	gk := gameKey(game.ID)
	gameBytes, err := proto.Marshal(game)
	if err != nil {
		return errors.Wrap(err, "unable to marshal game state")
	}
	pipe := rs.client.TxPipeline()
	pipe.HSet(gk, "state", gameBytes)
	pipe.HSet(gk, "status", game.Status)
	pipe.HSet(gk, "id", game.ID)
	pipe.Expire(gk, DefaultDataTTL)

	// Marshal the frames
	if len(frames) > 0 {
		framesKey := framesKey(game.ID)
		var frameData []interface{}

		for _, f := range frames {
			var data []byte
			data, err = proto.Marshal(f)
			if err != nil {
				return errors.Wrap(err, "unable to marshal frame")
			}
			frameData = append(frameData, data)
		}
		pipe.LPush(framesKey, frameData...)
		// Frames will expire the same time as the game
		pipe.Expire(framesKey, DefaultDataTTL)
	}

	// Execute the entire set of operations in one big transactional pipeline
	_, err = pipe.Exec()
	if err != nil {
		return errors.Wrap(err, "unexpected redis error while saving game state")
	}

	return nil
}

// PushGameFrame will push a game frame onto the list of frames.
func (rs *Store) PushGameFrame(c context.Context, id string, t *pb.GameFrame) error {
	frameBytes, err := proto.Marshal(t)
	if err != nil {
		return errors.Wrap(err, "frame marshalling error")
	}
	// Do not update expiry here, we don't want the frames kept longer than the corresponding game
	numAdded, err := rs.client.RPush(framesKey(id), frameBytes).Result()
	if err != nil {
		return errors.Wrap(err, "unexpected redis error")
	}
	if numAdded != 1 {
		return errors.Wrap(err, "unexpected redis result")
	}

	return nil
}

// ListGameFrames will list frames by an offset and limit, it supports
// negative offset.
func (rs *Store) ListGameFrames(c context.Context, id string, limit, offset int) ([]*pb.GameFrame, error) {
	if limit <= 0 {
		return nil, errors.Errorf("invalid limit %d", limit)
	}

	// Calculate list indexes
	start := int64(offset)
	end := int64(limit + offset)
	if offset <= 0 {
		end--
	}

	// Retrieve serialized frames
	frameData, err := rs.client.LRange(framesKey(id), start, end).Result()
	if err != nil {
		return nil, errors.Wrap(err, "unexpected redis error when getting frames")
	}

	// No frames
	if len(frameData) == 0 {
		return nil, nil
	}

	// Deserialize each frame
	frames := make([]*pb.GameFrame, len(frameData))
	for i, data := range frameData {
		var f pb.GameFrame
		err = proto.Unmarshal([]byte(data), &f)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to unmarshal frame %s", data)
		}
		frames[i] = &f
	}

	return frames, nil
}

// GetGame will fetch the game.
func (rs *Store) GetGame(c context.Context, id string) (*pb.Game, error) {
	// Marshal the game
	gk := gameKey(id)

	pipe := rs.client.TxPipeline()
	gameData := pipe.HGet(gk, "state")
	gameStatus := pipe.HGet(gk, "status")

	_, err := pipe.Exec()
	if err != nil {
		return nil, errors.Wrap(err, "unexpected redis error")
	}
	var game pb.Game
	gameBytes, err := gameData.Bytes()
	if err != nil {
		return nil, errors.Wrap(err, "unexpected redis error")
	}
	err = proto.Unmarshal(gameBytes, &game)
	if err != nil {
		return nil, errors.Wrap(err, "unable to unmarshal game data")
	}
	game.Status = gameStatus.Val()

	return &game, nil
}

var unlockCmd = redis.NewScript(`
	if redis.call("GET", KEYS[1]) == ARGV[1] then
		redis.call("DEL", KEYS[1])
		return true
	end
	return false
`)

var findUnlockedGameCmd = redis.NewScript(fmt.Sprintf(`
	local cursor = "0";
	local done = false;
	repeat
		local result = redis.call("SCAN", cursor, "match", "game:*:state");
		cursor = result[1];
		local keys = result[2];
		for _, key in ipairs(keys) do
			local id = redis.call("HGET", key, "id");
			if redis.call("EXISTS", "game:" .. id .. ":locks") == 0 then
				if redis.call("HGET", key, "status") == "%s" then
					return id;
				end
			end
		end
		if cursor == "0" then
        	done = true;
    	end
	until done
	return ""
`, rules.GameStatusRunning))

// generates the redis key for a game
func gameKey(gameID string) string {
	return fmt.Sprintf("game:%s:state", gameID)
}

// generates the redis key for game frames
func framesKey(gameID string) string {
	return fmt.Sprintf("game:%s:frames", gameID)
}

// generates the redis key for game lock state
func gameLockKey(gameID string) string {
	return fmt.Sprintf("game:%s:locks", gameID)
}
