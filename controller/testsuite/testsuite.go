package testsuite

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/require"
)

func testStoreLock(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()

	ctx := context.Background()

	// Lock random key.
	tok, err := s.Lock(ctx, key, "", 1*time.Second)
	require.Nil(t, err)
	require.NotEmpty(t, tok)

	// Lock with valid token, no error same token returned.
	tok2, err := s.Lock(ctx, key, tok, 1*time.Second)
	require.Nil(t, err)
	require.Equal(t, tok, tok2)

	// Unlock without valid token returns error.
	err = s.Unlock(ctx, key, "")
	require.Error(t, err)

	// Unlock with valid token no error.
	err = s.Unlock(ctx, key, tok)
	require.Nil(t, err)

	// Unlock where lock doesn't exist returns no error.
	err = s.Unlock(ctx, key+"-missing", "")
	require.Nil(t, err)
}

func testStoreLockExpiry(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()
	ctx := context.Background()

	// Lock random key.
	tok, err := s.Lock(ctx, key, "", -10*time.Second)
	require.Nil(t, err)
	require.NotEmpty(t, tok)

	// Lock (with token) has expired.
	tok2, err := s.Lock(ctx, key, tok, 1*time.Second)
	require.Nil(t, err)
	require.Equal(t, tok, tok2)

	// Unlock (no token) has expired.
	err = s.Unlock(ctx, key, "")
	require.NoError(t, err)

	// Lock (no token) has expired.
	_, err = s.Lock(ctx, key, "", 1*time.Second)
	require.Nil(t, err)

	// Unlock (no token) has expired.
	err = s.Unlock(ctx, key, "")
	require.Nil(t, err)
}

func testStoreGameStatus(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()
	ctx := context.Background()

	// Create and fetch a game.
	err := s.CreateGame(ctx, &pb.Game{
		ID: key, Status: string(rules.GameStatusRunning)}, nil)
	require.Nil(t, err)

	// Set game to running.
	err = s.SetGameStatus(ctx, key, "running")
	require.Nil(t, err)

	// Pop game can find it.
	id, err := s.PopGameID(ctx)
	require.Nil(t, err)
	require.Equal(t, key, id)

	// Set game to error.
	err = s.SetGameStatus(ctx, key, "error")
	require.Nil(t, err)

	// Cannot pop.
	_, err = s.PopGameID(ctx)
	require.NotNil(t, err)
}

func testStoreGames(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()
	ctx := context.Background()

	// Create and fetch a game.
	err := s.CreateGame(ctx, &pb.Game{
		ID: key, Status: string(rules.GameStatusRunning)}, nil)
	require.Nil(t, err)
	g, err := s.GetGame(ctx, key)
	require.Nil(t, err)
	require.Equal(t, key, g.ID)

	// NotFound error thrown.
	_, err = s.GetGame(ctx, key+"-missing")
	require.Equal(t, controller.ErrNotFound, err)

	// Pop game can find it.
	id, err := s.PopGameID(ctx)
	require.Nil(t, err)
	require.Equal(t, key, id)

	// Lock test key, cannot pop.
	_, err = s.Lock(ctx, key, "", 1*time.Second)
	require.Nil(t, err)
	p, err := s.PopGameID(ctx)
	fmt.Printf("%v", p)
	require.NotNil(t, err)
}

func testStoreGameFrames(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()
	ctx := context.Background()

	// Create and fetch a game.
	err := s.CreateGame(ctx, &pb.Game{
		ID: key, Status: string(rules.GameStatusRunning)}, nil)
	require.Nil(t, err)
	g, err := s.GetGame(ctx, key)
	require.Nil(t, err)
	require.Equal(t, key, g.ID)

	// Read game frames, too high offset.
	frames, err := s.ListGameFrames(ctx, key, 10, 100)
	require.Nil(t, err)
	require.Equal(t, 0, len(frames))

	// Read game frames, 0 offset.
	frames, err = s.ListGameFrames(ctx, key, 10, 0)
	require.Nil(t, err)
	require.Equal(t, 0, len(frames))

	// Push a game frame.
	err = s.PushGameFrame(ctx, key, &pb.GameFrame{})
	require.Nil(t, err)

	// Read the game frames.
	frames, err = s.ListGameFrames(ctx, key, 1, 0)
	require.Nil(t, err)
	require.Equal(t, 1, len(frames))

	// Read game frames that don't exist.
	frames, err = s.ListGameFrames(ctx, key+"-missing", 1, 0)
	require.Equal(t, controller.ErrNotFound, err)
	require.Equal(t, 0, len(frames))

	// Read the game frames, too high offset.
	frames, err = s.ListGameFrames(ctx, key, 10, 100)
	require.Nil(t, err)
	require.Equal(t, 0, len(frames))
}

func testStoreConcurrentWriters(t *testing.T, s controller.Store) {
	key := uuid.NewV4().String()
	ctx := context.Background()

	// Create and fetch a game.
	err := s.CreateGame(ctx,
		&pb.Game{ID: key, Status: string(rules.GameStatusRunning)}, nil)
	require.Nil(t, err)

	var ok uint32 // How many got the lock.
	var wg sync.WaitGroup
	wg.Add(20)

	for i := 0; i < 20; i++ {
		go func() {
			// Lock key, push allowed.
			_, errl := s.Lock(ctx, key, "", 1*time.Second)
			// If locked, push should be allowed. If not locked, push not
			// allowed.
			if errl == nil {
				atomic.AddUint32(&ok, 1)
			}
			wg.Done()
		}()
	}

	wg.Wait()

	require.Equal(t, uint32(1), ok)
}

// Suite will execute the store testsuite.
func Suite(t *testing.T, s controller.Store, pretest func()) {
	t.Run("Lock", func(t *testing.T) { pretest(); testStoreLock(t, s) })
	t.Run("LockExpiry", func(t *testing.T) { pretest(); testStoreLockExpiry(t, s) })
	t.Run("Games", func(t *testing.T) { pretest(); testStoreGames(t, s) })
	t.Run("GameStatus", func(t *testing.T) { pretest(); testStoreGameStatus(t, s) })
	t.Run("GameFrames", func(t *testing.T) { pretest(); testStoreGameFrames(t, s) })
	t.Run("ConcurrentWriters", func(t *testing.T) { pretest(); testStoreConcurrentWriters(t, s) })
}
