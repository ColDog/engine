package worker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func server() (pb.ControllerClient, controller.Store) {
	store := controller.InMemStore()
	ctrl := controller.New(store)
	go func() {
		if err := ctrl.Serve(":0"); err != nil {
			panic(err)
		}
	}()
	var err error
	client, err := pb.Dial(ctrl.DialAddress())
	if err != nil {
		panic(err)
	}
	return client, store
}

func TestWorker_Run(t *testing.T) {
	client, store := server()

	w := &Worker{
		ControllerClient: client,
		PollInterval:     1 * time.Millisecond,
		RunGame:          Runner,
	}
	ctx := context.Background()

	setup := func() string {
		res, err := client.Create(context.Background(), &pb.CreateRequest{})
		require.Nil(t, err)
		_, err = client.Start(context.Background(), &pb.StartRequest{ID: res.ID})
		require.Nil(t, err)
		return res.ID
	}

	t.Run("RunNoGame", func(t *testing.T) {
		err := w.run(ctx, 1)
		require.NotNil(t, err)
		require.Equal(t,
			"rpc error: code = NotFound desc = controller: game not found",
			err.Error(),
		)
	})

	t.Run("PopCorrectGame", func(t *testing.T) {
		gameID := setup()
		w.RunGame = func(c context.Context, cl pb.ControllerClient, id string) error {
			if gameID != id {
				return fmt.Errorf("game expected %s found %s", gameID, id)
			}
			_, err := cl.EndGame(c, &pb.EndGameRequest{ID: id})
			return err
		}
		err := w.run(ctx, 1)
		require.Nil(t, err)
	})

	t.Run("PushGameFrames", func(t *testing.T) {
		gameID := setup()
		w.RunGame = func(c context.Context, cl pb.ControllerClient, id string) error {
			if gameID != id {
				return fmt.Errorf("game expected %s found %s", gameID, id)
			}
			for i := 0; i < 5; i++ {
				_, err := cl.AddGameFrame(c, &pb.AddGameFrameRequest{
					ID: id,
					GameFrame: &pb.GameFrame{
						Turn: int32(i + 1),
					},
				})
				if err != nil {
					return err
				}
			}
			_, err := cl.EndGame(c, &pb.EndGameRequest{ID: id})
			return err
		}
		err := w.run(ctx, 1)
		require.Nil(t, err)
	})

	t.Run("PushGameFrameTimeout", func(t *testing.T) {
		gameID := setup()
		w.RunGame = func(c context.Context, cl pb.ControllerClient, id string) error {
			if gameID != id {
				return fmt.Errorf("game expected %s found %s", gameID, id)
			}
			// Unlock the given game.
			md, _ := metadata.FromOutgoingContext(c)
			err := store.Unlock(c, id, md[pb.TokenKey][0])
			require.NoError(t, err)
			// Lock the game
			_, err = client.Pop(ctx, &pb.PopRequest{})
			require.NoError(t, err)
			// Push game frame.
			_, err = cl.AddGameFrame(c, &pb.AddGameFrameRequest{
				ID:        id,
				GameFrame: &pb.GameFrame{},
			})
			return err
		}
		err := w.run(ctx, 1)
		require.NotNil(t, err)
		require.Equal(t,
			"rpc error: code = ResourceExhausted desc = controller: game is locked",
			err.Error(),
		)
	})
}

func TestWorker_RunLoop(t *testing.T) {
	client, _ := server()

	w := &Worker{
		ControllerClient: client,
		PollInterval:     1 * time.Millisecond,
		RunGame: func(c context.Context, cl pb.ControllerClient, id string) error {
			for i := 0; i < 5; i++ {
				_, err := cl.AddGameFrame(c, &pb.AddGameFrameRequest{
					ID:        id,
					GameFrame: &pb.GameFrame{},
				})
				if err != nil {
					return err
				}
			}
			_, err := cl.EndGame(c, &pb.EndGameRequest{ID: id})
			return err
		},
	}

	resp, err := client.Create(context.Background(), &pb.CreateRequest{})
	require.NoError(t, err)
	_, err = client.Start(context.Background(), &pb.StartRequest{ID: resp.ID})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	w.Run(ctx, 1)
}

func TestWorker_RunLoopError(t *testing.T) {
	client, _ := server()

	w := &Worker{
		ControllerClient: client,
		PollInterval:     1 * time.Millisecond,
		RunGame: func(c context.Context, cl pb.ControllerClient, id string) error {
			_, err := cl.EndGame(c, &pb.EndGameRequest{ID: id})
			if err != nil {
				return err
			}
			return errors.New("im an error")
		},
	}

	resp, _ := client.Create(context.Background(), &pb.CreateRequest{})
	_, err := client.Start(context.Background(), &pb.StartRequest{ID: resp.ID})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	w.Run(ctx, 1)
}
