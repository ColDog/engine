package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"fmt"
	"os"

	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/davecgh/go-spew/spew"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

var snakeURL string

func init() {
	tst := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`{"move": "up"}`))
		if err != nil {
			fmt.Println("unable to write bytes", err)
			os.Exit(1)
		}
	}))
	snakeURL = tst.URL
}

func TestWorker_RunnerErrors(t *testing.T) {
	client, store := server()

	ctx := context.Background()

	w := &Worker{
		ControllerClient: client,
		PollInterval:     1 * time.Millisecond,
		RunGame:          Runner,
	}

	t.Run("NoGame", func(t *testing.T) {
		err := Runner(ctx, client, "")
		require.NotNil(t, err)
		require.Equal(t,
			"rpc error: code = NotFound desc = controller: game not found",
			err.Error(),
		)
	})

	t.Run("GameFrameError", func(t *testing.T) {
		err := store.CreateGame(ctx, &pb.Game{ID: "1", Status: string(rules.GameStatusRunning)}, nil)
		require.NoError(t, err)

		err = w.run(ctx, 1)
		require.NotNil(t, err)
		require.Equal(t, "rules: invalid state, previous frame is nil", err.Error())
	})

	t.Run("GameFrameLocked", func(t *testing.T) {
		err := store.CreateGame(ctx,
			&pb.Game{ID: "2", Status: string(rules.GameStatusRunning)},
			[]*pb.GameFrame{{}},
		)
		require.NoError(t, err)
		w.RunGame = func(c context.Context, cl pb.ControllerClient, id string) error {
			md, _ := metadata.FromOutgoingContext(c)
			err = store.Unlock(c, id, md[pb.TokenKey][0])
			require.NoError(t, err)
			_, err = store.Lock(c, id, "", 1*time.Second)
			require.NoError(t, err)

			return Runner(c, cl, id)
		}

		err = w.run(ctx, 1)
		require.NotNil(t, err)
		require.Equal(t,
			"rpc error: code = ResourceExhausted desc = controller: game is locked",
			err.Error(),
		)
	})

}

func TestWorker_Runner(t *testing.T) {
	client, _ := server()

	games := map[string]*pb.CreateRequest{
		"Simple": {
			Width:  5,
			Height: 5,
			Food:   5,
			Snakes: []*pb.SnakeOptions{
				{
					Name: "1",
					URL:  snakeURL,
					ID:   "1",
				},
				{
					Name: "2",
					URL:  snakeURL,
					ID:   "2",
				},
			},
		},
		"InvalidURL": {
			Width:  5,
			Height: 5,
			Food:   5,
			Snakes: []*pb.SnakeOptions{
				{
					Name: "1",
					URL:  snakeURL,
					ID:   "1",
				},
				{
					Name: "2",
					URL:  "invalid",
					ID:   "2",
				},
			},
		},
		"LargerBoard": {
			Width:  25,
			Height: 25,
			Food:   10,
			Snakes: []*pb.SnakeOptions{
				{
					Name: "1",
					URL:  snakeURL,
					ID:   "1",
				},
				{
					Name: "2",
					URL:  snakeURL,
					ID:   "2",
				},
			},
		},
	}
	ctx := context.Background()

	w := &Worker{
		ControllerClient: client,
		PollInterval:     1 * time.Millisecond,
		RunGame:          Runner,
	}

	for key, game := range games {
		t.Run(key, func(t *testing.T) {
			g, err := client.Create(ctx, game)
			require.Nil(t, err)

			_, err = client.Start(ctx, &pb.StartRequest{ID: g.ID})
			require.Nil(t, err)

			err = w.run(ctx, 1)
			require.Nil(t, err)

			st, err := client.Status(ctx, &pb.StatusRequest{ID: g.ID})
			require.Nil(t, err)

			spew.Dump(st)
		})
	}
}
