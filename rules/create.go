package rules

import (
	"errors"

	"github.com/battlesnakeio/engine/controller/pb"
	uuid "github.com/satori/go.uuid"
)

// SnakeTimeout represents the default snake timeout in milliseconds.
const SnakeTimeout = 1000

// GameMode represents the mode the game is running in
type GameMode string

const (
	// GameModeSinglePlayer represents the game running in single player mode, this means the game will
	// run until the only snake in the game dies
	GameModeSinglePlayer GameMode = "single-player"
	// GameModeMultiPlayer represents when there is more then 1 snake in the game, this means the game will
	// run until there is zero or one snakes left alive in the game.
	GameModeMultiPlayer GameMode = "multi-player"
)

// CreateInitialGame creates a new game based on the create request passed in
func CreateInitialGame(req *pb.CreateRequest) (*pb.Game, []*pb.GameFrame, error) {
	snakes, err := getSnakes(req)
	if err != nil {
		return nil, nil, err
	}
	food, err := generateFood(req, snakes)
	if err != nil {
		return nil, nil, err
	}

	id := uuid.NewV4().String()
	game := &pb.Game{
		ID:           id,
		Width:        req.Width,
		Height:       req.Height,
		Status:       string(GameStatusStopped),
		SnakeTimeout: SnakeTimeout,                      // TODO: make this configurable
		TurnTimeout:  int32(SnakeTimeout * len(snakes)), // TODO: make this configurable
		Mode:         string(GameModeMultiPlayer),
	}

	if len(snakes) == 1 {
		game.Mode = string(GameModeSinglePlayer)
	}

	frames := []*pb.GameFrame{
		{
			Turn:   0,
			Food:   food,
			Snakes: snakes,
		},
	}

	notifyGameStart(game, frames[0])

	return game, frames, nil
}

func getSnakes(req *pb.CreateRequest) ([]*pb.Snake, error) {
	snakes := []*pb.Snake{}

	for _, opts := range req.Snakes {
		startPoint := getUnoccupiedPoint(req.Width, req.Height, []*pb.Point{}, snakes)
		if startPoint == nil {
			return nil, errors.New("no unoccupied spots left for new snake")
		}
		snake := &pb.Snake{
			ID:     opts.ID,
			Name:   opts.Name,
			URL:    opts.URL,
			Health: 100,
			Body: []*pb.Point{
				startPoint,
				startPoint.Clone(),
				startPoint.Clone(),
			},
		}
		if len(snake.ID) == 0 {
			snake.ID = uuid.NewV4().String()
		}

		for _, s := range snakes {
			if s.ID == snake.ID {
				return nil, errors.New("duplicate snake id found, create aborted")
			}
		}

		snakes = append(snakes, snake)
	}

	return snakes, nil
}

func generateFood(req *pb.CreateRequest, snakes []*pb.Snake) ([]*pb.Point, error) {
	food := []*pb.Point{}

	for i := int32(0); i < req.Food; i++ {
		p := getUnoccupiedPoint(req.Width, req.Height, food, snakes)
		if p != nil {
			food = append(food, p)
		}
	}

	return food, nil
}
