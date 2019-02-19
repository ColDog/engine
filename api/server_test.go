package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type MockController struct {
	pb.ControllerClient

	Error                  error
	CreateResponse         *pb.CreateResponse
	StartResponse          *pb.StartResponse
	StatusResponse         *pb.StatusResponse
	ValidateSnakeResponse  *pb.ValidateSnakeResponse
	ListGameFramesResponse func() *pb.ListGameFramesResponse
}

func (mc *MockController) Create(ctx context.Context, req *pb.CreateRequest, opts ...grpc.CallOption) (*pb.CreateResponse, error) {
	return mc.CreateResponse, mc.Error
}

func (mc *MockController) Start(ctx context.Context, req *pb.StartRequest, opts ...grpc.CallOption) (*pb.StartResponse, error) {
	return mc.StartResponse, mc.Error
}

func (mc *MockController) Status(ctx context.Context, req *pb.StatusRequest, opts ...grpc.CallOption) (*pb.StatusResponse, error) {
	return mc.StatusResponse, mc.Error
}

func (mc *MockController) ListGameFrames(ctx context.Context, req *pb.ListGameFramesRequest, opts ...grpc.CallOption) (*pb.ListGameFramesResponse, error) {
	return mc.ListGameFramesResponse(), mc.Error
}
func (mc *MockController) ValidateSnake(ctx context.Context, req *pb.ValidateSnakeRequest, opts ...grpc.CallOption) (*pb.ValidateSnakeResponse, error) {
	return mc.ValidateSnakeResponse, mc.Error
}

func createAPIServer() (*Server, *MockController) {
	var client = &MockController{
		CreateResponse:        &pb.CreateResponse{},
		StartResponse:         &pb.StartResponse{},
		StatusResponse:        &pb.StatusResponse{},
		ValidateSnakeResponse: &pb.ValidateSnakeResponse{},
		ListGameFramesResponse: func() *pb.ListGameFramesResponse {
			return &pb.ListGameFramesResponse{}
		},
	}
	s := New(":1234", client)
	return s, client
}

func createAPIServerWithError(err error) (*Server, *MockController) {
	var client = &MockController{
		Error:          err,
		CreateResponse: &pb.CreateResponse{},
		StartResponse:  &pb.StartResponse{},
		StatusResponse: &pb.StatusResponse{},
		ListGameFramesResponse: func() *pb.ListGameFramesResponse {
			return &pb.ListGameFramesResponse{}
		},
	}
	s := New(":1234", client)
	return s, client
}

func basicCreateTest(t *testing.T, bodyJSON string, expectedStatusCode int, controllerError error) {
	s, _ := createAPIServerWithError(controllerError)

	buf := &bytes.Buffer{}
	buf.WriteString(bodyJSON)
	req, _ := http.NewRequest("POST", "/games", buf)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, expectedStatusCode, rr.Code)
}

func TestCreate(t *testing.T) {
	basicCreateTest(t, "{}", http.StatusOK, nil)
}

func TestCreateWithBadBodyJSON(t *testing.T) {
	basicCreateTest(t, "invalid json", http.StatusBadRequest, nil)
}

func TestCreateHandlesErrors(t *testing.T) {
	basicCreateTest(t, "{}", http.StatusInternalServerError, errors.New("fail"))
}

type errorReader struct{}

func (errorReader) Read([]byte) (int, error) {
	return 0, errors.New("bad reader")
}

func TestCreateHandlesBadReader(t *testing.T) {
	s, _ := createAPIServer()

	badReader := errorReader{}
	req, _ := http.NewRequest("POST", "/games", badReader)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestStart(t *testing.T) {
	s, _ := createAPIServer()

	req, _ := http.NewRequest("POST", "/games/abc_123/start", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestStartHandlesControllerError(t *testing.T) {
	s, _ := createAPIServerWithError(errors.New("controller crashed"))

	req, _ := http.NewRequest("POST", "/games/abc_123/start", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
}

func basicStatusTest(t *testing.T, id string, expectedStatusCode int, controllerError error) {
	s, _ := createAPIServerWithError(controllerError)

	req, _ := http.NewRequest("GET", "/games/"+id, nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, expectedStatusCode, rr.Code)
}

func TestStatus(t *testing.T) {
	basicStatusTest(t, "abc_123", http.StatusOK, nil)
}

func TestStatusHandlesErrors(t *testing.T) {
	basicStatusTest(t, "12345", http.StatusInternalServerError, errors.New("fail"))
}

func TestGetFrames(t *testing.T) {
	s, _ := createAPIServer()

	req, _ := http.NewRequest("GET", "/games/abc_123/frames", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestValidateSnake(t *testing.T) {
	s, _ := createAPIServer()

	req, _ := http.NewRequest("GET", "/validateSnake?url=dsnek.heroku.com", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestValidateSnakeBlankUrl(t *testing.T) {
	s, _ := createAPIServer()

	req, _ := http.NewRequest("GET", "/validateSnake?url=", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestValidateSnakeMissingUrl(t *testing.T) {
	s, _ := createAPIServer()

	req, _ := http.NewRequest("GET", "/validateSnake", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestGetFramesWithControllerError(t *testing.T) {
	s, _ := createAPIServerWithError(errors.New("uh oh"))

	req, _ := http.NewRequest("GET", "/games/abc_123/frames", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	require.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestGatherFrames(t *testing.T) {
	_, mc := createAPIServer()
	mc.StatusResponse = &pb.StatusResponse{
		Game: &pb.Game{Status: string(rules.GameStatusComplete)},
	}
	calls := 0
	mc.ListGameFramesResponse = func() *pb.ListGameFramesResponse {
		defer func() {
			calls++
		}()
		if calls == 0 {
			return &pb.ListGameFramesResponse{
				Frames: []*pb.GameFrame{
					&pb.GameFrame{},
				},
			}
		}
		return &pb.ListGameFramesResponse{}
	}
	frames := make(chan *pb.GameFrame)
	go gatherFrames(context.Background(), frames, mc, "fake-id")
	frameCount := 0
	for range frames {
		frameCount++
	}
	require.Equal(t, 1, frameCount)
}

func TestGetFramesContainsZeroValues(t *testing.T) {
	s, mc := createAPIServer()
	mc.ListGameFramesResponse = func() *pb.ListGameFramesResponse {
		return &pb.ListGameFramesResponse{
			Frames: []*pb.GameFrame{
				{
					Food: []*pb.Point{
						{X: 0, Y: 1},
						{X: 0, Y: 0},
						{X: 1, Y: 0},
					},
					Snakes: []*pb.Snake{
						{
							Name:   "Zero Snake",
							Health: 0,
							Body: []*pb.Point{
								{X: 0, Y: 1},
								{X: 0, Y: 0},
								{X: 1, Y: 0},
							},
						},
					},
				},
			},
		}
	}

	req, _ := http.NewRequest("GET", "/games/abc_123/frames", nil)
	rr := httptest.NewRecorder()

	s.hs.Handler.ServeHTTP(rr, req)
	body, err := ioutil.ReadAll(rr.Body)
	require.NoError(t, err)

	var resp map[string]interface{}
	err = json.Unmarshal(body, &resp)
	require.NoError(t, err)

	frames := castJSONInterface(resp["Frames"], 0)
	snake := castJSONInterface(frames["Snakes"], 0)
	food := frames["Food"]

	require.Equal(t, 0, castPointInArray(snake["Body"], 0, "X"))
	require.Equal(t, 1, castPointInArray(snake["Body"], 0, "Y"))
	require.Equal(t, 0, castPointInArray(snake["Body"], 1, "X"))
	require.Equal(t, 0, castPointInArray(snake["Body"], 1, "Y"))
	require.Equal(t, 1, castPointInArray(snake["Body"], 2, "X"))
	require.Equal(t, 0, castPointInArray(snake["Body"], 2, "Y"))

	require.Equal(t, 0, int(snake["Health"].(float64)))

	require.Equal(t, 0, castPointInArray(food, 0, "X"))
	require.Equal(t, 1, castPointInArray(food, 0, "Y"))
	require.Equal(t, 0, castPointInArray(food, 1, "X"))
	require.Equal(t, 0, castPointInArray(food, 1, "Y"))
	require.Equal(t, 1, castPointInArray(food, 2, "X"))
	require.Equal(t, 0, castPointInArray(food, 2, "Y"))
}

func castPointInArray(resp interface{}, index int, key string) int {
	return int(resp.([]interface{})[index].(map[string]interface{})[key].(float64))
}

func castJSONInterface(resp interface{}, index int) map[string]interface{} {
	return resp.([]interface{})[index].(map[string]interface{})
}
