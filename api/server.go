package api

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/rules"
	"github.com/golang/protobuf/jsonpb"
	"github.com/gorilla/websocket"
	"github.com/julienschmidt/httprouter"
	"github.com/pkg/errors"
	"github.com/rs/cors"
	log "github.com/sirupsen/logrus"
)

// Server this is the api server
type Server struct {
	hs *http.Server
}

// clientHandle is a function that handles an http route and accepts a ControllerClient
// in addition to the normal httprouter.Handle parameters.
type clientHandle func(http.ResponseWriter, *http.Request, httprouter.Params, pb.ControllerClient)

// New creates a new api server
func New(addr string, c pb.ControllerClient) *Server {
	router := httprouter.New()
	router.POST("/games", logging(newClientHandle(c, createGame)))
	router.POST("/games/:id/start", logging(newClientHandle(c, startGame)))
	router.GET("/games/:id", logging(newClientHandle(c, getStatus)))
	router.GET("/games/:id/frames", logging(newClientHandle(c, getFrames)))
	router.GET("/socket/:id", logging(newClientHandle(c, framesSocket)))
	router.GET("/validateSnake", logging(newClientHandle(c, validateSnake)))

	handler := cors.Default().Handler(router)

	return &Server{
		hs: &http.Server{
			Addr:    addr,
			Handler: handler,
		},
	}
}

func logging(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		start := time.Now()
		h(w, r, p)
		log.WithFields(log.Fields{
			"duration": time.Since(start),
			"url":      r.URL.String(),
		}).Info("API Call")
	}
}

func newClientHandle(c pb.ControllerClient, innerHandle clientHandle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		innerHandle(w, r, p, c)
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func framesSocket(w http.ResponseWriter, r *http.Request, ps httprouter.Params, c pb.ControllerClient) {
	id := ps.ByName("id")
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.WithError(err).Error("Unable to upgrade connection")
		return
	}
	defer func() {
		err = ws.Close()
		if err != nil {
			log.WithError(err).Error("Unable to close websocket stream")
		}
	}()
	frames := make(chan *pb.GameFrame)
	go gatherFrames(r.Context(), frames, c, id)
	for frame := range frames {
		m := jsonpb.Marshaler{EmitDefaults: true}

		var jsonStr string
		jsonStr, err = m.MarshalToString(frame)
		if err != nil {
			log.WithError(err).Error("Unable to serialize frame for websocket")
		}

		data := []byte(jsonStr)

		err = ws.WriteMessage(websocket.TextMessage, data)
		if err != nil {
			log.WithError(err).Error("Unable to write to websocket")
			break
		}
	}
	err = ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	if err != nil {
		log.WithError(err).Error("Problem closing websocket")
	}
}

func gatherFrames(ctx context.Context, frames chan<- *pb.GameFrame, c pb.ControllerClient, id string) {
	offset := int32(0)
	for {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		resp, err := c.ListGameFrames(ctx, &pb.ListGameFramesRequest{
			ID:     id,
			Offset: offset,
		})
		if err != nil {
			log.WithError(err).Error("Error while retrieving frames from controller")
			cancel()
			break
		}

		for _, f := range resp.Frames {
			for _, s := range f.Snakes {
				s.URL = ""
			}
			frames <- f
		}

		offset += int32(len(resp.Frames))

		if len(resp.Frames) == 0 {
			sg, err := c.Status(ctx, &pb.StatusRequest{
				ID: id,
			})
			if err != nil {
				log.WithError(err).Error("Error while retrieving game status from controller")
				cancel()
				break
			}

			if sg.Game.Status != string(rules.GameStatusRunning) {
				cancel()
				break
			}
		}

		cancel()
	}
	close(frames)
}

func writeError(w http.ResponseWriter, err error, statusCode int, msg string, fields log.Fields) {
	log.WithError(err).WithFields(fields).Error(msg)
	w.WriteHeader(statusCode)
	_, err = w.Write([]byte(msg + " " + err.Error()))
	if err != nil {
		log.WithError(err).Error("Unable to write error to stream")
	}
}

func createGame(w http.ResponseWriter, r *http.Request, _ httprouter.Params, c pb.ControllerClient) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError, "Unable to read request body", log.Fields{})
		return
	}
	req := &pb.CreateRequest{}

	err = json.Unmarshal(body, req)
	if err != nil {
		writeError(w, err, http.StatusBadRequest, "Invalid JSON: ", log.Fields{})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.Create(ctx, req)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError, "Error creating game", log.Fields{})
		return
	}

	m := jsonpb.Marshaler{EmitDefaults: true}
	err = m.Marshal(w, resp)
	if err != nil {
		log.WithError(err).Error("Unable to write response to stream")
	}
}

func startGame(w http.ResponseWriter, r *http.Request, ps httprouter.Params, c pb.ControllerClient) {
	id := ps.ByName("id")
	req := &pb.StartRequest{
		ID: id,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.Start(ctx, req)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError, "Error while calling controller start", log.Fields{
			"req": req,
		})
		return
	}
	w.WriteHeader(http.StatusOK)
}

func getStatus(w http.ResponseWriter, r *http.Request, ps httprouter.Params, c pb.ControllerClient) {
	id := ps.ByName("id")
	req := &pb.StatusRequest{
		ID: id,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := c.Status(ctx, req)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError, "Error while calling controller status", log.Fields{
			"req": req,
		})
		return
	}

	if resp.LastFrame != nil {
		for _, s := range resp.LastFrame.Snakes {
			s.URL = ""
		}
	}

	m := jsonpb.Marshaler{EmitDefaults: true}
	err = m.Marshal(w, resp)
	if err != nil {
		log.WithError(err).Error("Unable to write response to stream")
	}
}

func getFrames(w http.ResponseWriter, r *http.Request, ps httprouter.Params, c pb.ControllerClient) {
	id := ps.ByName("id")
	offset, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 0) // nolint: gas, gosec
	limit, _ := strconv.ParseInt(r.URL.Query().Get("limit"), 10, 0)   // nolint: gas, gosec
	if limit == 0 {
		limit = 100
	}
	req := &pb.ListGameFramesRequest{
		ID:     id,
		Offset: int32(offset),
		Limit:  int32(limit),
	}
	// TODO: use a context with timeout
	resp, err := c.ListGameFrames(r.Context(), req)
	if err != nil {
		writeError(w, err, http.StatusInternalServerError, "Error while calling controller status", log.Fields{
			"resp": resp,
		})
		return
	}

	for _, f := range resp.Frames {
		for _, s := range f.Snakes {
			s.URL = ""
		}
	}

	m := jsonpb.Marshaler{EmitDefaults: true}

	err = m.Marshal(w, resp)

	if err != nil {
		log.WithError(err).Error("Unable to write response to stream")
	}
}

func validateSnake(w http.ResponseWriter, r *http.Request, ps httprouter.Params, c pb.ControllerClient) {
	queryValues := r.URL.Query()
	url := queryValues.Get("url")
	if url == "" {
		err := errors.New("url parameter not provided")
		writeError(w, err, http.StatusBadRequest, "You must provide a url parameter", nil)
	}
	m := jsonpb.Marshaler{EmitDefaults: true}
	req := &pb.ValidateSnakeRequest{
		URL: url,
	}

	resp, err := c.ValidateSnake(r.Context(), req)
	if err != nil {
		writeError(w, err, http.StatusBadRequest, "Error validating snake", nil)
	}

	err = m.Marshal(w, resp)
	if err != nil {
		log.WithError(err).Error("Unable to write response to stream")
	}
}

// WaitForExit starts up the server and blocks until the server shuts down.
func (s *Server) WaitForExit() error { return s.hs.ListenAndServe() }
