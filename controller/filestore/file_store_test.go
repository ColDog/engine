package filestore

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/battlesnakeio/engine/controller/testsuite"
	"github.com/battlesnakeio/engine/rules"
	"github.com/stretchr/testify/require"
)

func TestSuite(t *testing.T) {
	t.Skip("TODO: Figure out why testsuite doesn't work here")
	testsuite.Suite(t, NewFileStore(""), func() {})
}

func TestFileStore(t *testing.T) {
	fs, w := testFileStore()
	frames := []*pb.GameFrame{basicFrames()[0]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NoError(t, err)

	game, err := fs.GetGame(context.Background(), "myid")
	require.NoError(t, err)
	require.Equal(t, basicGame(), game)

	err = fs.PushGameFrame(context.Background(), "myid", basicFrames()[1])
	require.NoError(t, err)

	newFrames, err := fs.ListGameFrames(context.Background(), "myid", 5, 0)
	require.NoError(t, err)
	require.Len(t, newFrames, 2)
	require.Equal(t, basicFrames(), newFrames)

	err = fs.SetGameStatus(context.Background(), "myid", rules.GameStatusComplete)
	require.NoError(t, err)
	require.True(t, w.closed)
}

func TestCreateGameHandlesWriteError(t *testing.T) {
	fs, w := testFileStore()
	w.err = errors.New("fail")
	frames := []*pb.GameFrame{basicFrames()[0]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NotNil(t, err)
}

func TestCreateGameHandlesOpenFileError(t *testing.T) {
	openFileWriter = func(dir string, id string, _ bool) (writer, error) {
		return nil, errors.New("fail")
	}
	openFileReader = func(dir string, id string) (reader, error) {
		return nil, errors.New("fail")
	}
	fs := NewFileStore("")
	frames := []*pb.GameFrame{basicFrames()[0]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NotNil(t, err)
}

func TestCreateGetGameFound(t *testing.T) {
	fs, _ := testFileStore()

	_, err := fs.GetGame(context.Background(), "notfound")
	require.NotNil(t, err)
}

func TestPushGameFrameInvalidGame(t *testing.T) {
	fs, _ := testFileStore()

	err := fs.PushGameFrame(context.Background(), "notfound", basicFrames()[1])
	require.NotNil(t, err)
}

func TestListGameFramesInvalidGame(t *testing.T) {
	fs, _ := testFileStore()

	_, err := fs.ListGameFrames(context.Background(), "notfound", 5, 0)
	require.NotNil(t, err)
}

func TestListGameFramesNegativeOffset(t *testing.T) {
	fs, _ := testFileStore()
	frames := []*pb.GameFrame{basicFrames()[0], basicFrames()[1]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NoError(t, err)

	newFrames, err := fs.ListGameFrames(context.Background(), "myid", 5, -1)
	require.NoError(t, err)
	require.Len(t, newFrames, 1)
	require.Equal(t, frames[1], newFrames[0])
}

func TestListGameFramesOutOfRange(t *testing.T) {
	fs, _ := testFileStore()
	frames := []*pb.GameFrame{basicFrames()[0], basicFrames()[1]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NoError(t, err)

	newFrames, err := fs.ListGameFrames(context.Background(), "myid", 5, 3)
	require.NoError(t, err)
	require.Nil(t, newFrames)
}

func TestListGameFramesEmpty(t *testing.T) {
	fs, _ := testFileStore()
	frames := []*pb.GameFrame{}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NoError(t, err)

	newFrames, err := fs.ListGameFrames(context.Background(), "myid", 5, 0)
	require.NoError(t, err)
	require.Nil(t, newFrames)
}

func TestSetGameStatusInvalidGame(t *testing.T) {
	fs, _ := testFileStore()

	err := fs.SetGameStatus(context.Background(), "notfound", rules.GameStatusComplete)
	require.NotNil(t, err)
}

func TestLockUnlock(t *testing.T) {
	fs, _ := testFileStore()
	token, err := fs.Lock(context.Background(), "asdf", "", 1*time.Second)
	require.NoError(t, err)
	token, err = fs.Lock(context.Background(), "asdf", token, 1*time.Second)
	require.NoError(t, err)
	err = fs.Unlock(context.Background(), "asdf", token)
	require.NoError(t, err)
}

func TestLockExpired(t *testing.T) {
	fs, _ := testFileStore()
	token, err := fs.Lock(context.Background(), "asdf", "", -1*time.Second)
	require.NoError(t, err)
	_, err = fs.Lock(context.Background(), "asdf", token, -1*time.Second)
	require.NoError(t, err)
}

func TestLockBadToken(t *testing.T) {
	fs, _ := testFileStore()
	_, err := fs.Lock(context.Background(), "asdf", "", 1*time.Second)
	require.NoError(t, err)
	_, err = fs.Lock(context.Background(), "asdf", "badtoken", 1*time.Second)
	require.NotNil(t, err)
}

func TestUnlockNothing(t *testing.T) {
	fs, _ := testFileStore()
	err := fs.Unlock(context.Background(), "wqer", "aaa")
	require.NoError(t, err)
}

func TestUnlockBadToken(t *testing.T) {
	fs, _ := testFileStore()
	_, err := fs.Lock(context.Background(), "aaa", "", 1*time.Second)
	require.NoError(t, err)
	err = fs.Unlock(context.Background(), "aaa", "wrong")
	require.NotNil(t, err)
}

func TestPopGameID(t *testing.T) {
	fs, _ := testFileStore()
	frames := []*pb.GameFrame{basicFrames()[0]}
	err := fs.CreateGame(context.Background(), basicGame(), frames)
	require.NoError(t, err)
	_, err = fs.PopGameID(context.Background())
	require.NoError(t, err)
}

func TestPopGameIDNotFound(t *testing.T) {
	fs, _ := testFileStore()
	_, err := fs.PopGameID(context.Background())
	require.NotNil(t, err)
}

func testFileStore() (controller.Store, *mockWriter) {
	w := &mockWriter{
		closed: false,
	}
	openFileWriter = func(dir string, id string, _ bool) (writer, error) {
		return w, nil
	}
	openFileReader = func(dir string, id string) (reader, error) {
		return newMockReader(w.text), nil
	}
	return NewFileStore(""), w
}
