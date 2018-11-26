package controller_test

import (
	"testing"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/testsuite"
)

func TestInMemSuite(t *testing.T) {
	testsuite.Suite(t, controller.InMemStore(), func() {})
}
