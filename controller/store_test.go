package controller_test

import (
	"testing"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/testsuite"
)

func TestInMemSuite(t *testing.T) {
	s := controller.InMemStore()
	testsuite.Suite(t, s, func() { s.(interface{ Clear() }).Clear() })
}
