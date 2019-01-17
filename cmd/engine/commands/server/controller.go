package server

import (
	"io"
	"os"

	"github.com/battlesnakeio/engine/controller/sqlstore"

	"github.com/battlesnakeio/engine/controller"
	"github.com/battlesnakeio/engine/controller/filestore"
	"github.com/battlesnakeio/engine/controller/redis"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	controllerListen      = ":3004"
	controllerBackend     = "inmem"
	controllerBackendArgs = ""
)

func init() {
	controllerCmd.Flags().StringVarP(&controllerListen, "listen", "l", controllerListen, "address for the controller to bind to")
	controllerCmd.Flags().StringVarP(&controllerBackend, "backend", "b", controllerBackend, "controller backend, as one of: [inmem, file, redis, sql]")
	controllerCmd.Flags().StringVarP(&controllerBackendArgs, "backend-args", "a", controllerBackendArgs, "options to pass to the backend being used")
	RootCmd.Flags().AddFlagSet(controllerCmd.Flags())
}

var controllerCmd = &cobra.Command{
	Use:    "controller",
	Short:  "runs the engine controller",
	PreRun: func(c *cobra.Command, args []string) { prometheus() },
	Run: func(c *cobra.Command, args []string) {
		var store controller.Store
		var err error
		switch controllerBackend {
		case "inmem":
			store = controller.InMemStore()
		case "file":
			store = filestore.NewFileStore(controllerBackendArgs)
		case "redis":
			store, err = redis.NewStore(controllerBackendArgs)
		case "sql":
			store, err = sqlstore.NewSQLStore(controllerBackendArgs)
		default:
			log.WithField("backend", controllerBackend).Fatal("invalid backend")
		}

		if c, ok := store.(io.Closer); ok {
			defer func() {
				err = c.Close()
				if err != nil {
					log.WithError(err).Error("unable to close store")
				}
			}()
		}

		if err != nil {
			log.WithError(err).Error("unable to start up backend store")
			os.Exit(1)
		}

		ctrl := controller.New(store)
		log.WithField("listen", controllerListen).
			Info("Battlesnake controller serving")
		if err := ctrl.Serve(controllerListen); err != nil {
			log.WithError(err).
				WithField("listen", controllerListen).
				Fatal("Controller failed to serve")
		}
	},
}
