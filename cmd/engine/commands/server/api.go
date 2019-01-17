package server

import (
	log "github.com/sirupsen/logrus"

	"github.com/battlesnakeio/engine/api"
	"github.com/battlesnakeio/engine/controller/pb"
	"github.com/spf13/cobra"
)

var (
	apiListen = ":3005"
)

func init() {
	apiCmd.Flags().StringVarP(&apiListen, "listen", "l", apiListen, "api address to listen on")
	apiCmd.Flags().StringVarP(&controllerAddr, "controller-addr", "c", controllerAddr, "address of the controller")
	RootCmd.Flags().AddFlagSet(apiCmd.Flags())
}

var apiCmd = &cobra.Command{
	Use:    "api",
	Short:  "runs the engine api",
	PreRun: func(c *cobra.Command, args []string) { prometheus() },
	Run: func(c *cobra.Command, args []string) {
		client, err := pb.Dial(controllerAddr)
		if err != nil {
			log.WithError(err).
				WithField("address", controllerAddr).
				Fatal("failed to dial controller")
		}

		api := api.New(apiListen, client)
		log.WithField("listen", apiListen).Info("Battlesnake api serving")
		err = api.WaitForExit()
		if err != nil {
			log.WithError(err).
				WithField("listen", apiListen).
				Fatal("api server failed")
		}
	},
}
