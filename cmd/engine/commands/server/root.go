package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	controllerAddr = "127.0.0.1:3004"
	promEnable     = true
	promListen     = ":9000"
)

// RootCmd provides the root run command.
var RootCmd = &cobra.Command{
	Use:    "server",
	Short:  "serve the battlesnake game engine",
	PreRun: func(c *cobra.Command, args []string) { prometheus() },
	Run: func(c *cobra.Command, args []string) {
		go controllerCmd.Run(c, args)
		go apiCmd.Run(c, args)
		workerCmd.Run(c, args)
	},
}

// Execute runs the root command,
func init() {
	RootCmd.Flags().BoolVar(&promEnable, "prometheus", promEnable, "enable prometheus metrics")
	RootCmd.Flags().StringVar(&promListen, "prometheus-listen", promListen, "prometheus http endpoint")

	RootCmd.AddCommand(apiCmd)
	RootCmd.AddCommand(controllerCmd)
	RootCmd.AddCommand(workerCmd)
}

func prometheus() {
	if !promEnable {
		log.Info("prometheus exporter not enabled")
		return
	}

	log.WithField("addr", promListen).Info("starting prometheus exporter")
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		r := http.NewServeMux()
		r.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(promListen, r); err != nil {
			log.WithError(err).Warn("prometheus failes to listen")
		}
	}()
}
