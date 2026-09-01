// Command pulse-server is the Pulse broker daemon (docs/Architecture.md).
package main

import (
	"fmt"
	"os"

	"github.com/pulse-stream/pulse/internal/infrastructure/config"
	"github.com/pulse-stream/pulse/internal/server"
	"github.com/spf13/cobra"
)

func main() {
	var (
		configPath string
		showVer    bool
	)
	root := &cobra.Command{
		Use:   "pulse-server",
		Short: "Pulse event streaming broker",
		RunE: func(_ *cobra.Command, _ []string) error {
			if showVer {
				fmt.Println(server.Version)
				return nil
			}
			cfg, err := config.Load(configPath)
			if err != nil {
				return err
			}
			return server.Run(cfg)
		},
	}
	root.Flags().StringVar(&configPath, "config", "", "path to YAML config file")
	root.Flags().BoolVar(&showVer, "version", false, "print version and exit")
	root.AddCommand(newHealthcheckCmd())
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
