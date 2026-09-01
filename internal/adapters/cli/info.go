package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newInfoCmd(opts *Options) *cobra.Command {
	return &cobra.Command{
		Use:   "info",
		Short: "Show broker identity and lifecycle state",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := unaryContext(cmd)
			defer cancel()
			c, err := connect(opts, ctx)
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			b, err := c.BrokerInfo(ctx)
			if err != nil {
				return err
			}
			topics, err := c.ListTopics(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("cluster: %s\n", b.ClusterID)
			fmt.Printf("broker:  %s\n", b.BrokerID)
			fmt.Printf("node:    %s\n", b.NodeID)
			fmt.Printf("version: %s\n", b.Version)
			fmt.Printf("state:   %s\n", b.State)
			fmt.Printf("topics:  %d\n", len(topics))
			return nil
		},
	}
}
