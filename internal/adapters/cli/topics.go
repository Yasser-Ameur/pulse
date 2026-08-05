package cli

import (
	"fmt"

	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/spf13/cobra"
)

func newTopicsCmd(opts *Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "topics",
		Short: "Administer topics",
	}
	cmd.AddCommand(
		newTopicsCreateCmd(opts),
		newTopicsListCmd(opts),
		newTopicsDeleteCmd(opts),
	)
	return cmd
}

func newTopicsCreateCmd(opts *Options) *cobra.Command {
	var partitions int
	cmd := &cobra.Command{
		Use:   "create NAME",
		Short: "Create a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := connect(opts, cmd.Context())
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			t, err := c.CreateTopic(cmd.Context(), args[0], topic.DefaultConfig(), partitions)
			if err != nil {
				return err
			}
			fmt.Printf("created topic %s (%d partitions)\n", t.Name, t.Partitions)
			return nil
		},
	}
	cmd.Flags().IntVarP(&partitions, "partitions", "p", 1, "partition count")
	return cmd
}

func newTopicsListCmd(opts *Options) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List topics",
		RunE: func(cmd *cobra.Command, _ []string) error {
			c, err := connect(opts, cmd.Context())
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			topics, err := c.ListTopics(cmd.Context())
			if err != nil {
				return err
			}
			for _, t := range topics {
				fmt.Printf("%s\tpartitions=%d\n", t.Name, t.Partitions)
			}
			return nil
		},
	}
}

func newTopicsDeleteCmd(opts *Options) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a topic and its data",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := connect(opts, cmd.Context())
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			if err := c.DeleteTopic(cmd.Context(), args[0]); err != nil {
				return err
			}
			fmt.Printf("deleted topic %s\n", args[0])
			return nil
		},
	}
}
