package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/pulse-stream/pulse/internal/domain/topic"
	"github.com/pulse-stream/pulse/pkg/client"
	"github.com/spf13/cobra"
)

func newPublishCmd(opts *Options) *cobra.Command {
	var (
		partitionID int
		key         string
		contentType string
		payload     string
	)
	cmd := &cobra.Command{
		Use:   "publish TOPIC",
		Short: "Publish messages to a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := unaryContext(cmd)
			defer cancel()
			c, err := connect(opts, ctx)
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			name, err := topic.NewName(args[0])
			if err != nil {
				return err
			}
			if key != "" && !cmd.Flags().Changed("partition") {
				topics, err := c.ListTopics(ctx)
				if err != nil {
					return err
				}
				for _, t := range topics {
					if t.Name == name.String() {
						partitionID = int(client.PartitionForKey(key, t.Config.Partitions))
						break
					}
				}
			}
			body := payload
			if payload == "" && !isStdinEmpty() {
				data, err := io.ReadAll(os.Stdin)
				if err != nil {
					return err
				}
				body = string(data)
			}
			offsets, err := c.Publish(ctx, name.String(), int32(partitionID), client.Message{
				Key:         key,
				Payload:     []byte(body),
				ContentType: contentType,
			})
			if err != nil {
				return err
			}
			fmt.Printf("published offset %d\n", offsets[0])
			return nil
		},
	}
	cmd.Flags().IntVar(&partitionID, "partition", 0, "target partition (default: routed from --key via PartitionForKey, or 0 if --key is also unset)")
	cmd.Flags().StringVarP(&key, "key", "k", "", "message key")
	cmd.Flags().StringVar(&contentType, "content-type", "", "message content type")
	cmd.Flags().StringVarP(&payload, "message", "m", "", "message payload (or stdin)")
	return cmd
}

func newSubscribeCmd(opts *Options) *cobra.Command {
	var (
		partitionID int
		consumerID  string
		from        int64
		follow      bool
		forever     bool
	)
	cmd := &cobra.Command{
		Use:   "subscribe TOPIC",
		Short: "Read records from a topic",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := connect(opts, cmd.Context())
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			name, err := topic.NewName(args[0])
			if err != nil {
				return err
			}
			subOpts := client.SubscribeOptions{
				Consumer: consumerID,
				Follow:   follow,
			}
			if cmd.Flags().Changed("from") {
				subOpts.StartOffset = &from
			}
			ctx := cmd.Context()
			if forever {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
			}
			emit := func(r client.Record) error {
				fmt.Printf("%d\t%s\t%s\n", r.Offset, r.Timestamp.Format(time.RFC3339), r.Message.Payload)
				return nil
			}
			return c.Subscribe(ctx, name.String(), int32(partitionID), subOpts, emit)
		},
	}
	cmd.Flags().IntVar(&partitionID, "partition", 0, "partition to read")
	cmd.Flags().StringVar(&consumerID, "consumer", "", "consumer id (for cursors)")
	cmd.Flags().Int64Var(&from, "from", 0, "start offset")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "tail the partition")
	cmd.Flags().BoolVar(&forever, "forever", false, "keep reading until interrupted")
	return cmd
}

func newAckCmd(opts *Options) *cobra.Command {
	var (
		partitionID int
		consumerID  string
		at          int64
	)
	cmd := &cobra.Command{
		Use:   "ack TOPIC",
		Short: "Advance a consumer cursor",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := unaryContext(cmd)
			defer cancel()
			c, err := connect(opts, ctx)
			if err != nil {
				return err
			}
			defer func() { _ = c.Close() }()
			name, err := topic.NewName(args[0])
			if err != nil {
				return err
			}
			cursor, err := c.Ack(ctx, consumerID, name.String(), int32(partitionID), at)
			if err != nil {
				return err
			}
			fmt.Printf("cursor now %d\n", cursor)
			return nil
		},
	}
	cmd.Flags().IntVar(&partitionID, "partition", 0, "partition")
	cmd.Flags().StringVar(&consumerID, "consumer", "", "consumer id")
	cmd.Flags().Int64Var(&at, "offset", 0, "next offset to consume (one past the last processed)")
	_ = cmd.MarkFlagRequired("consumer")
	_ = cmd.MarkFlagRequired("offset")
	return cmd
}

func isStdinEmpty() bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		return true
	}
	return info.Mode()&os.ModeCharDevice != 0
}
