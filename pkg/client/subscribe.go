package client

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Subscribe streams records for topic/partition, invoking fn per record.
// With SubscribeOptions.Follow false it replays to the current end of log and
// returns; with Follow true it streams until ctx is canceled, transparently
// redialing the stream on codes.Unavailable or a transport error and
// resuming from the offset one past the last record it delivered (or the
// original start, if it delivered nothing yet). Any other error, or fn
// itself returning an error, ends Subscribe immediately.
func (c *Client) Subscribe(ctx context.Context, topic string, partition int32, opts SubscribeOptions, fn func(Record) error) error {
	start := opts.StartOffset
	backoff := publishInitialBackoff
	for {
		req := &pulsepb.SubscribeRequest{
			Consumer:  opts.Consumer,
			Topic:     topic,
			Partition: partition,
			Follow:    opts.Follow,
		}
		if start != nil {
			v := *start
			req.StartOffset = &v
		}

		stream, err := c.pubsub.Subscribe(ctx, req)
		if err == nil {
			err = recvLoop(stream, topic, fn, &start, &backoff)
		}
		if err == nil {
			return nil
		}
		if opts.Follow && shouldResume(err) {
			if !sleepBackoff(ctx, backoff) {
				return ctx.Err()
			}
			backoff = nextBackoff(backoff)
			continue
		}
		return wrapErr(err)
	}
}

// recvLoop drains one Subscribe stream, advancing *start past every
// delivered record and resetting *backoff on the first successful receive so
// a later disconnect starts its own backoff from scratch.
func recvLoop(stream pulsepb.PubSub_SubscribeClient, topic string, fn func(Record) error, start **int64, backoff *time.Duration) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, pb := range resp.Records {
			rec := fromPBRecord(topic, pb)
			if err := fn(rec); err != nil {
				return err
			}
			next := rec.Offset + 1
			*start = &next
			*backoff = publishInitialBackoff
		}
	}
}

// shouldResume reports whether err is transient and worth redialing for: the
// broker's own draining signal (codes.Unavailable), or a non-status
// transport failure (e.g. a dropped connection) that never reached the
// broker's gRPC status layer. context cancellation is never transient.
func shouldResume(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if status.Code(err) == codes.Unavailable {
		return true
	}
	_, isStatus := status.FromError(err)
	return !isStatus
}
