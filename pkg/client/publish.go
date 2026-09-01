package client

import (
	"context"
	"time"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// publishInitialBackoff and publishMaxBackoff shape Publish's retry loop: the
// broker returns Unavailable while draining or restarting, and the loop keeps
// trying until the caller's deadline, so the caller's context is the budget.
const (
	publishInitialBackoff = 50 * time.Millisecond
	publishMaxBackoff     = 2 * time.Second
)

// Publish appends messages to a topic partition and returns one offset per
// message, aligned by index. It retries automatically on codes.Unavailable
// (the broker returns it while draining) with exponential backoff until ctx
// is done; a ctx without a deadline is bounded by the call timeout. Any other
// error returns immediately. When the budget runs out the last Unavailable
// error is returned, so errors.Is(err, ErrUnavailable) holds.
func (c *Client) Publish(ctx context.Context, topic string, partition int32, msgs ...Message) ([]int64, error) {
	pbMsgs := make([]*pulsepb.Message, len(msgs))
	for i, m := range msgs {
		pbMsgs[i] = toPBMessage(m)
	}
	req := &pulsepb.PublishRequest{Topic: topic, Partition: partition, Messages: pbMsgs}

	opCtx, cancel := c.unaryCtx(ctx)
	defer cancel()
	backoff := publishInitialBackoff
	for {
		resp, err := c.pubsub.Publish(opCtx, req)
		if err == nil {
			return resp.Offsets, nil
		}
		if status.Code(err) != codes.Unavailable || !sleepBackoff(opCtx, backoff) {
			return nil, wrapErr(err)
		}
		backoff = nextBackoff(backoff)
	}
}

// sleepBackoff waits for d or until ctx is done, reporting which happened.
func sleepBackoff(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func nextBackoff(d time.Duration) time.Duration {
	d *= 2
	if d > publishMaxBackoff {
		return publishMaxBackoff
	}
	return d
}
