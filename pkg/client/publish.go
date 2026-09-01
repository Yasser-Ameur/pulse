package client

import (
	"context"
	"time"

	"github.com/pulse-stream/pulse/pkg/api/pulse/v1/pulsepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// publishInitialBackoff, publishMaxBackoff and publishMaxAttempts bound
// Publish's retry loop: the broker returns Unavailable while draining, and a
// short bounded backoff is enough to ride that out without stalling a
// producer indefinitely.
const (
	publishInitialBackoff = 50 * time.Millisecond
	publishMaxBackoff     = 2 * time.Second
	publishMaxAttempts    = 5
)

// Publish appends messages to a topic partition and returns one offset per
// message, aligned by index. It retries automatically on codes.Unavailable
// (the broker returns it while draining) with exponential backoff, up to
// publishMaxAttempts attempts; any other error returns immediately.
func (c *Client) Publish(ctx context.Context, topic string, partition int32, msgs ...Message) ([]int64, error) {
	pbMsgs := make([]*pulsepb.Message, len(msgs))
	for i, m := range msgs {
		pbMsgs[i] = toPBMessage(m)
	}
	req := &pulsepb.PublishRequest{Topic: topic, Partition: partition, Messages: pbMsgs}

	backoff := publishInitialBackoff
	var lastErr error
	for attempt := 1; attempt <= publishMaxAttempts; attempt++ {
		callCtx, cancel := c.unaryCtx(ctx)
		resp, err := c.pubsub.Publish(callCtx, req)
		cancel()
		if err == nil {
			return resp.Offsets, nil
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable || attempt == publishMaxAttempts {
			break
		}
		if !sleepBackoff(ctx, backoff) {
			return nil, ctx.Err()
		}
		backoff = nextBackoff(backoff)
	}
	return nil, wrapErr(lastErr)
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
