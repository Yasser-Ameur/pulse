package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

func TestStateTransitions(t *testing.T) {
	cases := []struct {
		from State
		to   State
		ok   bool
	}{
		{StateStarting, StateRecovering, true},
		{StateStarting, StateStopping, true},
		{StateStarting, StateRunning, false},
		{StateRecovering, StateRunning, true},
		{StateRecovering, StateStopping, true},
		{StateRunning, StateDraining, true},
		{StateRunning, StateStopping, true},
		{StateRunning, StateRecovering, false},
		{StateDraining, StateStopping, true},
		{StateDraining, StateRunning, false},
		{StateStopping, StateStopped, true},
		{StateStopped, StateRunning, false},
		{StateStopped, StateStopping, false},
	}
	for _, c := range cases {
		if got := c.from.CanTransitionTo(c.to); got != c.ok {
			t.Errorf("CanTransitionTo(%s -> %s) = %v, want %v", c.from, c.to, got, c.ok)
		}
	}
}

func TestStateString(t *testing.T) {
	names := map[State]string{
		StateStarting:   "starting",
		StateRecovering: "recovering",
		StateRunning:    "running",
		StateDraining:   "draining",
		StateStopping:   "stopping",
		StateStopped:    "stopped",
	}
	for s, want := range names {
		if got := s.String(); got != want {
			t.Errorf("State(%d).String() = %q, want %q", s, got, want)
		}
	}
	if got := State(99).String(); got != "unknown" {
		t.Errorf("State(99).String() = %q, want %q", got, "unknown")
	}
}

func TestBrokerTransitionTo(t *testing.T) {
	b := &Broker{State: StateStarting}
	if err := b.TransitionTo(StateRecovering); err != nil {
		t.Fatalf("TransitionTo(Recovering) error = %v", err)
	}
	if b.State != StateRecovering {
		t.Fatalf("State = %v, want Recovering", b.State)
	}
	if err := b.TransitionTo(StateStopped); err == nil {
		t.Fatal("TransitionTo(Stopped) from Recovering should fail")
	}
}

func TestIDsAreULIDs(t *testing.T) {
	now := time.Unix(1700000000, 0).UTC()
	for _, id := range []string{
		string(NewClusterID(now)),
		string(NewBrokerID(now)),
	} {
		if len(id) != 26 {
			t.Errorf("id %q length = %d, want 26", id, len(id))
		}
		if _, err := ulid.Parse(strings.ToUpper(id)); err != nil {
			t.Errorf("id %q is not a valid ULID: %v", id, err)
		}
	}
}
