package partition

import "testing"

func TestIDValid(t *testing.T) {
	cases := []struct {
		id    ID
		parts int
		ok    bool
	}{
		{0, 1, true},
		{0, 3, true},
		{2, 3, true},
		{3, 3, false},
		{1, 1, false},
		{0, 0, false},
		{-1, 3, false},
	}
	for _, c := range cases {
		if got := c.id.Valid(c.parts); got != c.ok {
			t.Errorf("ID(%d).Valid(%d) = %v, want %v", c.id, c.parts, got, c.ok)
		}
	}
	if got := ID(2).Int32(); got != 2 {
		t.Errorf("Int32() = %d, want 2", got)
	}
}
