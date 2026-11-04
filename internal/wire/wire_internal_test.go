package wire

import (
	"slices"
	"strings"
	"testing"
	"time"
)

func TestForWhatIsLeftTakesOffTheTimeAlreadyWatched(t *testing.T) {
	started := time.Now().Add(-10 * time.Minute)
	for _, tt := range []struct {
		name  string
		argv  []string
		value int
		low   time.Duration
		high  time.Duration
	}{
		{"separate value", []string{"cci", "watch", "--for", "29m", "--to", "x"}, 3, 18 * time.Minute, 19 * time.Minute},
		{"joined value", []string{"cci", "watch", "--for=29m"}, 2, 18 * time.Minute, 19 * time.Minute},
		{"already over", []string{"cci", "watch", "--for", "1m"}, 3, time.Millisecond, time.Millisecond},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := forWhatIsLeft(tt.argv, started)
			value := got[tt.value]
			if joined, ok := strings.CutPrefix(value, "--for="); ok {
				value = joined
			}
			left, err := time.ParseDuration(value)
			if err != nil {
				t.Fatalf("%v: %v", got, err)
			}
			if left < tt.low || left > tt.high {
				t.Errorf("left = %s, want between %s and %s", left, tt.low, tt.high)
			}
			if slices.Equal(got, tt.argv) {
				t.Errorf("argv unchanged: %v", got)
			}
		})
	}
	for _, argv := range [][]string{
		{"cci", "watch", "--to", "x"},
		{"cci", "watch", "--for", "soon"},
		{"cci", "watch", "--for"},
	} {
		if got := forWhatIsLeft(argv, started); !slices.Equal(got, argv) {
			t.Errorf("forWhatIsLeft(%v) = %v, want it unchanged", argv, got)
		}
	}
}

func TestABoundedWatchResumesFromACursorOfItsOwn(t *testing.T) {
	got := resumable([]string{"cci", "watch", "--to", "x", "--for", "29m"})
	if !slices.ContainsFunc(got, func(arg string) bool { return strings.HasPrefix(arg, "--cursor=") }) {
		t.Errorf("resumable = %v, want a cursor it can resume from", got)
	}
}
