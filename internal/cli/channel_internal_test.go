package cli

import (
	"slices"
	"testing"
)

func TestChannelArgsReadOnlyTheChannelFlags(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want bool
	}{
		{[]string{"claude", "--channels", "plugin:cc-review@cc-review", "plugin:cc-inbox@cc-inbox", "--resume", "s1"}, true},
		{[]string{"claude", "--channels=plugin:cc-slack@cc-slack,plugin:cc-inbox@skills"}, true},
		{[]string{"claude", "--dangerously-load-development-channels", "plugin:cc-inbox@dev"}, true},
		{[]string{"claude", "--resume", "s1"}, false},
		{[]string{"claude", "--channels", "plugin:cc-slack@cc-slack", "--model", "opus", "plugin:cc-inbox@cc-inbox"}, false},
	} {
		if got := slices.Contains(channelArgs(tt.args), "plugin:cc-inbox@"); got != tt.want {
			t.Fatalf("channelArgs(%q) carries cc-inbox = %v, want %v", tt.args, got, tt.want)
		}
	}
}
