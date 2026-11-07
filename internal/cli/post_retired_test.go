package cli_test

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/peers"
)

func TestAPostToARetiredLaneIsRefusedNamingItsHeir(t *testing.T) {
	for _, tt := range []struct {
		name, lane, kind, to string
		setup                [][]string
		refusal              string
	}{
		{"live lane", "root", "note", "w-1", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
		}, ""},
		{"retired lane with a successor", "root", "note", "w-1", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
			{"--lane", "supervisor", "--kind", "stand-down", "--to", "w-1", "--successor", "w-2", "--text", "w-1 retired (done)"},
			{"--lane", "w-2", "--kind", "state", "--text", "w-2 up"},
		}, "post to its successor w-2, or to root"},
		{"retired lane with no successor", "root", "note", "w-1", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
			{"--lane", "supervisor", "--kind", "stand-down", "--to", "w-1", "--text", "w-1 retired (done)"},
		}, "post to root"},
		{"alias whose every lane retired", "root", "note", "w", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
			{"--lane", "supervisor", "--kind", "stand-down", "--to", "w-1", "--text", "w-1 retired (done)"},
		}, "post to root"},
		{"alias whose newest lane is live", "root", "note", "w", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
			{"--lane", "supervisor", "--kind", "stand-down", "--to", "w-1", "--successor", "w-2", "--text", "w-1 retired (done)"},
			{"--lane", "w-2", "--kind", "state", "--text", "w-2 up"},
		}, ""},
		{"owner record to a retired lane", "owner-relay", "owner", "w-1", [][]string{
			{"--lane", "w-1", "--kind", "state", "--text", "w-1 up"},
			{"--lane", "supervisor", "--kind", "stand-down", "--to", "w-1", "--text", "w-1 retired (done)"},
		}, "post to root"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
			for _, name := range peers.IdentityEnv {
				t.Setenv(name, "")
			}
			for _, args := range tt.setup {
				if _, err := run(t, append([]string{"post", "--drive", "d"}, args...)...); err != nil {
					t.Fatal(err)
				}
			}
			const words = "build the upload link on the File type"
			_, err := run(t, "post", "--drive", "d", "--lane", tt.lane, "--kind", tt.kind, "--to", tt.to, "--text", words)
			stored, readErr := run(t, "grep", "--drive", "d", "-F", words, "--to", tt.to)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if tt.refusal == "" {
				if err != nil || !strings.Contains(stored, words) {
					t.Fatalf("post --to %s = %v, stored %q; want it accepted", tt.to, err, stored)
				}
				return
			}
			if err == nil || !strings.HasPrefix(err.Error(), "REFUSED: --to "+tt.to+": w-1 stood down at ") || !strings.HasSuffix(err.Error(), tt.refusal) || strings.Contains(stored, words) {
				t.Fatalf("post --to %s = %v, stored %q; want it refused ending %q and nothing written", tt.to, err, stored, tt.refusal)
			}
			blockers, readErr := run(t, "grep", "--drive", "d", "-F", words, "--kind", "blocker", "--to", "owner-relay")
			if readErr != nil {
				t.Fatal(readErr)
			}
			if raised := strings.Contains(blockers, "REFUSED: --to "+tt.to); raised != (tt.kind == "owner") {
				t.Fatalf("owner-relay blockers = %q; want one raised only for an owner record", blockers)
			}
		})
	}
}

func TestALanePostingLiveUnderABareNameWinsItOverARetiredHolder(t *testing.T) {
	for _, desk := range []string{"supervisor", "desk-runner", "owner-relay"} {
		t.Run(desk, func(t *testing.T) {
			t.Setenv("CCI_HOME", t.TempDir())
			t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
			for _, name := range peers.IdentityEnv {
				t.Setenv(name, "")
			}
			for _, args := range [][]string{
				{"--lane", desk, "--kind", "state", "--text", desk + " up"},
				{"--lane", "mq-health-6", "--kind", "state", "--text", "an old generation that never stood down"},
				{"--lane", "mq-health-10", "--kind", "decide", "--owned-by", "mq-health-10", "--to", desk, "--text", desk + " is owned by mq-health-10"},
				{"--lane", "root", "--kind", "stand-down", "--to", desk, "--successor", desk + "-2", "--text", "stand down"},
				{"--lane", desk + "-2", "--kind", "state", "--text", desk + "-2 up"},
				{"--lane", "root", "--kind", "stand-down", "--to", desk + "-2", "--text", desk + "-2 retired (done)"},
				{"--lane", "root", "--kind", "stand-down", "--to", "mq-health-10", "--text", "mq-health-10 retired (done)"},
				{"--lane", desk, "--kind", "state", "--text", desk + " still running"},
			} {
				if _, err := run(t, append([]string{"post", "--drive", "d"}, args...)...); err != nil {
					t.Fatal(err)
				}
			}
			for _, kind := range []string{"note", "decide"} {
				out, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", kind, "--to", desk, "--text", "lr request "+kind, "--json")
				if err != nil || !strings.Contains(out, `"to":["`+desk+`"]`) {
					t.Fatalf("%s --to %s = %q, %v; want the live lane alone, with no retired successor or owner copied", kind, desk, out, err)
				}
			}
			out, err := run(t, "tail", "--drive", "d", "--since", "0", "--reader", desk)
			if err != nil || !strings.Contains(out, "lr request note") || !strings.Contains(out, "lr request decide") {
				t.Fatalf("tail --reader %s = %q, %v; want both requests delivered", desk, out, err)
			}
		})
	}
}
