package cli_test

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/peers"
)

func withoutResolved(out string) string {
	var kept []string
	for _, line := range strings.SplitAfter(out, "\n") {
		if !strings.HasPrefix(line, "cci: resolved ") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

func TestAResolvePrintsWhatItClosedAndRefusesARecordThatOpensNothing(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
	for _, name := range peers.IdentityEnv {
		t.Setenv(name, "")
	}
	long := "Which release does the wasm seam land in, the one that carries the dashboard publish or the next? A or B"
	for _, args := range [][]string{
		{"--lane", "lane-a", "--kind", "ask", "--to", "root", "--text", long},
		{"--lane", "lane-a", "--kind", "decide", "--to", "root", "--text", "ship it?"},
		{"--lane", "lane-a", "--kind", "defect", "--to", "root", "--text", "stale page"},
		{"--lane", "lane-a", "--kind", "blocker", "--to", "root", "--text", "no token"},
		{"--lane", "lane-a", "--kind", "incident", "--to", "root", "--text", "queue down"},
		{"--lane", "lane-a", "--kind", "answer", "--to", "root", "--topic", "t", "--text", "an answer"},
		{"--lane", "lane-a", "--kind", "note", "--to", "root", "--text", "a note"},
	} {
		if _, err := run(t, append([]string{"post", "--drive", "d"}, args...)...); err != nil {
			t.Fatal(err)
		}
	}
	out, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "answer", "--resolves", "1", "--to", "lane-a", "--text", "A")
	if err != nil || out != "#8\ncci: resolved #1 ask lane-a: "+string([]rune(long)[:79])+"…\n" {
		t.Fatalf("resolve the ask = %q, %v; want its name, kind, lane and first 80 characters", out, err)
	}
	for seq, kind := range map[string]string{"2": "decide", "3": "defect", "4": "blocker", "5": "incident"} {
		closer := "answer"
		if kind == "defect" || kind == "incident" {
			closer = "done"
		}
		if out, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", closer, "--resolves", seq, "--to", "lane-a", "--text", "closed "+kind); err != nil || !strings.Contains(out, "cci: resolved #"+seq+" "+kind+" lane-a: ") {
			t.Fatalf("resolve the %s = %q, %v; want it closed and named", kind, out, err)
		}
	}
	for seq, kind := range map[string]string{"6": "answer", "7": "note"} {
		_, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "answer", "--resolves", seq, "--to", "lane-a", "--text", "meant another "+kind)
		if err == nil || !strings.Contains(err.Error(), "--resolves #"+seq+": it is a "+kind+" from lane-a, which opens nothing to resolve") {
			t.Fatalf("resolve the %s = %v; want it refused", kind, err)
		}
		if stored, err := run(t, "grep", "--drive", "d", "-F", "meant another "+kind); err != nil || strings.Contains(stored, "meant another") {
			t.Fatalf("grep after the refusal = %q, %v; want nothing written", stored, err)
		}
	}
}
