package cli_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestTailOpenListsTheAsksTheBoundLaneOwes(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "")
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.BindLane(ctx, store.PeerKey{Session: "session-1"}, "d", "api", time.Now()); err != nil {
		t.Fatal(err)
	}
	long := "CI is red on main " + strings.Repeat("and every rerun fails the same way ", 4)
	posts := []struct{ kind, to, text string }{
		{"ask", "api", "which branch?"},
		{"ask", "api", "answered already?"},
		{"ask", "web,api", "copied only?"},
		{"decide", "api", "ship it"},
		{"blocker", "api", long},
		{"ask", "web", "for web?"},
	}
	seqs := make([]string, 0, len(posts))
	for _, p := range posts {
		out, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", p.kind, "--to", p.to, "--text", p.text)
		if err != nil {
			t.Fatal(err)
		}
		posted := strings.Split(strings.TrimSpace(out), "\n")
		seqs = append(seqs, strings.Fields(posted[len(posted)-1])[0])
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "api", "--kind", "answer", "--re", strings.TrimPrefix(seqs[1], "#"), "--text", "main"); err != nil {
		t.Fatal(err)
	}
	listed := func(args ...string) []string {
		t.Helper()
		out, err := run(t, append([]string{"tail", "--open"}, args...)...)
		if err != nil {
			t.Fatalf("tail --open %v = %q, %v", args, out, err)
		}
		return strings.Split(strings.TrimSpace(out), "\n")
	}
	lines := listed()
	if len(lines) != 3 || lines[0] != "open asks addressed to api (2, newest first):" ||
		!strings.HasPrefix(lines[1], seqs[4]+" ") || !strings.Contains(lines[1], "BLOCKER root -> api CI is red on main") || !strings.HasSuffix(lines[1], "(open 0m)") ||
		!strings.HasPrefix(lines[2], seqs[0]+" ") || !strings.Contains(lines[2], "which branch?") {
		t.Fatalf("tail --open = %q; want the open blocker, then the open ask, and not the answered ask, the copy, the statement decide or web's ask", lines)
	}
	if lines := listed("--kind", "ask"); len(lines) != 2 || !strings.Contains(lines[1], "which branch?") {
		t.Fatalf("tail --open --kind ask = %q", lines)
	}
	if lines := listed("--to", "web"); len(lines) != 3 || lines[0] != "open asks addressed to web (2, newest first):" || !strings.Contains(lines[1], "for web?") || !strings.Contains(lines[2], "copied only?") {
		t.Fatalf("tail --open --to web = %q; want web's ask, then the one that names web first", lines)
	}
	if lines := listed("--budget", "200"); len(lines) != 2 || lines[1] != "... 2 older open asks; raise --budget" {
		t.Fatalf("tail --open --budget 200 = %q; want the header and a trailer counting both", lines)
	}
	if out, err := run(t, "tail", "--open", "--mail", "--lane", "api"); err == nil || !strings.Contains(err.Error(), "--open does not support --lane, --mail") {
		t.Fatalf("tail --open --mail --lane = %q, %v; want both refused", out, err)
	}
	if out, err := run(t, "tail", "--open", "--drive", "other"); err == nil || !strings.Contains(err.Error(), "--open needs --to <lane>") {
		t.Fatalf("tail --open on a drive this thread is not bound to = %q, %v", out, err)
	}
}

func TestALanesWaitSkipsWhatASeedBackfilledAfterItBoundButItsOpenAsksCountIt(t *testing.T) {
	ctx := context.Background()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "session-1")
	t.Setenv("CCI_AGENT_ID", "")
	st, err := store.Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.BindLane(ctx, store.PeerKey{Session: "session-1"}, "d", "api", time.Now()); err != nil {
		t.Fatal(err)
	}
	backfilled := time.Now().Add(-time.Hour)
	if _, err := st.Backfill(ctx, []store.Row{
		{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: "ask", At: backfilled, Text: "a backfilled ask", To: []string{"api"}, Source: "post"},
		{Origin: "0123456789abcdef", OriginSeq: 2, Drive: "d", Lane: "root", Kind: "go", At: backfilled, Text: "a backfilled go", To: []string{"api"}, Source: "post"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(ctx, []store.Row{
		{Origin: "0123456789abcdef", OriginSeq: 3, Drive: "d", Lane: "root", Kind: "ask", At: time.Now().Add(-10 * time.Minute), Text: "asked while the lane spawned", To: []string{"api"}, Source: "post"},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "ask", "--to", "api", "--text", "asked since the lane started"); err != nil {
		t.Fatal(err)
	}
	out, err := run(t, "tail", "--drive", "d", "--to", "api", "--cursor", "api-wake", "--seed", "api", "--mail")
	if err != nil || !strings.Contains(out, "asked since the lane started") || !strings.Contains(out, "asked while the lane spawned") || strings.Contains(out, "backfilled") {
		t.Fatalf("the lane's wait = %q, %v; want both asks and nothing a seed backfilled after the lane bound", out, err)
	}
	if out, err := run(t, "tail", "--open"); err != nil || !strings.Contains(out, "asked since the lane started") || !strings.Contains(out, "asked while the lane spawned") || !strings.Contains(out, "a backfilled ask") || strings.Contains(out, "a backfilled go") {
		t.Fatalf("tail --open = %q, %v; want all three asks, the backfilled one included, and no go", out, err)
	}
}
