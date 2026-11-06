package render_test

import (
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/pacific"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestCompactDropsWhatTheLineAlreadyCarries(t *testing.T) {
	at := time.Date(2026, 11, 2, 21, 5, 0, 0, pacific.Zone)
	r := store.Record{
		Seq: 308532, Drive: "d", Lane: "progress-desk-2", Kind: "evidence", At: at, Re: 308500, Resolves: 308400, AlsoResolves: []int64{308401},
		To:   []string{"root", "drive-watchdog@d"},
		Text: "9:05 PM progress-desk-2: It waits on the change below it.  Owner: devbox-explore-7.",
		Refs: store.Refs{PRs: []store.PR{{Repo: "Forge-AI/monorepo", Number: 47663}, {Number: 12}}, Path: "/home/me/.cc-inbox/blobs/ab.txt"},
	}
	want := "308532 evidence progress-desk-2>root,drive-watchdog ^308500 x308400 x308401 monorepo#47663 #12 + It waits on the change below it. Owner: devbox-explore-7."
	if got := render.Compact(r, 0); got != want {
		t.Errorf("Compact = %q\nwant      %q", got, want)
	}
	if got := render.Compact(r, 20); !strings.HasSuffix(got, " It waits on the cha…") {
		t.Errorf("Compact width 20 = %q, want the text clipped to 20 characters", got)
	}
	w := render.Wire{Now: at}
	first, second := w.Line(r, 0), w.Line(r, 0)
	if !strings.HasPrefix(first, "@9:05 PM\n308532 ") || strings.HasPrefix(second, "@") {
		t.Errorf("Wire lines = %q, %q; want one minute header before the first", first, second)
	}
}
