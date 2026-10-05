package inbox_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestResolvesClosesDefectsAndBlockedStacks(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	stack := []string{"api/plat-usw2-prod"}
	defect := testutil.Post(t, st, store.Record{Lane: "walker", Kind: kinds.Defect, Text: "wave cycle blocks every held release", Refs: store.Refs{Stacks: stack}})
	blocked := testutil.Post(t, st, store.Record{Lane: "walker", Kind: kinds.Blocked, Text: "Platy cannot deploy api", Refs: store.Refs{Stacks: stack, Targets: []string{"api"}}, Re: defect.Seq})
	testutil.Post(t, st, store.Record{Lane: "other", Kind: kinds.State, Text: "unrelated", Refs: store.Refs{Stacks: []string{"web/plat-usw2-prod"}}})
	fix := testutil.Post(t, st, store.Record{Lane: "wave-fix", Kind: kinds.Opened, Text: "break the cycle", Refs: store.Refs{PRs: []int{30440}, Stacks: stack}, Resolves: defect.Seq})

	open, err := st.OpenItems(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 || open[0].Seq != blocked.Seq {
		t.Fatalf("open = %+v, want only the blocked record", open)
	}
	about, err := st.Query(ctx, store.Filter{Drive: "d", Stack: "api/plat-usw2-prod"})
	if err != nil {
		t.Fatal(err)
	}
	if err := inbox.Annotate(ctx, st, about); err != nil {
		t.Fatal(err)
	}
	got := make([]string, 0, len(about))
	for _, r := range about {
		got = append(got, fmt.Sprintf("%s:%s", r.Kind, r.Status))
	}
	if strings.Join(got, " ") != "defect:closed blocked:open opened:" {
		t.Fatalf("stack records = %v", got)
	}
	var out bytes.Buffer
	if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d", Stack: "api/plat-usw2-prod"}}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), fmt.Sprintf("wave cycle blocks every held release stack:api/plat-usw2-prod [RE #%d] [RESOLVED #%d]", blocked.Seq, fix.Seq)) {
		t.Fatalf("tail = %s", out.String())
	}
	unblocked := testutil.Post(t, st, store.Record{Lane: "walker", Kind: kinds.Unblock, Re: blocked.Seq, Text: "api deploys again", Refs: store.Refs{Stacks: stack}})
	if open, err = st.OpenItems(ctx, "d"); err != nil || len(open) != 0 {
		t.Fatalf("after unblock #%d open = %+v, %v", unblocked.Seq, open, err)
	}
}

func TestResolvesMustNameAnExistingRecord(t *testing.T) {
	st, _ := testutil.Store(t)
	if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "l", Kind: kinds.Go, Text: "x", Resolves: 99}, 0); err == nil || !strings.Contains(err.Error(), "names no record") {
		t.Fatalf("resolves to a missing record: %v", err)
	}
	other := testutil.Post(t, st, store.Record{Drive: "other", Kind: kinds.Defect, Text: "elsewhere"})
	if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "l", Kind: kinds.Go, Text: "x", Resolves: other.Seq}, 0); err == nil {
		t.Fatal("resolves across drives was accepted")
	}
	v, err := inbox.Digest(context.Background(), st, "other", st.Now().Add(-time.Hour))
	if err != nil || len(v.Defects) != 1 {
		t.Fatalf("digest defects = %+v, %v", v.Defects, err)
	}
}

func TestStackRefsAreValidated(t *testing.T) {
	st, _ := testutil.Store(t)
	for _, bad := range []string{"api", "api/", "/plat", "a/b/c"} {
		if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "l", Kind: kinds.Note, Text: "x", Refs: store.Refs{Stacks: []string{bad}}}, 0); err == nil || !strings.Contains(err.Error(), "is not <project>/<env>") {
			t.Errorf("stack %q: err = %v", bad, err)
		}
	}
	if _, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "l", Kind: kinds.Applied, Text: "x", Fields: store.Fields{Mode: "robot"}}, 0); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Errorf("bad mode accepted: %v", err)
	}
}
