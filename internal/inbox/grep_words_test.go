package inbox

import (
	"context"
	"regexp"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestRequiredWordsNeverDropAMatch(t *testing.T) {
	ctx := context.Background()
	st, _ := testutil.Store(t)
	asked := testutil.Post(t, st, store.Record{Lane: "finder-desk", Kind: kinds.Ask, Text: "REVIEW the  plan for 991d899, then close sess_01abc", Topic: "rollout-7"})
	testutil.Post(t, st, store.Record{Lane: "review-tab-1", Kind: kinds.Review, Text: "finder: ok", To: []string{"root", "desk-2"}, Re: asked.Seq})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "landed", Refs: store.Refs{PRs: []store.PR{{Repo: "yasyf/cc-inbox", Number: 49333}, {Number: 12345}}, Stacks: []string{"infra/dev"}, Targets: []string{"api"}, URL: "https://example.com/a-b.c?x=1&y=2", Path: "/tmp/file.md", CCN: "doc-c3230a2", Builds: []string{"8821"}, Board: "main"}})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Answer, Text: "resolved it", Resolves: asked.Seq})
	testutil.Post(t, st, store.Record{Lane: "worker", Kind: kinds.Note, Text: "unrelated"})
	all, err := st.QueryText(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	now := st.Now()
	for _, tt := range []struct {
		pattern string
		pruned  bool
	}{
		{"(?i)991D899", true},
		{"49333", true},
		{"12345", true},
		{"(?i)(finder-desk|root) .*plan", true},
		{"(?i)review", true},
		{"REVIEW", true},
		{"review-tab-1", true},
		{"sess_01abc", true},
		{"(?i)stack:infra/dev", true},
		{"example.com/a-b.c", true},
		{"(ok|plan) 991", false},
		{"(?i)target", false},
		{"stack:", false},
		{"Oct", false},
		{"resolves", false},
		{"#1\\b", false},
		{"pr#", false},
		{"->", false},
		{"[0-9]+ PM", false},
		{".*", false},
	} {
		t.Run(tt.pattern, func(t *testing.T) {
			pattern := regexp.MustCompile(tt.pattern)
			words := requiredWords(pattern.String())
			if tt.pruned && words == nil {
				t.Fatalf("requiredWords(%q) = nil, want words", tt.pattern)
			}
			matching := func(records []store.Record) []int64 {
				out := []int64{}
				for _, r := range records {
					if pattern.MatchString(render.Line(r, now)) {
						out = append(out, r.Seq)
					}
				}
				return out
			}
			want := matching(all)
			pruned, err := st.QueryText(ctx, store.Filter{Drive: "d", Contains: words})
			if err != nil {
				t.Fatal(err)
			}
			if got := matching(pruned); !slices.Equal(got, want) {
				t.Fatalf("pattern %q with words %v matched %v, want %v", tt.pattern, words, got, want)
			}
		})
	}
}

func TestDigestOfOpenItemsMatchesTheWholeRecordReads(t *testing.T) {
	ctx := context.Background()
	st, clock := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "older open ask", To: []string{"root"}})
	clock.Advance(48 * time.Hour)
	since := st.Now().Add(-time.Hour)
	ask := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "open ask", To: []string{"root"}})
	closed := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Ask, Text: "answered ask", To: []string{"root"}})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Answer, Text: "yes", Re: closed.Seq})
	hold := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Hold, Text: "tracked hold", Refs: store.Refs{Targets: []string{"api"}}})
	lifted := testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Hold, Text: "lifted hold", Refs: store.Refs{Stacks: []string{"p/e"}}})
	testutil.Post(t, st, store.Record{Lane: "a", Kind: kinds.Lift, Text: "lift", Resolves: lifted.Seq, Refs: store.Refs{Stacks: []string{"p/e"}}})
	open, untracked, older, err := st.OpenSince(ctx, "d", since)
	if err != nil {
		t.Fatal(err)
	}
	got := make([]int64, 0, len(open))
	for _, r := range open {
		if r.Drive != "d" || r.Text == "" {
			t.Fatalf("open record %+v is not whole", r)
		}
		got = append(got, r.Seq)
	}
	if want := []int64{ask.Seq, hold.Seq}; !slices.Equal(got, want) || len(untracked) != 0 || older != 1 {
		t.Fatalf("OpenSince = %v, untracked %v, older %d; want %v, none, 1", got, untracked, older, want)
	}
	items, err := st.OpenItems(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("OpenItems = %+v, want the older ask, the open ask and the tracked hold", items)
	}
}
