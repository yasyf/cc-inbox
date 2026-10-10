package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestClaimsExplicitAndDerived(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, "root-1", "d", true); err != nil {
		t.Fatal(err)
	}
	if err := st.BindLane(ctx, "lane-1", "d", "api", clock.Now()); err != nil {
		t.Fatal(err)
	}
	opened := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Opened, Text: "fix", Refs: store.Refs{PRs: []int{34363}, Builds: []string{"https://buildkite.com/forge/ci/builds/4860"}}})
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.State, Text: "deploying", Refs: store.Refs{Stacks: []string{"api/plat-usw2-prod"}, Targets: []string{"api"}}})
	testutil.Post(t, st, store.Record{Lane: "nobody", Kind: kinds.Opened, Text: "orphan", Refs: store.Refs{PRs: []int{1}}})
	clock.Advance(time.Minute)
	if _, err := st.Claim(ctx, store.Claim{Session: "lane-1", Rel: "watches", Kind: "pr", Key: "34363"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, store.Claim{Session: "root-1", Rel: "owns", Kind: "thread", Key: "C1/1.2"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, store.Claim{Session: "", Rel: "owns", Kind: "pr", Key: "1"}); err == nil {
		t.Fatal("Claim() accepted an empty session")
	}
	type want struct{ session, rel, kind, key, source string }
	tests := []struct {
		name string
		f    store.ClaimFilter
		want []want
	}{
		{"by pr, newest first", store.ClaimFilter{Kind: "pr", Key: "34363"}, []want{{"lane-1", "watches", "pr", "34363", "claim"}, {"root-1", "opened", "pr", "34363", "#1"}}},
		{"by build number against a URL", store.ClaimFilter{Kind: "build", Key: "4860"}, []want{{"root-1", "opened", "build", "https://buildkite.com/forge/ci/builds/4860", "#1"}}},
		{"by stack", store.ClaimFilter{Kind: "stack", Key: "api/plat-usw2-prod"}, []want{{"lane-1", "mentioned", "stack", "api/plat-usw2-prod", "#2"}}},
		{"by rel", store.ClaimFilter{Rel: "owns"}, []want{{"root-1", "owns", "thread", "C1/1.2", "claim"}}},
		{"by session", store.ClaimFilter{Sessions: []string{"lane-1"}, Kind: "target"}, []want{{"lane-1", "mentioned", "target", "api", "#2"}}},
		{"unbound lane derives nothing", store.ClaimFilter{Kind: "pr", Key: "1"}, nil},
		{"build wildcards are literal", store.ClaimFilter{Kind: "build", Key: "48_0"}, nil},
		{"build percent is literal", store.ClaimFilter{Kind: "build", Key: "%"}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := st.Claims(ctx, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("Claims() = %+v, want %+v", got, tt.want)
			}
			for i, w := range tt.want {
				if got[i].Session != w.session || got[i].Rel != w.rel || got[i].Kind != w.kind || got[i].Key != w.key || got[i].Source != w.source {
					t.Fatalf("Claims()[%d] = %+v, want %+v", i, got[i], w)
				}
			}
		})
	}
	if got, err := st.Claims(ctx, store.ClaimFilter{Kind: "pr", Key: "34363", Sessions: []string{"root-1"}}); err != nil || len(got) != 1 || !got[0].At.Equal(opened.At) {
		t.Fatalf("derived claim time = %+v, %v; want %v", got, err, opened.At)
	}
	removed, err := st.Unclaim(ctx, store.Claim{Session: "lane-1", Rel: "watches", Kind: "pr", Key: "34363"})
	if err != nil || !removed {
		t.Fatalf("Unclaim() = %v, %v", removed, err)
	}
	if removed, err = st.Unclaim(ctx, store.Claim{Session: "lane-1", Rel: "watches", Kind: "pr", Key: "34363"}); err != nil || removed {
		t.Fatalf("second Unclaim() = %v, %v", removed, err)
	}
	if _, _, err := store.ParseRef("pr"); err == nil {
		t.Fatal("ParseRef() accepted a bare kind")
	}
}

func TestCrossDriveDelivery(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	ask := testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Ask, Text: "take it?", To: []string{store.PeerAddress("sess-b")}})
	testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Go, Text: "go", To: []string{"root@b"}})
	testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Go, Text: "api go", To: []string{"api@b"}})
	testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.State, Text: "a broadcast"})
	testutil.Post(t, st, store.Record{Drive: "b", Lane: "walker", Kind: kinds.State, Text: "b broadcast"})
	tests := []struct {
		name string
		f    store.Filter
		want []string
	}{
		{"b's root reader", store.Filter{Drive: "b", Peer: "sess-b", For: "root"}, []string{"take it?", "go", "b broadcast"}},
		{"b's root by main alias", store.Filter{Drive: "b", For: "main"}, []string{"go", "b broadcast"}},
		{"b's api lane hook delivery", store.Filter{Drive: "b", Peer: "sess-api", To: "api"}, []string{"api go"}},
		{"plain drive read stays in the drive", store.Filter{Drive: "b"}, []string{"b broadcast"}},
		{"kind selector gates only the drive branch", store.Filter{Drive: "b", Peer: "sess-b", For: "root", Kinds: []kinds.Kind{kinds.Go}}, []string{"take it?", "go"}},
		{"peer with no drive match", store.Filter{Drive: "c", Peer: "sess-b"}, []string{"take it?"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := st.Query(ctx, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			texts := make([]string, len(got))
			for i, r := range got {
				texts[i] = r.Text
			}
			if strings.Join(texts, "|") != strings.Join(tt.want, "|") {
				t.Fatalf("Query() = %v, want %v", texts, tt.want)
			}
		})
	}
	if _, _, err := st.Post(ctx, store.Record{Drive: "b", Lane: "root", Kind: kinds.Answer, Text: "yes", Resolves: ask.Seq, Source: "post"}, 0); err != nil {
		t.Fatalf("cross-drive resolve: %v", err)
	}
	local := testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Ask, Text: "local only"})
	if _, _, err := st.Post(ctx, store.Record{Drive: "b", Lane: "root", Kind: kinds.Answer, Text: "no", Resolves: local.Seq, Source: "post"}, 0); err == nil {
		t.Fatal("resolved a record of another drive that was not addressed across drives")
	}
	resolved, err := st.Resolvers(ctx, "a")
	if err != nil || resolved[ask.Seq] == 0 {
		t.Fatalf("Resolvers(a) = %v, %v; want %d resolved", resolved, err, ask.Seq)
	}
	if err := store.Validate(store.Record{Drive: "a", Lane: "l", Kind: kinds.Go, Text: "x", To: []string{"@b"}}); err == nil {
		t.Fatal("Validate() accepted an empty lane before @")
	}
	if !store.CrossDrive([]string{"root", "peer:x"}) || store.CrossDrive([]string{"root"}) {
		t.Fatal("CrossDrive() misreads recipients")
	}
	if got, err := st.Query(ctx, store.Filter{Drive: "b", For: "root", To: "api"}); err != nil || len(got) != 1 || got[0].Text != "api go" {
		t.Fatalf("--reader root --to api = %+v, %v; want only the record addressed to api@b", got, err)
	}
}

func TestLaneCursorStartsBeforeCrossDriveRecords(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	started := clock.Now()
	clock.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Go, Text: "go api", To: []string{"api@b"}})
	testutil.Post(t, st, store.Record{Drive: "b", Lane: "api", Kind: kinds.State, Text: "first local post"})
	if err := st.BindLane(ctx, "s-api", "b", "api", started); err != nil {
		t.Fatal(err)
	}
	seq, ok, err := st.Cursor(ctx, store.LaneCursor("s-api"), "b")
	if err != nil || !ok || seq != 0 {
		t.Fatalf("lane cursor = %d %v %v; want 0 so #1 (api@b) is delivered", seq, ok, err)
	}
}

func TestCompactKeepsCrossDriveResolvers(t *testing.T) {
	st, clock := testutil.Store(t)
	ctx := context.Background()
	ask := testutil.Post(t, st, store.Record{Drive: "a", Lane: "root", Kind: kinds.Ask, Text: "take it?", To: []string{store.PeerAddress("sess-b")}})
	testutil.Post(t, st, store.Record{Drive: "b", Lane: "root", Kind: kinds.Answer, Text: "yes", Resolves: ask.Seq, To: []string{store.PeerAddress("sess-a")}})
	testutil.Post(t, st, store.Record{Drive: "b", Lane: "root", Kind: kinds.State, Text: "old state"})
	clock.Advance(72 * time.Hour)
	res, err := st.Compact(ctx, "b", 48*time.Hour)
	if err != nil || res.Folded != 1 {
		t.Fatalf("Compact(b) = %+v, %v; want the state folded and the resolver kept", res, err)
	}
	open, err := st.OpenItems(ctx, "a")
	if err != nil || len(open) != 0 {
		t.Fatalf("OpenItems(a) after compacting b = %+v, %v; want the ask still resolved", open, err)
	}
}
