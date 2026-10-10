package store_test

import (
	"context"
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
