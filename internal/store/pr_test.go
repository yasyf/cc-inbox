package store_test

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestParsePR(t *testing.T) {
	for _, tt := range []struct {
		in   string
		want store.PR
	}{
		{"30431", store.PR{Number: 30431}},
		{"Forge-FDE/iris-chime#243", store.PR{Repo: "Forge-FDE/iris-chime", Number: 243}},
		{"https://github.com/Forge-FDE/iris-chime/pull/243", store.PR{Repo: "Forge-FDE/iris-chime", Number: 243}},
		{"https://github.com/yasyf/cc-inbox/pull/79#issuecomment-1", store.PR{Repo: "yasyf/cc-inbox", Number: 79}},
	} {
		if got, err := store.ParsePR(tt.in); err != nil || got != tt.want {
			t.Errorf("ParsePR(%q) = %+v, %v; want %+v", tt.in, got, err, tt.want)
		}
	}
	for _, in := range []string{"", "0", "-3", "iris-chime#243", "https://github.com/Forge-FDE/iris-chime/issues/243", "#243"} {
		if _, err := store.ParsePR(in); err == nil {
			t.Errorf("ParsePR(%q) succeeded", in)
		}
	}
}

func TestPRRefsKeepBareNumbersAsJSONNumbers(t *testing.T) {
	refs := store.Refs{PRs: []store.PR{{Number: 30431}, {Repo: "Forge-FDE/iris-chime", Number: 243}}}
	b, err := json.Marshal(refs)
	if err != nil || string(b) != `{"prs":[30431,"Forge-FDE/iris-chime#243"]}` {
		t.Fatalf("Marshal = %s, %v", b, err)
	}
	var back store.Refs
	if err := json.Unmarshal(b, &back); err != nil || len(back.PRs) != 2 || back.PRs[0] != refs.PRs[0] || back.PRs[1] != refs.PRs[1] {
		t.Fatalf("Unmarshal = %+v, %v", back, err)
	}
}

func TestPRFilterMatchesBareAndQualifiedRefs(t *testing.T) {
	st, _ := open(t)
	for text, pr := range map[string]store.PR{
		"bare":      {Number: 40339},
		"qualified": {Repo: "Forge-AI/monorepo", Number: 40339},
		"elsewhere": {Repo: "yasyf/cc-context", Number: 40339},
		"other":     {Repo: "Forge-AI/monorepo", Number: 40340},
	} {
		post(t, st, store.Record{Kind: kinds.Opened, Text: text, Refs: store.Refs{PRs: []store.PR{pr}}})
	}
	for _, tt := range []struct {
		pr   store.PR
		want string
	}{
		{store.PR{Repo: "Forge-AI/monorepo", Number: 40339}, "bare,qualified"},
		{store.PR{Number: 40339}, "bare,qualified,elsewhere"},
		{store.PR{Repo: "yasyf/cc-context", Number: 40339}, "bare,elsewhere"},
	} {
		got, err := st.Query(context.Background(), store.Filter{Drive: "d", PR: tt.pr})
		if err != nil {
			t.Fatal(err)
		}
		texts := map[string]bool{}
		for _, r := range got {
			texts[r.Text] = true
		}
		want := map[string]bool{}
		for _, text := range strings.Split(tt.want, ",") {
			want[text] = true
		}
		if !maps.Equal(texts, want) {
			t.Errorf("Query(PR %s) = %v, want %s", tt.pr, texts, tt.want)
		}
	}
}
