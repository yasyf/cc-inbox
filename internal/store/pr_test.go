package store_test

import (
	"encoding/json"
	"testing"

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
