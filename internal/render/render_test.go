package render_test

import (
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestWithBodyIndentsTheBodyUnderTheLine(t *testing.T) {
	r := store.Record{Seq: 3, Body: []byte(`{"phase":"live","links":["a"]}`)}
	want := "#3 line\n{\n  \"phase\": \"live\",\n  \"links\": [\n    \"a\"\n  ]\n}"
	if got := render.WithBody("#3 line", r); got != want {
		t.Fatalf("WithBody() = %q, want %q", got, want)
	}
	if got := render.WithBody("#4 line", store.Record{Seq: 4}); got != "#4 line" {
		t.Fatalf("WithBody(no body) = %q", got)
	}
}

func TestBriefDropsOnlyWhatTheTextAlreadyNames(t *testing.T) {
	at := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	record := func(topic, text string, prs ...store.PR) store.Record {
		return store.Record{Seq: 7, At: at, Lane: "desk", Kind: kinds.Report, Topic: topic, Text: text, Refs: store.Refs{PRs: prs}}
	}
	for _, tt := range []struct {
		name  string
		r     store.Record
		brief string
	}{
		{"topic named in the text", record("merge-ask:42@abc", "MERGE-ASK merge-ask:42@abc: ready"), " MERGE-ASK merge-ask:42@abc: ready"},
		{"topic only mentioned inside a word", record("42", "READY #42"), " [42] READY #42"},
		{"pr named in the text", record("", "READY (#42), green", store.PR{Number: 42}), " READY (#42), green"},
		{"pr with its repo named in the text", record("", "opened #o/r#42", store.PR{Repo: "o/r", Number: 42}), " opened #o/r#42"},
		{"pr named without the repo the ref carries", record("", "opened #42", store.PR{Repo: "o/r", Number: 42}), " opened #42 pr:o/r#42"},
		{"pr not named", record("", "READY #420", store.PR{Number: 42}), " READY #420 pr#42"},
		{"topic and pr before a question mark", record("rollout", "take #42 for the rollout?", store.PR{Number: 42}), " take #42 for the rollout?"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stamp := render.Stamp(at, at)
			if got := render.Untagged(tt.r, at, 0); got != stamp+tt.brief {
				t.Fatalf("Untagged() = %q, want %q", got, stamp+tt.brief)
			}
			if got, header := render.Brief(tt.r, at, 0), "#7 "+stamp+" REPORT desk"; got != header+tt.brief {
				t.Fatalf("Brief() = %q, want %q", got, header+tt.brief)
			}
			whole := render.Line(tt.r, at)
			if tt.r.Topic != "" && !strings.Contains(whole, " ["+tt.r.Topic+"] ") {
				t.Fatalf("Line() = %q, want the topic spelled out", whole)
			}
			for _, pr := range tt.r.Refs.PRs {
				if !strings.HasSuffix(whole, " pr#"+pr.String()) && !strings.HasSuffix(whole, " pr:"+pr.String()) {
					t.Fatalf("Line() = %q, want pr %s spelled out", whole, pr)
				}
			}
		})
	}
}

func TestBriefKeepsTheFieldsAClipWouldHideInTheText(t *testing.T) {
	at := time.Date(2026, 10, 27, 9, 0, 0, 0, time.UTC)
	r := store.Record{Seq: 7, At: at, Lane: "desk", Kind: kinds.Report, Topic: "rollout", Text: strings.Repeat("filler ", 10) + "rollout"}
	for _, got := range []string{render.Brief(r, at, 40), render.Untagged(r, at, 40)} {
		if !strings.Contains(got, "[rollout]") {
			t.Fatalf("clipped line %q hides the topic", got)
		}
	}
	if got := render.Brief(r, at, 0); strings.Contains(got, "[rollout]") {
		t.Fatalf("unclipped line %q repeats the topic its text names", got)
	}
}
