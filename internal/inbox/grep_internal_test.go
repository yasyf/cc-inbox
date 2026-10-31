package inbox

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestGrepMatchesPagedOutput(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	history := make([]store.Ingestion, 0, 1005)
	for i := range 1005 {
		history = append(history, store.Ingestion{
			Record:   store.Record{Drive: "d", Lane: "reviewer", Kind: kinds.Note, At: st.Now(), Text: fmt.Sprintf("REVIEW history %d", i), Source: "import:/history"},
			LineHash: fmt.Sprintf("history-%d", i),
		})
	}
	if _, err := st.IngestAll(ctx, history); err != nil {
		t.Fatal(err)
	}
	ask := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "REVIEW open question", To: []string{"peer:reader"}})
	testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "REVIEW answer", Re: ask.Seq})
	other := testutil.Post(t, st, store.Record{Kind: kinds.Ask, Text: "REVIEW another question"})
	testutil.Post(t, st, store.Record{Kind: kinds.Done, Text: "REVIEW closed", Resolves: other.Seq, AlsoResolves: []int64{ask.Seq}})
	for _, tt := range []struct {
		name    string
		pattern string
		filter  store.Filter
		limit   int
		budget  int
	}{
		{"default", "(?i)review", store.Filter{Drive: "d"}, 0, 6144},
		{"wide", "REVIEW", store.Filter{Drive: "d"}, 0, 1 << 20},
		{"oversized first", "REVIEW", store.Filter{Drive: "d"}, 0, 1},
		{"marks fill budget", "REVIEW", store.Filter{Drive: "d"}, 0, 230},
		{"negative limit", "REVIEW", store.Filter{Drive: "d"}, -1, 6144},
		{"one", "REVIEW", store.Filter{Drive: "d"}, 1, 6144},
		{"cross page limit", "REVIEW", store.Filter{Drive: "d"}, 501, 1 << 20},
		{"none", "absent", store.Filter{Drive: "d"}, 0, 6144},
		{"whole line regex", "ASK lane-a.*peer:reader", store.Filter{Drive: "d"}, 0, 6144},
		{"peer and filters", "REVIEW", store.Filter{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Ask}}, 0, 6144},
		{"keyset", "REVIEW", store.Filter{Drive: "d", After: 300, Before: 900}, 0, 6144},
	} {
		for _, asJSON := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/json=%t", tt.name, asJSON), func(t *testing.T) {
				pattern := regexp.MustCompile(tt.pattern)
				var got, want bytes.Buffer
				if err := Grep(ctx, st, tt.filter, pattern, tt.limit, tt.budget, asJSON, &got); err != nil {
					t.Fatal(err)
				}
				if err := pagedGrep(ctx, st, tt.filter, pattern, tt.limit, tt.budget, asJSON, &want); err != nil {
					t.Fatal(err)
				}
				if got.String() != want.String() {
					t.Fatalf("grep differs from paged output:\ngot %q\nwant %q", got.String(), want.String())
				}
			})
		}
	}
}

func pagedGrep(ctx context.Context, st *store.Store, f store.Filter, pattern *regexp.Regexp, limit, budget int, asJSON bool, out *bytes.Buffer) error {
	b := render.NewBudget(out, budget)
	matched, printed := 0, 0
	f.Descending, f.Limit = true, pageRows
	now := st.Now()
	for {
		records, err := st.Query(ctx, f)
		if err != nil {
			return err
		}
		var hits []store.Record
		for _, r := range records {
			if pattern.MatchString(render.Line(r, now)) {
				hits = append(hits, r)
			}
		}
		rendered, _, err := lines(ctx, st, hits, view{JSON: asJSON, Whole: true})
		if err != nil {
			return err
		}
		for _, line := range rendered {
			matched++
			if (limit == 0 || printed < limit) && b.Line(line) {
				printed++
			}
		}
		if len(records) < pageRows {
			break
		}
		f.Before = records[len(records)-1].Seq
	}
	if asJSON {
		return nil
	}
	if matched == 0 {
		head, err := st.MaxSeq(ctx)
		if err != nil {
			return err
		}
		b.Trailer(fmt.Sprintf("no records on %s match; store head #%d", f.Drive, head))
	}
	if matched > printed {
		b.Trailer(fmt.Sprintf("... %d more matches; narrow with --kind, --lane or --since", matched-printed))
	}
	return nil
}

func TestGrepPreservesReadErrorOutput(t *testing.T) {
	for _, asJSON := range []bool{false, true} {
		for _, annotationError := range []bool{false, true} {
			if asJSON && annotationError {
				continue
			}
			t.Run(fmt.Sprintf("json=%t/annotation_error=%t", asJSON, annotationError), func(t *testing.T) {
				st, _ := testutil.Store(t)
				ctx := context.Background()
				history := make([]store.Ingestion, 0, 1005)
				for i := range 1005 {
					history = append(history, store.Ingestion{
						Record:   store.Record{Drive: "d", Lane: "reviewer", Kind: kinds.Note, At: st.Now(), Text: fmt.Sprintf("REVIEW history %d", i), Source: "import:/history"},
						LineHash: fmt.Sprintf("history-%d", i),
					})
				}
				if _, err := st.IngestAll(ctx, history); err != nil {
					t.Fatal(err)
				}
				db, err := sql.Open("sqlite", filepath.Join(st.HomeDir(), store.DBFile))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				if _, err := db.ExecContext(ctx, "UPDATE records SET refs = '{' WHERE seq = 1"); err != nil {
					t.Fatal(err)
				}
				wantError := "decode refs of #1:"
				if annotationError {
					reply := testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "answer", Re: 1005})
					if _, err := db.ExecContext(ctx, "UPDATE records SET refs = '{' WHERE seq = ?", reply.Seq); err != nil {
						t.Fatal(err)
					}
					wantError = fmt.Sprintf("decode refs of #%d:", reply.Seq)
				}
				for _, budget := range []int{1, 6144, 1 << 20} {
					var got, want bytes.Buffer
					pattern := regexp.MustCompile("REVIEW")
					gotErr := Grep(ctx, st, store.Filter{Drive: "d"}, pattern, 0, budget, asJSON, &got)
					wantErr := pagedGrep(ctx, st, store.Filter{Drive: "d"}, pattern, 0, budget, asJSON, &want)
					if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() || !strings.Contains(gotErr.Error(), wantError) {
						t.Fatalf("budget=%d errors got %v, want %v with %q", budget, gotErr, wantErr, wantError)
					}
					if got.String() != want.String() {
						t.Fatalf("budget=%d error output got %q, want %q", budget, got.String(), want.String())
					}
				}
			})
		}
	}
}

func TestGrepPreservesLaterAnnotationErrorPrefix(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	db, err := sql.Open("sqlite", filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < 1005
)
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'd', 'reviewer', 'note', n, 'REVIEW history ' || n, 'post', 'history-' || n FROM history`); err != nil {
		t.Fatal(err)
	}
	reply := testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "answer", Re: 500})
	if _, err := db.ExecContext(ctx, "UPDATE records SET refs = '{' WHERE seq = ?", reply.Seq); err != nil {
		t.Fatal(err)
	}
	var got, want bytes.Buffer
	pattern := regexp.MustCompile("REVIEW")
	gotErr := Grep(ctx, st, store.Filter{Drive: "d"}, pattern, 0, 1<<20, false, &got)
	wantErr := pagedGrep(ctx, st, store.Filter{Drive: "d"}, pattern, 0, 1<<20, false, &want)
	if gotErr == nil || wantErr == nil || gotErr.Error() != wantErr.Error() || !strings.Contains(gotErr.Error(), fmt.Sprintf("decode refs of #%d:", reply.Seq)) {
		t.Fatalf("selected annotation errors got %v, want %v", gotErr, wantErr)
	}
	if got.String() != want.String() || strings.Count(got.String(), "\n") != 500 {
		t.Fatalf("annotation error prefixes got %d lines, paged %d lines", strings.Count(got.String(), "\n"), strings.Count(want.String(), "\n"))
	}
	t.Logf("same_selected_annotation_error=true candidate_prefix_lines=%d paged_prefix_lines=%d", strings.Count(got.String(), "\n"), strings.Count(want.String(), "\n"))
}

func TestGrepReplyMetadataErrorAttribution(t *testing.T) {
	for _, tt := range []struct {
		name      string
		set       string
		before    int64
		wantLines int
	}{
		{"typed array", `re = 500, also_resolves = '["oops"]'`, 0, 500},
		{"typed array with earlier owner", `re = 500, also_resolves = '[1000, "oops"]'`, 0, 0},
		{"typed object with earlier owner", `re = 500, also_resolves = '{"x":1000}'`, 0, 0},
		{"real earlier owner", `re = 500, also_resolves = '[1000.0]'`, 0, 0},
		{"string owner", `re = 500, also_resolves = '["1000", "oops"]'`, 0, 500},
		{"invalid re", "re = 'oops', resolves = 500", 0, 500},
		{"malformed JSON", "re = 500, also_resolves = '['", 0, 0},
		{"malformed JSON after first page reply", "re = 500, also_resolves = '['", 1000, 0},
		{"malformed JSON after later page reply", "re = 500, also_resolves = '['", 499, 0},
		{"malformed JSON first page owner", "re = 1000, also_resolves = '['", 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			ctx := context.Background()
			db, err := sql.Open("sqlite", filepath.Join(st.HomeDir(), store.DBFile))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			if _, err := db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < 1005
)
INSERT INTO records (drive, lane, kind, at, text, source, hash)
SELECT 'd', 'reviewer', 'note', n, 'REVIEW history ' || n, 'post', 'history-' || n FROM history`); err != nil {
				t.Fatal(err)
			}
			if tt.before != 0 {
				testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "earlier answer", Re: tt.before})
			}
			reply := testutil.Post(t, st, store.Record{Drive: "elsewhere", Kind: kinds.Answer, Text: "answer", Re: 500})
			if _, err := db.ExecContext(ctx, "UPDATE records SET "+tt.set+" WHERE seq = ?", reply.Seq); err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			err = Grep(ctx, st, store.Filter{Drive: "d"}, regexp.MustCompile("REVIEW"), 0, 1<<20, false, &out)
			if err == nil {
				t.Fatal("corrupt selected reply did not return an error")
			}
			wantLines := tt.wantLines
			if got := strings.Count(out.String(), "\n"); got != wantLines {
				t.Fatalf("corruption prefix has %d lines, want %d: %v", got, wantLines, err)
			}
			t.Logf("prefix_lines=%d error=%v", wantLines, err)
		})
	}
}
