package store

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func selectedPost(t *testing.T, st *Store, r Record) Record {
	t.Helper()
	r.Drive, r.Lane, r.Source = "d", "lane", "post"
	got, _, err := st.Post(context.Background(), r, 0)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func assertSelectedOpen(t *testing.T, st *Store, seq int64, want bool) {
	t.Helper()
	got, err := st.OpenAmong(context.Background(), "d", []int64{seq})
	if err != nil || (len(got) == 1) != want {
		t.Fatalf("OpenAmong #%d = %+v, %v; want open=%t", seq, got, err, want)
	}
}

func TestSelectedClosureKeysFollowRawWriterMutations(t *testing.T) {
	ctx := context.Background()
	st, _ := openStore(t)
	opener := selectedPost(t, st, Record{Kind: "ask", Text: "selected"})
	closer := selectedPost(t, st, Record{Kind: "answer", Text: "closer", Re: opener.Seq})
	assertSelectedOpen(t, st, opener.Seq, false)
	for _, step := range []struct {
		query string
		want  bool
	}{
		{"UPDATE records SET re=0 WHERE seq=?", true},
		{"UPDATE records SET topic='shared' WHERE seq=?", false},
		{"UPDATE records SET kind='note' WHERE seq=?", true},
		{"UPDATE records SET kind='fix-live', topic='', refs='{\"STACKS\":[\"a/x\"]}' WHERE seq=?", false},
		{"UPDATE records SET refs='{\"stacks\":[\"b/y\"],\"targets\":[\"api\"]}' WHERE seq=?", true},
		{"UPDATE records SET refs='{\"tArGeTs\":[\"api\"]}' WHERE seq=?", false},
		{"UPDATE records SET drive='foreign' WHERE seq=?", true},
		{"UPDATE records SET drive='d' WHERE seq=?", false},
	} {
		if strings.Contains(step.query, "topic='shared'") {
			if _, err := st.db.ExecContext(ctx, "UPDATE records SET topic='shared' WHERE seq=?", opener.Seq); err != nil {
				t.Fatal(err)
			}
		}
		if strings.Contains(step.query, "kind='fix-live'") {
			if _, err := st.db.ExecContext(ctx, `UPDATE records SET kind='incident', topic='', refs='{"stacks":["a/x"],"targets":["api"]}' WHERE seq=?`, opener.Seq); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := st.db.ExecContext(ctx, step.query, closer.Seq); err != nil {
			t.Fatal(err)
		}
		assertSelectedOpen(t, st, opener.Seq, step.want)
	}
	newSeq := closer.Seq + 100
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET seq=? WHERE seq=?", newSeq, closer.Seq); err != nil {
		t.Fatal(err)
	}
	assertSelectedOpen(t, st, opener.Seq, false)
	var stale int
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM record_closer_keys WHERE source_seq=?", closer.Seq).Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("stale rewritten-seq keys = %d, %v", stale, err)
	}
	if _, err := st.db.ExecContext(ctx, "DELETE FROM records WHERE seq=?", newSeq); err != nil {
		t.Fatal(err)
	}
	assertSelectedOpen(t, st, opener.Seq, true)
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM record_closer_keys").Scan(&stale); err != nil || stale != 0 {
		t.Fatalf("deleted closer keys = %d, %v", stale, err)
	}
}

func TestSelectedClosureMaintenanceIsExplicitAndIdempotent(t *testing.T) {
	ctx := context.Background()
	st, _ := openStore(t)
	opener := selectedPost(t, st, Record{Kind: "ask", Text: "selected"})
	fingerprint := st.SchemaFingerprint()
	for i := len(closerMemberships) - 1; i >= 0; i-- {
		object := closerMemberships[i]
		if _, err := st.db.ExecContext(ctx, "DROP "+object.kind+" "+object.name); err != nil {
			t.Fatal(err)
		}
	}
	selectedPost(t, st, Record{Kind: "answer", Text: "historical closer", Re: opener.Seq})
	reopened, err := Open(ctx, st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	installed, err := projectionInstalled(ctx, reopened.db, closerMemberships[:], "closer membership")
	if err != nil || installed {
		t.Fatalf("ordinary Open provisioned closer keys = %t, %v", installed, err)
	}
	if _, err := reopened.OpenAmong(ctx, "d", []int64{opener.Seq}); err == nil {
		t.Fatal("missing projection silently fell back to a history scan")
	}
	report, err := MaintainIndexes(ctx, st.HomeDir())
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(closerMemberships))
	for _, object := range closerMemberships {
		names = append(names, object.name)
	}
	if !slices.Equal(report.Memberships, names) || st.SchemaFingerprint() != fingerprint {
		t.Fatalf("maintenance = %+v; want only physical closer keys and same wire/schema", report)
	}
	assertSelectedOpen(t, reopened, opener.Seq, false)
	report, err = MaintainIndexes(ctx, st.HomeDir())
	if err != nil || len(report.Memberships) != 0 || len(report.Created) != 0 {
		t.Fatalf("second maintenance = %+v, %v", report, err)
	}
}

func TestSelectedClosureRefProjectionDefersToGoDecoding(t *testing.T) {
	for _, row := range []struct {
		name, target, refs string
	}{
		{"case folded field", "api", `{"TaRgEtS":["api"]}`},
		{"surrogate pair", "🌎", `{"TARGETS":["\ud83c\udf0e"]}`},
		{"unpaired surrogate replacement", "�", `{"targets":["\ud800"]}`},
		{"invalid UTF8 replacement", "�", "{\"targets\":[\"" + string([]byte{0xff}) + "\"]}"},
	} {
		t.Run(row.name, func(t *testing.T) {
			st, _ := openStore(t)
			opener := selectedPost(t, st, Record{Kind: "incident", Text: "selected", Refs: Refs{Targets: []string{row.target}}})
			closer := selectedPost(t, st, Record{Kind: "fix-live", Text: "closer"})
			if _, err := st.db.ExecContext(context.Background(), "UPDATE records SET refs=? WHERE seq=?", row.refs, closer.Seq); err != nil {
				t.Fatal(err)
			}
			assertSelectedOpen(t, st, opener.Seq, false)
		})
	}
}

func TestSelectedClosureProjectionFollowsActualOldWriter(t *testing.T) {
	probe := os.Getenv("CCI_OLD_STORE_PROBE")
	if probe == "" {
		t.Skip("CI builds the immutable old-store writer")
	}
	ctx := context.Background()
	st, _ := openStore(t)
	opener, _, err := st.Post(ctx, Record{Drive: "compat", Lane: "selected", Kind: "ask", Text: "selected old-writer target", Source: "post"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	output, err := exec.Command(probe, "matrix", st.HomeDir(), strconv.FormatInt(opener.Seq, 10)).CombinedOutput()
	if err != nil {
		t.Fatalf("actual old writer = %v: %s", err, output)
	}
	var written struct{ Done, Remote int64 }
	if err := json.Unmarshal(output, &written); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []int64{written.Done, written.Remote} {
		var keys int
		if seq == 0 {
			t.Fatal("old writer did not return its closer sequence")
		}
		if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM record_closer_keys WHERE source_seq=?", seq).Scan(&keys); err != nil || keys == 0 {
			t.Fatalf("old-writer closer #%d keys=%d, %v", seq, keys, err)
		}
	}
	open, err := st.OpenAmong(ctx, "compat", []int64{opener.Seq})
	if err != nil || len(open) != 0 {
		t.Fatalf("old-writer cross-record closure = %+v, %v", open, err)
	}
}

func TestSelectedClosureProvisioningWithinWriterBudget(t *testing.T) {
	raw := os.Getenv("CCI_CLOSER_PROVISION_ROWS")
	if raw == "" {
		t.Skip("CI supplies the representative closer projection population")
	}
	population, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	st, _ := openStore(t)
	for i := len(closerMemberships) - 1; i >= 0; i-- {
		object := closerMemberships[i]
		if _, err := st.db.ExecContext(ctx, "DROP "+object.kind+" "+object.name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
 VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n < ?
 )
 INSERT INTO records(drive,lane,kind,at,text,re,topic,refs,source,hash)
 SELECT 'd','history','answer',n,'closer',1000000+n,'topic-'||n,'{"stacks":["a/x"],"targets":["api"]}','post','closer-'||n FROM history`, population); err != nil {
		t.Fatal(err)
	}
	fingerprint := st.SchemaFingerprint()
	report, err := MaintainIndexes(ctx, st.HomeDir())
	if err != nil || len(report.Memberships) != len(closerMemberships) || report.Elapsed >= 5*time.Second {
		t.Fatalf("new closer projection writer budget: records=%d report=%+v error=%v", population, report, err)
	}
	var keys, records int
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM record_closer_keys").Scan(&keys); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records").Scan(&records); err != nil {
		t.Fatal(err)
	}
	if keys != 4*population || records != population || st.SchemaFingerprint() != fingerprint {
		t.Fatalf("projection changed logical state/keys: keys=%d records=%d fingerprint=%s", keys, records, st.SchemaFingerprint())
	}
	t.Logf("closer_projection_records=%d keys=%d elapsed=%s writer_budget=%s before=%+v after=%+v", records, keys, report.Elapsed, 5*time.Second, report.Before, report.After)
}
