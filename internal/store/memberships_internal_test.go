package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

var membershipKeys = map[string]string{
	"record_recipients":    "address",
	"record_also_resolves": "target",
	"record_json_errors":   "field",
}

func TestRecordMembershipsFollowEveryWriter(t *testing.T) {
	ctx := context.Background()
	st := openMembershipStore(ctx, t, t.TempDir())
	post := func(r Record) Record {
		t.Helper()
		got, _, err := st.Post(ctx, r, 0)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := st.db.ExecContext(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	ask := post(Record{Drive: "d", Lane: "root", Kind: kinds.Ask, Text: "question", To: []string{"peer:reader", "api@other", "peer:reader"}})
	other := post(Record{Drive: "d", Lane: "api", Kind: kinds.Ask, Text: "second question"})
	done := post(Record{Drive: "d", Lane: "api", Kind: kinds.Done, Text: "both answered", Resolves: ask.Seq, AlsoResolves: []int64{other.Seq}, To: []string{"root"}})
	assertMembership(ctx, t, st, "record_recipients", "'peer:reader'", ask.Seq, 1)
	assertMembership(ctx, t, st, "record_recipients", "'api@other'", ask.Seq, 1)
	assertMembership(ctx, t, st, "record_also_resolves", strconv.FormatInt(other.Seq, 10), done.Seq, 1)
	assertMembershipsDerived(ctx, t, st)

	imported := Ingestion{
		Record:   Record{Drive: "d", Lane: "importer", Kind: kinds.Note, At: st.Now(), Text: "imported line", Source: "import:/inbox.md", To: []string{"peer:imported"}},
		LineHash: "membership-line",
		Line:     "imported line",
	}
	if n, err := st.IngestAll(ctx, []Ingestion{imported}); err != nil || n != 1 {
		t.Fatalf("IngestAll() = %d, %v", n, err)
	}
	var importedSeq int64
	if err := st.db.QueryRowContext(ctx, "SELECT seq FROM records WHERE line_hash = ?", imported.LineHash).Scan(&importedSeq); err != nil {
		t.Fatal(err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:imported'", importedSeq, 1)
	imported.Record.To = []string{"peer:reparsed"}
	if updated, _, err := st.Reparse(ctx, []Ingestion{imported}, false); err != nil || updated != 1 {
		t.Fatalf("Reparse() = %d, %v", updated, err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:imported'", importedSeq, 0)
	assertMembership(ctx, t, st, "record_recipients", "'peer:reparsed'", importedSeq, 1)
	assertMembershipsDerived(ctx, t, st)

	remote := "0123456789abcdef"
	applied, err := st.Apply(ctx, []Row{
		{Origin: remote, OriginSeq: 2, Drive: "d", Lane: "remote", Kind: kinds.Done, Text: "remote reply", At: st.Now(), To: []string{"peer:remote"}, Re: &Pointer{Origin: remote, Seq: 1}, Resolves: &Pointer{Origin: st.Self(), Seq: ask.Seq}, AlsoResolves: []Pointer{{Origin: st.Self(), Seq: other.Seq}}, Source: "post"},
		{Origin: remote, OriginSeq: 1, Drive: "d", Lane: "remote", Kind: kinds.Note, Text: "remote target", At: st.Now(), Source: "post"},
	}, nil)
	if err != nil || applied != (Applied{Inserted: 1, Buffered: 1, Released: 1}) {
		t.Fatalf("Apply() = %+v, %v", applied, err)
	}
	var replySeq int64
	if err := st.db.QueryRowContext(ctx, "SELECT seq FROM records WHERE origin = ? AND origin_seq = 2", remote).Scan(&replySeq); err != nil {
		t.Fatal(err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:remote'", replySeq, 1)
	assertMembership(ctx, t, st, "record_also_resolves", strconv.FormatInt(other.Seq, 10), replySeq, 1)
	assertMembershipsDerived(ctx, t, st)

	for _, set := range []string{
		`recipients = '['`,
		`also_resolves = '['`,
		`recipients = '[''peer:json5'', /* comment */ "peer:quoted",]'`,
		`recipients = jsonb('["peer:blob"]')`,
		`recipients = X'1B0D'`,
		`recipients = '"peer:scalar"'`,
		`recipients = '{"k":"peer:object"}'`,
		`recipients = '[1, true, null, ["peer:nested"], "peer:typed", "peer:typed"]'`,
		`also_resolves = '[" 7", "7.0", true, null, "oops", [1], 9.5, 7, 1e1]'`,
		`also_resolves = jsonb('[3]')`,
		`also_resolves = NULL`,
		`recipients = ''`,
		`recipients = '[]', also_resolves = '[]'`,
	} {
		exec("UPDATE records SET "+set+" WHERE seq = ?", done.Seq)
		assertMembershipsDerived(ctx, t, st)
	}
	exec(`UPDATE records SET recipients = '[', also_resolves = '[' WHERE seq = ?`, done.Seq)
	assertMembership(ctx, t, st, "record_json_errors", "'recipients'", done.Seq, 1)
	assertMembership(ctx, t, st, "record_json_errors", "'also_resolves'", done.Seq, 1)
	exec(`UPDATE records SET recipients = '["peer:fixed"]', also_resolves = NULL WHERE seq = ?`, done.Seq)
	assertMembership(ctx, t, st, "record_json_errors", "", done.Seq, 0)
	assertMembership(ctx, t, st, "record_recipients", "'peer:fixed'", done.Seq, 1)

	tx, err := st.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, source, hash) VALUES ('d', 'rolled', 'note', 1, 'rolled back', '["peer:rolled"]', '[1]', 'post', 'rolled')`); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:rolled'", 0, 0)
	assertMembershipsDerived(ctx, t, st)

	for i := range 3 {
		post(Record{Drive: "c", Lane: "compact", Kind: kinds.Note, Text: fmt.Sprintf("folded %d", i), To: []string{"peer:folded"}})
	}
	if res, err := st.Compact(ctx, "c", -time.Hour); err != nil || res.Folded != 3 {
		t.Fatalf("Compact() = %+v, %v", res, err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:folded'", 0, 0)
	post(Record{Drive: "c", Lane: "compact", Kind: kinds.Note, Text: "folded later", To: []string{"peer:folded"}})
	if res, err := st.Compact(ctx, "c", -time.Hour); err != nil || res.Folded != 1 || res.Digests != 1 {
		t.Fatalf("second Compact() = %+v, %v", res, err)
	}
	assertMembership(ctx, t, st, "record_recipients", "'peer:folded'", 0, 0)
	assertMembershipsDerived(ctx, t, st)

	inserted, err := st.db.ExecContext(ctx, `INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, source, hash) VALUES ('elsewhere', 'raw', 'note', 1, 'raw corruption', '[', '[', 'post', 'raw')`)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := inserted.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	assertMembership(ctx, t, st, "record_json_errors", "", raw, 2)
	assertMembershipsDerived(ctx, t, st)

	exec("UPDATE records SET seq = 100000 WHERE seq = ?", ask.Seq)
	assertMembership(ctx, t, st, "record_recipients", "'peer:reader'", ask.Seq, 0)
	assertMembership(ctx, t, st, "record_recipients", "'peer:reader'", 100000, 1)
	exec("DELETE FROM records WHERE seq IN (?, ?)", replySeq, raw)
	assertMembership(ctx, t, st, "record_also_resolves", "", replySeq, 0)
	assertMembership(ctx, t, st, "record_json_errors", "", raw, 0)
	assertMembershipsDerived(ctx, t, st)
}

func TestInitializationInstallsMembershipsOnlyWhileMigrating(t *testing.T) {
	ctx := context.Background()
	t.Run("fresh", func(t *testing.T) {
		st := openMembershipStore(ctx, t, t.TempDir())
		assertMembershipsInstalled(ctx, t, st, true)
		assertMembershipsDerived(ctx, t, st)
	})
	t.Run("schema one", func(t *testing.T) {
		dir := t.TempDir()
		db, err := sql.Open("sqlite", "file:"+filepath.Join(dir, DBFile))
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range []string{
			migrations[0],
			`INSERT INTO records (drive, lane, kind, at, text, recipients, source, hash) VALUES ('d', 'l', 'ask', 1, 'old', '["peer:old", "l@other", "peer:old"]', 'post', 'h1')`,
			`INSERT INTO records (drive, lane, kind, at, text, recipients, source, hash) VALUES ('elsewhere', 'l', 'ask', 1, 'broken', '[', 'post', 'h2')`,
			"PRAGMA user_version = 1",
		} {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Fatalf("%s: %v", stmt, err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		st := openMembershipStore(ctx, t, dir)
		assertMembershipsInstalled(ctx, t, st, true)
		assertMembership(ctx, t, st, "record_recipients", "'peer:old'", 1, 1)
		assertMembership(ctx, t, st, "record_json_errors", "'recipients'", 2, 1)
		assertMembershipsDerived(ctx, t, st)
		got, err := st.Query(ctx, Filter{Drive: "d", Peer: "missing"})
		if err == nil || !strings.Contains(err.Error(), "malformed JSON") {
			t.Fatalf("migrated corrupt recipients read = %+v, %v; want the native malformed JSON error", got, err)
		}
	})
	t.Run("current schema", func(t *testing.T) {
		home := t.TempDir()
		st, err := Open(ctx, home)
		if err != nil {
			t.Fatal(err)
		}
		dropMemberships(ctx, t, st)
		if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, source, hash) VALUES ('f', 'l', 'note', 1, 'unprovisioned', '["peer:reader"]', '[1]', 'post', 'h')`); err != nil {
			t.Fatal(err)
		}
		before := schemaObjects(ctx, t, st)
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
		reopened := openMembershipStore(ctx, t, home)
		if after := schemaObjects(ctx, t, reopened); !slices.Equal(after, before) {
			t.Fatalf("current-schema Open changed objects:\nbefore %v\nafter %v", before, after)
		}
		assertMembershipsInstalled(ctx, t, reopened, false)
		if _, err := reopened.Query(ctx, Filter{Drive: "d", Peer: "reader"}); err == nil || !strings.Contains(err.Error(), "no such table: record_") {
			t.Fatalf("unprovisioned peer read error = %v, want the missing membership table", err)
		}
		if _, err := reopened.Resolvers(ctx, "d"); err == nil || !strings.Contains(err.Error(), "no such table: record_") {
			t.Fatalf("unprovisioned resolver read error = %v, want the missing membership table", err)
		}
		report, err := MaintainIndexes(ctx, home)
		if err != nil {
			t.Fatal(err)
		}
		if len(report.Created) != 0 || len(report.Memberships) != len(recordMemberships) {
			t.Fatalf("provisioning report = %+v, want every membership object and no lane index", report)
		}
		assertMembershipsInstalled(ctx, t, reopened, true)
		assertMembership(ctx, t, reopened, "record_recipients", "'peer:reader'", 1, 1)
		assertMembership(ctx, t, reopened, "record_also_resolves", "1", 1, 1)
		assertMembershipsDerived(ctx, t, reopened)
		repeated, err := MaintainIndexes(ctx, home)
		if err != nil || len(repeated.Created) != 0 || len(repeated.Memberships) != 0 {
			t.Fatalf("repeated provisioning = %+v, %v", repeated, err)
		}
		got, err := reopened.Query(ctx, Filter{Drive: "d", Peer: "reader"})
		if err != nil || len(got) != 1 || got[0].Seq != 1 {
			t.Fatalf("provisioned peer read = %+v, %v", got, err)
		}
	})
}

func TestMaintainIndexesRefusesForeignMembershipObjects(t *testing.T) {
	for _, tt := range []struct {
		name   string
		damage []string
	}{
		{"missing trigger", []string{"DROP TRIGGER record_memberships_delete"}},
		{"changed index", []string{"DROP INDEX record_recipients_address", "CREATE INDEX record_recipients_address ON record_recipients(address)"}},
		{"changed table", []string{"DROP TABLE record_json_errors", "CREATE TABLE record_json_errors (field TEXT, source_seq INTEGER)"}},
		{"object kind", []string{"DROP TRIGGER record_memberships_update", "CREATE TABLE record_memberships_update (x)"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			st := openMembershipStore(ctx, t, t.TempDir())
			dropLaneIndexes(t, st)
			for _, query := range tt.damage {
				if _, err := st.db.ExecContext(ctx, query); err != nil {
					t.Fatal(err)
				}
			}
			before := schemaObjects(ctx, t, st)
			if _, err := MaintainIndexes(ctx, st.home); err == nil {
				t.Fatal("maintenance accepted foreign membership objects")
			}
			if after := schemaObjects(ctx, t, st); !slices.Equal(after, before) {
				t.Fatalf("refused maintenance changed objects:\nbefore %v\nafter %v", before, after)
			}
			if _, err := membershipsInstalled(ctx, st.db); err == nil {
				t.Fatal("readiness accepted foreign membership objects")
			}
		})
	}
}

func TestBoundedReadPlansStayIndexed(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	now := st.Now()
	scans := regexp.MustCompile(`^SCAN (records|r|record_\w+)\b`)
	driveCandidates := regexp.MustCompile(`SEARCH records USING COVERING INDEX \w+ \(drive=\?`)
	for _, tt := range []struct {
		name string
		f    Filter
	}{
		{"actual tail", Filter{Drive: "d", Peer: "reader", Since: now.Add(-36 * time.Hour), Limit: 80000, Descending: true}},
		{"cursor page", Filter{Drive: "d", Peer: "reader", After: 10, PeerAfter: 20, Limit: 500}},
		{"hook", Filter{Drive: "d", Peer: "reader", For: "root", After: 10, PeerAfter: 20, Limit: 500}},
		{"addressed", Filter{Drive: "d", Addressed: "api", After: 10, Before: 99}},
		{"to first only", Filter{Drive: "d", To: "api", FirstOnly: []kinds.Kind{kinds.Note}, Limit: 500}},
		{"peer only", Filter{Peer: "reader", Kinds: []kinds.Kind{kinds.Ask}}},
		{"keyset", Filter{Drive: "d", Peer: "reader", Before: 99, Limit: 500, Descending: true}},
		{"refs", Filter{Drive: "d", Peer: "reader", Stack: "p/e", PR: PR{Repo: "o/r", Number: 1}, RepeatWindow: time.Hour, Mute: []kinds.Kind{kinds.Note}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			q, params := st.selection(tt.f)
			where, countParams := tt.f.bounded(st.Self(), st.Now())
			for name, plan := range map[string][]string{
				"query": queryPlan(ctx, t, st, q, params...),
				"count": queryPlan(ctx, t, st, "SELECT COUNT(*) FROM records WHERE "+where, countParams...),
			} {
				joined := strings.Join(plan, "\n")
				if name == "count" && tt.f.PeerAfter > 0 {
					nativeWhere, nativeParams := tt.f.where(st.Self(), st.Now())
					nativePlan := queryPlan(ctx, t, st, "SELECT COUNT(*) FROM records WHERE "+nativeWhere, nativeParams...)
					if plan[0] != nativePlan[0] {
						t.Fatalf("peer-after count access path = %v, want original %v", plan, nativePlan)
					}
				} else if !strings.Contains(joined, "SEARCH records USING INTEGER PRIMARY KEY (rowid=?)") {
					t.Fatalf("%s plan lacks keyed record lookup:\n%s", name, joined)
				}
				for _, want := range []string{
					"SEARCH record_recipients USING COVERING INDEX record_recipients_address (address=?",
					"SEARCH record_json_errors USING COVERING INDEX record_json_errors_field (field=?",
				} {
					if !strings.Contains(joined, want) {
						t.Fatalf("%s plan lacks %q:\n%s", name, want, joined)
					}
				}
				if !driveCandidates.MatchString(joined) || strings.Contains(joined, "USE TEMP B-TREE FOR ORDER BY") {
					t.Fatalf("%s plan does not walk indexed candidates in seq order:\n%s", name, joined)
				}
				for _, line := range plan {
					if scans.MatchString(line) {
						t.Fatalf("%s plan scans a whole table: %q\n%s", name, line, joined)
					}
				}
				t.Logf("%s plan:\n%s", name, joined)
			}
		})
	}
	origin := Filter{Drive: "d", Peer: "reader", Origin: "feedfacefeedface", Limit: 2, Descending: true}
	q, params := st.selection(origin)
	where, unboundedParams := origin.where(st.Self(), st.Now())
	got, want := queryPlan(ctx, t, st, q, params...), queryPlan(ctx, t, st, "SELECT "+columns+" FROM records WHERE "+where+" ORDER BY seq DESC LIMIT 2", unboundedParams...)
	sorted := "USE TEMP B-TREE FOR ORDER BY"
	if got[0] != want[0] || slices.Contains(got, sorted) != slices.Contains(want, sorted) {
		t.Fatalf("origin plan = %v, want the unbounded access path %v", got, want)
	}
	t.Logf("origin plan: %v; unbounded: %v", got, want)
	plan := queryPlan(ctx, t, st, resolversSQL, slices.Repeat([]any{"d"}, 7)...)
	joined := strings.Join(plan, "\n")
	for _, want := range []string{
		"SEARCH records USING COVERING INDEX records_resolves (drive=?)",
		"SEARCH records USING COVERING INDEX records_resolves_target (resolves=?)",
		"SEARCH r USING INTEGER PRIMARY KEY (rowid=?)",
		"SEARCH record_also_resolves USING COVERING INDEX record_also_resolves_target (target=?)",
		"SEARCH record_json_errors USING COVERING INDEX record_json_errors_field (field=?)",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("resolver plan lacks %q:\n%s", want, joined)
		}
	}
	for _, line := range plan {
		if scans.MatchString(line) {
			t.Fatalf("resolver plan scans a whole table: %q\n%s", line, joined)
		}
	}
	t.Logf("resolver plan:\n%s", joined)
	for _, statement := range strings.Split(strings.TrimSpace(forgetMemberships), "\n") {
		cleanup := strings.Replace(strings.TrimSuffix(statement, ";"), "OLD.seq", "?", 1)
		table := strings.Fields(cleanup)[2]
		got := strings.Join(queryPlan(ctx, t, st, cleanup, 1), "\n")
		if !strings.HasPrefix(got, "SEARCH "+table+" USING ") || !strings.Contains(got, "INDEX "+table+"_source (source_seq=?)") {
			t.Fatalf("trigger cleanup %q plan = %s, want a %s_source search", cleanup, got, table)
		}
	}
}

func TestBoundedReadsMatchUnboundedPredicates(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	since := now.Add(-36 * time.Hour)
	foreignOrigin := "feedfacefeedface"
	filters := []struct {
		name string
		f    Filter
	}{
		{"actual tail frontier", Filter{Drive: "d", Peer: "reader", Since: since, Limit: 3, Descending: true}},
		{"actual tail", Filter{Drive: "d", Peer: "reader", Since: since, Limit: 80000, Descending: true}},
		{"page", Filter{Drive: "d", Peer: "reader", Limit: 500}},
		{"after", Filter{Drive: "d", Peer: "reader", After: 5, Limit: 500}},
		{"before", Filter{Drive: "d", Peer: "reader", Before: 13, Limit: 2, Descending: true}},
		{"peer after", Filter{Drive: "d", Peer: "reader", PeerAfter: 4, Limit: 500}},
		{"addressed", Filter{Drive: "d", Addressed: "api"}},
		{"hook", Filter{Drive: "d", Peer: "reader", For: "root", After: 1, PeerAfter: 1, Limit: 500}},
		{"to first only", Filter{Drive: "d", To: "api", FirstOnly: []kinds.Kind{kinds.Note}}},
		{"peer only", Filter{Peer: "reader"}},
		{"origin", Filter{Drive: "d", Peer: "reader", Origin: foreignOrigin, Limit: 2, Descending: true}},
		{"kinds", Filter{Drive: "d", Peer: "reader", Kinds: []kinds.Kind{kinds.Note}, Mute: []kinds.Kind{kinds.Ask}}},
		{"stack", Filter{Drive: "d", Peer: "reader", Stack: "p/e"}},
		{"window", Filter{Drive: "d", Peer: "reader", IncludeExpired: true, Since: since, Until: now}},
		{"drive only", Filter{Drive: "d", Limit: 3, Descending: true}},
	}
	type outcome struct {
		query, count bool
	}
	expected := map[string]map[string]outcome{
		"late malformed recipients":  {"actual tail frontier": {true, true}},
		"early malformed recipients": {"actual tail frontier": {false, true}, "drive only": {false, false}},
		"stale malformed recipients": {"actual tail": {false, false}},
	}
	for _, variant := range membershipVariants {
		t.Run(variant.name, func(t *testing.T) {
			st := openMembershipStore(ctx, t, t.TempDir())
			st.Now = func() time.Time { return now }
			seedMembershipFixture(ctx, t, st, now)
			for _, set := range variant.sets {
				if _, err := st.db.ExecContext(ctx, "UPDATE records SET "+set.set+" WHERE seq = ?", set.seq); err != nil {
					t.Fatalf("%s: %v", set.set, err)
				}
			}
			assertMembershipsDerived(ctx, t, st)
			for _, tt := range filters {
				got, gotErr := st.Query(ctx, tt.f)
				want, wantErr := unboundedQuery(ctx, st, tt.f)
				assertSameRead(t, tt.name+" query", got, gotErr, want, wantErr)
				n, nErr := st.Count(ctx, tt.f)
				m, mErr := unboundedCount(ctx, st, tt.f)
				assertSameRead(t, tt.name+" count", n, nErr, m, mErr)
				if want, ok := expected[variant.name][tt.name]; ok && (want.query != (gotErr != nil) || want.count != (nErr != nil)) {
					t.Fatalf("%s errors query=%v count=%v, want query=%t count=%t", tt.name, gotErr, nErr, want.query, want.count)
				}
				t.Logf("%s: rows=%d count=%d query_error=%v count_error=%v", tt.name, len(got), n, gotErr, nErr)
			}
			for _, drive := range []string{"d", "f", "x", "missing"} {
				got, gotErr := st.Resolvers(ctx, drive)
				want, wantErr := unboundedResolvers(ctx, st, drive)
				assertSameRead(t, "resolvers "+drive, got, gotErr, want, wantErr)
				opened, openErr := st.OpenItems(ctx, drive)
				t.Logf("resolvers %s: %v error=%v open=%d open_error=%v", drive, got, gotErr, len(opened), openErr)
			}
		})
	}
}

type membershipSet struct {
	seq int64
	set string
}

var membershipVariants = []struct {
	name string
	sets []membershipSet
}{
	{"clean", nil},
	{"late malformed recipients", []membershipSet{{14, `recipients = '['`}}},
	{"early malformed recipients", []membershipSet{{3, `recipients = '['`}}},
	{"stale malformed recipients", []membershipSet{{3, `recipients = '[', at = 0`}}},
	{"origin early malformed recipients", []membershipSet{{11, `recipients = '[', origin = 'feedfacefeedface'`}}},
	{"drive malformed recipients", []membershipSet{{12, `recipients = '['`}}},
	{"late typed peer recipients", []membershipSet{{14, `recipients = '["peer:reader", 1]'`}}},
	{"late scalar peer recipients", []membershipSet{{14, `recipients = '"peer:reader"'`}}},
	{"late object peer recipients", []membershipSet{{14, `recipients = '{"to":"peer:reader"}'`}}},
	{"late json5 peer recipients", []membershipSet{{14, `recipients = '[''peer:reader'']'`}}},
	{"late jsonb peer recipients", []membershipSet{{14, `recipients = jsonb('["peer:reader"]')`}}},
	{"late malformed jsonb recipients", []membershipSet{{14, `recipients = X'1B0D'`}}},
	{"late empty recipients", []membershipSet{{14, `recipients = ''`}}},
	{"late reader json5", []membershipSet{{14, `recipients = '[''api@d'']'`}}},
	{"drive malformed refs", []membershipSet{{12, `refs = '{'`}}},
	{"early malformed also_resolves", []membershipSet{{3, `also_resolves = '['`}}},
	{"late malformed also_resolves", []membershipSet{{14, `also_resolves = '['`}}},
	{"malformed jsonb also_resolves", []membershipSet{{14, `also_resolves = X'1B0D'`}}},
	{"drive typed also_resolves", []membershipSet{{4, `also_resolves = '["oops"]'`}}},
	{"drive null also_resolves element", []membershipSet{{4, `also_resolves = '[null]'`}}},
	{"offdrive spaced target", []membershipSet{{11, `also_resolves = '[" 9"]'`}}},
	{"offdrive real text target", []membershipSet{{11, `also_resolves = '["9.0"]'`}}},
	{"offdrive text target", []membershipSet{{11, `also_resolves = '["9"]'`}}},
	{"offdrive bool target", []membershipSet{{11, `also_resolves = '[true]'`}}},
	{"offdrive real target", []membershipSet{{11, `also_resolves = '[9.0, 9.5]'`}}},
	{"offdrive scalar target", []membershipSet{{11, `also_resolves = '9'`}}},
	{"offdrive jsonb target", []membershipSet{{11, `also_resolves = jsonb('[9]')`}}},
	{"corrupt drive resolves", []membershipSet{{4, `resolves = 'b'`}, {12, `resolves = 'a'`}}},
	{"real drive resolves", []membershipSet{{4, `resolves = 4.5`}}},
}

func seedMembershipFixture(ctx context.Context, t *testing.T, st *Store, now time.Time) {
	t.Helper()
	at := now.Add(-time.Hour).UnixMilli()
	node := PeerAddressOn(st.Self(), "reader")
	for i, row := range []struct {
		drive, lane, kind, recipients string
		also                          any
		resolves                      int64
		refs                          string
	}{
		{"d", "root", "ask", `["api"]`, nil, 0, `{}`},
		{"f", "api", "note", `["peer:reader"]`, nil, 0, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900001]`, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{"stacks":["p/e"]}`},
		{"f", "api", "note", `["` + node + `"]`, nil, 0, `{}`},
		{"f", "api", "note", `["api@d"]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, nil, 1, `{}`},
		{"f", "api", "note", `["root@d"]`, nil, 0, `{}`},
		{"d", "api", "ask", `[]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, `[9]`, 4, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900002]`, 900003, `{}`},
		{"d", "root", "note", `[]`, nil, 0, `{}`},
		{"d", "root", "note", `["peer:reader"]`, nil, 0, `{"stacks":["p/e"]}`},
		{"x", "irrelevant", "note", `["someone"]`, nil, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{}`},
	} {
		res, err := st.db.ExecContext(ctx, `INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, resolves, refs, source, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'post', ?)`,
			row.drive, row.lane, row.kind, at, fmt.Sprintf("fixture %d", i+1), row.recipients, row.also, row.resolves, row.refs, fmt.Sprintf("fixture-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		if seq, err := res.LastInsertId(); err != nil || seq != int64(i+1) {
			t.Fatalf("fixture seq = %d, %v; want %d", seq, err, i+1)
		}
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET origin = 'feedfacefeedface' WHERE seq >= 12"); err != nil {
		t.Fatal(err)
	}
}

func unboundedQuery(ctx context.Context, s *Store, f Filter) ([]Record, error) {
	if f.Drive == "" && f.Peer == "" {
		return nil, ErrNoDrive
	}
	where, params := f.where(s.Self(), s.Now())
	order := "ASC"
	if f.Descending {
		order = "DESC"
	}
	q := "SELECT " + columns + " FROM records WHERE " + where + " ORDER BY seq " + order
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	rows, err := s.db.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, fmt.Errorf("query records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

func unboundedCount(ctx context.Context, s *Store, f Filter) (int, error) {
	where, params := f.where(s.Self(), s.Now())
	var n int
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM records WHERE "+where, params...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count records: %w", err)
	}
	return n, nil
}

func unboundedResolvers(ctx context.Context, s *Store, drive string) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT resolves, seq FROM records WHERE resolves != 0 AND (drive = ? OR resolves IN (SELECT seq FROM records WHERE drive = ?))
UNION ALL SELECT j.value, r.seq FROM records r, json_each(r.also_resolves) j WHERE r.drive = ? OR j.value IN (SELECT seq FROM records WHERE drive = ?)`, drive, drive, drive, drive)
	if err != nil {
		return nil, fmt.Errorf("read resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]int64{}
	for rows.Next() {
		var target, seq int64
		if err := rows.Scan(&target, &seq); err != nil {
			return nil, fmt.Errorf("scan resolver: %w", err)
		}
		out[target] = max(out[target], seq)
	}
	return out, rows.Err()
}

func assertSameRead[T any](t *testing.T, name string, got T, gotErr error, want T, wantErr error) {
	t.Helper()
	if fmt.Sprint(gotErr) != fmt.Sprint(wantErr) || !reflect.DeepEqual(got, want) {
		t.Fatalf("%s = %+v, %v; unbounded predicate = %+v, %v", name, got, gotErr, want, wantErr)
	}
}

func TestMaintainIndexesProvisionsMembershipsWithinWriterBudget(t *testing.T) {
	raw := os.Getenv("CCI_MEMBERSHIP_PROVISION_ROWS")
	if raw == "" {
		t.Skip("CCI_MEMBERSHIP_PROVISION_ROWS sets the record population CI provisions outside the race build")
	}
	population, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatal(err)
	}
	budget := 5 * time.Second
	ctx := context.Background()
	home := t.TempDir()
	st := openMembershipStore(ctx, t, home)
	dropMemberships(ctx, t, st)
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
	VALUES(1) UNION ALL SELECT n + 1 FROM history WHERE n < ?
)
INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, resolves, source, hash)
SELECT 'drive-' || (n % 64), 'lane-' || (n % 16), 'note', n, 'history ' || n,
	json_array('peer:session-' || (n % 997), 'lane-' || (n % 16) || '@drive-' || ((n + 1) % 64)),
	CASE WHEN n % 3 = 0 THEN json_array(n - 1, n - 2) END,
	CASE WHEN n % 3 = 0 THEN n - 1 ELSE 0 END,
	'post', 'history-' || n FROM history`, population); err != nil {
		t.Fatal(err)
	}
	report, err := MaintainIndexes(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	assertMembershipsInstalled(ctx, t, st, true)
	if len(report.Memberships) != len(recordMemberships) || report.Elapsed >= budget {
		t.Fatalf("provisioning %d records = %+v, want every membership object within %s of the 10s writer busy timeout", population, report, budget)
	}
	if _, _, err := st.Post(ctx, Record{Drive: "drive-1", Lane: "after", Kind: kinds.Note, Text: "after provisioning", To: []string{"peer:session-1"}}, 0); err != nil {
		t.Fatal(err)
	}
	assertMembershipsDerived(ctx, t, st)
	var memberships int
	if err := st.db.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM record_recipients) + (SELECT COUNT(*) FROM record_also_resolves)").Scan(&memberships); err != nil {
		t.Fatal(err)
	}
	t.Logf("records=%d memberships=%d provisioning_elapsed=%s writer_budget=%s before=%+v after=%+v", population, memberships, report.Elapsed, budget, report.Before, report.After)
}

func openMembershipStore(ctx context.Context, t *testing.T, home string) *Store {
	t.Helper()
	st, err := Open(ctx, home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func dropMemberships(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	for _, object := range slices.Backward(recordMemberships[:]) {
		if _, err := st.db.ExecContext(ctx, "DROP "+strings.ToUpper(object.kind)+" IF EXISTS "+object.name); err != nil {
			t.Fatal(err)
		}
	}
}

func assertMembershipsInstalled(ctx context.Context, t *testing.T, st *Store, want bool) {
	t.Helper()
	got, err := membershipsInstalled(ctx, st.db)
	if err != nil || got != want {
		t.Fatalf("record memberships installed = %v, %v; want %v", got, err, want)
	}
}

func assertMembership(ctx context.Context, t *testing.T, st *Store, table, key string, seq int64, want int) {
	t.Helper()
	query, args := "SELECT COUNT(*) FROM "+table+" WHERE 1", []any{}
	if key != "" {
		query += " AND quote(" + membershipKeys[table] + ") = ?"
		args = append(args, key)
	}
	if seq != 0 {
		query += " AND source_seq = ?"
		args = append(args, seq)
	}
	var got int
	if err := st.db.QueryRowContext(ctx, query, args...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s rows for %s #%d = %d, want %d", table, key, seq, got, want)
	}
}

func assertMembershipsDerived(ctx context.Context, t *testing.T, st *Store) {
	t.Helper()
	conn, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, object := range recordMemberships {
		if object.fill == "" {
			continue
		}
		expected := "expected_" + object.name
		for _, query := range []string{
			"DROP TABLE IF EXISTS temp." + expected,
			strings.Replace(object.sql, "CREATE TABLE "+object.name, "CREATE TEMP TABLE "+expected, 1),
			strings.Replace(object.fill, "INSERT INTO "+object.name, "INSERT INTO temp."+expected, 1),
		} {
			if _, err := conn.ExecContext(ctx, query); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
		}
		got := membershipRows(ctx, t, conn, "main."+object.name, object.name)
		want := membershipRows(ctx, t, conn, "temp."+expected, object.name)
		if !slices.Equal(got, want) {
			t.Fatalf("%s = %v, want backfill derivation %v", object.name, got, want)
		}
	}
}

func membershipRows(ctx context.Context, t *testing.T, conn *sql.Conn, table, name string) []string {
	t.Helper()
	rows, err := conn.QueryContext(ctx, "SELECT quote("+membershipKeys[name]+") || ' #' || source_seq FROM "+table+" ORDER BY 1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func schemaObjects(ctx context.Context, t *testing.T, st *Store) []string {
	t.Helper()
	rows, err := st.db.QueryContext(ctx, "SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_schema ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var object string
		if err := rows.Scan(&object); err != nil {
			t.Fatal(err)
		}
		out = append(out, object)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func queryPlan(ctx context.Context, t *testing.T, st *Store, query string, args ...any) []string {
	t.Helper()
	rows, err := st.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return plan
}
