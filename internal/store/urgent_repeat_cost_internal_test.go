package store

import (
	"fmt"
	"math/bits"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestUrgentRepeatIndexDiagnostic(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_urgent_topic_%d", laneCollationID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			st := countedColumnsStore(t, collation, "drive", "topic")
			now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
			st.Now = func() time.Time { return now }
			ctx := t.Context()
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
SELECT 'd','worker','urgent',?,'unrouted','topic-'||n,'["elsewhere"]','post','urgent-history-'||n FROM history`, population, now.UnixMilli()); err != nil {
				t.Fatal(err)
			}
			if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
VALUES ('d','worker','urgent',?,'selected','selected-topic','["root"]','post','urgent-selected')`, now.UnixMilli()); err != nil {
				t.Fatal(err)
			}
			filter := (Subscription{Drive: "d", Session: "reader", Reader: "root", Kinds: []kinds.Kind{kinds.Urgent}, ToKinds: DefaultToKinds("root")}).Filter()
			filter.After = int64(population)
			filter.Limit = 500
			bound := int64(4*bits.Len(uint(population)) + 8)
			var baseline []Record
			for _, index := range []struct {
				name, sql, planIndex string
			}{
				{"canonical", "", "records_urgent_repeat"},
				{"stock", "DROP INDEX IF EXISTS records_urgent_repeat", ""},
				{"sequence", "CREATE INDEX urgent_repeat_trial ON records(drive,lane,kind,topic,seq,at)", "urgent_repeat_trial"},
				{"time", "CREATE INDEX urgent_repeat_trial ON records(drive,lane,kind,topic,at,seq)", "urgent_repeat_trial"},
			} {
				if os.Getenv("CCI_URGENT_REPEAT_REQUIRE_STOCK_BOUND") == "1" && index.name != "stock" {
					continue
				}
				if index.sql != "" {
					if _, err := st.db.ExecContext(ctx, index.sql); err != nil {
						t.Fatal(err)
					}
				}
				comparisons.Store(0)
				got, err := st.Query(ctx, filter)
				work := comparisons.Load()
				if err != nil || len(got) != 1 || got[0].Seq != int64(population+1) {
					t.Fatalf("index=%s records=%v err=%v", index.name, got, err)
				}
				if baseline == nil {
					baseline = got
				} else if !reflect.DeepEqual(got, baseline) {
					t.Fatalf("index=%s changed complete records", index.name)
				}
				query, args := st.selection(filter)
				plan := queryPlan(ctx, t, st, query, args...)
				t.Logf("URGENT_REPEAT_WORK history=%d index=%s drive_topic_comparisons=%d bound=%d plan=%v", population, index.name, work, bound, plan)
				if work == 0 {
					t.Fatalf("urgent repeat comparison counter inactive: history=%d index=%s", population, index.name)
				}
				if (index.name != "stock" || os.Getenv("CCI_URGENT_REPEAT_REQUIRE_STOCK_BOUND") == "1") && work > bound {
					t.Errorf("URGENT_REPEAT_BOUND_EXCEEDED history=%d index=%s drive_topic_comparisons=%d bound=%d", population, index.name, work, bound)
				}
				if index.planIndex != "" {
					if !strings.Contains(strings.Join(plan, "\n"), "SEARCH prior USING COVERING INDEX "+index.planIndex) {
						t.Errorf("index=%s repeat lookup did not select trial index: %v", index.name, plan)
					}
				}
				if index.name == "sequence" || index.name == "time" {
					if _, err := st.db.ExecContext(ctx, "DROP INDEX urgent_repeat_trial"); err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestUrgentRepeatIndexParity(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st.Now = func() time.Time { return now }
	if _, err := st.db.ExecContext(ctx, "DROP INDEX IF EXISTS records_urgent_repeat"); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		drive, lane, topic, recipients string
		at                             int64
	}{
		{"d", "worker", "suppressed", `["elsewhere"]`, now.UnixMilli()},
		{"d", "worker", "suppressed", `["root"]`, now.UnixMilli()},
		{"d", "worker", "outside-window", `["elsewhere"]`, now.Add(-UrgentRepeat).UnixMilli()},
		{"d", "worker", "outside-window", `["root"]`, now.UnixMilli()},
		{"foreign", "worker", "foreign-drive", `["elsewhere"]`, now.UnixMilli()},
		{"d", "worker", "foreign-drive", `["root"]`, now.UnixMilli()},
		{"d", "other-lane", "other-lane", `["elsewhere"]`, now.UnixMilli()},
		{"d", "worker", "other-lane", `["root"]`, now.UnixMilli()},
		{"d", "worker", "", `["root"]`, now.UnixMilli()},
		{"d", "worker", "both-routes", `["root","all"]`, now.UnixMilli()},
	} {
		if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
VALUES (?,?,'urgent',?,'payload',?,?,'post',?)`, row.drive, row.lane, row.at, row.topic, row.recipients, fmt.Sprintf("urgent-parity-%s-%s-%d-%s", row.drive, row.lane, row.at, row.recipients+row.topic)); err != nil {
			t.Fatal(err)
		}
	}
	filters := []Filter{
		{Drive: "d", For: "root", Peer: "reader", RepeatWindow: UrgentRepeat},
		{Drive: "d", For: "root", Kinds: []kinds.Kind{kinds.Note}, ToKinds: []kinds.Kind{kinds.Urgent}, RepeatWindow: UrgentRepeat},
		{Drive: "d", For: "root", Kinds: []kinds.Kind{kinds.Urgent}, ToKinds: []kinds.Kind{kinds.Note}, RepeatWindow: UrgentRepeat},
		{Drive: "d", For: "root", Mute: []kinds.Kind{kinds.Urgent}, RepeatWindow: UrgentRepeat},
		{Drive: "d", For: "root", After: 3, Before: 10, Limit: 2, Descending: true, RepeatWindow: UrgentRepeat},
	}
	for _, mutation := range []string{
		"SELECT 1",
		"UPDATE records SET topic='reparsed' WHERE seq=1",
		"UPDATE records SET topic='suppressed',at=0 WHERE seq=1",
		fmt.Sprintf("UPDATE records SET lane='changed',at=%d WHERE seq=1", now.UnixMilli()),
		"DELETE FROM records WHERE seq=1",
	} {
		if _, err := st.db.ExecContext(ctx, mutation); err != nil {
			t.Fatal(err)
		}
		for i, filter := range filters {
			baseline, baselineErr := st.Query(ctx, filter)
			count, countErr := st.Count(ctx, filter)
			for _, order := range []string{"seq,at", "at,seq"} {
				if _, err := st.db.ExecContext(ctx, "CREATE INDEX urgent_repeat_trial ON records(drive,lane,kind,topic,"+order+")"); err != nil {
					t.Fatal(err)
				}
				got, gotErr := st.Query(ctx, filter)
				assertSameRead(t, "urgent repeat query", got, gotErr, baseline, baselineErr)
				gotCount, gotCountErr := st.Count(ctx, filter)
				assertSameRead(t, "urgent repeat count", gotCount, gotCountErr, count, countErr)
				if _, err := st.db.ExecContext(ctx, "DROP INDEX urgent_repeat_trial"); err != nil {
					t.Fatal(err)
				}
			}
			if mutation == "SELECT 1" && i == 0 {
				var seqs []int64
				for _, record := range baseline {
					seqs = append(seqs, record.Seq)
				}
				if baselineErr != nil || !slices.Equal(seqs, []int64{4, 6, 8, 9, 10}) {
					t.Fatalf("canonical repeat oracle seqs=%v err=%v", seqs, baselineErr)
				}
			}
		}
	}
}

func TestUrgentRepeatIndexErrorFrontier(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	now := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	st.Now = func() time.Time { return now }
	if _, err := st.db.ExecContext(ctx, "DROP INDEX IF EXISTS records_urgent_repeat"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE selected(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM selected WHERE n<500
)
INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
SELECT 'd','worker','urgent',?,'selected','frontier-topic-'||n,'["root"]','post','frontier-'||n FROM selected`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, `INSERT INTO records (drive,lane,kind,at,text,topic,recipients,source,hash)
VALUES ('d','worker','urgent',?,'unread corruption','unread-topic','{','post','frontier-corrupt')`, now.UnixMilli()); err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{500, 0} {
		filter := Filter{Drive: "d", For: "root", Peer: "reader", RepeatWindow: UrgentRepeat, Limit: limit}
		baseline, baselineErr := st.Query(ctx, filter)
		if limit == 500 && (baselineErr != nil || len(baseline) != 500) || limit == 0 && baselineErr == nil {
			t.Fatalf("stock frontier limit=%d records=%d err=%v", limit, len(baseline), baselineErr)
		}
		for _, order := range []string{"seq,at", "at,seq"} {
			if _, err := st.db.ExecContext(ctx, "CREATE INDEX urgent_repeat_trial ON records(drive,lane,kind,topic,"+order+")"); err != nil {
				t.Fatal(err)
			}
			got, gotErr := st.Query(ctx, filter)
			assertSameRead(t, "urgent repeat error frontier", got, gotErr, baseline, baselineErr)
			if _, err := st.db.ExecContext(ctx, "DROP INDEX urgent_repeat_trial"); err != nil {
				t.Fatal(err)
			}
		}
	}
}
