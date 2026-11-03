package store

import (
	"context"
	"fmt"
	"math/bits"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestAPostToAStemReadsTheSameRegistryAtOneAndAThousandLanes(t *testing.T) {
	ctx := context.Background()
	counts := map[int]int64{}
	for _, population := range []int{1, 1000} {
		t.Run(fmt.Sprintf("lanes=%d", population), func(t *testing.T) {
			var comparisons atomic.Int64
			collation := fmt.Sprintf("cci_lane_holds_%d", laneCollationID.Add(1))
			if err := sqlite.RegisterCollationUtf8(collation, func(a, b string) int {
				comparisons.Add(1)
				return strings.Compare(a, b)
			}); err != nil {
				t.Fatal(err)
			}
			st, err := Open(ctx, t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if _, err := st.db.ExecContext(ctx, "DROP TABLE lane_holds"); err != nil {
				t.Fatal(err)
			}
			for _, object := range laneHoldObjects[:3] {
				definition := object.sql
				for _, column := range []string{"stem", "lane", "successor"} {
					definition = strings.Replace(definition, " "+column+" TEXT", " "+column+" TEXT COLLATE "+collation, 1)
				}
				if _, err := st.db.ExecContext(ctx, definition); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < ?)
INSERT INTO records (drive, lane, kind, at, text, source, hash) SELECT 'd', 'other-' || i || '-1', 'state', 1, 'up', 'post', 'other-' || i FROM n`, population); err != nil {
				t.Fatal(err)
			}
			var registered int
			if err := st.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM lane_holds WHERE drive = 'd'").Scan(&registered); err != nil || registered != population {
				t.Fatalf("registered lanes = %d, %v; want %d", registered, err, population)
			}
			for _, lane := range []string{"sv-2", "sv-3"} {
				if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: lane, Kind: kinds.State, Text: lane + " up"}, 0); err != nil {
					t.Fatal(err)
				}
			}
			comparisons.Store(0)
			reach, err := st.Reach(ctx, "d", "sv")
			if err != nil || reach.Lane != "sv-3" {
				t.Fatalf("Reach(sv) = %+v, %v; want sv-3", reach, err)
			}
			if _, _, err := st.Post(ctx, Record{Drive: "d", Lane: "root", Kind: kinds.Note, To: []string{"sv"}, Text: "for the stem"}, 0); err != nil {
				t.Fatal(err)
			}
			aliases, err := st.Aliases(ctx, "d", "sv-3")
			if err != nil || len(aliases) != 1 || aliases[0] != "sv" {
				t.Fatalf("Aliases(sv-3) = %v, %v; want [sv]", aliases, err)
			}
			counts[population] = comparisons.Load()
			t.Logf("LANE_HOLDS_POST_COST registered=%d registry_comparisons=%d", population, counts[population])
		})
	}
	if grew, bound := counts[1000]-counts[1], int64(16*bits.Len(1000)); grew > bound {
		t.Fatalf("one post's registry comparisons grew by %d from 1 to 1,000 registered lanes (%d to %d); want at most the %d an indexed lookup's depth adds", grew, counts[1], counts[1000], bound)
	}
}
