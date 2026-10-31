package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSelectedRecordsPKWorkStaysBounded(t *testing.T) {
	for _, count := range []int{32, 10000} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			st, comparisons, seqs := selectedSequenceStore(t, count)
			records, err := st.RecordsAmong(t.Context(), "drive", seqs)
			if err != nil {
				t.Fatal(err)
			}
			if len(records) != 2 || records[0].Seq != 1 || records[1].Seq != int64(count) {
				t.Fatalf("selected records = %+v", records)
			}
			if records[0].ExpiresAt == nil || records[0].Num != "#1" || records[1].Num != fmt.Sprintf("#%d", count) {
				t.Fatalf("selected expired record or numbering lost: %+v", records)
			}
			if got := comparisons.Load(); got != 3 {
				t.Fatalf("selected lookup compared drive %d times for %d history rows, want 3", got, count)
			}
			t.Logf("history=%d selected=%d actual_drive_comparisons=%d", count, len(records), comparisons.Load())
			assertSelectedPKPlan(t, st, seqs)
		})
	}
}

func assertSelectedPKPlan(t *testing.T, st *Store, seqs []int64) {
	t.Helper()
	ids, _ := json.Marshal(seqs)
	rows, err := st.db.QueryContext(t.Context(), "EXPLAIN QUERY PLAN "+selectedRecordsSQL, string(ids), "drive")
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
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "SEARCH records USING INTEGER PRIMARY KEY") || strings.Contains(joined, "SCAN records") {
		t.Fatalf("selected query is not a PK seek:\n%s", joined)
	}
	t.Log(joined)
}

func TestSelectedRecordsValidationScopeAndContext(t *testing.T) {
	st, comparisons, seqs := selectedSequenceStore(t, 32)
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq IN (16,33)"); err != nil {
		t.Fatal(err)
	}
	if records, err := st.RecordsAmong(t.Context(), "drive", seqs); err != nil || len(records) != 2 {
		t.Fatalf("unselected or other-drive corruption reached selected query: %v, %v", records, err)
	}
	if _, err := st.RecordsAmong(t.Context(), "drive", []int64{16}); err == nil {
		t.Fatal("selected malformed refs were accepted")
	}
	comparisons.Store(0)
	if records, err := st.RecordsAmong(t.Context(), "drive", nil); err != nil || len(records) != 0 || comparisons.Load() != 0 {
		t.Fatalf("empty selection = %v, %v, comparisons=%d", records, err, comparisons.Load())
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, selection := range [][]int64{nil, {1, 32}} {
		if _, err := st.RecordsAmong(ctx, "drive", selection); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled selection %v = %v", selection, err)
		}
	}
	if _, err := st.RecordsAmong(t.Context(), "", []int64{1}); !errors.Is(err, ErrNoDrive) {
		t.Fatalf("empty drive = %v", err)
	}
}

func TestSelectedRecordsUseInternalSequencesAndOriginNumbers(t *testing.T) {
	st, _, _ := selectedSequenceStore(t, 32)
	st.SetOwner(false)
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET origin='bbbbbbbbbbbbbbbb',origin_seq=7 WHERE seq=32"); err != nil {
		t.Fatal(err)
	}
	records, err := st.RecordsAmong(t.Context(), "drive", []int64{32})
	if err != nil || len(records) != 1 || records[0].Seq != 32 || records[0].Num != "#7@bbbbbbbb" {
		t.Fatalf("internal selection returned %v, %v", records, err)
	}
}
