package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

type denseGrepPlan struct {
	Before int64
	SQL    string
	Args   []any
	Plan   []string
}

type denseGrepResult struct {
	Population       int
	Limit            int
	Rows             int
	Output           string
	OutputBytes      int
	DriveComparisons int64
	CacheHits        int
	CacheMisses      int
	Plans            []denseGrepPlan
}

type denseGrepResults struct {
	Fixture string
	Budget  int
	Cases   []denseGrepResult
}

func TestDenseRecentPagedGrepDiagnostic(t *testing.T) {
	if os.Getenv("CCI_TIME_DENSE_GREP_DIAGNOSTIC") != "1" {
		t.Skip("the isolated dense-Grep workflow supplies the diagnostic gate")
	}
	const budget = 8 << 20
	actual := denseGrepResults{Fixture: "dense-recent-v1:drive=d,peer=reader,origin=feedfacefeedface,all-note,all-match,nonmonotonic-at", Budget: budget, Cases: make([]denseGrepResult, 0, 6)}
	for _, population := range []int{32, 10000} {
		for _, limit := range []int{1, 800, 80000} {
			t.Run(fmt.Sprintf("N=%d/limit=%d", population, limit), func(t *testing.T) {
				st, now, measure, plan := store.DenseGrepDiagnosticFixture(t, population)
				filter := store.Filter{Drive: "d", Peer: "reader", Since: now.Add(-24 * time.Hour), Descending: true, Limit: 500}
				result := denseGrepResult{Population: population, Limit: limit, Rows: min(population, limit), Plans: make([]denseGrepPlan, 0, 3)}
				before := []int64{0}
				if population >= 500 {
					before = append(before, int64(population-499), 1)
				}
				for _, cursor := range before {
					page := filter
					page.Before = cursor
					sql, args, details := plan(page)
					result.Plans = append(result.Plans, denseGrepPlan{Before: cursor, SQL: sql, Args: args, Plan: details})
					t.Logf("DENSE_GREP_PLAN N=%d limit=%d before=%d sql=%q args=%v plan=%v", population, limit, cursor, sql, args, details)
				}
				var output bytes.Buffer
				measure(true)
				err := inbox.Grep(t.Context(), st, filter, regexp.MustCompile("pure port"), limit, budget, false, &output)
				result.DriveComparisons, result.CacheHits, result.CacheMisses = measure(false)
				if err != nil {
					t.Fatal(err)
				}
				var expected strings.Builder
				for seq := population; seq > population-result.Rows; seq-- {
					record := store.Record{Seq: int64(seq), Origin: "feedfacefeedface", OriginSeq: int64(seq), Num: fmt.Sprintf("#%d@feedface", seq), Drive: "d", Lane: "writer", Kind: kinds.Note, At: now.Add(-time.Hour - time.Duration((seq*7919)%10000)*time.Second), Text: fmt.Sprintf("pure port dense %d", seq), Source: "post"}
					fmt.Fprintln(&expected, render.Line(record, now))
				}
				if population > result.Rows {
					fmt.Fprintf(&expected, "... %d more matches; narrow with --kind, --lane or --since\n", population-result.Rows)
				}
				result.Output, result.OutputBytes = output.String(), output.Len()
				if result.Output != expected.String() {
					t.Errorf("Grep output does not match %d descending rows plus exact trailer: actual_bytes=%d expected_bytes=%d", result.Rows, output.Len(), expected.Len())
				}
				if result.CacheHits+result.CacheMisses == 0 {
					t.Fatal("whole Grep native cache observations are empty")
				}
				t.Logf("DENSE_GREP_WORK N=%d limit=%d rows=%d output_bytes=%d actual_cache_hits=%d actual_cache_misses=%d actual_cache_accesses=%d actual_drive_comparisons=%d authority=whole_inbox_Grep_native_DBStatus_cache_drive_collation_is_partial_component", population, limit, result.Rows, result.OutputBytes, result.CacheHits, result.CacheMisses, result.CacheHits+result.CacheMisses, result.DriveComparisons)
				actual.Cases = append(actual.Cases, result)
			})
		}
	}
	if len(actual.Cases) != 6 {
		t.Fatalf("completed cases=%d, want=6", len(actual.Cases))
	}
	directory := filepath.Join("..", "..", ".ci-time-dense-grep")
	filename := "results-current.json"
	recording := os.Getenv("CCI_TIME_DENSE_GREP_RECORD") == "1"
	if recording {
		directory = filepath.Join("..", "..", "..", ".ci-time-dense-grep")
		filename = "results-old.json"
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(actual, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, filename), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if recording {
		return
	}
	encoded, err = os.ReadFile(filepath.Join("..", "..", ".ci-time-dense-grep", "results-old.json"))
	if err != nil {
		t.Fatal(err)
	}
	var old denseGrepResults
	if err := json.Unmarshal(encoded, &old); err != nil {
		t.Fatal(err)
	}
	if old.Fixture != actual.Fixture || old.Budget != actual.Budget || len(old.Cases) != len(actual.Cases) {
		t.Fatal("immutable/current fixture identity or case coverage differs")
	}
	for i, result := range actual.Cases {
		previous := old.Cases[i]
		if previous.Population != result.Population || previous.Limit != result.Limit || previous.Rows != result.Rows || previous.OutputBytes != result.OutputBytes || previous.Output != result.Output {
			t.Errorf("immutable/current exact output differs N=%d limit=%d", result.Population, result.Limit)
		}
		t.Logf("DENSE_GREP_PAIR N=%d limit=%d old_cache_accesses=%d current_cache_accesses=%d old_drive_comparisons=%d current_drive_comparisons=%d output_bytes=%d", result.Population, result.Limit, previous.CacheHits+previous.CacheMisses, result.CacheHits+result.CacheMisses, previous.DriveComparisons, result.DriveComparisons, result.OutputBytes)
	}
}
