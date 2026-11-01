package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/bits"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

type denseReadPlan struct {
	Operation string
	SQL       string
	Args      []any
	Plan      []string
}

type denseReadResult struct {
	Peer             bool
	Indexed          bool
	Operation        string
	Population       int
	Limit            int
	Rows             int
	Output           string
	OutputBytes      int
	TypedRows        string
	Error            string
	Tail             inbox.TailResult
	DriveComparisons int64
	CacheHits        int
	CacheMisses      int
	Plans            []denseReadPlan
}

type denseReadResults struct {
	Fixture string
	Budget  int
	Cases   []denseReadResult
}

func TestCountTimestampDenseQueryTail(t *testing.T) {
	if os.Getenv("CCI_COUNT_TIMESTAMP_DENSE") != "1" {
		t.Skip("the isolated dense workflow supplies the Query/Tail diagnostic")
	}
	const budget = 8 << 20
	actual := denseReadResults{Fixture: "dense-count-v1:drive=d,peer=false-or-reader,index=missing-or-driveAt,since24h,origin=feedfacefeedface,all-note,nonmonotonic-at", Budget: budget, Cases: make([]denseReadResult, 0, 48)}
	for _, peer := range []bool{false, true} {
		for _, indexed := range []bool{false, true} {
			for _, operation := range []string{"query", "tail"} {
				for _, population := range []int{32, 10000} {
					for _, limit := range []int{1, 800, 80000} {
						t.Run(fmt.Sprintf("peer=%t/indexed=%t/%s/N=%d/limit=%d", peer, indexed, operation, population, limit), func(t *testing.T) {
							st, now, measure := store.CountTimestampDenseFixture(t, population, indexed)
							filter := store.Filter{Drive: "d", Since: now.Add(-24 * time.Hour), Descending: true, Limit: limit}
							if peer {
								filter.Peer = "reader"
							}
							result := denseReadResult{Peer: peer, Indexed: indexed, Operation: operation, Population: population, Limit: limit, Plans: make([]denseReadPlan, 0, 2)}
							query, args, details := store.DenseReadDiagnosticPlan(t, st, filter, false)
							result.Plans = append(result.Plans, denseReadPlan{Operation: "query", SQL: query, Args: args, Plan: details})
							if operation == "tail" {
								rest := filter
								rest.After, rest.Limit, rest.Descending = int64(population), 0, false
								query, args, details = store.DenseReadDiagnosticPlan(t, st, rest, true)
								result.Plans = append(result.Plans, denseReadPlan{Operation: "count-after-tail", SQL: query, Args: args, Plan: details})
							}
							for _, plan := range result.Plans {
								t.Logf("DENSE_READ_PLAN operation=%s N=%d limit=%d suboperation=%s sql=%q args=%v plan=%v", operation, population, limit, plan.Operation, plan.SQL, plan.Args, plan.Plan)
							}
							wantRows := min(population, limit)
							if operation == "query" {
								measure(true)
								records, readErr := st.Query(t.Context(), filter)
								result.DriveComparisons, result.CacheHits, result.CacheMisses = measure(false)
								if readErr != nil {
									result.Error = readErr.Error()
									t.Errorf("Query error=%v, want nil", readErr)
								}
								result.Rows, result.TypedRows = len(records), fmt.Sprintf("%#v", records)
								expected := make([]store.Record, 0, wantRows)
								for seq := population; seq > population-wantRows; seq-- {
									expected = append(expected, denseReadRecord(seq, now))
								}
								if !reflect.DeepEqual(records, expected) {
									t.Errorf("Query full ordered records differ: N=%d limit=%d got_rows=%d want_rows=%d", population, limit, len(records), len(expected))
								}
								encoded, err := json.Marshal(records)
								if err != nil {
									t.Fatal(err)
								}
								result.Output, result.OutputBytes = string(encoded), len(encoded)
							} else {
								var output bytes.Buffer
								measure(true)
								tail, readErr := inbox.Tail(t.Context(), st, inbox.TailOptions{Filter: filter, Limit: limit, Budget: budget}, &output)
								result.DriveComparisons, result.CacheHits, result.CacheMisses = measure(false)
								result.Tail, result.Rows = tail, tail.Printed
								result.Output, result.OutputBytes = output.String(), output.Len()
								if readErr != nil {
									result.Error = readErr.Error()
									t.Errorf("Tail error=%v, want nil", readErr)
								}
								var expected strings.Builder
								for seq := population - wantRows + 1; seq <= population; seq++ {
									fmt.Fprintln(&expected, render.Brief(denseReadRecord(seq, now), now, 0))
								}
								wantTail := inbox.TailResult{Printed: wantRows, Last: int64(population), More: 0}
								if tail != wantTail || result.Output != expected.String() {
									t.Errorf("Tail exact result/output differs: result=%+v want=%+v actual_bytes=%d expected_bytes=%d", tail, wantTail, output.Len(), expected.Len())
								}
							}
							if result.CacheHits+result.CacheMisses == 0 {
								t.Fatal("whole API native cache observations are empty")
							}
							t.Logf("DENSE_READ_WORK operation=%s peer=%t indexed=%t N=%d limit=%d rows=%d output_bytes=%d actual_cache_hits=%d actual_cache_misses=%d actual_cache_accesses=%d actual_drive_comparisons=%d authority=whole_public_API_native_DBStatus_drive_collation_is_partial_component", operation, peer, indexed, population, limit, result.Rows, result.OutputBytes, result.CacheHits, result.CacheMisses, result.CacheHits+result.CacheMisses, result.DriveComparisons)
							actual.Cases = append(actual.Cases, result)
						})
					}
				}
			}
		}
	}
	if len(actual.Cases) != 48 {
		t.Fatalf("completed cases=%d, want=48", len(actual.Cases))
	}
	directory := filepath.Join("..", "..", ".ci-count-timestamp-dense")
	filename := "results-read-current.json"
	recording := os.Getenv("CCI_COUNT_TIMESTAMP_RECORD") == "1"
	if recording {
		directory = filepath.Join("..", "..", "..", ".ci-count-timestamp-dense")
		filename = "results-read-old.json"
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
	encoded, err = os.ReadFile(filepath.Join("..", "..", ".ci-count-timestamp-dense", "results-read-old.json"))
	if err != nil {
		t.Fatal(err)
	}
	var old denseReadResults
	if err := json.Unmarshal(encoded, &old); err != nil {
		t.Fatal(err)
	}
	if old.Fixture != actual.Fixture || old.Budget != actual.Budget || len(old.Cases) != len(actual.Cases) {
		t.Fatal("immutable/current fixture identity or case coverage differs")
	}
	for i, result := range actual.Cases {
		previous := old.Cases[i]
		allowance := int64(16*bits.Len(uint(result.Population)) + 32)
		if int64(result.CacheHits+result.CacheMisses) > int64(previous.CacheHits+previous.CacheMisses)+allowance || result.DriveComparisons > previous.DriveComparisons+allowance {
			t.Errorf("COUNT_TIMESTAMP_DENSE_WORK_REGRESSION control=same_config operation=%s peer=%t indexed=%t N=%d limit=%d old_cache=%d new_cache=%d old_drive=%d new_drive=%d allowance=%d", result.Operation, result.Peer, result.Indexed, result.Population, result.Limit, previous.CacheHits+previous.CacheMisses, result.CacheHits+result.CacheMisses, previous.DriveComparisons, result.DriveComparisons, allowance)
		}

		for p, plan := range result.Plans {
			if plan.Operation == "query" {
				if plan.SQL != previous.Plans[p].SQL || denseArgs(t, plan.Args) != denseArgs(t, previous.Plans[p].Args) {
					t.Errorf("Query SQL/params changed peer=%t indexed=%t N=%d limit=%d", result.Peer, result.Indexed, result.Population, result.Limit)
				}
			}
		}
		if result.Indexed {
			for _, missing := range old.Cases {
				if !missing.Indexed && missing.Peer == result.Peer && missing.Operation == result.Operation && missing.Population == result.Population && missing.Limit == result.Limit {
					outputAllowance := int64(0)
					if result.Limit >= result.Population {
						outputAllowance = int64(8 * result.Rows)
					}
					if int64(result.CacheHits+result.CacheMisses) > int64(missing.CacheHits+missing.CacheMisses)+allowance+outputAllowance || result.DriveComparisons > missing.DriveComparisons+allowance+outputAllowance {
						t.Errorf("COUNT_TIMESTAMP_DENSE_WORK_REGRESSION control=old_missing operation=%s peer=%t N=%d limit=%d old_cache=%d new_cache=%d old_drive=%d new_drive=%d allowance=%d", result.Operation, result.Peer, result.Population, result.Limit, missing.CacheHits+missing.CacheMisses, result.CacheHits+result.CacheMisses, missing.DriveComparisons, result.DriveComparisons, allowance)
					}

					introducedSort := denseSort(result.Plans[0].Plan) && !denseSort(missing.Plans[0].Plan)
					if introducedSort {
						t.Errorf("COUNT_TIMESTAMP_INDEX_INTRODUCED_QUERY_SORT operation=%s peer=%t N=%d limit=%d old_missing_cache=%d current_present_cache=%d", result.Operation, result.Peer, result.Population, result.Limit, missing.CacheHits+missing.CacheMisses, result.CacheHits+result.CacheMisses)
					}
				}
			}
		}

		if previous.Peer != result.Peer || previous.Indexed != result.Indexed || previous.Operation != result.Operation || previous.Population != result.Population || previous.Limit != result.Limit || previous.Rows != result.Rows || previous.OutputBytes != result.OutputBytes || previous.Output != result.Output || previous.TypedRows != result.TypedRows || previous.Error != result.Error || previous.Tail != result.Tail {
			t.Errorf("immutable/current full rows/bytes/errors differ operation=%s N=%d limit=%d", result.Operation, result.Population, result.Limit)
		}
		t.Logf("DENSE_READ_PAIR operation=%s peer=%t indexed=%t N=%d limit=%d old_cache_accesses=%d current_cache_accesses=%d old_drive_comparisons=%d current_drive_comparisons=%d output_bytes=%d", result.Operation, result.Peer, result.Indexed, result.Population, result.Limit, previous.CacheHits+previous.CacheMisses, result.CacheHits+result.CacheMisses, previous.DriveComparisons, result.DriveComparisons, result.OutputBytes)
	}
}

func denseReadRecord(seq int, now time.Time) store.Record {
	return store.Record{Seq: int64(seq), Origin: "feedfacefeedface", OriginSeq: int64(seq), Num: fmt.Sprintf("#%d@feedface", seq), Drive: "d", Lane: "writer", Kind: kinds.Note, At: now.Add(-time.Hour - time.Duration((seq*7919)%10000)*time.Second), Text: fmt.Sprintf("pure port dense %d", seq), Source: "post"}
}

func denseArgs(t *testing.T, args []any) string {
	t.Helper()
	raw, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func denseSort(plan []string) bool {
	for _, line := range plan {
		if strings.Contains(line, "USE TEMP B-TREE") && strings.Contains(line, "ORDER BY") {
			return true
		}
	}
	return false
}
