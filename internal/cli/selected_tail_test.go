package cli_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func selectedTailArgs(drive, seqs string) []string {
	return []string{"tail", "--drive", drive, "--since-seq", "0", "--seqs", seqs, "--json", "--budget", "0"}
}

func selectedTailParity(t *testing.T, handler http.Handler, fixture nativeTailFixture, call nativeTailCall) nativeTailResult {
	t.Helper()
	want := runNativeTailReference(t, call.args)
	got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, call)))
	if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
		t.Fatalf("selected CLI/native mismatch: code=%d/%d stdout=%q/%q stderr=%q/%q", got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
	}
	return got
}

func TestTailSelectedRecordsNativeContract(t *testing.T) {
	call := nativeTailCall{claude: "reader", agent: "worker", args: selectedTailArgs("d", "17,1,12,4,15,9,7,16,999999,17,1")}
	fixture := newNativeTailFixture(t, call, nil)
	for _, query := range []string{
		`UPDATE records SET kind='ask',source='import:fixture' WHERE seq=12`,
		`UPDATE records SET id='selected-note',fields='{"attrs":{"scope":"selected"}}',expires_at=1 WHERE seq=15`,
		`UPDATE records SET recipients='[' WHERE seq=13`,
		`INSERT INTO cursors(name,drive,seq) VALUES('selected-cursor','d',8)`,
	} {
		if _, err := fixture.db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	records := nativeTailRows(t, selectedTailParity(t, handler, fixture, call))
	seqs := []int64{1, 4, 9, 12, 15, 17}
	statuses := []string{"closed", "", "closed", "imported", "", "open"}
	if len(records) != len(seqs) {
		t.Fatalf("selected rows=%+v; want internal sequences %v", records, seqs)
	}
	for i, record := range records {
		if record.Seq != seqs[i] || record.Drive != "d" || record.Status != statuses[i] {
			t.Fatalf("selected row %d=%+v; want seq=%d status=%q", i, record, seqs[i], statuses[i])
		}
	}
	if !slices.Equal(records[0].To, []string{"api"}) || records[3].Origin != "feedfacefeedface" || records[3].OriginSeq != 12 || records[3].Num != "#12@feedface" {
		t.Fatalf("selected metadata changed: first=%+v imported=%+v", records[0], records[3])
	}
	if record := records[4]; record.ID != "selected-note" || record.Fields.Attrs["scope"] != "selected" || record.ExpiresAt == nil || record.ExpiresAt.UnixMilli() != 1 {
		t.Fatalf("selected expired metadata changed: %+v", record)
	}
	if len(records[5].Refs.PRs) != 1 || records[5].Refs.PRs[0].Number != 29020 {
		t.Fatalf("selected refs changed: %+v", records[5])
	}
	var cursor int64
	if err := fixture.db.QueryRowContext(t.Context(), "SELECT seq FROM cursors WHERE name='selected-cursor' AND drive='d'").Scan(&cursor); err != nil || cursor != 8 {
		t.Fatalf("selected mode changed cursor: %d %v", cursor, err)
	}
	call.args = selectedTailArgs("d", "")
	if rows := nativeTailRows(t, selectedTailParity(t, handler, fixture, call)); len(rows) != 0 {
		t.Fatalf("explicit empty selection returned rows: %+v", rows)
	}
}

func TestTailSelectedInternalSequencesWithMixedOriginNumbers(t *testing.T) {
	call := nativeTailCall{claude: "reader", tier: "service"}
	fixture := newNativeTailFixture(t, call, nil)
	first := seedNativeTailMixedOrigins(t, fixture)
	call.args = selectedTailArgs("mixed", fmt.Sprintf("%d,%d,%d,%d", first+2, first, first+1, first))
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	records := nativeTailRows(t, selectedTailParity(t, handler, fixture, call))
	if len(records) != 3 || records[0].Seq != first || records[1].Seq != first+1 || records[2].Seq != first+2 {
		t.Fatalf("selected mixed origins lost internal ordering: %+v", records)
	}
	if records[0].Num != "#2" || records[1].Num != fmt.Sprintf("#%d@%s", first+1, fixture.node[:8]) || records[2].Num != "#100@bbbbbbbb" {
		t.Fatalf("selected mode changed human numbering: %+v", records)
	}
	call.args = selectedTailArgs("mixed", "2,100")
	if rows := nativeTailRows(t, selectedTailParity(t, handler, fixture, call)); len(rows) != 0 {
		t.Fatalf("human numbers were converted into internal sequences: %+v", rows)
	}
}

func TestTailSelectedExternalResolveUpdateDeleteAndReinsert(t *testing.T) {
	call := nativeTailCall{claude: "reader", args: selectedTailArgs("d", "17")}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	read := func(status, text string) {
		t.Helper()
		records := nativeTailRows(t, selectedTailParity(t, handler, fixture, call))
		if len(records) != 1 || records[0].Seq != 17 || records[0].Status != status || records[0].Text != text {
			t.Fatalf("selected mutation was stale: %+v; want status=%q text=%q", records, status, text)
		}
	}
	read("open", "hold #29020")
	result, err := fixture.db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,resolves,source,hash) VALUES('other','closer','done',4102444800000,'external close',17,'post','selected-close')`)
	if err != nil {
		t.Fatal(err)
	}
	closer, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	read("closed", "hold #29020")
	if _, err := fixture.db.ExecContext(t.Context(), "DELETE FROM records WHERE seq=?", closer); err != nil {
		t.Fatal(err)
	}
	read("open", "hold #29020")
	if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET kind='note',text='external update',expires_at=1 WHERE seq=17"); err != nil {
		t.Fatal(err)
	}
	read("", "external update")
	if _, err := fixture.db.ExecContext(t.Context(), "DELETE FROM records WHERE seq=17"); err != nil {
		t.Fatal(err)
	}
	if records := nativeTailRows(t, selectedTailParity(t, handler, fixture, call)); len(records) != 0 {
		t.Fatalf("deleted selected record remained visible: %+v", records)
	}
	if _, err := fixture.db.ExecContext(t.Context(), `INSERT INTO records(seq,drive,lane,kind,at,text,source,hash) VALUES(17,'d','held','hold',4102444800000,'reinserted','post','selected-reinsert')`); err != nil {
		t.Fatal(err)
	}
	read("open", "reinserted")
}

func TestTailSelectedNoDefaultLimitOrResponseBudget(t *testing.T) {
	call := nativeTailCall{args: selectedTailArgs("many", "")}
	fixture := newNativeTailFixture(t, call, nil)
	tx, err := fixture.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	var ids []string
	for i := range 32 {
		result, err := tx.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,source,hash) VALUES('many','all','note',4102444800000,?,'post',?)`, strings.Repeat("x", 2048), fmt.Sprintf("selected-many-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, strconv.FormatInt(seq, 10))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	slices.Reverse(ids)
	call.args = selectedTailArgs("many", strings.Join(ids, ","))
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	result := selectedTailParity(t, handler, fixture, call)
	records := nativeTailRows(t, result)
	if len(records) != 32 || len(result.Out) < 64<<10 {
		t.Fatalf("selected output was limited: rows=%d bytes=%d", len(records), len(result.Out))
	}
	for i, record := range records {
		if i > 0 && records[i-1].Seq >= record.Seq || len(record.Text) != 2048 {
			t.Fatalf("selected complete output lost order or data at %d: %+v", i, record)
		}
	}
}

func TestTailSelectedRejectsInvalidAndIncompatibleModesWithoutWrites(t *testing.T) {
	call := nativeTailCall{claude: "reader", args: selectedTailArgs("d", "17")}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	for _, value := range []string{"0", "-1", "1,0", "1.5", "#17", "9223372036854775808", "not-a-seq"} {
		t.Run(value, func(t *testing.T) {
			bad := call
			bad.args = selectedTailArgs("d", value)
			result := selectedTailParity(t, handler, fixture, bad)
			if result.Code == 0 || len(result.Out) != 0 {
				t.Fatalf("invalid sequence accepted: %+v", result)
			}
		})
	}
	for _, extra := range [][]string{
		{"--cursor", "other"},
		{"--reader", "root"},
		{"--to", "api"},
		{"--kind", "hold"},
		{"--mute-kinds", "note"},
		{"--to-kind", "hold"},
		{"--lane", "api"},
		{"--topic", "topic"},
		{"--stack", "p/e"},
		{"--target", "prod"},
		{"--pr", "29020"},
		{"--origin", "feedfacefeedface"},
		{"--limit", "0"},
		{"--width", "0"},
		{"--all=false"},
		{"--since", "0"},
		{"--json=false"},
		{"--budget", "1"},
		{"--since-seq", "1"},
	} {
		t.Run(strings.Join(extra, " "), func(t *testing.T) {
			bad := call
			bad.args = append(selectedTailArgs("d", "17"), extra...)
			result := selectedTailParity(t, handler, fixture, bad)
			if result.Code == 0 || len(result.Out) != 0 {
				t.Fatalf("incompatible selected mode accepted: %+v", result)
			}
		})
	}
	for _, args := range [][]string{
		{"tail", "--since-seq", "0", "--seqs", "17", "--json", "--budget", "0"},
		{"tail", "--drive", "d", "--seqs", "17", "--json", "--budget", "0"},
		{"tail", "--drive", "", "--since-seq", "0", "--seqs", "17", "--json", "--budget", "0"},
	} {
		if result := runNativeTailReference(t, args); result.Code == 0 || len(result.Out) != 0 {
			t.Fatalf("missing explicit selected-mode scope accepted: %+v", result)
		}
		bad := call
		bad.args = args
		if response := nativeTailResponse(t, handler, nativeTailBody(t, fixture, bad)); response.Code != http.StatusBadRequest {
			t.Fatalf("native selected request missing scope accepted: status=%d body=%s", response.Code, response.Body.String())
		}
	}
}

func TestTailSelectedCorruptionRemainsVisible(t *testing.T) {
	call := nativeTailCall{args: selectedTailArgs("d", "17")}
	fixture := newNativeTailFixture(t, call, nil)
	if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=17"); err != nil {
		t.Fatal(err)
	}
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	result := selectedTailParity(t, handler, fixture, call)
	if result.Code == 0 || len(result.Out) != 0 || len(result.Err) == 0 {
		t.Fatalf("selected malformed row was hidden: %+v", result)
	}
}

func TestTailSelectedBinaryCLIParityAndOldUnknownFlag(t *testing.T) {
	old, current := os.Getenv("CCI_SELECTED_OLD_CLI"), os.Getenv("CCI_SELECTED_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable old480 and current CLI binaries")
	}
	call := nativeTailCall{claude: "reader", args: selectedTailArgs("d", "17,1,4,17,999999")}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	want := selectedTailParity(t, handler, fixture, call)
	if want.Code != 0 {
		t.Fatalf("current selected-mode fixture failed: %+v", want)
	}
	invoke := func(binary string) nativeTailResult {
		t.Helper()
		command := exec.CommandContext(t.Context(), binary, call.args...)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		if err := command.Run(); err != nil {
			var exited *exec.ExitError
			if !errors.As(err, &exited) {
				t.Fatal(err)
			}
		}
		return nativeTailResult{Code: command.ProcessState.ExitCode(), Out: stdout.Bytes(), Err: stderr.Bytes()}
	}
	got := invoke(current)
	if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
		t.Fatalf("real selected CLI differs: code=%d/%d stdout=%q/%q stderr=%q/%q", got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
	}
	prior := invoke(old)
	if prior.Code == 0 || !bytes.Contains(prior.Err, []byte("unknown flag: --seqs")) || len(prior.Out) != 0 {
		t.Fatalf("immutable CLI did not prove unavailable selected mode: %+v", prior)
	}
}

type selectedTailCancelWriter struct {
	out    bytes.Buffer
	cancel context.CancelFunc
}

func (w *selectedTailCancelWriter) Write(data []byte) (int, error) {
	n, err := w.out.Write(data)
	w.cancel()
	return n, err
}

func TestTailSelectedCancellationAfterFirstRecord(t *testing.T) {
	call := nativeTailCall{args: selectedTailArgs("d", "17,4,1")}
	fixture := newNativeTailFixture(t, call, nil)
	forbidNativeTailWrites(t, fixture)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	output := &selectedTailCancelWriter{cancel: cancel}
	var stderr bytes.Buffer
	cmd := cli.NewRootCmd()
	cmd.SetArgs(call.args)
	cmd.SetOut(output)
	cmd.SetErr(&stderr)
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("selected rendering ignored cancellation after first record: %v, stderr=%q", err, stderr.Bytes())
	}
	records := nativeTailRows(t, nativeTailResult{Out: output.out.Bytes()})
	if len(records) != 1 || records[0].Seq != 1 {
		t.Fatalf("selected rendering continued after cancellation: %+v", records)
	}
}
