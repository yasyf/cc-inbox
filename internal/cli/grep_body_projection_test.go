package cli_test

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestGrepTextBodyProjectionWork(t *testing.T) {
	for _, population := range []int{32, 10000} {
		t.Run(fmt.Sprint(population), func(t *testing.T) {
			db, body := grepBodyStore(t, population)
			var reads, transferred atomic.Int64
			function := fmt.Sprintf("cci_grep_body_%d", grepCounterID.Add(1))
			if err := sqlite.RegisterDeterministicScalarFunction(function, 1, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
				reads.Add(1)
				transferred.Add(int64(len(args[0].(string))))
				return args[0], nil
			}); err != nil {
				t.Fatal(err)
			}
			for _, statement := range []string{
				"ALTER TABLE records RENAME COLUMN body TO counted_body",
				"ALTER TABLE records ADD COLUMN body TEXT GENERATED ALWAYS AS (" + function + "(counted_body)) VIRTUAL",
			} {
				if _, err := db.ExecContext(t.Context(), statement); err != nil {
					t.Fatal(err)
				}
			}
			for _, asJSON := range []bool{false, true} {
				args := []string{"grep", "pure port", "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000"}
				if asJSON {
					args = append(args, "--json")
				}
				reads.Store(0)
				transferred.Store(0)
				out, err := run(t, args...)
				if err != nil {
					t.Fatal(err)
				}
				wantReads := int64(0)
				if asJSON {
					wantReads = int64(population)
				}
				if reads.Load() != wantReads {
					t.Errorf("GREP_BODY_TRANSFER_BOUND_EXCEEDED rows=%d json=%t reads=%d want=%d", population, asJSON, reads.Load(), wantReads)
				}
				if asJSON {
					tailCostOutput(t, out, []int64{int64(population), 1}, "")
					lines := strings.Split(strings.TrimSpace(out), "\n")
					var record store.Record
					if err := json.Unmarshal([]byte(lines[1]), &record); err != nil {
						t.Fatal(err)
					}
					if !bytes.Equal(record.Body, body) || transferred.Load() < int64(len(body)) {
						t.Fatalf("JSON body bytes=%d, want=%d; transferred=%d", len(record.Body), len(body), transferred.Load())
					}
				} else if strings.Count(out, "\n") != 2 || !strings.Contains(out, "selected global peer") || !strings.Contains(out, "selected local") || strings.Contains(out, "body-only-token") {
					t.Fatalf("text grep output=%q", out)
				}
				t.Logf("GREP_BODY_TRANSFER rows=%d json=%t actual_body_reads=%d actual_body_bytes=%d selected_body_bytes=%d", population, asJSON, reads.Load(), transferred.Load(), len(body))
			}
		})
	}
}

func TestGrepTextBodyProjectionPublicParity(t *testing.T) {
	old, current := os.Getenv("CCI_GREP_BODY_OLD_CLI"), os.Getenv("CCI_GREP_BODY_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable and current native CLI binaries")
	}
	for _, test := range []struct {
		name    string
		updates []string
		pattern string
		args    []string
		code    int
	}{
		{"text large body", nil, "pure port", nil, 0},
		{"JSON large body", nil, "pure port", []string{"--json"}, 0},
		{"body is outside regex line", nil, "body-only-token", nil, 0},
		{"malformed body stays opaque in text", []string{"UPDATE records SET body='{' WHERE seq=1"}, "pure port", nil, 0},
		{"nonmatching malformed fields", []string{"UPDATE records SET fields='{' WHERE seq=2"}, "pure port", nil, 1},
		{"nonmatching malformed refs", []string{"UPDATE records SET refs='{' WHERE seq=2"}, "pure port", nil, 1},
		{"nonmatching malformed recipients", []string{"UPDATE records SET recipients='[' WHERE seq=2"}, "pure port", nil, 1},
		{"nonmatching malformed also", []string{"UPDATE records SET also_resolves='[' WHERE seq=2"}, "pure port", nil, 1},
		{"nonmatching malformed timestamp", []string{"UPDATE records SET at=X'FF' WHERE seq=2"}, "pure port", nil, 1},
		{"out of order timestamps", []string{"UPDATE records SET at=at-90000000 WHERE seq=32", "UPDATE records SET text='pure port selected older' WHERE seq=2"}, "pure port", nil, 0},
		{"fields suppress PR rendering", []string{`UPDATE records SET refs='{"prs":[7]}', fields='{"attrs":{"head:7":"abc"}}' WHERE seq=1`}, "pure port", nil, 0},
		{"fixed case options", nil, "PURE PORT", []string{"-F", "--ignore-case=false"}, 0},
		{"negative limit", nil, "pure port", []string{"--limit", "-1"}, 0},
		{"regex identity", []string{"UPDATE records SET origin='feedfacefeedface', origin_seq=900 WHERE seq=1"}, "#900@feedface", nil, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, _ := grepBodyStore(t, 32)
			for _, update := range test.updates {
				if _, err := db.ExecContext(t.Context(), update); err != nil {
					t.Fatal(err)
				}
			}
			invoke := func(binary string) ([]byte, []byte, int) {
				t.Helper()
				args := make([]string, 0, 10+len(test.args))
				args = append(args, "grep", test.pattern, "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000")
				args = append(args, test.args...)
				command := exec.CommandContext(t.Context(), binary, args...)
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				if err := command.Run(); err != nil {
					var exited *exec.ExitError
					if !errors.As(err, &exited) {
						t.Fatal(err)
					}
				}
				return stdout.Bytes(), stderr.Bytes(), command.ProcessState.ExitCode()
			}
			wantOut, wantErr, wantCode := invoke(old)
			gotOut, gotErr, gotCode := invoke(current)
			if wantCode != test.code {
				t.Fatalf("immutable CLI exit=%d, want=%d; stderr=%s", wantCode, test.code, wantErr)
			}
			if !bytes.Equal(gotOut, wantOut) || !bytes.Equal(gotErr, wantErr) || gotCode != wantCode {
				t.Fatalf("CLI parity code=%d/%d stdout=%q/%q stderr=%q/%q", gotCode, wantCode, gotOut, wantOut, gotErr, wantErr)
			}
		})
	}
}

func grepBodyStore(t *testing.T, population int) (*sql.DB, json.RawMessage) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "grep-body-session")
	t.Setenv("CODEX_THREAD_ID", "")
	t.Setenv("CCI_AGENT_ID", "")
	st, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	payload := strings.Repeat("body-only-token ", 2048)
	body, err := json.Marshal(map[string]string{"payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records (drive,lane,kind,at,text,recipients,body,source,hash)
SELECT CASE WHEN n=? THEN 'foreign' ELSE 'd' END,'writer',
CASE WHEN n%2=0 THEN 'state' ELSE 'note' END,?,
CASE WHEN n=1 THEN 'pure port selected local' WHEN n=? THEN 'pure port selected global peer' ELSE 'unrelated '||n END,
CASE WHEN n=? THEN '["peer:grep-body-session"]' ELSE '[]' END,
CASE WHEN n=1 THEN ? ELSE '{"payload":"tiny"}' END,'post','body-history-'||n FROM history`, population, population, time.Now().UnixMilli(), population, population, string(body)); err != nil {
		t.Fatal(err)
	}
	return db, body
}
