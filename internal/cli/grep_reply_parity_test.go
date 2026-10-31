package cli_test

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestGrepIndexedRepliesPreservePublicErrorsAndBytes(t *testing.T) {
	old, current := os.Getenv("CCI_GREP_CURRENT_BASELINE"), os.Getenv("CCI_GREP_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable public45 and current native CLI binaries")
	}
	for _, test := range []struct {
		name, also, fields string
		re, resolves       int64
		prefix, code       int
	}{
		{"offdrive selected fields", "[]", "{", 501, 0, 500, 1},
		{"selected malformed also", "{", "{}", 501, 0, -1, 1},
		{"global malformed also", "{", "{}", 0, 0, -1, 1},
		{"selected string also", "[\"1\"]", "{}", 501, 0, 500, 1},
		{"unselected numeric string", "[\"1\"]", "{}", 0, 0, -1, 0},
		{"selected boolean", "[true]", "{}", 0, 0, 1000, 1},
		{"selected real", "[1.0]", "{}", 0, 0, 1000, 1},
		{"unselected nested", "[[1]]", "{}", 0, 0, -1, 0},
		{"resolver", "[]", "{}", 0, 501, -1, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			home := t.TempDir()
			t.Setenv("CCI_HOME", home)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "")
			t.Setenv("CODEX_THREAD_ID", "")
			t.Setenv("CCI_AGENT_ID", "")
			st, err := store.Open(ctx, home)
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
			if _, err := db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1001)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','reviewer','review',x,'REVIEW selected '||x,'post','selected-'||x FROM n`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,source,hash,re,resolves,also_resolves,fields) VALUES('foreign','reply','note',1,'reply','post','reply',?,?,?,?)`, test.re, test.resolves, test.also, test.fields); err != nil {
				t.Fatal(err)
			}
			invoke := func(binary string) ([]byte, []byte, int) {
				t.Helper()
				command := exec.CommandContext(ctx, binary, "grep", "REVIEW", "--drive", "d", "--kind", "review", "--budget", "4194304")
				var stdout, stderr bytes.Buffer
				command.Stdout, command.Stderr = &stdout, &stderr
				err := command.Run()
				if err != nil {
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
				t.Fatalf("oracle exit=%d, want %d: %s", wantCode, test.code, wantErr)
			}
			if !bytes.Equal(gotOut, wantOut) || !bytes.Equal(gotErr, wantErr) || gotCode != wantCode {
				t.Fatalf("CLI parity code=%d/%d stdout=%q/%q stderr=%q/%q", gotCode, wantCode, gotOut, wantOut, gotErr, wantErr)
			}
			if test.prefix >= 0 && (wantCode == 0 || strings.Count(string(wantOut), "\n") != test.prefix) {
				t.Fatalf("oracle did not prove partial-prefix case: code=%d rows=%d stderr=%s", wantCode, strings.Count(string(wantOut), "\n"), wantErr)
			}
		})
	}
}
