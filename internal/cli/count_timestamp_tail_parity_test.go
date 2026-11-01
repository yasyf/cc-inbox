package cli_test

import (
	"bytes"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestCountTimestampTailPublicParity(t *testing.T) {
	old, current := os.Getenv("CCI_TIMESTAMP_OLD_CLI"), os.Getenv("CCI_TIMESTAMP_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable and current native CLI binaries")
	}
	for _, test := range []struct {
		name   string
		update string
		limit  string
		budget string
		code   int
		more   bool
	}{
		{"budget More", "", "0", "2000", 0, true},
		{"newest small budget More", "", "5", "600", 0, true},
		{"late foreign malformed recipients", "UPDATE records SET drive='foreign', recipients='[' WHERE seq=601", "0", "2000", 1, false},
		{"late foreign stale malformed recipients", "UPDATE records SET drive='foreign', recipients='[', at=0 WHERE seq=601", "0", "2000", -1, false},
		{"late foreign malformed refs", "UPDATE records SET drive='foreign', recipients='[\"peer:timestamp-count-tail\"]', refs='{' WHERE seq=601", "0", "2000", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("CCI_HOME", home)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "timestamp-count-tail")
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
			if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<700)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash,origin,origin_seq)
SELECT 'd','writer','note',?,'count tail '||n,'[]','post','count-tail-'||n,'feedfacefeedface',n FROM history`, time.Now().Add(-time.Hour).UTC().Truncate(time.Minute).UnixMilli()); err != nil {
				t.Fatal(err)
			}
			if test.update != "" {
				if _, err := db.ExecContext(t.Context(), test.update); err != nil {
					t.Fatal(err)
				}
			}
			invoke := func(binary string) ([]byte, []byte, int) {
				t.Helper()
				command := exec.CommandContext(t.Context(), binary, "tail", "--drive", "d", "--since", "24h", "--json", "--limit", test.limit, "--budget", test.budget)
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
			if test.code >= 0 && wantCode != test.code {
				t.Errorf("immutable Tail exit=%d want=%d stderr=%s", wantCode, test.code, wantErr)
			}
			if test.more && (len(wantOut) == 0 || !strings.Contains(string(wantErr), "more; resume with")) {
				t.Errorf("immutable Tail did not exercise More after prefix: stdout_bytes=%d stderr=%s", len(wantOut), wantErr)
			}
			if !bytes.Equal(gotOut, wantOut) || !bytes.Equal(gotErr, wantErr) || gotCode != wantCode {
				t.Errorf("Tail native parity code=%d/%d stdout=%q/%q stderr=%q/%q", gotCode, wantCode, gotOut, wantOut, gotErr, wantErr)
			}
			t.Logf("COUNT_TIMESTAMP_TAIL_PUBLIC_PARITY case=%s old_exit=%d current_exit=%d stdout_bytes=%d stderr_bytes=%d", test.name, wantCode, gotCode, len(wantOut), len(wantErr))
		})
	}
}
