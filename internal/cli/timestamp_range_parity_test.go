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

func TestTimestampRangePublicParity(t *testing.T) {
	old, current := os.Getenv("CCI_TIMESTAMP_OLD_CLI"), os.Getenv("CCI_TIMESTAMP_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable and current native CLI binaries")
	}
	for _, test := range []struct {
		name       string
		population int
		updates    []string
		args       []string
		code       int
		prefix     int
	}{
		{"window", 32, nil, nil, 0, -1},
		{"JSON window", 32, nil, []string{"--json"}, 0, -1},
		{"pages and exact trailer", 1001, nil, nil, 0, 801},
		{"partial prefix error", 1001, []string{"UPDATE records SET fields='{' WHERE seq=501"}, nil, 1, 500},
		{"nonmonotonic sparse history", 1001, []string{"UPDATE records SET at=at-90000000 WHERE seq%2=0"}, nil, 0, 501},
		{"stale malformed recipients", 32, []string{"UPDATE records SET recipients='[', at=at-90000000 WHERE seq=31"}, nil, -1, -1},
		{"current malformed recipients", 32, []string{"UPDATE records SET recipients='[' WHERE seq=31"}, nil, 1, -1},
		{"current malformed refs", 32, []string{"UPDATE records SET refs='{' WHERE seq=31"}, nil, 1, -1},
		{"current malformed also", 32, []string{"UPDATE records SET also_resolves='[' WHERE seq=31"}, nil, 1, -1},
		{"raw real timestamp", 32, []string{"UPDATE records SET at=at+0.5 WHERE seq=31"}, nil, 1, -1},
		{"raw text timestamp", 32, []string{"UPDATE records SET at='invalid' WHERE seq=31"}, nil, 1, -1},
		{"raw blob timestamp", 32, []string{"UPDATE records SET at=X'FF' WHERE seq=31"}, nil, 1, -1},
		{"negative limit", 32, nil, []string{"--limit", "-1"}, 0, -1},
		{"root reader", 32, []string{"UPDATE records SET recipients='[\"root@d\"]' WHERE seq=32"}, []string{"--reader", "root"}, 0, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("CCI_HOME", home)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "timestamp-public-reader")
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
			if _, err := db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records (drive,lane,kind,at,text,recipients,source,hash)
SELECT CASE WHEN n=? THEN 'foreign' ELSE 'd' END,'writer','note',?,'pure port selected '||n,
CASE WHEN n=? THEN '["peer:timestamp-public-reader"]' ELSE '[]' END,'post','public-'||n FROM history`, test.population, test.population, time.Now().Add(-time.Hour).UnixMilli(), test.population); err != nil {
				t.Fatal(err)
			}
			for _, update := range test.updates {
				if _, err := db.ExecContext(t.Context(), update); err != nil {
					t.Fatal(err)
				}
			}
			invoke := func(binary string) ([]byte, []byte, int) {
				t.Helper()
				args := make([]string, 0, 10+len(test.args))
				args = append(args, "grep", "pure port", "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000")
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
			if test.code >= 0 && wantCode != test.code {
				t.Fatalf("immutable CLI exit=%d, want=%d, stderr=%s", wantCode, test.code, wantErr)
			}
			if test.prefix >= 0 && strings.Count(string(wantOut), "\n") != test.prefix {
				t.Fatalf("immutable CLI lines=%d, want=%d, stderr=%s", strings.Count(string(wantOut), "\n"), test.prefix, wantErr)
			}
			if !bytes.Equal(gotOut, wantOut) || !bytes.Equal(gotErr, wantErr) || gotCode != wantCode {
				t.Fatalf("CLI parity code=%d/%d stdout=%q/%q stderr=%q/%q", gotCode, wantCode, gotOut, wantOut, gotErr, wantErr)
			}
			t.Logf("TIMESTAMP_RANGE_PUBLIC_PARITY case=%s rows=%d immutable_exit=%d stdout_bytes=%d stderr_bytes=%d", test.name, test.population, wantCode, len(wantOut), len(wantErr))
		})
	}
}
