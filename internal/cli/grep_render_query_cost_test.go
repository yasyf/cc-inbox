package cli_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGrepRenderedTextQueryCost(t *testing.T) {
	old, current := os.Getenv("CCI_RENDER_GREP_OLD_CLI"), os.Getenv("CCI_RENDER_GREP_CURRENT_CLI")
	if old == "" || current == "" {
		t.Skip("CI supplies immutable494 and current direct native CLI binaries")
	}
	for _, population := range []int{32, 10000} {
		for _, text := range []struct {
			name, value string
		}{
			{"ASCII", strings.Repeat("only, no non-test ", 20)},
			{"Unicode", strings.Repeat("only,\u00a0no\u3000non-test\u0085", 15)},
		} {
			for _, matches := range []bool{false, true} {
				t.Run(fmt.Sprintf("N%d/%s/matches=%t", population, text.name, matches), func(t *testing.T) {
					db, _ := grepBodyStore(t, population)
					if _, err := db.ExecContext(t.Context(), "UPDATE records SET text = ?", text.value); err != nil {
						t.Fatal(err)
					}
					pattern := "absent-render-query-sentinel"
					if matches {
						pattern = "only, no non-test"
					}
					args := []string{"grep", pattern, "--drive", "d", "--since", "24h", "-n", "800", "--budget", "600000"}
					invoke := func(binary, version string, sample int) []byte {
						t.Helper()
						dir := t.TempDir()
						stdoutPath, stderrPath := filepath.Join(dir, "stdout"), filepath.Join(dir, "stderr")
						stdout, err := os.Create(stdoutPath)
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = stdout.Close() }()
						stderr, err := os.Create(stderrPath)
						if err != nil {
							t.Fatal(err)
						}
						defer func() { _ = stderr.Close() }()
						command := exec.CommandContext(t.Context(), binary, args...)
						command.Env = append(os.Environ(), "CCI_HANDED_OFF_FROM=render-query-cost")
						command.Stdout, command.Stderr = stdout, stderr
						started := time.Now()
						err = command.Run()
						wall := time.Since(started)
						out, readErr := os.ReadFile(stdoutPath)
						if readErr != nil {
							t.Fatal(readErr)
						}
						errOut, readErr := os.ReadFile(stderrPath)
						if readErr != nil {
							t.Fatal(readErr)
						}
						if err != nil || !pipedStderrMatches(out, errOut, nil) {
							t.Fatalf("direct native %s: %v stderr=%q", version, err, errOut)
						}
						data, err := json.Marshal(map[string]any{
							"revision": version, "population": population, "text": text.name, "matches": matches, "sample": sample,
							"wall_ns": wall.Nanoseconds(), "user_cpu_ns": command.ProcessState.UserTime().Nanoseconds(), "system_cpu_ns": command.ProcessState.SystemTime().Nanoseconds(),
							"stdout_bytes": len(out), "stdout_sha256": fmt.Sprintf("%x", sha256.Sum256(out)), "stderr_bytes": len(errOut), "exit": command.ProcessState.ExitCode(),
							"scope": "direct native CLI in-process query and file output; setup and parent byte verification excluded",
						})
						if err != nil {
							t.Fatal(err)
						}
						t.Logf("GREP_RENDER_QUERY_COST %s", data)
						return out
					}
					for sample := range 3 {
						var before, after []byte
						if sample%2 == 0 {
							before, after = invoke(old, "immutable494", sample), invoke(current, "current", sample)
						} else {
							after, before = invoke(current, "current", sample), invoke(old, "immutable494", sample)
						}
						if !bytes.Equal(before, after) {
							t.Fatalf("sample=%d native grep bytes differ: old=%q current=%q", sample, before, after)
						}
						if !matches {
							if string(before) != fmt.Sprintf("no records on d match; store head #%d\n", population) {
								t.Fatalf("no-match output=%q", before)
							}
							continue
						}
						lines := min(population, 800)
						if population > lines {
							trailer := fmt.Sprintf("... %d more matches; narrow with --kind, --lane or --since\n", population-lines)
							if !bytes.HasSuffix(before, []byte(trailer)) {
								t.Fatalf("full-history match count trailer missing: %q", before)
							}
							lines++
						}
						if got := bytes.Count(before, []byte{'\n'}); got != lines {
							t.Fatalf("output lines=%d, want=%d", got, lines)
						}
					}
				})
			}
		}
	}
}
