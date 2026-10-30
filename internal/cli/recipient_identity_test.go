package cli_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestPostRecipientUsesHarnessSessionIdentity(t *testing.T) {
	p, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		t.Fatal(err)
	}
	started, err := p.CreateTime()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name       string
		pid        int
		pidStarted int64
		deadWindow bool
		duplicate  bool
	}{
		{"no stored pid", 0, 0, false, false},
		{"stale stored pid", 1 << 30, started, false, false},
		{"stale stored start", os.Getpid(), started - 1, false, false},
		{"matching stored process", os.Getpid(), started, false, false},
		{"dead harness overrides fresh row", 0, 0, true, false},
		{"last registry identity wins", os.Getpid(), started, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home, config := t.TempDir(), t.TempDir()
			t.Setenv("CCI_HOME", home)
			t.Setenv("CLAUDE_CONFIG_DIR", config)
			t.Setenv("CLAUDE_CODE_SESSION_ID", "sender")
			t.Setenv("CCI_AGENT_ID", "")
			ctx := context.Background()
			st, err := store.Open(ctx, home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			k := store.PeerKey{Session: "receiver"}
			if err := st.BindLane(ctx, k, "other-drive", "receiver", time.Now()); err != nil {
				t.Fatal(err)
			}
			if _, err := st.Announce(ctx, store.Peer{Session: k.Session, PID: tt.pid, PIDStarted: tt.pidStarted}); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(filepath.Join(config, "sessions"), 0o700); err != nil {
				t.Fatal(err)
			}
			window := harness.Session{SessionID: k.Session, PID: os.Getpid(), ProcStart: time.UnixMilli(started).UTC().Format(time.ANSIC)}
			if tt.deadWindow {
				window.ProcStart = time.UnixMilli(started).Add(-time.Hour).UTC().Format(time.ANSIC)
			}
			body, err := json.Marshal(window)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(config, "sessions", fmt.Sprintf("%d.json", os.Getpid())), body, 0o600); err != nil {
				t.Fatal(err)
			}
			if tt.duplicate {
				window.PID = 999999999
				body, err = json.Marshal(window)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(config, "sessions", "999999999.json"), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			out, err := run(t, "post", "--drive", "sender-drive", "--lane", "sender", "--kind", "note", "--to", "receiver", "--text", "identity fixture")
			want, wantTo := "cci: --to receiver: receiver reads drive other-drive, not sender-drive; addressed receiver@other-drive\n#1\n", "receiver@other-drive"
			if tt.deadWindow || tt.duplicate {
				want, wantTo = "#1\n", "receiver"
			}
			if err != nil || out != want {
				t.Fatalf("post with %s = %q, %v; want %q", tt.name, out, err, want)
			}
			stored, err := st.Get(ctx, 1)
			if err != nil || strings.Join(stored.To, ",") != wantTo {
				t.Fatalf("stored recipient = %v, %v", stored.To, err)
			}
		})
	}
}
