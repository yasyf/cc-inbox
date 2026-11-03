package harness_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/yasyf/cc-inbox/internal/harness"
)

func TestSessionsReadsRegistry(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, "14575.json", `{"pid":14575,"sessionId":"bf929897-c36e","cwd":"/w/a","procStart":"Wed Sep 30 17:36:07 2026","kind":"interactive","messagingSocketPath":"/tmp/cc-socks/14575.sock","name":"a-c6","status":"idle","statusUpdatedAt":1790830196802}`)
	write(t, dir, "14575.key", "not json")
	write(t, dir, "99.json", `{"pid":99,"sessionId":"other","name":"b-01"}`)
	got, err := harness.Sessions(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := got["bf929897-c36e"]
	if len(got) != 2 || a.PID != 14575 || a.Name != "a-c6" || a.Cwd != "/w/a" || a.Status != "idle" || a.Address() != "uds:/tmp/cc-socks/14575.sock" || got["other"].Address() != "" {
		t.Fatalf("Sessions() = %+v", got)
	}
	if missing, err := harness.Sessions(filepath.Join(dir, "nope")); err != nil || len(missing) != 0 {
		t.Fatalf("Sessions(missing) = %v, %v", missing, err)
	}
	write(t, dir, "57786.json", `{"pid":57786,"sessionId":"half`)
	if got, err := harness.Sessions(dir); err != nil || len(got) != 2 {
		t.Fatalf("Sessions() with a half-written file = %v, %v; want the two whole files and no error", got, err)
	}
}

func TestAlive(t *testing.T) {
	p, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // G115: OS pids fit in int32.
	if err != nil {
		t.Fatal(err)
	}
	created, err := p.CreateTime()
	if err != nil {
		t.Fatal(err)
	}
	start := time.UnixMilli(created).UTC()
	tests := []struct {
		name string
		s    harness.Session
		want bool
	}{
		{"this process", harness.Session{PID: os.Getpid(), ProcStart: start.Format(time.ANSIC)}, true},
		{"a reused pid", harness.Session{PID: os.Getpid(), ProcStart: start.Add(-time.Hour).Format(time.ANSIC)}, false},
		{"local time instead of UTC", harness.Session{PID: os.Getpid(), ProcStart: start.In(time.FixedZone("PDT", -7*3600)).Format(time.ANSIC)}, false},
		{"unparseable start", harness.Session{PID: os.Getpid(), ProcStart: "yesterday"}, false},
		{"no process", harness.Session{PID: 1 << 30, ProcStart: start.Format(time.ANSIC)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.Alive(harness.Procs{}); got != tt.want {
				t.Errorf("Alive() = %v, want %v", got, tt.want)
			}
		})
	}
	procs := harness.Procs{}
	if !procs.Alive(os.Getpid(), created) || procs.Alive(os.Getpid(), created+1) || procs.Alive(0, 0) {
		t.Fatal("Procs.Alive() does not match on the exact create time")
	}
}

func TestAliveReadsLinuxRegistryTicks(t *testing.T) {
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Skip("no /proc on this platform")
	}
	text := string(stat)
	ticks := strings.Fields(text[strings.LastIndex(text, ")")+1:])[19]
	if !(harness.Session{PID: os.Getpid(), ProcStart: ticks}).Alive(harness.Procs{}) {
		t.Errorf("a live process with procStart %q reads as gone", ticks)
	}
	if (harness.Session{PID: os.Getpid(), ProcStart: ticks + "0"}).Alive(harness.Procs{}) {
		t.Error("a reused pid with other ticks reads as alive")
	}
	if (harness.Session{PID: 1 << 30, ProcStart: ticks}).Alive(harness.Procs{}) {
		t.Error("a gone process reads as alive")
	}
	if (harness.Session{PID: 1 << 30, ProcStart: ""}).Alive(harness.Procs{}) {
		t.Error("a gone process with no procStart reads as alive")
	}
}

func write(t *testing.T, dir, name, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}
