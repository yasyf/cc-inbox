package cli

import (
	"bufio"
	"context"
	"fmt"
	"os/exec"
	"slices"
	"syscall"
	"testing"
)

func TestChannelArgsReadOnlyTheChannelFlags(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want bool
	}{
		{[]string{"claude", "--channels", "plugin:cc-review@cc-review", "plugin:cc-inbox@cc-inbox", "--resume", "s1"}, true},
		{[]string{"claude", "--channels=plugin:cc-slack@cc-slack,plugin:cc-inbox@skills"}, true},
		{[]string{"claude", "--dangerously-load-development-channels", "plugin:cc-inbox@dev"}, true},
		{[]string{"claude", "--resume", "s1"}, false},
		{[]string{"claude", "--channels", "plugin:cc-slack@cc-slack", "--model", "opus", "plugin:cc-inbox@cc-inbox"}, false},
	} {
		if got := slices.Contains(channelArgs(tt.args), "plugin:cc-inbox@"); got != tt.want {
			t.Fatalf("channelArgs(%q) carries cc-inbox = %v, want %v", tt.args, got, tt.want)
		}
	}
}

func TestEachBgSessionUnderOneClaudeDaemonIsItsOwnWindow(t *testing.T) {
	daemon := exec.Command("bash", "-c", `for i in 1 2; do (exec -a "claude bg-spare" bash -c 'bash -c "echo \$\$ \$PPID; exec sleep 60" & wait') & done; wait`)
	daemon.Args[0] = "/Users/me/.local/bin/claude"
	daemon.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	out, err := daemon.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-daemon.Process.Pid, syscall.SIGKILL)
		_ = daemon.Wait()
	})
	lines := bufio.NewScanner(out)
	for range 2 {
		var client, session int
		if !lines.Scan() {
			t.Fatalf("the process tree printed no pids: %v", lines.Err())
		}
		if _, err := fmt.Sscan(lines.Text(), &client, &session); err != nil {
			t.Fatal(err)
		}
		window, err := thisWindow(context.WithValue(context.Background(), invocationKey{}, &invocation{pid: client}))
		if err != nil {
			t.Fatal(err)
		}
		if window.PID != session {
			t.Fatalf("the window of client %d is pid %d, want its claude bg-spare session %d, not the daemon %d", client, window.PID, session, daemon.Process.Pid)
		}
	}
}
