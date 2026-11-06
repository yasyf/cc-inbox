package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

func TestAnUpgradeStepFinishesInItsOwnSessionAfterItsCallerIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	led := filepath.Join(t.TempDir(), "led")

	if err := runUpgradeStep(cmd, fmt.Sprintf(`[ "$(ps -o pgid= -p $$ | tr -d ' ')" = "$$" ] && echo led > %q`, led)); err != nil {
		t.Fatalf("runUpgradeStep = %v (output %q), want the step to lead its own process group and finish", err, out.String())
	}
	if _, err := os.Stat(led); err != nil {
		t.Fatalf("the upgrade step did not finish: %v", err)
	}
}

func TestADaemonMoveFinishesAfterItsCallerStopsWaiting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	moved := filepath.Join(t.TempDir(), "moved")
	move := exec.Command("/bin/sh", "-c", fmt.Sprintf(`sleep 1; [ "$(ps -o pgid= -p $$ | tr -d ' ')" = "$$" ] && echo moved > %q`, moved))

	if err := runDetached(ctx, move); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runDetached = %v, want it to stop waiting at its caller's deadline", err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if _, err := os.Stat(moved); err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the move was cut off when its caller stopped waiting, want it to lead its own process group and finish")
		}
	}
}
