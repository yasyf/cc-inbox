package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

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
