package cli_test

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && slices.Contains([]string{"hook", "post"}, os.Args[1]) {
		root := cli.NewRootCmd()
		root.SetArgs(os.Args[1:])
		if err := root.Execute(); err != nil {
			fmt.Fprintln(os.Stderr, "cci:", err)
			if down := (*cli.BusDown)(nil); errors.As(err, &down) {
				os.Exit(cli.BusDownExit)
			}
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
