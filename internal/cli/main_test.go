package cli_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		root := cli.NewRootCmd()
		root.SetArgs(os.Args[1:])
		if err := root.Execute(); err != nil {
			fmt.Fprintln(os.Stderr, "cci:", err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
