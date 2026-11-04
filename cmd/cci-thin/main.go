// Command cci-thin is the cci a release installs: it has the daemon run the
// command, and execs the full program beside it for anything the daemon does not.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yasyf/cc-inbox/internal/wire"
)

var version = "dev"

func main() {
	if code, ok := wire.Relay(version); ok {
		os.Exit(code)
	}
	exe, err := os.Executable()
	if err != nil {
		fail(err)
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		fail(err)
	}
	core := filepath.Join(filepath.Dir(exe), "cci-core")
	if err := os.Chmod(core, 0o755); err != nil { //nolint:gosec // G302: binrun extracts every archive member but the entry point 0600.
		fail(err)
	}
	fail(syscall.Exec(core, os.Args, os.Environ())) //nolint:gosec // G204: the program released beside this one.
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "cci:", err)
	os.Exit(1)
}
