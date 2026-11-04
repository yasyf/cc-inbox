package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	applog "github.com/yasyf/cc-inbox/internal/log"
	"github.com/yasyf/cc-inbox/internal/version"
)

const handoffEnv = "CCI_HANDED_OFF_FROM"

func main() {
	applog.Setup()
	roots, err := version.DefaultRoots()
	if err != nil {
		fail(err)
	}
	handOff(roots)
	markStarted(roots)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root := cli.NewRootCmd()
	if err := root.ExecuteContext(ctx); err != nil {
		if path, ok := cli.External(root, os.Args[1:]); ok {
			err = syscall.Exec(path, append([]string{path}, os.Args[2:]...), os.Environ()) //nolint:gosec // G204: a cci-<verb> program on PATH, run the way git runs its external commands.
		}
		fail(err)
	}
}

func handOff(roots version.Roots) {
	if os.Getenv(handoffEnv) != "" {
		if err := os.Unsetenv(handoffEnv); err != nil {
			fail(err)
		}
		return
	}
	newer, ok, err := roots.NewerThan(version.String())
	if err != nil {
		fail(err)
	}
	if !ok {
		return
	}
	if !version.Started(newer.Path) {
		warm(newer.Path)
		return
	}
	env := append(os.Environ(), handoffEnv+"="+version.String())
	fail(syscall.Exec(newer.Path, append([]string{newer.Path}, os.Args[1:]...), env)) //nolint:gosec // G204: the newest cci install this machine holds.
}

func warm(path string) {
	claimed, err := version.ClaimWarm(path, time.Now())
	if err != nil {
		fail(err)
	}
	if !claimed {
		return
	}
	c := exec.Command(path, "--version") //nolint:gosec // G204: the newest cci install this machine holds.
	c.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := c.Start(); err != nil {
		fail(err)
	}
	if err := c.Process.Release(); err != nil {
		fail(err)
	}
}

func markStarted(roots version.Roots) {
	if !version.IsRelease(version.String()) {
		return
	}
	path := filepath.Join(os.Getenv("BINRUN_PLUGIN_ROOT"), "bin", "cci")
	if os.Getenv("BINRUN_PLUGIN_ROOT") == "" {
		exe, err := os.Executable()
		if err != nil {
			fail(err)
		}
		if path, err = filepath.EvalSymlinks(exe); err != nil {
			fail(err)
		}
	}
	if err := roots.MarkStarted(path); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "cci:", err)
	os.Exit(cli.ExitCode(err))
}
