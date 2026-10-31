package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yasyf/cc-inbox/internal/cli"
	applog "github.com/yasyf/cc-inbox/internal/log"
	"github.com/yasyf/cc-inbox/internal/version"
)

const handoffEnv = "CCI_HANDED_OFF_FROM"

func main() {
	applog.Setup()
	handOff()

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

func handOff() {
	if os.Getenv(handoffEnv) != "" {
		if err := os.Unsetenv(handoffEnv); err != nil {
			fail(err)
		}
		return
	}
	roots, err := version.DefaultRoots()
	if err != nil {
		fail(err)
	}
	newer, ok, err := roots.NewerThan(version.String())
	if err != nil {
		fail(err)
	}
	if !ok {
		return
	}
	env := append(os.Environ(), handoffEnv+"="+version.String())
	fail(syscall.Exec(newer.Path, append([]string{newer.Path}, os.Args[1:]...), env)) //nolint:gosec // G204: the newest cci install this machine holds.
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "cci:", err)
	os.Exit(1)
}
