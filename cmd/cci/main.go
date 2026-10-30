package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yasyf/cc-inbox/internal/cli"
	applog "github.com/yasyf/cc-inbox/internal/log"
)

func main() {
	applog.Setup()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	root := cli.NewRootCmd()
	if path, ok := cli.External(root, os.Args[1:]); ok {
		err := syscall.Exec(path, append([]string{path}, os.Args[2:]...), os.Environ())
		fmt.Fprintln(os.Stderr, "cci:", err)
		os.Exit(1)
	}
	if err := root.ExecuteContext(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "cci:", err)
		os.Exit(1)
	}
}
