//go:build !linux

package cli

import (
	"context"
	"errors"
)

func runSupervisor(context.Context) error {
	return errors.New("cci supervise runs on linux only; launchd supervises the daemon elsewhere, so run cci serve")
}
