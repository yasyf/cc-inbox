package cli

import (
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

const externalPrefix = "cci-"

// External resolves `cci <name> [args...]`, when <name> is no built-in command, to the executable
// cci-<name> on PATH, the way git and kubectl find their plugins, so a tool that ships cci-<name>
// adds the verb `cci <name>`.
func External(root *cobra.Command, args []string) (string, bool) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", false
	}
	if found, _, err := root.Find(args); err == nil && found != root {
		return "", false
	}
	path, err := exec.LookPath(externalPrefix + args[0])
	return path, err == nil
}
