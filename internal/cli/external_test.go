package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestExternalRunsAPluginOnPathOnlyForAnUnknownVerb(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"cci-owner-waits", "cci-tail"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	root := cli.NewRootCmd()
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"owner-waits", "--drive", "d"}, filepath.Join(dir, "cci-owner-waits")},
		{[]string{"tail", "--drive", "d"}, ""},
		{[]string{"no-such-verb"}, ""},
		{[]string{"--version"}, ""},
		{nil, ""},
	} {
		got, ok := cli.External(root, tt.args)
		if got != tt.want || ok != (tt.want != "") {
			t.Errorf("External(%q) = %q, %v; want %q", tt.args, got, ok, tt.want)
		}
	}
}
