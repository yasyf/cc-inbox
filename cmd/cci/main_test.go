package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestMainDispatchPreservesCobraCommands(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"help", "completion", "__complete", "__completeNoDesc", "tail", "owner-waits"} {
		path := filepath.Join(dir, "cci-"+name)
		if err := os.WriteFile(path, []byte("#!/bin/sh\nprintf 'external\\n'\nprintf '<%s>\\n' \"$@\"\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", dir)
	t.Setenv("CCI_TEST_CLI_DISPATCH", "1")
	for _, tt := range []struct {
		name string
		args []string
		want string
		exit int
	}{
		{"help", []string{"help", "post"}, "Append one record", 0},
		{"completion", []string{"completion", "bash"}, "__start_cci", 0},
		{"completion request", []string{"__complete", "po"}, "post\tAppend one record", 0},
		{"completion request without descriptions", []string{"__completeNoDesc", "po"}, "post\n", 0},
		{"built-in error", []string{"tail", "--not-a-real-flag"}, "cci: unknown flag: --not-a-real-flag", 1},
		{"missing extension", []string{"no-such-verb"}, "unknown command \"no-such-verb\" for \"cci\"", 1},
		{"external arguments", []string{"owner-waits", "--drive", "d", "--unknown=value", "two words"}, "external\n<--drive>\n<d>\n<--unknown=value>\n<two words>\n", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestMainDispatchProcess$", "--"}, tt.args...)...)
			out, err := cmd.CombinedOutput()
			if got := cmd.ProcessState.ExitCode(); got != tt.exit {
				t.Fatalf("exit = %d, want %d: %v\n%s", got, tt.exit, err, out)
			}
			if !strings.Contains(string(out), tt.want) {
				t.Fatalf("output = %q, want %q", out, tt.want)
			}
			if tt.args[0] == "owner-waits" {
				if string(out) != tt.want {
					t.Fatalf("external output = %q, want %q", out, tt.want)
				}
			} else if strings.Contains(string(out), "external\n") {
				t.Fatalf("built-in command ran its PATH extension: %q", out)
			}
		})
	}
}

func TestMainDispatchProcess(t *testing.T) {
	if os.Getenv("CCI_TEST_CLI_DISPATCH") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 {
		t.Fatal("missing CLI argument separator")
	}
	os.Args = append([]string{"cci"}, os.Args[separator+1:]...)
	main()
	os.Exit(0)
}
