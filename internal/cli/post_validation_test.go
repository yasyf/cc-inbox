package cli_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestPostRejectsInvalidArgumentsBeforeOpeningStore(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"kind", []string{"--kind", "unknown", "--text", "summary"}, "unknown kind"},
		{"kind before path", []string{"--kind", "unknown", "--path", "missing-body"}, "unknown kind"},
		{"targets before spill", []string{"--kind", "note", "--re", "1,2", "--resolves", "3,4", "--text", strings.Repeat("word ", 100)}, "not both"},
		{"targets before recipients", []string{"--kind", "note", "--re", "1,2", "--resolves", "3,4", "--to", "peer:missing", "--text", "summary"}, "not both"},
		{"text shape before kind", []string{"--kind", "unknown", "a", "b"}, "one quoted argument"},
		{"missing text before kind", []string{"--kind", "unknown"}, "post needs its text"},
		{"required kind before targets", []string{"--re", "1,2", "--resolves", "3,4", "--text", "summary"}, `required flag(s) "kind" not set`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), "cci")
			t.Setenv("CCI_HOME", home)
			out, err := run(t, append([]string{"post", "--drive", "test-drive", "--lane", "root"}, tt.args...)...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("post = %q, %v; want %q", out, err, tt.want)
			}
			if _, err := os.Stat(home); !os.IsNotExist(err) {
				t.Errorf("CCI_HOME exists after rejected post: %v; output %q", err, out)
			}
		})
	}
}

type postInput struct {
	reads int
}

func (r *postInput) Read([]byte) (int, error) {
	r.reads++
	return 0, io.EOF
}

func TestPostRejectsInvalidKindBeforeReadingStdin(t *testing.T) {
	home := filepath.Join(t.TempDir(), "cci")
	t.Setenv("CCI_HOME", home)
	input := &postInput{}
	var out bytes.Buffer
	root := cli.NewRootCmd()
	root.SetIn(input)
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs([]string{"post", "--drive", "test-drive", "--lane", "root", "--kind", "unknown", "--text", "-"})
	if err := root.Execute(); err == nil || !strings.HasPrefix(err.Error(), "unknown kind") {
		t.Errorf("post = %v; want unknown kind", err)
	}
	if input.reads != 0 {
		t.Errorf("stdin read %d times before rejecting kind", input.reads)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Errorf("CCI_HOME exists after rejected post: %v", err)
	}
}

func TestPostValidArgumentsCreateStore(t *testing.T) {
	home := filepath.Join(t.TempDir(), "cci")
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")
	if out, err := run(t, "post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "summary"); err != nil || out != "#1\n" {
		t.Fatalf("post = %q, %v; want #1", out, err)
	}
	if info, err := os.Stat(home); err != nil || !info.IsDir() {
		t.Fatalf("CCI_HOME after valid post: %v, %v", info, err)
	}
	if out, err := run(t, "show", "1", "--json"); err != nil || !strings.Contains(out, `"text":"summary"`) {
		t.Errorf("show = %q, %v; want stored summary", out, err)
	}
}

func TestPostTakesKindsAsReadersPrintThem(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	for kind, want := range map[string]string{"DECIDE": `"kind":"decide"`, "status": `"kind":"state"`} {
		if _, err := run(t, "post", "--drive", "test-drive", "--lane", "root", "--kind", kind, "--text", "x"); err != nil {
			t.Fatalf("post --kind %s = %v", kind, err)
		}
		if out, err := run(t, "tail", "--drive", "test-drive", "--since", "0", "--json", "-n", "1"); err != nil || !strings.Contains(out, want) {
			t.Errorf("post --kind %s stored %q, %v; want %s", kind, out, err, want)
		}
	}
}

func TestPostLongTextBesideAPathSpillsBoth(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	attached := filepath.Join(t.TempDir(), "diff.txt")
	if err := os.WriteFile(attached, []byte("ATTACHED diff\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	long := strings.Repeat("a finding past the cap ", 20) + "TEXT END"
	if _, err := run(t, "post", "--drive", "test-drive", "--lane", "root", "--kind", "report", "--text", long, "--path", attached); err != nil {
		t.Fatalf("post --text <long> --path = %v; want the text spilled with the file", err)
	}
	out, err := run(t, "show", "1")
	if err != nil || !strings.Contains(out, "TEXT END") || !strings.Contains(out, "ATTACHED diff") {
		t.Errorf("show 1 = %q, %v; want the whole text and the attached file", out, err)
	}
}
