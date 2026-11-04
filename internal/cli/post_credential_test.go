package cli_test

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/cli"
)

func TestPostRefusesACredentialOnEveryWayIntoARecordAndKeepsNoCopy(t *testing.T) {
	value := "AS" + "IA" + strings.Repeat("A1B2", 4)
	said := "the role's key is " + value
	bare := []string{"post", "--drive", "d", "--lane", "root", "--kind", "report"}
	inFile := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "said")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	for _, tt := range []struct {
		name  string
		args  func(t *testing.T) []string
		stdin string
		tool  map[string]any
	}{
		{name: "--text", args: func(*testing.T) []string { return []string{"--text", said} }},
		{name: "the one argument", args: func(*testing.T) []string { return []string{said} }},
		{name: "--quoted", args: func(*testing.T) []string { return []string{"--quoted", said} }},
		{name: "--attr", args: func(*testing.T) []string { return []string{"--text", "clean", "--attr", "key=" + value} }},
		{name: "--text from stdin", args: func(*testing.T) []string { return []string{"--text", "-"} }, stdin: said},
		{name: "a long --text from stdin", args: func(*testing.T) []string { return []string{"--text", "-"} }, stdin: strings.Repeat("padding ", 100) + said},
		{name: "--path file", args: func(t *testing.T) []string {
			return []string{"--text", "clean", "--path", inFile(t, "first line\n"+said)}
		}},
		{name: "--path file first line", args: func(t *testing.T) []string { return []string{"--path", inFile(t, said)} }},
		{name: "--path from stdin", args: func(*testing.T) []string { return []string{"--text", "clean", "--path", "-"} }, stdin: said},
		{name: "--body-file", args: func(t *testing.T) []string {
			return []string{"--text", "clean", "--body-file", inFile(t, `{"said":"`+said+`"}`)}
		}},
		{name: "--body-file from stdin", args: func(*testing.T) []string { return []string{"--text", "clean", "--body-file", "-"} }, stdin: `{"said":"` + said + `"}`},
		{name: "the post tool's text", tool: map[string]any{"text": said}},
		{name: "the post tool's body", tool: map[string]any{"text": "clean", "body": said}},
		{name: "the post tool's record", tool: map[string]any{"records": []map[string]any{{"kind": "report", "text": said}}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("CCI_HOME", home)
			if _, err := run(t, append(bare, "--text", "the store exists")...); err != nil {
				t.Fatal(err)
			}
			var refusal string
			if tt.tool != nil {
				args := map[string]any{"drive": "d", "lane": "root"}
				for key, v := range tt.tool {
					args[key] = v
				}
				if _, listed := tt.tool["records"]; !listed {
					args["kind"] = "report"
				}
				text, isErr := callTool(t, "post", args)
				if !isErr {
					t.Fatalf("post tool = %q, want a refusal", text)
				}
				refusal = text
			} else {
				var out bytes.Buffer
				root := cli.NewRootCmd()
				root.SetIn(strings.NewReader(tt.stdin))
				root.SetOut(&out)
				root.SetErr(&out)
				root.SetArgs(append(append([]string{}, bare...), tt.args(t)...))
				err := root.Execute()
				if err == nil {
					t.Fatalf("post = %q, want a refusal", out.String())
				}
				refusal = err.Error() + out.String()
			}
			if !strings.Contains(refusal, "an AWS access key id") {
				t.Errorf("refusal %q does not name the shape", refusal)
			}
			if strings.Contains(refusal, value) {
				t.Errorf("refusal %q echoes the value", refusal)
			}
			if err := filepath.WalkDir(home, func(path string, entry fs.DirEntry, err error) error {
				if err != nil || entry.IsDir() {
					return err
				}
				held, err := os.ReadFile(path)
				if err != nil {
					return err
				}
				if bytes.Contains(held, []byte(value)) {
					t.Errorf("%s holds the value after the refusal", path)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if got, err := run(t, "show", "2"); err == nil {
				t.Errorf("show 2 = %q; a refused post wrote a record", got)
			}
		})
	}
}

func TestPostStillTakesACredentialsNameAndACleanBody(t *testing.T) {
	t.Setenv("CCI_HOME", t.TempDir())
	body := filepath.Join(t.TempDir(), "body")
	if err := os.WriteFile(body, []byte("the env var TEMP_EXECUTOR_AUTH_TOKEN and the prefix gho_ are names\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := run(t, "post", "--drive", "d", "--lane", "root", "--kind", "report", "--text", "AWS_ACCESS_KEY_ID leaked, value not repeated", "--path", body); err != nil {
		t.Fatalf("post = %v, want names to pass", err)
	}
}
