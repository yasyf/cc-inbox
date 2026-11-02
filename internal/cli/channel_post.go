package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"

	mcp "github.com/yasyf/cc-interact/channel"
)

var anchoredFlags = []string{"--path", "--body-file"}

func postTool() mcp.Tool {
	return mcp.Tool{
		Name: "post",
		Description: "Post cci records exactly as `cci post <args>` does from this session, without a Bash call: the same flags, checks, deduplication and kept posts, writing as this window's thread. " +
			"Returns what cci post prints, such as the new record's number.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"args": map[string]any{
					"type":        "array",
					"items":       map[string]any{"type": "string"},
					"description": `the arguments cci post takes, one per item, e.g. ["--lane", "desk", "--kind", "report", "--text", "landed #42"]; stdin (--text -) is not available, so long bodies go in a file named with --path or --body-file`,
				},
				"cwd": map[string]any{
					"type":        "string",
					"description": "absolute directory that relative --path and --body-file values resolve against, such as the lane's worktree (default: the directory this window started in)",
				},
			},
			"required": []string{"args"},
		},
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			var in struct {
				Args []string `json:"args"`
				Cwd  string   `json:"cwd"`
			}
			if err := json.Unmarshal(raw, &in); err != nil {
				return "decode post arguments: " + err.Error(), true
			}
			var out bytes.Buffer
			root := NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below, which contextcheck cannot follow.
			root.SetArgs(append([]string{"post"}, anchored(in.Args, in.Cwd)...))
			root.SetIn(strings.NewReader(""))
			root.SetOut(&out)
			root.SetErr(&out)
			if err := root.ExecuteContext(ctx); err != nil {
				return strings.TrimSpace(out.String() + "\ncci: " + err.Error()), true
			}
			return strings.TrimSpace(out.String()), false
		},
	}
}

func anchored(args []string, cwd string) []string {
	if cwd == "" {
		return args
	}
	out := make([]string, len(args))
	copy(out, args)
	anchor := func(v string) string {
		if v == "" || v == "-" || filepath.IsAbs(v) {
			return v
		}
		return filepath.Join(cwd, v)
	}
	for i, a := range out {
		for _, flag := range anchoredFlags {
			switch {
			case a == flag && i+1 < len(out):
				out[i+1] = anchor(out[i+1])
			case strings.HasPrefix(a, flag+"="):
				out[i] = flag + "=" + anchor(strings.TrimPrefix(a, flag+"="))
			}
		}
	}
	return out
}
