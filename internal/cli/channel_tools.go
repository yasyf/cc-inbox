package cli

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	mcp "github.com/yasyf/cc-interact/channel"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	readBudget      = 6000
	inboxBudget     = 4000
	digestBudget    = 3000
	findLimit       = 20
	findWidth       = 240
	broadcastWidth  = 160
	jsonPerLineByte = 2
	inboxFirstRead  = "2h"
)

func channelTools() []mcp.Tool {
	return []mcp.Tool{postTool(), readTool(), inboxTool(), findTool(), closeTool(), whoTool(), digestTool()}
}

type words []string

func (w *words) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		var one json.RawMessage
		if json.Unmarshal(b, &one) != nil {
			return err
		}
		raw = []json.RawMessage{one}
	}
	for _, r := range raw {
		*w = append(*w, word(r))
	}
	return nil
}

func word(r json.RawMessage) string {
	var s string
	if json.Unmarshal(r, &s) == nil {
		return s
	}
	return string(r)
}

type toolRecord map[string]json.RawMessage

func (r toolRecord) args(lane, drive string) ([]string, string, error) {
	var body string
	if raw, ok := r["body"]; ok {
		body = word(raw)
	}
	args := []string{"post", "--lane=" + cmp.Or(r.text("lane"), lane), "--kind=" + r.text("kind")}
	if d := cmp.Or(r.text("drive"), drive); d != "" {
		args = append(args, "--drive="+d)
	}
	flags := newPostCmd().Flags()
	for _, key := range slices.Sorted(maps.Keys(r)) {
		switch key {
		case "lane", "kind", "drive", "body", "records":
			continue
		}
		name := flagOf(key)
		if flags.Lookup(name) == nil {
			return nil, "", fmt.Errorf("post: no field %s", key)
		}
		flagArgs, err := flagValues(name, r[key])
		if err != nil {
			return nil, "", fmt.Errorf("post: %s: %w", key, err)
		}
		args = append(args, flagArgs...)
	}
	if body != "" {
		return append(args, "--path=-"), body, nil
	}
	return args, "", nil
}

func (r toolRecord) text(key string) string {
	if raw, ok := r[key]; ok {
		return word(raw)
	}
	return ""
}

func flagOf(key string) string {
	if key == "attrs" {
		return "attr"
	}
	return strings.ReplaceAll(key, "_", "-")
}

func fieldOf(flag string) string {
	if flag == "attr" {
		return "attrs"
	}
	return strings.ReplaceAll(flag, "-", "_")
}

func flagValues(name string, raw json.RawMessage) ([]string, error) {
	var on bool
	if json.Unmarshal(raw, &on) == nil {
		if on {
			return []string{"--" + name}, nil
		}
		return nil, nil
	}
	var pairs map[string]string
	if json.Unmarshal(raw, &pairs) == nil && pairs != nil {
		out := make([]string, 0, len(pairs))
		for _, k := range slices.Sorted(maps.Keys(pairs)) {
			out = append(out, "--"+name+"="+k+"="+pairs[k])
		}
		return out, nil
	}
	var values words
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		out = append(out, "--"+name+"="+v)
	}
	return out, nil
}

func runCCI(ctx context.Context, stdin string, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	root := NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below, which contextcheck cannot follow.
	root.SetArgs(args)
	root.SetIn(strings.NewReader(stdin))
	root.SetOut(&out)
	root.SetErr(&errOut)
	err := root.ExecuteContext(ctx)
	return out.String(), errOut.String(), err
}

func failed(stdout, stderr string, err error) (string, bool) {
	return strings.TrimSpace(stdout + stderr + "\ncci: " + err.Error()), true
}

func decode[T any](raw json.RawMessage) (T, string, bool) {
	var in T
	if err := json.Unmarshal(raw, &in); err != nil {
		return in, "decode arguments: " + err.Error(), false
	}
	return in, "", true
}

func schema(required []string, props map[string]any) map[string]any {
	out := map[string]any{"type": "object", "properties": props}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func prop(typ, desc string) map[string]any {
	return map[string]any{"type": typ, "description": desc}
}

func list(desc string) map[string]any {
	return map[string]any{"type": "array", "items": map[string]any{"type": []string{"string", "integer"}}, "description": desc}
}

var (
	laneProp  = prop("string", "your lane")
	driveProp = prop("string", "drive (default: this session's)")
)

func recordProps() map[string]any {
	props := map[string]any{
		"lane":     laneProp,
		"kind":     prop("string", "record kind, any case"),
		"text":     prop("string", "the record line; over 400 chars the rest moves to the body"),
		"body":     prop("string", "full body of any length, read back with read"),
		"path":     prop("string", "absolute path of a file to attach as the body"),
		"to":       list("addressed lanes"),
		"re":       map[string]any{"type": []string{"string", "integer"}, "description": "seq this answers, or an alias: t:<topic>, ccn:<id or text prefix>, card:<id>, pr#N, last:<kind>[:<lane>]"},
		"resolves": list("seqs or aliases this closes"),
		"pr":       list("pull requests: N, pr#N, owner/repo#N or URL, or an alias whose record's pull requests this carries"),
		"topic":    prop("string", "pairing key"),
		"attrs":    map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}},
	}
	newPostCmd().Flags().VisitAll(func(f *pflag.Flag) {
		field := fieldOf(f.Name)
		if _, named := props[field]; named || f.Name == "help" || f.Name == "json" {
			return
		}
		switch f.Value.Type() {
		case "bool":
			props[field] = prop("boolean", f.Usage)
		case "stringToString":
			props[field] = map[string]any{"type": "object", "additionalProperties": map[string]any{"type": "string"}, "description": f.Usage}
		case "stringSlice":
			props[field] = list(f.Usage)
		default:
			props[field] = prop("string", f.Usage)
		}
	})
	return props
}

func postTool() mcp.Tool {
	props := recordProps()
	props["drive"] = driveProp
	props["records"] = map[string]any{"type": "array", "items": map[string]any{"type": "object", "properties": recordProps()}, "description": "several records in one call; each inherits lane"}
	return mcp.Tool{
		Name:        "post",
		Description: "Post one record, or several with records. cci stamps the time; no shell quoting. Returns the new seqs.",
		InputSchema: schema([]string{"lane"}, props),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[toolRecord](raw)
			if !ok {
				return msg, true
			}
			var records []toolRecord
			if in["kind"] != nil {
				records = append(records, in)
			}
			if listed, ok := in["records"]; ok {
				var more []toolRecord
				if err := json.Unmarshal(listed, &more); err != nil {
					return "decode records: " + err.Error(), true
				}
				records = append(records, more...)
			}
			if len(records) == 0 {
				return "post: give kind and text, or records", true
			}
			seqs := make([]string, 0, len(records))
			for i, r := range records {
				args, stdin, err := r.args(in.text("lane"), in.text("drive"))
				if err != nil {
					return strings.TrimSpace(fmt.Sprintf("posted %s\nrecord %d: %s", strings.Join(seqs, " "), i+1, err)), true
				}
				out, errOut, err := runCCI(ctx, stdin, args...)
				if err != nil {
					text, _ := failed(out, errOut, err)
					return strings.TrimSpace(fmt.Sprintf("posted %s\nrecord %d: %s", strings.Join(seqs, " "), i+1, text)), true
				}
				seqs = append(seqs, strings.TrimPrefix(strings.TrimSpace(out), "#"))
			}
			return strings.Join(seqs, " "), false
		},
	}
}

func readTool() mcp.Tool {
	return mcp.Tool{
		Name:        "read",
		Description: "Read records by seq, each once: its line, then the rest of its body, within budget bytes split across seqs.",
		InputSchema: schema([]string{"seqs"}, map[string]any{
			"seqs":   list("seqs to read"),
			"budget": prop("integer", "body bytes in all (default 6000)"),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Seqs   words `json:"seqs"`
				Budget int   `json:"budget"`
			}](raw)
			if !ok {
				return msg, true
			}
			if len(in.Seqs) == 0 {
				return "read: give seqs", true
			}
			out, errOut, err := runCCI(ctx, "", append([]string{"show", "--json"}, in.Seqs...)...)
			if err != nil {
				return failed(out, errOut, err)
			}
			records, err := wireRecords(out)
			if err != nil {
				return err.Error(), true
			}
			share := max(cmp.Or(in.Budget, readBudget)/len(records), 1)
			var b strings.Builder
			w := render.Wire{Now: time.Now()}
			for _, r := range records {
				b.WriteString(w.Line(r, 0) + "\n")
				if r.Refs.Path == "" {
					continue
				}
				body, err := os.ReadFile(r.Refs.Path)
				if err != nil {
					fmt.Fprintf(&b, "body: %v\n", err)
					continue
				}
				body = inbox.BodyAfterText(body, r.Text)
				if len(body) > share {
					body = fmt.Appendf(body[:share:share], "\n... cut %d bytes; read seqs [%d] budget %d returns it whole", len(body)-share, r.Seq, len(body)+1)
				}
				if len(body) > 0 {
					b.Write(bytes.TrimRight(body, "\n"))
					b.WriteByte('\n')
				}
			}
			return strings.TrimSpace(b.String()), false
		},
	}
}

func inboxTool() mcp.Tool {
	return mcp.Tool{
		Name:        "inbox",
		Description: "Records new since your last inbox call, the first covering two hours: addressed to you whole, subscribed broadcasts cut to 160 chars.",
		InputSchema: schema([]string{"lane"}, map[string]any{
			"lane":   laneProp,
			"drive":  driveProp,
			"budget": prop("integer", "bytes (default 4000)"),
			"kinds":  list("only these kinds of broadcasts"),
			"mute":   list("never these kinds"),
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Lane   string `json:"lane"`
				Drive  string `json:"drive"`
				Budget int    `json:"budget"`
				Kinds  words  `json:"kinds"`
				Mute   words  `json:"mute"`
			}](raw)
			if !ok {
				return msg, true
			}
			st, release, err := hookStore(ctx)
			if err != nil {
				return err.Error(), true
			}
			defer release()
			drive, err := resolveDrive(ctx, st, in.Drive)
			if err != nil {
				return err.Error(), true
			}
			cursor := "mcp-" + in.Lane
			since := inboxFirstRead
			if seq, ok, err := st.Cursor(ctx, cursor, drive); err != nil {
				return err.Error(), true
			} else if ok {
				since = strconv.FormatInt(seq, 10)
			}
			args := []string{"tail", "--json", "--drive=" + drive, "--reader=" + in.Lane, "--since=" + since, "--budget=" + strconv.Itoa(jsonPerLineByte*cmp.Or(in.Budget, inboxBudget))}
			args = appendEach(args, "--kind", in.Kinds)
			args = appendEach(args, "--mute-kinds", in.Mute)
			text, last, isErr := renderWire(ctx, in.Lane, broadcastWidth, args)
			if isErr || last == 0 {
				return text, isErr
			}
			if err := st.MoveCursor(ctx, cursor, drive, last); err != nil {
				return err.Error(), true
			}
			return text, false
		},
	}
}

func findTool() mcp.Tool {
	return mcp.Tool{
		Name:        "find",
		Description: "Search records newest first by pattern and filters; prs answers many pull requests in one call.",
		InputSchema: schema(nil, map[string]any{
			"pattern": prop("string", "regular expression over the whole line (default: any)"),
			"kinds":   list("only these kinds"),
			"lanes":   list("only these writers"),
			"to":      prop("string", "only records addressed to this lane"),
			"prs":     list("only records about these pull requests, each answered apart"),
			"since":   prop("string", "seq, duration, clock time or date"),
			"limit":   prop("integer", "matches per query (default 20)"),
			"width":   prop("integer", "chars per line (default 240)"),
			"drive":   driveProp,
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Pattern string `json:"pattern"`
				Kinds   words  `json:"kinds"`
				Lanes   words  `json:"lanes"`
				To      string `json:"to"`
				PRs     words  `json:"prs"`
				Since   string `json:"since"`
				Limit   int    `json:"limit"`
				Width   int    `json:"width"`
				Drive   string `json:"drive"`
			}](raw)
			if !ok {
				return msg, true
			}
			args := []string{"grep", cmp.Or(in.Pattern, "."), "--json", "--budget=1048576", "-n", strconv.Itoa(cmp.Or(in.Limit, findLimit))}
			args = appendDrive(args, in.Drive)
			args = appendEach(args, "--kind", in.Kinds)
			args = appendEach(args, "--lane", in.Lanes)
			if in.To != "" {
				args = append(args, "--to="+in.To)
			}
			if in.Since != "" {
				args = append(args, "--since="+in.Since)
			}
			width := cmp.Or(in.Width, findWidth)
			if len(in.PRs) == 0 {
				text, _, isErr := renderWire(ctx, "", width, args)
				return text, isErr
			}
			var b strings.Builder
			for _, pr := range in.PRs {
				text, _, isErr := renderWire(ctx, "", width, append(slices.Clone(args), "--pr="+pr))
				if isErr {
					return text, true
				}
				fmt.Fprintf(&b, "pr %s\n%s\n", pr, text)
			}
			return strings.TrimSpace(b.String()), false
		},
	}
}

func closeTool() mcp.Tool {
	return mcp.Tool{
		Name:        "close",
		Description: "One record that closes every seq given, addressed to their writers unless to says otherwise.",
		InputSchema: schema([]string{"lane", "seqs", "how", "text"}, map[string]any{
			"lane":  laneProp,
			"seqs":  list("seqs to close"),
			"how":   map[string]any{"type": "string", "enum": []string{"answer", "done", "withdraw", "lift", "go", "unblock", "fix-live"}},
			"text":  prop("string", "the closing line"),
			"to":    list("addressed lanes (default: the closed records' writers)"),
			"drive": driveProp,
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Lane  string `json:"lane"`
				Seqs  words  `json:"seqs"`
				How   string `json:"how"`
				Text  string `json:"text"`
				To    words  `json:"to"`
				Drive string `json:"drive"`
			}](raw)
			if !ok {
				return msg, true
			}
			to := in.To
			if len(to) == 0 {
				out, errOut, err := runCCI(ctx, "", append([]string{"show", "--json"}, in.Seqs...)...)
				if err != nil {
					return failed(out, errOut, err)
				}
				records, err := wireRecords(out)
				if err != nil {
					return err.Error(), true
				}
				for _, r := range records {
					if r.Lane != in.Lane && !slices.Contains(to, r.Lane) {
						to = append(to, r.Lane)
					}
				}
			}
			args := []string{"post", "--lane=" + in.Lane, "--kind=" + in.How, "--resolves=" + strings.Join(in.Seqs, ",")}
			if in.Drive != "" {
				args = append(args, "--drive="+in.Drive)
			}
			for _, lane := range to {
				args = append(args, "--to="+lane)
			}
			if in.Text != "" {
				args = append(args, "--text="+in.Text)
			}
			out, errOut, err := runCCI(ctx, "", args...)
			if err != nil {
				return failed(out, errOut, err)
			}
			return strings.TrimPrefix(strings.TrimSpace(out), "#"), false
		},
	}
}

func whoTool() mcp.Tool {
	return mcp.Tool{
		Name:        "who",
		Description: "Live sessions and lanes, one line each, narrowed by lane or pull request.",
		InputSchema: schema(nil, map[string]any{
			"lane":  prop("string", "sessions bound as this lane"),
			"pr":    prop("string", "sessions with a claim on this pull request"),
			"drive": driveProp,
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Lane  string `json:"lane"`
				PR    string `json:"pr"`
				Drive string `json:"drive"`
			}](raw)
			if !ok {
				return msg, true
			}
			args := appendDrive([]string{"who"}, in.Drive)
			if in.Lane != "" {
				args = append(args, "--lane="+in.Lane)
			}
			if in.PR != "" {
				args = append(args, "--pr="+in.PR)
			}
			return passthrough(ctx, args)
		},
	}
}

func digestTool() mcp.Tool {
	return mcp.Tool{
		Name:        "digest",
		Description: "One screen of the drive: counts, open asks, blockers, holds, incidents, the latest line per lane.",
		InputSchema: schema(nil, map[string]any{
			"since":  prop("string", "seq, duration, clock time or date (default 24h)"),
			"budget": prop("integer", "bytes (default 3000)"),
			"drive":  driveProp,
		}),
		Handler: func(ctx context.Context, raw json.RawMessage, _ func(string)) (string, bool) {
			in, msg, ok := decode[struct {
				Since  string `json:"since"`
				Budget int    `json:"budget"`
				Drive  string `json:"drive"`
			}](raw)
			if !ok {
				return msg, true
			}
			args := appendDrive([]string{"digest", "--budget=" + strconv.Itoa(cmp.Or(in.Budget, digestBudget))}, in.Drive)
			if in.Since != "" {
				args = append(args, "--since="+in.Since)
			}
			return passthrough(ctx, args)
		},
	}
}

func appendDrive(args []string, drive string) []string {
	if drive == "" {
		return args
	}
	return append(args, "--drive="+drive)
}

func appendEach(args []string, flag string, values []string) []string {
	for _, v := range values {
		args = append(args, flag+"="+v)
	}
	return args
}

func passthrough(ctx context.Context, args []string) (string, bool) {
	out, errOut, err := runCCI(ctx, "", args...)
	if err != nil {
		return failed(out, errOut, err)
	}
	return strings.TrimSpace(out + errOut), false
}

func renderWire(ctx context.Context, reader string, width int, args []string) (string, int64, bool) {
	out, errOut, err := runCCI(ctx, "", args...)
	if err != nil {
		text, isErr := failed(out, errOut, err)
		return text, 0, isErr
	}
	records, err := wireRecords(out)
	if err != nil {
		return err.Error(), 0, true
	}
	var last int64
	var b strings.Builder
	w := render.Wire{Now: time.Now()}
	for _, r := range records {
		clip := width
		if reader != "" && slices.Contains(r.To, reader) {
			clip = 0
		}
		if reader != "" {
			r.To = nil
		}
		b.WriteString(w.Line(r, clip) + "\n")
		last = max(last, r.Seq)
	}
	for _, note := range strings.Split(strings.TrimSpace(errOut), "\n") {
		if strings.HasPrefix(note, "...") {
			b.WriteString(note + "\n")
		}
	}
	if b.Len() == 0 {
		return "none", last, false
	}
	return strings.TrimSpace(b.String()), last, false
}

func wireRecords(out string) ([]store.Record, error) {
	var records []store.Record
	for _, line := range strings.Split(out, "\n") {
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var r store.Record
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			return nil, fmt.Errorf("decode record: %w", err)
		}
		records = append(records, r)
	}
	return records, nil
}
