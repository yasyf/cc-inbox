package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const (
	captureFile = "traffic.capture"
	matrixSpan  = 500
	matrixLanes = 60
)

var (
	capturedEnv = regexp.MustCompile(`^(CLAUDE|CODEX|CCI)_`)
	secretEnv   = regexp.MustCompile(`TOKEN|KEY|SECRET|PASSWORD|COOKIE|AUTH`)
)

func newWindowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "window",
		Short: "Turn the daemon's in-memory window of recent records on or off, or show it",
	}
	home := func(cmd *cobra.Command) (string, error) {
		if inv := invoked(cmd.Context()); inv != nil && inv.st != nil {
			return inv.st.HomeDir(), nil
		}
		return store.Home()
	}
	turn := func(on bool) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, _ []string) error {
			at, err := home(cmd)
			if err != nil {
				return err
			}
			return store.SetHotWindow(at, on)
		}
	}
	cmd.AddCommand(
		&cobra.Command{Use: "on", Short: "Serve reads from the window; the daemon follows within a second", Args: cobra.NoArgs, RunE: turn(true)},
		&cobra.Command{Use: "off", Short: "Serve every read from the store; the daemon follows within a second", Args: cobra.NoArgs, RunE: turn(false)},
		&cobra.Command{Use: "status", Short: "Print what the window holds and how often it answered", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			at, err := home(cmd)
			if err != nil {
				return err
			}
			inv := invoked(cmd.Context())
			if inv == nil || inv.st == nil || inv.st.ServedHotWindow() == nil {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "switch on=%t; no window is serving here\n", store.HotWindowOn(at))
				return err
			}
			s := inv.st.ServedHotWindow().Stats()
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "switch on=%t floor=%d head=%d bytes=%d cap=%d hits=%d misses=%d failures=%d\n", store.HotWindowOn(at), s.Floor, s.Head, s.Bytes, store.HotWindowBytes, s.Hits, s.Misses, s.Failures)
			return err
		}},
		newWindowParityCmd(),
	)
	return cmd
}

type capturedRequest struct {
	At         int64             `json:"at"`
	Argv       []string          `json:"argv,omitempty"`
	Env        map[string]string `json:"env,omitempty"`
	Cwd        string            `json:"cwd,omitempty"`
	PID        int               `json:"pid,omitempty"`
	StdoutChar bool              `json:"stdout_char,omitempty"`
	Hook       string            `json:"hook,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
}

func capture(home string, entry capturedRequest) {
	destination, err := os.ReadFile(filepath.Join(home, captureFile))
	if err != nil {
		return
	}
	entry.At = time.Now().UnixMilli()
	line, err := json.Marshal(entry)
	if err != nil {
		return
	}
	file, err := os.OpenFile(strings.TrimSpace(string(destination)), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600) //nolint:gosec // G304: the path is the owner's own capture destination.
	if err != nil {
		return
	}
	defer func() { _ = file.Close() }()
	_, _ = file.Write(append(line, '\n'))
}

func captureCommand(home string, request wire.Request) {
	if _, err := os.Stat(filepath.Join(home, captureFile)); err != nil {
		return
	}
	env := map[string]string{}
	for name, value := range requestEnv(request.Env) {
		if capturedEnv.MatchString(name) && !secretEnv.MatchString(name) {
			env[name] = value
		}
	}
	capture(home, capturedRequest{Argv: request.Argv, Env: env, Cwd: request.Cwd, PID: request.PID, StdoutChar: request.StdoutChar})
}

func captureHook(home, event string, payload []byte) {
	if _, err := os.Stat(filepath.Join(home, captureFile)); err != nil {
		return
	}
	capture(home, capturedRequest{Hook: event, Payload: payload})
}

type replayed struct {
	stdout, stderr string
	code           int
	cursors        string
	records        string
}

func replay(ctx context.Context, st *store.Store, home string, entry capturedRequest) (replayed, error) {
	view := *st
	at := time.UnixMilli(entry.At)
	view.Now = func() time.Time { return at }
	if err := configureStoreOwner(&view, home); err != nil {
		return replayed{}, err
	}
	var stdout, stderr bytes.Buffer
	var run error
	inv := &invocation{env: entry.Env, cwd: entry.Cwd, pid: entry.PID, stdoutChar: entry.StdoutChar, st: &view}
	hosted := channel.Hosted(context.WithValue(ctx, invocationKey{}, inv))
	if entry.Hook != "" {
		p, err := hook.Read(bytes.NewReader(entry.Payload))
		if err != nil {
			return replayed{}, err
		}
		run = runHookEvent(hosted, &view, entry.Hook, p, &stdout)
	} else {
		root := NewRootCmd()
		root.SetArgs(entry.Argv[1:])
		root.SetOut(&stdout)
		root.SetErr(&stderr)
		root.SetIn(strings.NewReader(""))
		run = root.ExecuteContext(hosted)
	}
	out := replayed{stdout: stdout.String(), stderr: stderr.String()}
	if run != nil {
		out.code, out.stderr = ExitCode(run), out.stderr+run.Error()
	}
	var err error
	if out.cursors, out.records, err = view.ParityDigest(ctx); err != nil {
		return replayed{}, err
	}
	return out, nil
}

func parityClass(entry capturedRequest) string {
	switch {
	case entry.Hook != "":
		return "delivery: hook " + entry.Hook
	case len(entry.Argv) < 2:
		return "other"
	case slices.Contains(entry.Argv, "--cursor") || slices.ContainsFunc(entry.Argv, func(a string) bool { return strings.HasPrefix(a, "--cursor=") }):
		return "cursors: " + entry.Argv[1]
	case slices.Contains(entry.Argv, "--for") || slices.Contains(entry.Argv, "--to") || slices.Contains(entry.Argv, "--lane"):
		return "audience: " + entry.Argv[1]
	case slices.Contains(entry.Argv, "--kind"):
		return "kinds: " + entry.Argv[1]
	case entry.Argv[1] == "show" || entry.Argv[1] == "digest":
		return "closures: " + entry.Argv[1]
	default:
		return "other: " + entry.Argv[1]
	}
}

func newWindowParityCmd() *cobra.Command {
	var plain, windowed, traffic, report string
	cmd := &cobra.Command{
		Use:    "parity",
		Short:  "Replay captured traffic through the store and the window on two copies of a store, and diff everything",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWindowParity(cmd.Context(), cmd.OutOrStdout(), plain, windowed, traffic, report)
		},
	}
	cmd.Flags().StringVar(&plain, "plain", "", "home of the copy that reads the store alone")
	cmd.Flags().StringVar(&windowed, "windowed", "", "home of the copy that reads through the window")
	cmd.Flags().StringVar(&traffic, "traffic", "", "captured requests, one JSON document a line")
	cmd.Flags().StringVar(&report, "report", "", "file for each difference, one JSON document a line")
	for _, name := range []string{"plain", "windowed", "traffic", "report"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}

func runWindowParity(ctx context.Context, w io.Writer, plainHome, windowedHome, traffic, report string) error {
	plain, err := store.Open(ctx, plainHome)
	if err != nil {
		return err
	}
	defer func() { _ = plain.Close() }()
	windowed, err := store.Open(ctx, windowedHome)
	if err != nil {
		return err
	}
	defer func() { _ = windowed.Close() }()
	built := time.Now()
	window, err := windowed.BuildHotWindow(ctx, func(context.Context) error { return nil })
	if err != nil {
		return err
	}
	defer func() { _ = window.Close() }()
	windowed.ServeHotWindow(window)
	fmt.Fprintf(w, "window built in %s: %+v\n", time.Since(built).Round(time.Millisecond), window.Stats())
	file, err := os.Open(traffic) //nolint:gosec // G304: the owner names the capture to replay.
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	out, err := os.Create(report) //nolint:gosec // G304: the owner names the report.
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	differences := json.NewEncoder(out)
	type tally struct{ same, different int }
	classes := map[string]*tally{}
	skipped, different := 0, 0
	lines := bufio.NewScanner(file)
	lines.Buffer(nil, 64<<20)
	for lines.Scan() {
		var entry capturedRequest
		if err := json.Unmarshal(lines.Bytes(), &entry); err != nil {
			return fmt.Errorf("read a captured request: %w", err)
		}
		if entry.Hook == "" && (wire.Stream(entry.Argv) || len(entry.Argv) > 1 && entry.Argv[1] == "window") {
			skipped++
			continue
		}
		a, err := replay(ctx, plain, plainHome, entry)
		if err != nil {
			return fmt.Errorf("replay %v on the store: %w", entry.Argv, err)
		}
		b, err := replay(ctx, windowed, windowedHome, entry)
		if err != nil {
			return fmt.Errorf("replay %v through the window: %w", entry.Argv, err)
		}
		class := parityClass(entry)
		if classes[class] == nil {
			classes[class] = &tally{}
		}
		if a == b {
			classes[class].same++
			continue
		}
		classes[class].different++
		different++
		if err := differences.Encode(map[string]any{
			"class": class, "argv": entry.Argv, "hook": entry.Hook, "at": entry.At,
			"store":  map[string]any{"stdout": a.stdout, "stderr": a.stderr, "code": a.code, "cursors": a.cursors, "records": a.records},
			"window": map[string]any{"stdout": b.stdout, "stderr": b.stderr, "code": b.code, "cursors": b.cursors, "records": b.records},
		}); err != nil {
			return err
		}
	}
	if err := lines.Err(); err != nil {
		return err
	}
	names := make([]string, 0, len(classes))
	for name := range classes {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		fmt.Fprintf(w, "%-34s same=%d different=%d\n", name, classes[name].same, classes[name].different)
	}
	matrix, mismatches, err := windowMatrix(ctx, windowed, window, differences)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "streams skipped=%d\nreader matrix compared=%d different=%d\nwindow %+v\n", skipped, matrix, mismatches, window.Stats())
	if different+mismatches > 0 {
		return fmt.Errorf("%d requests and %d reader filters differ; see %s", different, mismatches, report)
	}
	return nil
}

func windowMatrix(ctx context.Context, st *store.Store, window *store.MemWindow, differences *json.Encoder) (int, int, error) {
	frozen := *st
	at := st.Now()
	frozen.Now = func() time.Time { return at }
	st = &frozen
	readers, err := st.RecentReaders(ctx, matrixLanes)
	if err != nil {
		return 0, 0, err
	}
	stats := window.Stats()
	afters := []int64{stats.Floor - 1, max(stats.Floor-1, stats.Head-matrixSpan)}
	compared, mismatched := 0, 0
	askKinds := []kinds.Kind{kinds.Ask, kinds.Decide, kinds.Blocker, kinds.Hold}
	for _, reader := range readers {
		shapes := []store.Filter{
			{Drive: reader.Drive, For: reader.Lane},
			{Drive: reader.Drive, To: reader.Lane},
			{Drive: reader.Drive, To: reader.Lane, ToKinds: askKinds},
			{Drive: reader.Drive, Addressed: reader.Lane},
			{Drive: reader.Drive, Lanes: []string{reader.Lane}},
			{Drive: reader.Drive, Kinds: askKinds},
			{Drive: reader.Drive, Mute: askKinds},
			{Drive: reader.Drive, For: reader.Lane, RepeatWindow: store.UrgentRepeat},
			{Drive: reader.Drive, For: reader.Lane, Kinds: askKinds, ToKinds: askKinds},
			{Drive: reader.Drive, IncludeExpired: true},
		}
		for _, shape := range shapes {
			for _, after := range afters {
				for _, f := range []store.Filter{parityAfter(shape, after), parityNewest(shape)} {
					got, answered, err := window.Answer(ctx, st, f)
					if err != nil {
						return compared, mismatched, err
					}
					if !answered {
						continue
					}
					st.ServeHotWindow(nil)
					want, err := st.Query(ctx, f)
					st.ServeHotWindow(window)
					if err != nil {
						return compared, mismatched, err
					}
					compared++
					a, _ := json.Marshal(want)
					b, _ := json.Marshal(got)
					if !bytes.Equal(a, b) {
						mismatched++
						if err := differences.Encode(map[string]any{"class": "matrix", "reader": reader, "filter": fmt.Sprintf("%+v", f), "store": len(want), "window": len(got)}); err != nil {
							return compared, mismatched, err
						}
					}
				}
			}
		}
	}
	return compared, mismatched, nil
}

func parityAfter(f store.Filter, after int64) store.Filter {
	f.After = after
	return f
}

func parityNewest(f store.Filter) store.Filter {
	f.Descending, f.Limit = true, 40
	return f
}
