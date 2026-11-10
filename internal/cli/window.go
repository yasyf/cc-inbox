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
	"runtime"
	"runtime/pprof"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/channel"
	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/hook"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/wire"
)

const (
	captureFile = "traffic.capture"
	matrixSpan  = 500
	matrixLanes = 16
	matrixPage  = 500
)

var (
	capturedEnv = regexp.MustCompile(`^(CLAUDE|CODEX|CCI)_`)
	secretEnv   = regexp.MustCompile(`TOKEN|KEY|SECRET|PASSWORD|COOKIE|AUTH`)
)

func newWindowCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "window",
		Short: "Turn the daemon's read model on or off, show it, or compare it with the store",
	}
	home := func(cmd *cobra.Command) (string, error) {
		if inv := invoked(cmd.Context()); inv != nil && inv.st != nil {
			return inv.st.HomeDir(), nil
		}
		return store.Home()
	}
	cmd.AddCommand(newWindowParityCmd(), newWindowModelCmd(home))
	return cmd
}

func newWindowModelCmd(home func(*cobra.Command) (string, error)) *cobra.Command {
	cmd := &cobra.Command{Use: "model", Short: "Turn the daemon's read model on or off, or show it"}
	turn := func(on bool) func(*cobra.Command, []string) error {
		return func(cmd *cobra.Command, _ []string) error {
			at, err := home(cmd)
			if err != nil {
				return err
			}
			return store.SetReadModel(at, on)
		}
	}
	cmd.AddCommand(
		&cobra.Command{Use: "on", Short: "Serve record selections from memory; the daemon follows within a second", Args: cobra.NoArgs, RunE: turn(true)},
		&cobra.Command{Use: "off", Short: "Serve every read from the store; the daemon follows within a second", Args: cobra.NoArgs, RunE: turn(false)},
		&cobra.Command{Use: "status", Short: "Print what the read model holds and how often it answered", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
			at, err := home(cmd)
			if err != nil {
				return err
			}
			inv := invoked(cmd.Context())
			if inv == nil || inv.st == nil || inv.st.ServedReadModel() == nil {
				_, err := fmt.Fprintf(cmd.OutOrStdout(), "switch on=%t; no read model is serving here\n", store.ReadModelOn(at))
				return err
			}
			s := inv.st.ServedReadModel().Stats()
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "switch on=%t rows=%d floor=%d search_floor=%d text_bytes=%d head=%d bytes=%d cap=%d hits=%d misses=%d failures=%d\n", store.ReadModelOn(at), s.Rows, s.Floor, s.SearchFloor, s.TextBytes, s.Head, s.Bytes, int64(store.ReadModelBytes), s.Hits, s.Misses, s.Failures)
			return err
		}},
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
	Embedder   bool              `json:"embedder,omitempty"`
	Hook       string            `json:"hook,omitempty"`
	Payload    []byte            `json:"payload,omitempty"`
	Stdin      []byte            `json:"stdin,omitempty"`
}

func capture(home string, entry capturedRequest) {
	destination, err := os.ReadFile(filepath.Join(home, captureFile)) //nolint:gosec // G304: home is the store's own directory.
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

func capturedEnvOf(all map[string]string) map[string]string {
	env := map[string]string{}
	for name, value := range all {
		if capturedEnv.MatchString(name) && !secretEnv.MatchString(name) {
			env[name] = value
		}
	}
	return env
}

func captureCommand(home string, request wire.Request) {
	if _, err := os.Stat(filepath.Join(home, captureFile)); err != nil {
		return
	}
	if len(request.Argv) > 1 && request.Argv[1] == "embed" {
		return
	}
	capture(home, capturedRequest{Argv: request.Argv, Env: capturedEnvOf(requestEnv(request.Env)), Cwd: request.Cwd, PID: request.PID, StdoutChar: request.StdoutChar, Embedder: request.Embedder})
}

func captureEmbedded(ctx context.Context, home, op string, c EmbedCall) {
	if _, err := os.Stat(filepath.Join(home, captureFile)); err != nil {
		return
	}
	inv := invoked(ctx)
	if inv == nil {
		return
	}
	stdin, err := json.Marshal(c)
	if err != nil {
		return
	}
	capture(home, capturedRequest{Argv: []string{"cci", "embed", op}, Env: capturedEnvOf(inv.env), Cwd: inv.cwd, PID: inv.pid, Stdin: stdin})
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
	inv := &invocation{env: entry.Env, cwd: entry.Cwd, pid: entry.PID, stdoutChar: entry.StdoutChar, embedder: entry.Embedder, st: &view}
	hosted := channel.Hosted(context.WithValue(ctx, invocationKey{}, inv))
	if entry.Hook != "" {
		p, err := hook.Read(bytes.NewReader(entry.Payload))
		if err != nil {
			return replayed{}, err
		}
		run = runHookEvent(hosted, &view, entry.Hook, p, &stdout)
	} else {
		root := NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below.
		root.SetArgs(entry.Argv[1:])
		root.SetOut(&stdout)
		root.SetErr(&stderr)
		root.SetIn(bytes.NewReader(entry.Stdin))
		run = root.ExecuteContext(hosted)
	}
	out := replayed{stdout: stdout.String(), stderr: stderr.String()}
	if run != nil {
		out.code, out.stderr = ExitCode(run), out.stderr+run.Error()
	}
	out.stdout, out.stderr = strings.ReplaceAll(out.stdout, home, "<home>"), strings.ReplaceAll(out.stderr, home, "<home>")
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
	var limit int64
	var opts parityOptions
	cmd := &cobra.Command{
		Use:    "parity",
		Short:  "Replay captured traffic through the store and the read model on two copies of a store, and diff everything",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runWindowParity(cmd.Context(), cmd.OutOrStdout(), plain, windowed, traffic, report, limit, opts)
		},
	}
	cmd.Flags().StringVar(&plain, "plain", "", "home of the copy that reads the store alone")
	cmd.Flags().StringVar(&windowed, "windowed", "", "home of the copy that reads through the read model")
	cmd.Flags().StringVar(&traffic, "traffic", "", "captured requests, one JSON document a line")
	cmd.Flags().StringVar(&report, "report", "", "file for each difference, one JSON document a line")
	cmd.Flags().Int64Var(&limit, "cap", store.ReadModelBytes, "bytes the read model may hold")
	cmd.Flags().BoolVar(&opts.replayOnly, "replay-only", false, "stop after the captured traffic, without the reader matrix")
	cmd.Flags().BoolVar(&opts.matrixOnly, "matrix-only", false, "run the reader matrix without replaying the captured traffic")
	cmd.Flags().StringVar(&opts.profile, "profile", "", "file for a CPU profile, labeled side=served on the read through the model")
	for _, name := range []string{"plain", "windowed", "traffic", "report"} {
		_ = cmd.MarkFlagRequired(name)
	}
	return cmd
}

type parityOptions struct {
	replayOnly, matrixOnly bool
	profile                string
}

func runWindowParity(ctx context.Context, w io.Writer, plainHome, windowedHome, traffic, report string, limit int64, opts parityOptions) error {
	say := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
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
	var heap0 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heap0)
	model, err := windowed.BuildReadModelUnder(ctx, func(context.Context) error { return nil }, limit)
	if err != nil {
		return err
	}
	defer func() { _ = model.Close() }()
	serve := func(on bool) {
		if on {
			windowed.ServeReadModel(model)
		} else {
			windowed.ServeReadModel(nil)
		}
	}
	describe := func() string { return fmt.Sprintf("%+v", model.Stats()) }
	serve(true)
	var heap1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heap1)
	say("built in %s, heap %.0f MB: %s\n", time.Since(built).Round(time.Millisecond), (float64(heap1.HeapAlloc)-float64(heap0.HeapAlloc))/1e6, describe())
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
	type tally struct {
		same, different           int
		storeNanos, windowNanos   int64
		storeFaults, windowFaults int64
	}
	usage := func() (int64, int64) {
		var u syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &u)
		return u.Utime.Nano() + u.Stime.Nano(), u.Minflt
	}
	classes := map[string]*tally{}
	skipped, different := 0, 0
	if opts.profile != "" {
		profile, err := os.Create(opts.profile)
		if err != nil {
			return err
		}
		defer func() { _ = profile.Close() }()
		if err := pprof.StartCPUProfile(profile); err != nil {
			return err
		}
		defer pprof.StopCPUProfile()
	}
	lines := bufio.NewScanner(file)
	lines.Buffer(nil, 64<<20)
	for lines.Scan() {
		var entry capturedRequest
		if err := json.Unmarshal(lines.Bytes(), &entry); err != nil {
			return fmt.Errorf("read a captured request: %w", err)
		}
		if opts.matrixOnly || entry.Hook == "" && (wire.Stream(entry.Argv) || len(entry.Argv) > 1 && entry.Argv[1] == "window") {
			skipped++
			continue
		}
		harness.KeepSessions(0)
		cpu0, faults0 := usage()
		a, err := replay(ctx, plain, plainHome, entry)
		if err != nil {
			return fmt.Errorf("replay %v on the store: %w", entry.Argv, err)
		}
		cpu1, faults1 := usage()
		harness.KeepSessions(time.Second)
		var b replayed
		pprof.Do(ctx, pprof.Labels("side", "served"), func(ctx context.Context) {
			b, err = replay(ctx, windowed, windowedHome, entry)
		})
		if err != nil {
			return fmt.Errorf("replay %v through the read model: %w", entry.Argv, err)
		}
		cpu2, faults2 := usage()
		class := parityClass(entry)
		if classes[class] == nil {
			classes[class] = &tally{}
		}
		t := classes[class]
		t.storeNanos += cpu1 - cpu0
		t.windowNanos += cpu2 - cpu1
		t.storeFaults += faults1 - faults0
		t.windowFaults += faults2 - faults1
		if a == b {
			classes[class].same++
			continue
		}
		classes[class].different++
		different++
		if err := differences.Encode(map[string]any{
			"class": class, "argv": entry.Argv, "hook": entry.Hook, "at": entry.At,
			"store":  map[string]any{"stdout": a.stdout, "stderr": a.stderr, "code": a.code, "cursors": a.cursors, "records": a.records},
			"served": map[string]any{"stdout": b.stdout, "stderr": b.stderr, "code": b.code, "cursors": b.cursors, "records": b.records},
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
		t := classes[name]
		count := int64(t.same + t.different)
		say("%-30s same=%-5d different=%-3d cpu/req store=%6.1fms served=%6.1fms faults/req store=%6d served=%6d\n", name, t.same, t.different,
			float64(t.storeNanos)/float64(count)/1e6, float64(t.windowNanos)/float64(count)/1e6, t.storeFaults/count, t.windowFaults/count)
	}
	var heapAfter runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&heapAfter)
	say("heap after replay %.0f MB\n", (float64(heapAfter.HeapAlloc)-float64(heap0.HeapAlloc))/1e6)
	if opts.profile != "" {
		heapProfile, err := os.Create(opts.profile + ".heap")
		if err != nil {
			return err
		}
		defer func() { _ = heapProfile.Close() }()
		if err := pprof.Lookup("heap").WriteTo(heapProfile, 0); err != nil {
			return err
		}
	}
	if opts.replayOnly {
		say("streams skipped=%d\nserved %s\n", skipped, describe())
		return nil
	}
	matrix, mismatches, err := windowMatrix(ctx, windowed, model, serve, differences)
	if err != nil {
		return err
	}
	say("streams skipped=%d\nreader matrix compared=%d different=%d\nserved %s\n", skipped, matrix, mismatches, describe())
	if different+mismatches > 0 {
		return fmt.Errorf("%d requests and %d reader filters differ; see %s", different, mismatches, report)
	}
	return nil
}

func windowMatrix(ctx context.Context, st *store.Store, reads *store.ReadModel, serve func(on bool), differences *json.Encoder) (int, int, error) {
	frozen := *st
	at := st.Now()
	frozen.Now = func() time.Time { return at }
	st = &frozen
	readers, err := st.RecentReaders(ctx, matrixLanes)
	if err != nil {
		return 0, 0, err
	}
	floor, head := reads.Bounds()
	afters := []int64{floor - 1, max(floor-1, head-matrixSpan)}
	compared, mismatched := 0, 0
	seenDrives := map[string]bool{}
	askKinds := []kinds.Kind{kinds.Ask, kinds.Decide, kinds.Blocker, kinds.Hold}
	for _, reader := range readers {
		if !seenDrives[reader.Drive] {
			seenDrives[reader.Drive] = true
			since := at.Add(-24 * time.Hour)
			open, untracked, older, err := st.OpenSince(ctx, reader.Drive, since)
			if err != nil {
				return compared, mismatched, err
			}
			serve(false)
			wantOpen, wantUntracked, wantOlder, err := st.OpenSince(ctx, reader.Drive, since)
			serve(true)
			if err != nil {
				return compared, mismatched, err
			}
			compared++
			a, _ := json.Marshal([]any{wantOpen, wantUntracked, wantOlder})
			b, _ := json.Marshal([]any{open, untracked, older})
			if !bytes.Equal(a, b) {
				mismatched++
				if err := differences.Encode(map[string]any{"class": "matrix-open-since", "drive": reader.Drive, "store": string(a), "served": string(b)}); err != nil {
					return compared, mismatched, err
				}
			}
		}
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
			{Drive: reader.Drive, To: reader.Lane, FirstOnly: askKinds},
			{Drive: reader.Drive, Contains: []string{reader.Lane}},
			{Drive: reader.Drive, Contains: []string{"pr", "45"}},
			{Drive: reader.Drive, Since: at.Add(-48 * time.Hour)},
			{Drive: reader.Drive, Lanes: []string{reader.Lane}, Until: at.Add(-time.Hour), Since: at.Add(-72 * time.Hour)},
			{Drive: reader.Drive, PR: store.PR{Number: 422}},
			{Drive: reader.Drive, Stack: reader.Lane},
			{Drive: reader.Drive, Peer: reader.Lane, Kinds: askKinds},
		}
		for _, shape := range shapes {
			filters := []store.Filter{parityNewest(shape), parityOldest(shape)}
			for _, after := range afters {
				filters = append(filters, parityAfter(shape, after))
			}
			for _, f := range filters {
				got, answered, err := reads.Answer(ctx, st, f)
				if err != nil {
					return compared, mismatched, err
				}
				if !answered {
					continue
				}
				serve(false)
				want, err := st.Query(ctx, f)
				if err != nil {
					serve(true)
					return compared, mismatched, err
				}
				wantCount, err := st.Count(ctx, f)
				serve(true)
				if err != nil {
					return compared, mismatched, err
				}
				gotCount, err := st.Count(ctx, f)
				if err != nil {
					return compared, mismatched, err
				}
				seqs := make([]int64, 0, len(want))
				for _, r := range want {
					if r.Drive == reader.Drive {
						seqs = append(seqs, r.Seq)
					}
				}
				gotOpen, err := st.OpenSeqsAmong(ctx, reader.Drive, seqs)
				if err != nil {
					return compared, mismatched, err
				}
				serve(false)
				wantOpen, err := st.OpenSeqsAmong(ctx, reader.Drive, seqs)
				serve(true)
				if err != nil {
					return compared, mismatched, err
				}
				slices.Sort(gotOpen)
				slices.Sort(wantOpen)
				if !slices.Equal(gotOpen, wantOpen) {
					mismatched++
					if err := differences.Encode(map[string]any{"class": "matrix-open", "reader": reader, "filter": fmt.Sprintf("%+v", f), "store": wantOpen, "served": gotOpen}); err != nil {
						return compared, mismatched, err
					}
				}
				compared++
				if wantCount != gotCount {
					mismatched++
					if err := differences.Encode(map[string]any{"class": "matrix-count", "reader": reader, "filter": fmt.Sprintf("%+v", f), "store": wantCount, "served": gotCount}); err != nil {
						return compared, mismatched, err
					}
				}
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

func parityOldest(f store.Filter) store.Filter {
	f.After, f.Limit = 0, matrixPage
	return f
}
