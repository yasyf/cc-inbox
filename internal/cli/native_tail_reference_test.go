package cli_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeTailCall struct {
	name                 string
	claude, codex, agent string
	args                 []string
	tier                 string
}

type nativeTailResult struct {
	Code int    `json:"returncode"`
	Out  []byte `json:"stdout_b64"`
	Err  []byte `json:"stderr_b64"`
}

type nativeTailFixture struct {
	home string
	node string
	st   *store.Store
	db   *sql.DB
}

func nativeTailCalls() []nativeTailCall {
	var calls []nativeTailCall
	for _, call := range nativeTailBaseCalls {
		if call.name == "json peer cursor" {
			continue
		}
		session := ""
		if call.session {
			session = "reader"
		}
		calls = append(calls, nativeTailCall{name: call.name, claude: session, args: append([]string(nil), call.args...)})
	}
	for _, call := range []struct {
		name string
		args []string
	}{
		{"equals since and kinds", []string{"--kind=hold", "--kind=note"}},
		{"repeat lanes", []string{"--lane", "api", "--lane=root"}},
		{"pr", []string{"--pr", "29020"}},
		{"topic", []string{"--topic", "#29020"}},
		{"mute", []string{"--mute-kinds", "note"}},
		{"to kind", []string{"--to-kind", "hold"}},
		{"stack", []string{"--stack", "p/e"}},
		{"target", []string{"--target", "prod"}},
		{"limit short", []string{"-n1"}},
		{"limit equals", []string{"--limit=1"}},
		{"width all", []string{"--width", "12", "--all"}},
		{"cursor ignored", []string{"--cursor", "other"}},
		{"human bare number", []string{"--since", "12"}},
		{"human qualified number", []string{"--since", "#12@feedface"}},
		{"unknown flag", []string{"--not-a-tail-flag"}},
		{"invalid kind", []string{"--kind", "not-a-kind"}},
		{"invalid to kind", []string{"--to-kind", "not-a-kind"}},
		{"invalid pr", []string{"--pr", "not-a-pr"}},
		{"invalid budget", []string{"--budget", "not-an-int"}},
		{"position", []string{"unexpected"}},
		{"help", []string{"--help"}},
	} {
		args := append([]string{"tail", "--drive=d", "--since=0", "--json", "--budget", "4000000"}, call.args...)
		calls = append(calls, nativeTailCall{name: call.name, claude: "reader", args: args})
	}
	calls = append(calls,
		nativeTailCall{name: "empty since", claude: "reader", args: []string{"tail", "--drive", "d", "--since", "", "--json"}},
		nativeTailCall{name: "bad since", claude: "reader", args: []string{"tail", "--drive", "d", "--since", "not-a-time", "--json"}},
		nativeTailCall{name: "missing since value", args: []string{"tail", "--drive", "d", "--since"}},
		nativeTailCall{name: "agent", claude: "reader", agent: "worker", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
		nativeTailCall{name: "codex fallback", codex: "reader", agent: "worker", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
		nativeTailCall{name: "claude precedence", claude: "reader", codex: "other", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
		nativeTailCall{name: "agent without session", agent: "worker", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
		nativeTailCall{name: "explicit to suppresses peer", claude: "reader", agent: "worker", args: []string{"tail", "--drive", "d", "--since", "0", "--to", "api", "--json"}},
	)
	calls = append(calls, []nativeTailCall{
		{name: "service json", tier: "service", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
		{name: "service text", tier: "service", args: []string{"tail", "--drive", "d", "--since", "0"}},
		{name: "service human bare number", tier: "service", args: []string{"tail", "--drive", "d", "--since", "12", "--json"}},
		{name: "service human qualified number", tier: "service", args: []string{"tail", "--drive", "d", "--since", "#12@feedface", "--json"}},
		{name: "explicit owner json", tier: "owner", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}},
	}...)
	return calls
}

func newNativeTailFixture(t *testing.T, call nativeTailCall, sets []nativeTailSet) nativeTailFixture {
	t.Helper()
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", call.claude)
	t.Setenv("CODEX_THREAD_ID", call.codex)
	t.Setenv("CCI_AGENT_ID", call.agent)
	started := time.Now()
	st, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	opened := time.Now()
	t.Cleanup(func() {
		started := time.Now()
		_ = st.Close()
		if os.Getenv("CCI_NATIVE_TAIL_FIXTURE_TIMINGS") == "1" {
			t.Logf("st_close=%s", time.Since(started))
		}
	})
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		started := time.Now()
		_ = db.Close()
		if os.Getenv("CCI_NATIVE_TAIL_FIXTURE_TIMINGS") == "1" {
			t.Logf("db_close=%s", time.Since(started))
		}
	})
	node := st.Self()
	seedNativeTailFixture(t, db, node)
	seeded := time.Now()
	if call.tier != "" {
		if err := relay.SaveConfig(home, relay.Config{Mesh: "native", URL: "https://relay.invalid", Tier: call.tier}); err != nil {
			t.Fatal(err)
		}
	}
	address, err := json.Marshal([]string{store.PeerAddressOn(node, store.PeerKey{Session: "reader", Agent: "worker"}.String())})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,recipients,source,hash)
VALUES('f','worker','note',4102444800000,'worker mail',?,'post','native-mail')`, string(address)); err != nil {
		t.Fatal(err)
	}
	refs, err := json.Marshal(store.Refs{PRs: []store.PR{{Number: 29020}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,topic,refs,source,hash)
VALUES('d','held','hold',4102444800000,'hold #29020','#29020',?,'post','native-hold')`, string(refs)); err != nil {
		t.Fatal(err)
	}
	for _, set := range sets {
		if _, err := db.ExecContext(t.Context(), "UPDATE records SET "+set.set+" WHERE seq = ?", set.seq); err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("CCI_NATIVE_TAIL_FIXTURE_TIMINGS") == "1" {
		t.Logf("fresh_store_open=%s seed_15_rows_and_origin=%s remaining_setup=%s", opened.Sub(started), seeded.Sub(opened), time.Since(seeded))
	}
	return nativeTailFixture{home: home, node: node, st: st, db: db}
}

func runNativeTailReference(t *testing.T, args []string) nativeTailResult {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := cli.NewRootCmd()
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	code := 0
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		_, _ = fmt.Fprintln(&stderr, "cci:", err)
		code = 1
	}
	return nativeTailResult{Code: code, Out: out.Bytes(), Err: stderr.Bytes()}
}

func normalizeNativeTail(result nativeTailResult, node string) nativeTailResult {
	result.Out = bytes.ReplaceAll(result.Out, []byte(node), []byte("<node>"))
	result.Err = bytes.ReplaceAll(result.Err, []byte(node), []byte("<node>"))
	result.Out = bytes.ReplaceAll(result.Out, []byte(node[:8]), []byte("<node-tag>"))
	result.Err = bytes.ReplaceAll(result.Err, []byte(node[:8]), []byte("<node-tag>"))
	return result
}

func nativeTailSnapshotPath(name string) string {
	key := strings.NewReplacer("/", "_", " ", "-").Replace("TestNativeTailCLIReference/" + name)
	return filepath.Join(os.Getenv("CCI_NATIVE_TAIL_SNAPSHOTS"), key+".json")
}

func TestNativeTailCLIReference(t *testing.T) {
	if os.Getenv("CCI_NATIVE_TAIL_SNAPSHOTS") == "" {
		t.Skip("CI supplies immutable native Tail reference snapshots")
	}
	for _, variant := range nativeTailVariants {
		for _, call := range nativeTailCalls() {
			t.Run(variant.name+"/"+call.name, func(t *testing.T) {
				fixture := newNativeTailFixture(t, call, variant.sets)
				got := normalizeNativeTail(runNativeTailReference(t, call.args), fixture.node)
				if variant.name == "clean" && call.name == "mute" && (got.Code != 0 || len(got.Out) == 0 || bytes.Contains(got.Out, []byte(`"kind":"note"`))) {
					t.Fatalf("--mute-kinds did not exclude notes: code=%d output=%q error=%q", got.Code, got.Out, got.Err)
				}
				path := nativeTailSnapshotPath(variant.name + "/" + call.name)
				if os.Getenv("CCI_NATIVE_TAIL_SNAPSHOT_WRITE") == "1" {
					raw, err := json.Marshal(got)
					if err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, raw, 0o600); err != nil {
						t.Fatal(err)
					}
					return
				}
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var want nativeTailResult
				if err := json.Unmarshal(raw, &want); err != nil {
					t.Fatal(err)
				}
				if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
					t.Fatalf("CLI changed from immutable reference: code=%d/%d stdout=%q/%q stderr=%q/%q", got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
				}
			})
		}
	}
}
