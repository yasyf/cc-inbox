package cli_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
)

type tailBodyCursor struct {
	Name  string
	Drive string
	Seq   int64
}

type tailBodyContractResult struct {
	CLI           nativeTailResult
	Panic         string
	CursorsBefore []tailBodyCursor
	CursorsAfter  []tailBodyCursor
}

func TestTailBodyContractReference(t *testing.T) {
	dir := os.Getenv("CCI_TAIL_BODY_CONTRACT_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable text Tail Body contract snapshots")
	}
	for _, variant := range []struct {
		name       string
		body       any
		jsonPanics bool
	}{
		{"empty", "", false},
		{"SQL NULL", nil, false},
		{"JSON null", "null", false},
		{"object", `{"payload":"opaque"}`, false},
		{"array", `[1,true,"opaque"]`, false},
		{"string", `"opaque"`, false},
		{"integer affinity", int64(123), false},
		{"real affinity", 1.5, false},
		{"JSON BLOB", []byte(`{}`), false},
		{"malformed", "{", true},
		{"invalid UTF8 BLOB", []byte{255}, true},
		{"large", `{"payload":"` + strings.Repeat("body-only-token ", 2048) + `"}`, false},
	} {
		for _, call := range []nativeTailCall{
			{name: "text", claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--budget", "4000000", "--width", "700"}},
			{name: "text tiny budget", claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--budget", "1"}},
			{name: "text reader and fold", claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--reader", "api", "--budget", "700"}},
			{name: "text whole", claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--all", "--budget", "4000000", "--width", "700"}},
			{name: "named cursor", claude: "reader", args: []string{"tail", "--drive", "d", "--cursor", "body-private", "--budget", "4000000", "--width", "700"}},
			{name: "JSON", claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--json", "--budget", "4000000"}},
		} {
			t.Run(variant.name+"/"+call.name, func(t *testing.T) {
				fixture := newNativeTailFixture(t, call, nil)
				var objectType, bodyType string
				if err := fixture.db.QueryRowContext(t.Context(), "SELECT type FROM sqlite_schema WHERE name='records'").Scan(&objectType); err != nil {
					t.Fatal(err)
				}
				if err := fixture.db.QueryRowContext(t.Context(), "SELECT type FROM pragma_table_info('records') WHERE name='body'").Scan(&bodyType); err != nil {
					t.Fatal(err)
				}
				if objectType != "table" || bodyType != "TEXT" {
					t.Fatalf("private production Body schema is %s/%s", objectType, bodyType)
				}
				if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET body=? WHERE seq=4", variant.body); err != nil {
					t.Fatal(err)
				}
				if call.name == "named cursor" {
					if err := fixture.st.SetCursor(t.Context(), "body-private", "d", 0); err != nil {
						t.Fatal(err)
					}
				}
				before := tailBodyRecords(t, fixture.db)
				cursors := tailBodyCursors(t, fixture.db)
				got := runTailBodyContract(t, call.args, fixture.node)
				got.CursorsBefore = cursors
				got.CursorsAfter = tailBodyCursors(t, fixture.db)
				if !slices.Equal(before, tailBodyRecords(t, fixture.db)) {
					t.Fatal("Tail changed private records")
				}
				if call.name == "JSON" && variant.jsonPanics {
					if !strings.HasPrefix(got.Panic, "marshal record #4:") {
						t.Fatalf("immutable invalid Body panic=%q", got.Panic)
					}
				} else if got.Panic != "" || got.CLI.Code != 0 {
					t.Fatalf("unexpected Tail failure: %+v", got)
				}
				if call.name == "named cursor" {
					if !slices.Equal(got.CursorsAfter, []tailBodyCursor{{"body-private", "d", 17}}) {
						t.Fatalf("named cursor changed to %+v", got.CursorsAfter)
					}
				} else if len(got.CursorsBefore) != 0 || len(got.CursorsAfter) != 0 {
					t.Fatalf("explicit since created cursors: %+v", got)
				}
				tailBodyCheckSnapshot(t, dir, got)
			})
		}
	}
}

func TestTailBodyContractPageFrontier(t *testing.T) {
	dir := os.Getenv("CCI_TAIL_BODY_CONTRACT_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable text Tail Body contract snapshots")
	}
	for _, budget := range []string{"64", "4000000"} {
		t.Run(budget, func(t *testing.T) {
			call := nativeTailCall{claude: "reader", args: []string{"tail", "--drive", "d", "--since-seq", "0", "--budget", budget, "--width", "700"}}
			fixture := newNativeTailFixture(t, call, nil)
			for _, statement := range []string{
				"UPDATE records SET drive='d'",
				`WITH RECURSIVE history(n) AS (VALUES(18) UNION ALL SELECT n+1 FROM history WHERE n<501)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','worker','note',4102444800000,'page-'||n,'post','page-'||n FROM history`,
				"UPDATE records SET refs='{' WHERE seq=1",
			} {
				if _, err := fixture.db.ExecContext(t.Context(), statement); err != nil {
					t.Fatal(err)
				}
			}
			before := tailBodyRecords(t, fixture.db)
			if len(before) != 501 {
				t.Fatalf("private page fixture rows=%d, want 501", len(before))
			}
			cursors := tailBodyCursors(t, fixture.db)
			got := runTailBodyContract(t, call.args, fixture.node)
			got.CursorsBefore = cursors
			got.CursorsAfter = tailBodyCursors(t, fixture.db)
			if !slices.Equal(before, tailBodyRecords(t, fixture.db)) || len(got.CursorsBefore) != 0 || len(got.CursorsAfter) != 0 || got.Panic != "" {
				t.Fatalf("private page state or panic changed: %+v", got)
			}
			if budget == "64" && got.CLI.Code != 0 || budget == "4000000" && (got.CLI.Code != 1 || !bytes.Contains(got.CLI.Err, []byte("decode refs of #1:"))) {
				t.Fatalf("unexpected private page frontier: %+v", got)
			}
			tailBodyCheckSnapshot(t, dir, got)
		})
	}
}

func tailBodyCheckSnapshot(t *testing.T, dir string, got tailBodyContractResult) {
	t.Helper()
	path := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".json")
	if os.Getenv("CCI_TAIL_BODY_CONTRACT_WRITE") == "1" {
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
	var want tailBodyContractResult
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if got.CLI.Code != want.CLI.Code || !bytes.Equal(got.CLI.Out, want.CLI.Out) || !bytes.Equal(got.CLI.Err, want.CLI.Err) || got.Panic != want.Panic || !slices.Equal(got.CursorsBefore, want.CursorsBefore) || !slices.Equal(got.CursorsAfter, want.CursorsAfter) {
		t.Fatalf("Tail Body contract changed: got=%+v want=%+v", got, want)
	}
}

func runTailBodyContract(t *testing.T, args []string, node string) (result tailBodyContractResult) {
	t.Helper()
	host := time.Local
	zone, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		t.Fatal(err)
	}
	time.Local = zone
	defer func() { time.Local = host }()
	var out, stderr bytes.Buffer
	code := 0
	defer func() {
		if value := recover(); value != nil {
			result.Panic = fmt.Sprint(value)
		}
		result.CLI = normalizeNativeTail(nativeTailResult{Code: code, Out: out.Bytes(), Err: stderr.Bytes()}, node)
	}()
	cmd := cli.NewRootCmd()
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	if err := cmd.ExecuteContext(t.Context()); err != nil {
		_, _ = fmt.Fprintln(&stderr, "cci:", err)
		code = 1
	}
	return result
}

func tailBodyCursors(t *testing.T, db *sql.DB) []tailBodyCursor {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT name,drive,seq FROM cursors ORDER BY name,drive")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []tailBodyCursor
	for rows.Next() {
		var row tailBodyCursor
		if err := rows.Scan(&row.Name, &row.Drive, &row.Seq); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func tailBodyRecords(t *testing.T, db *sql.DB) []string {
	t.Helper()
	columns, err := db.QueryContext(t.Context(), "SELECT name FROM pragma_table_info('records') ORDER BY cid")
	if err != nil {
		t.Fatal(err)
	}
	var projection []string
	for columns.Next() {
		var name string
		if err := columns.Scan(&name); err != nil {
			t.Fatal(err)
		}
		quoted := `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		projection = append(projection, "typeof("+quoted+")", "hex("+quoted+")")
	}
	if err := columns.Err(); err != nil {
		t.Fatal(err)
	}
	if err := columns.Close(); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(t.Context(), "SELECT json_array("+strings.Join(projection, ",")+") FROM records ORDER BY seq")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
