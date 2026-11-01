package cli_test

import (
	"fmt"
	"os"
	"slices"
	"testing"
)

func TestTailPageAssemblyContract(t *testing.T) {
	dir := os.Getenv("CCI_TAIL_PAGE_CONTRACT_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable current-head Tail page snapshots")
	}
	for _, variant := range []struct {
		name       string
		population int
		sets       []nativeTailSet
	}{
		{"empty", 0, nil},
		{"one", 1, nil},
		{"exact pages", 1000, nil},
		{"multiple pages", 1250, nil},
		{"scan frontier", 1250, []nativeTailSet{{18, `refs='{'`}, {518, `fields='{'`}, {1019, `also_resolves='['`}}},
		{"recipient frontier", 1250, []nativeTailSet{{18, `recipients='['`}, {1019, `recipients='['`}}},
		{"Body frontier", 1250, []nativeTailSet{{18, `body='{'`}, {1018, `body='['`}, {1019, `body='{'`}}},
	} {
		for _, mode := range []struct {
			name string
			args []string
		}{
			{"newest", []string{"--since-seq", "0"}},
			{"oldest", []string{"--since-seq", "0", "--oldest"}},
			{"limited", []string{"--since-seq", "0", "-n", "700"}},
			{"named reader", []string{"--reader", "api", "--cursor", "page-reader"}},
		} {
			for _, budget := range []string{"1", "4000000"} {
				for _, asJSON := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/%s/budget=%s/json=%t", variant.name, mode.name, budget, asJSON), func(t *testing.T) {
						args := []string{"tail", "--drive", "pages", "--budget", budget, "--width", "700"}
						args = append(args, mode.args...)
						if asJSON {
							args = append(args, "--json")
						}
						fixture := newNativeTailFixture(t, nativeTailCall{claude: "page-session", args: args}, nil)
						if _, err := fixture.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (
SELECT 1 WHERE ?>0 UNION ALL SELECT n+1 FROM history WHERE n<?
)
INSERT INTO records(drive,lane,kind,at,text,recipients,resolves,body,source,hash)
SELECT 'pages','worker',CASE WHEN n%500=0 THEN 'ask' WHEN n>1 AND n%500=1 THEN 'done' ELSE 'note' END,
4102444800000,'page row '||n||CASE WHEN n%500=0 THEN printf(' %0200d',n) ELSE '' END,
CASE WHEN n%100=0 THEN '["api"]' ELSE '[]' END,
CASE WHEN n>1 AND n%500=1 THEN n+16 ELSE 0 END,'{"row":'||n||'}','post','page-'||n FROM history`, variant.population, variant.population); err != nil {
							t.Fatal(err)
						}
						if variant.population > 0 {
							if _, err := fixture.db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash)
VALUES('foreign','root','note',4102444800000,'page addressed pending','["api@pages"]','{"pending":true}','post','page-pending')`); err != nil {
								t.Fatal(err)
							}
						}
						sent := min(variant.population, 500)
						if sent > 0 {
							sent += 17
						}
						if _, err := fixture.db.ExecContext(t.Context(), `INSERT INTO cursors(name,drive,seq)
VALUES('page-reader','pages',0),('page-reader#addressed','pages',?),('unrelated-page-cursor','d',7)`, sent); err != nil {
							t.Fatal(err)
						}
						for _, set := range variant.sets {
							if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET "+set.set+" WHERE seq=?", set.seq); err != nil {
								t.Fatal(err)
							}
						}
						records := tailBodyRecords(t, fixture.db)
						before := tailBodyCursors(t, fixture.db)
						got := runTailBodyContract(t, args, fixture.node)
						got.CursorsBefore, got.CursorsAfter = before, tailBodyCursors(t, fixture.db)
						if !slices.Equal(records, tailBodyRecords(t, fixture.db)) {
							t.Fatal("Tail page assembly changed physical records")
						}
						if len(variant.sets) == 0 && (got.CLI.Code != 0 || got.Panic != "") {
							t.Fatalf("clean Tail page contract failed: %+v", got)
						}
						tailBodyCheckSnapshot(t, dir, got)
					})
				}
			}
		}
	}
}
