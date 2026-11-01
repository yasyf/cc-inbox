package cli_test

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
)

var addressedBodyDeliveries = []struct {
	name string
	flag string
}{
	{"reader", "--reader"},
	{"laneMail", "--to"},
}

func TestAddressedDoneBodyContract(t *testing.T) {
	dir := os.Getenv("CCI_ADDRESSED_BODY_CONTRACT_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable addressed cursor Body snapshots")
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
		{"large", `{"payload":"` + strings.Repeat("done-body-only ", 2048) + `"}`, false},
	} {
		for _, delivery := range addressedBodyDeliveries {
			for _, asJSON := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/delivery=%s/json=%t", variant.name, delivery.name, asJSON), func(t *testing.T) {
					args := []string{"tail", "--drive", "d", delivery.flag, "api", "--cursor", "addressed-private", "--budget", "4000000", "--width", "700"}
					if asJSON {
						args = append(args, "--json")
					}
					fixture := newAddressedBodyFixture(t, args, delivery.name)
					if _, err := fixture.db.ExecContext(t.Context(), "UPDATE records SET body=? WHERE seq=6", variant.body); err != nil {
						t.Fatal(err)
					}
					records := tailBodyRecords(t, fixture.db)
					before := tailBodyCursors(t, fixture.db)
					got := runTailBodyContract(t, args, fixture.node)
					got.CursorsBefore, got.CursorsAfter = before, tailBodyCursors(t, fixture.db)
					if !slices.Equal(records, tailBodyRecords(t, fixture.db)) {
						t.Fatal("addressed cursor Tail changed private records")
					}
					if asJSON && variant.jsonPanics {
						if !strings.HasPrefix(got.Panic, "marshal record #6:") {
							t.Fatalf("invalid done Body panic changed: %+v", got)
						}
					} else {
						if got.Panic != "" || got.CLI.Code != 0 {
							t.Fatalf("addressed cursor Tail failed: %+v", got)
						}
						if asJSON && !bytes.Contains(got.CLI.Out, []byte(`"pending":true`)) {
							t.Fatalf("JSON pending record lost Body: %s", got.CLI.Out)
						}
					}
					tailBodyCheckSnapshot(t, dir, got)
				})
			}
		}
	}
}

func TestAddressedDoneErrorAndBudgetContract(t *testing.T) {
	dir := os.Getenv("CCI_ADDRESSED_BODY_CONTRACT_SNAPSHOTS")
	if dir == "" {
		t.Skip("CI supplies immutable addressed cursor error and budget snapshots")
	}
	for _, variant := range []struct {
		name string
		sets []nativeTailSet
	}{
		{"clean", nil},
		{"done refs", []nativeTailSet{{6, `refs='{'`}}},
		{"done fields", []nativeTailSet{{6, `fields='{'`}}},
		{"done also resolves", []nativeTailSet{{6, `also_resolves='['`}}},
		{"done recipients", []nativeTailSet{{6, `recipients='['`}}},
		{"pending refs", []nativeTailSet{{18, `refs='{'`}}},
	} {
		for _, delivery := range addressedBodyDeliveries {
			for _, budget := range []string{"1", "4000000"} {
				for _, asJSON := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/delivery=%s/budget=%s/json=%t", variant.name, delivery.name, budget, asJSON), func(t *testing.T) {
						args := []string{"tail", "--drive", "d", delivery.flag, "api", "--cursor", "addressed-private", "--budget", budget, "--width", "700"}
						if asJSON {
							args = append(args, "--json")
						}
						fixture := newAddressedBodyFixture(t, args, delivery.name)
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
							t.Fatal("addressed cursor Tail changed private records")
						}
						tailBodyCheckSnapshot(t, dir, got)
					})
				}
			}
		}
	}
}

func newAddressedBodyFixture(t *testing.T, args []string, delivery string) nativeTailFixture {
	t.Helper()
	fixture := newNativeTailFixture(t, nativeTailCall{claude: "reader", args: args}, nil)
	statements := []string{
		`INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash) VALUES('foreign','root','note',4102444800000,'new addressed pending','["api@d"]','{"pending":true}','post','addressed-pending')`,
		`INSERT INTO cursors(name,drive,seq) VALUES('addressed-private','d',0),('addressed-private#addressed','d',6),('unrelated-fixture','d',7)`,
	}
	if delivery == "laneMail" {
		statements = append(statements, `WITH RECURSIVE mailbox(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM mailbox WHERE n<501)
INSERT INTO records(drive,lane,kind,at,text,recipients,body,source,hash)
SELECT 'd','worker','note',4102444800000,'mailbox-'||n,'["api"]','{"mailbox":true}','post','mailbox-'||n FROM mailbox`)
	}
	for _, statement := range statements {
		if _, err := fixture.db.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
	}
	return fixture
}
