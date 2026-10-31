package cli_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func seedNativeTailMixedOrigins(t *testing.T, fixture nativeTailFixture) int64 {
	t.Helper()
	if _, err := fixture.db.ExecContext(t.Context(), `INSERT INTO members(node,public_key,tier,via,"by",at) VALUES('aaaaaaaaaaaaaaaa',X'','owner','relay','test',1)`); err != nil {
		t.Fatal(err)
	}
	var first int64
	for i, row := range []struct {
		origin string
		number int64
		text   string
	}{
		{"aaaaaaaaaaaaaaaa", 2, "owner record"},
		{"", 0, "local record"},
		{"bbbbbbbbbbbbbbbb", 100, "remote record"},
	} {
		result, err := fixture.db.ExecContext(t.Context(), `INSERT INTO records(drive,lane,kind,at,text,origin,origin_seq,source,hash) VALUES('mixed','root','note',4102444800000,?,?,?,'post',?)`, row.text, row.origin, row.number, fmt.Sprintf("mixed-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		seq, err := result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = seq
		}
	}
	return first
}

func nativeTailRows(t *testing.T, result nativeTailResult) []store.Record {
	t.Helper()
	if result.Code != 0 {
		t.Fatalf("native Tail failed: %q", result.Err)
	}
	var records []store.Record
	for line := range bytes.Lines(result.Out) {
		var record store.Record
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func TestNativeTailOwnerConfigurationIsFreshAndRequestLocal(t *testing.T) {
	call := nativeTailCall{args: []string{"tail", "--drive", "mixed", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	first := seedNativeTailMixedOrigins(t, fixture)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	for _, tier := range []string{"owner", "service", "owner"} {
		if err := relay.SaveConfig(fixture.home, relay.Config{Mesh: "native", URL: "https://relay.invalid", Tier: tier}); err != nil {
			t.Fatal(err)
		}
		want := runNativeTailReference(t, call.args)
		got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, call)))
		if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
			t.Fatalf("owner mode %s differs from CLI: code=%d/%d output=%q/%q error=%q/%q", tier, got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
		}
		rows := nativeTailRows(t, got)
		if len(rows) != 3 {
			t.Fatalf("mixed origin rows=%d", len(rows))
		}
		localName := fmt.Sprintf("#%d", first+1)
		if tier != "owner" {
			localName += "@" + fixture.node[:8]
		}
		if rows[0].Num != "#2" || rows[1].Num != localName || rows[2].Num != "#100@bbbbbbbb" {
			t.Fatalf("wrong current CLI numbering for %s: %+v", tier, rows)
		}
		shared, err := fixture.st.Query(t.Context(), store.Filter{Drive: "mixed"})
		if err != nil || len(shared) != 3 || shared[1].Num != fmt.Sprintf("#%d", first+1) {
			t.Fatalf("request mutated shared store owner: %+v err=%v", shared, err)
		}
	}
	if err := os.WriteFile(filepath.Join(fixture.home, "relay", "native", "config.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	want := runNativeTailReference(t, call.args)
	got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, call)))
	if got.Code != 1 || got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
		t.Fatalf("owner config error differs: code=%d/%d output=%q/%q error=%q/%q", got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
	}
}

func TestNativeTailInternalSequencePreservesMixedOriginMachineCursors(t *testing.T) {
	call := nativeTailCall{tier: "service", args: []string{"tail", "--drive", "mixed", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	first := seedNativeTailMixedOrigins(t, fixture)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	human := nativeTailCall{args: []string{"tail", "--drive", "mixed", "--since", strconv.FormatInt(first, 10), "--json"}}
	if result := runNativeTailReference(t, human.args); result.Code != 1 {
		t.Fatalf("fixture did not distinguish internal seq from owner number: %+v", result)
	}
	grep := runNativeTailReference(t, []string{"grep", "record", "--drive", "mixed", "--since-seq", strconv.FormatInt(first, 10), "--json"})
	if rows := nativeTailRows(t, grep); len(rows) != 2 || rows[0].Seq != first+2 || rows[1].Seq != first+1 {
		t.Fatalf("shared grep machine cursor lost internal positions: %+v", rows)
	}
	for _, cursor := range []int64{0, first, first + 1, first + 2} {
		machine := nativeTailCall{args: []string{"tail", "--drive", "mixed", "--since-seq", strconv.FormatInt(cursor, 10), "--json"}}
		want := runNativeTailReference(t, machine.args)
		got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, machine)))
		if got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
			t.Fatalf("internal cursor %d differs: code=%d/%d output=%q/%q error=%q/%q", cursor, got.Code, want.Code, got.Out, want.Out, got.Err, want.Err)
		}
		rows := nativeTailRows(t, got)
		var wantSeqs []int64
		for seq := first; seq <= first+2; seq++ {
			if seq > cursor {
				wantSeqs = append(wantSeqs, seq)
			}
		}
		if len(rows) != len(wantSeqs) {
			t.Fatalf("internal cursor %d returned %d rows, want %v", cursor, len(rows), wantSeqs)
		}
		for i, row := range rows {
			if row.Seq != wantSeqs[i] {
				t.Fatalf("internal cursor %d returned row seq%d, want%d", cursor, row.Seq, wantSeqs[i])
			}
		}
	}
	for _, args := range [][]string{
		{"tail", "--drive", "mixed", "--since-seq", "-1", "--json"},
		{"tail", "--drive", "mixed", "--since-seq", "not-an-int", "--json"},
		{"tail", "--drive", "mixed", "--since", "0", "--since-seq", "0", "--json"},
	} {
		want := runNativeTailReference(t, args)
		got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, nativeTailCall{args: args})))
		if got.Code != 1 || got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Equal(got.Err, want.Err) {
			t.Fatalf("internal cursor error differs for %q: %+v/%+v", args, got, want)
		}
	}
}

func TestNativeTailRelativeHomeConfigDiagnosticUsesOwnedCanonicalPath(t *testing.T) {
	call := nativeTailCall{tier: "service", args: []string{"tail", "--drive", "d", "--since", "0", "--json"}}
	fixture := newNativeTailFixture(t, call, nil)
	handler := nativeTailHandler(t, fixture)
	forbidNativeTailWrites(t, fixture)
	config := filepath.Join(fixture.home, "relay", "native", "config.json")
	if err := os.WriteFile(config, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Chdir(filepath.Dir(fixture.home))
	relativeHome := filepath.Base(fixture.home)
	t.Setenv("CCI_HOME", relativeHome)
	want := runNativeTailReference(t, call.args)
	got := nativeTailResultFromHTTP(t, nativeTailResponse(t, handler, nativeTailBody(t, fixture, call)))
	relativeConfig := filepath.Join(relativeHome, "relay", "native", "config.json")
	if want.Code != 1 || got.Code != want.Code || !bytes.Equal(got.Out, want.Out) || !bytes.Contains(want.Err, []byte(relativeConfig)) || !bytes.Contains(got.Err, []byte(config)) {
		t.Fatalf("relative-home diagnostic contract changed: CLI=%+v HTTP=%+v", want, got)
	}
	if !bytes.Equal(bytes.ReplaceAll(want.Err, []byte(relativeConfig), []byte(config)), got.Err) {
		t.Fatalf("diagnostics differ beyond the owned canonical path: %q/%q", got.Err, want.Err)
	}
}
