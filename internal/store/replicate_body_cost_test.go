package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestExportBodyPrefixKeepsLineAndUTF8Contract(t *testing.T) {
	for name, body := range map[string]string{
		"empty": "",
		"below": strings.Repeat("x", store.MaxBody-1),
		"exact": strings.Repeat("x", store.MaxBody),
		"above": strings.Repeat("x", store.MaxBody+1),
		"line":  strings.Repeat("entry\n", store.MaxBody/6+20),
		"utf8":  strings.Repeat("x", store.MaxBody-1) + "€",
	} {
		t.Run(name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			path := filepath.Join(t.TempDir(), "body.txt")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "selected body", Refs: store.Refs{Path: path}})
			rows, err := st.Export(t.Context(), st.Self(), 0, 0)
			if err != nil || len(rows) != 1 {
				t.Fatalf("export = %+v/%v", rows, err)
			}
			want := body
			if len(body) > store.MaxBody {
				cut := []byte(body)[:store.MaxBody]
				if line := bytes.LastIndexByte(cut, '\n'); line > 0 {
					cut = cut[:line+1]
				}
				for !utf8.Valid(cut) {
					cut = cut[:len(cut)-1]
				}
				want = fmt.Sprintf("%s… cut at %d KiB of %d KiB; the whole file is %s on the node that wrote this record\n", cut, len(cut)>>10, len(body)>>10, path)
			}
			if rows[0].PathBody != want {
				t.Fatalf("body prefix = %d bytes, want %d", len(rows[0].PathBody), len(want))
			}
		})
	}
}

func TestExportTreatsADirectoryPathAsNoBody(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "names a directory", Refs: store.Refs{Path: t.TempDir()}})
	rows, err := st.Export(t.Context(), st.Self(), 0, 0)
	if err != nil || len(rows) != 1 {
		t.Fatalf("export = %+v/%v", rows, err)
	}
	if rows[0].PathBody != "" {
		t.Errorf("PathBody = %q, want none", rows[0].PathBody)
	}
}
