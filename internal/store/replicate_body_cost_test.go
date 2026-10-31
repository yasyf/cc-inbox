package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

func TestNewestExportBoundsSelectedBodyAllocation(t *testing.T) {
	for _, size := range []int64{1 << 20, 32 << 20} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			st, _ := testutil.Store(t)
			path := filepath.Join(t.TempDir(), "body.txt")
			file, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			prefix := strings.Repeat("x", store.MaxBody-1) + "\n"
			if _, err := file.WriteString(prefix); err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(size); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			posted := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "selected body", Refs: store.Refs{Path: path}})
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			var rows []store.Row
			err = st.VisitNewestExports(t.Context(), st.Self(), 0, func(row store.Row) (bool, error) {
				rows = append(rows, row)
				return false, nil
			})
			runtime.ReadMemStats(&after)
			if err != nil || len(rows) != 1 || rows[0].OriginSeq != posted.Seq {
				t.Fatalf("selected export = %+v/%v", rows, err)
			}
			want := fmt.Sprintf("%s… cut at 64 KiB of %d KiB; the whole file is %s on the node that wrote this record\n", prefix, size>>10, path)
			if rows[0].PathBody != want {
				t.Fatalf("selected body has %d bytes, want %d", len(rows[0].PathBody), len(want))
			}
			allocated := after.TotalAlloc - before.TotalAlloc
			t.Logf("body_bytes=%d exported_bytes=%d allocated_bytes=%d", size, len(rows[0].PathBody), allocated)
			if os.Getenv("CCI_MESH_BODY_EXPECT_GLOBAL") == "1" {
				if allocated < uint64(size) {
					t.Fatalf("immutable whole-file baseline did not allocate the body: body_bytes=%d allocated_bytes=%d", size, allocated)
				}
				return
			}
			if allocated > 8<<20 {
				t.Fatalf("selected body allocation follows the unread file tail: body_bytes=%d allocated_bytes=%d", size, allocated)
			}
		})
	}
}
