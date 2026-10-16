package mesh

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

var update = flag.Bool("update", false, "record testdata/wire_format_<store.WireFormat>.json; refuses to rewrite an existing one")

func TestWireFormatGolden(t *testing.T) {
	at := time.Date(2026, 10, 16, 5, 11, 44, 123456789, time.UTC)
	expires := at.Add(48 * time.Hour)
	change := body{
		Node: "0123456789abcdef",
		Rows: []store.Row{{
			Origin:    "0123456789abcdef",
			OriginSeq: 42,
			ID:        "deploy-42",
			Drive:     "page:1",
			Lane:      "ic",
			Kind:      kinds.Note,
			At:        at,
			Text:      "rolled forward",
			Topic:     "deploy",
			To:        []string{"root"},
			Re:        &store.Pointer{Origin: "fedcba9876543210", Seq: 7},
			Resolves:  &store.Pointer{Origin: "0123456789abcdef", Seq: 41},
			Refs: store.Refs{
				Path:    "/work/notes/deploy.md",
				CCN:     "ccn-3",
				URL:     "https://example.com/runs/9",
				Board:   "board-2",
				PRs:     []store.PR{{Number: 61}, {Repo: "yasyf/cc-inbox", Number: 79}},
				Builds:  []string{"https://example.com/builds/9"},
				Stacks:  []string{"yasyf/cci-wire"},
				Targets: []string{"forge"},
				Lanes:   []string{"ic"},
			},
			Fields: store.Fields{
				Attrs:   map[string]string{"service": "api"},
				Envs:    []string{"prod"},
				Mode:    "cli",
				Outcome: "passed",
				Commit:  "585c82a",
				Census:  &store.Census{N: 3, Denominator: 4, Head: "585c82a", Drift: 1, StacksClean: []string{"yasyf/cci-wire"}},
				Counts:  map[string]int{"rows": 2},
			},
			ExpiresAt: &expires,
			Source:    "post",
			Body:      json.RawMessage(`{"detail":"full"}`),
		}},
		Presence: store.Presence{
			Origin: "0123456789abcdef",
			At:     at,
			Peers: []store.Peer{{
				Session:     "3785cb11-d400-486b",
				Agent:       "agent-1",
				AgentName:   "reviewer",
				Origin:      "0123456789abcdef",
				Drive:       "page:1",
				Root:        true,
				Lane:        "ic",
				Role:        "lane",
				Description: "reviews the wire format",
				Tags:        []string{"review"},
				Cwd:         "/work/cc-inbox",
				Repo:        "yasyf/cc-inbox",
				Worktree:    "/work/cci-wire",
				Branch:      "yasyf/cci-wire",
				Remote:      "origin",
				PID:         4242,
				PIDStarted:  1760591504,
				Env:         map[string]string{"TERM": "xterm"},
				AnnouncedAt: at,
				SeenAt:      at.Add(time.Minute),
				Superseded:  true,
			}},
			Claims: []store.Claim{{
				Session: "3785cb11-d400-486b",
				Agent:   "agent-1",
				Rel:     "opened",
				Kind:    "pr",
				Key:     "79",
				At:      at,
				Source:  "claim",
			}},
		},
	}
	if unset := zeroFields(reflect.ValueOf(change), "body"); len(unset) > 0 {
		t.Fatalf("the fixture leaves %v zero; set every replicated field so the golden covers its encoding", unset)
	}
	got, err := json.MarshalIndent(change, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", fmt.Sprintf("wire_format_%d.json", store.WireFormat))
	if *update {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if err != nil {
			t.Fatalf("record %s: %v; a changed encoding needs a new store.WireFormat, not a rewritten golden", path, err)
		}
		if _, err := f.Write(got); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v; record it with `go test ./internal/mesh -run TestWireFormatGolden -update`", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("the mesh change encoding no longer matches %s; bump store.WireFormat and record the new golden with `go test ./internal/mesh -run TestWireFormatGolden -update`. Got:\n%s", path, got)
	}
}

func zeroFields(v reflect.Value, root string) []string {
	set := map[string]bool{}
	var order []string
	var walk func(v reflect.Value, path string)
	walk = func(v reflect.Value, path string) {
		if _, ok := set[path]; !ok {
			order = append(order, path)
		}
		set[path] = set[path] || !v.IsZero()
		switch v.Kind() {
		case reflect.Pointer:
			if !v.IsNil() {
				walk(v.Elem(), path)
			}
		case reflect.Struct:
			for i := range v.NumField() {
				if f := v.Type().Field(i); f.IsExported() {
					walk(v.Field(i), path+"."+f.Name)
				}
			}
		case reflect.Slice:
			for i := range v.Len() {
				walk(v.Index(i), path+"[]")
			}
		case reflect.Map:
			for iter := v.MapRange(); iter.Next(); {
				walk(iter.Value(), path+"[]")
			}
		}
	}
	walk(v, root)
	return slices.DeleteFunc(order, func(path string) bool { return set[path] })
}
