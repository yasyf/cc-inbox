package render_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestRenderedTextWhitespace(t *testing.T) {
	at := time.Date(2026, 11, 1, 6, 22, 0, 0, time.UTC)
	for _, tt := range []struct {
		name, text, want string
	}{
		{"empty", "", ""},
		{"one word", "ready", "ready"},
		{"padded single word", strings.Repeat(" \t", 50) + "ready" + strings.Repeat("\u3000\r\n", 50), "ready"},
		{"padded invalid single word", "\u3000\t\xffword \r\n", "\xffword"},
		{"large one word", strings.Repeat("x", store.MaxText), strings.Repeat("x", store.MaxText)},
		{"already normalized", "only, no non-test", "only, no non-test"},
		{"trimmed normalized", " \tonly, no non-test\r\n", "only, no non-test"},
		{"double ASCII spaces", "only,  no   non-test", "only, no non-test"},
		{"single-byte separator", "only,\tno non-test", "only, no non-test"},
		{"canonical Unicode", "α β γ δ", "α β γ δ"},
		{"trimmed canonical Unicode", "\u3000α β γ δ\u0085", "α β γ δ"},
		{"canonical invalid UTF8", "\xffa \xc0b \x80", "\xffa \xc0b \x80"},
		{"canonical NUL", "a\x00b c", "a\x00b c"},
		{"ASCII whitespace", " \t only,\r\n no\v\f non-test\t ", "only, no non-test"},
		{"Unicode whitespace", "\u0085only,\u00a0\u1680no\u2000\u2001\u2028non-test\u2029\u3000", "only, no non-test"},
		{"mixed Unicode", " \tα\u00a0β \u3000γ\r\nδ ", "α β γ δ"},
		{"non-whitespace Unicode", "a\u200bb\ufeffc", "a\u200bb\ufeffc"},
		{"invalid UTF8", " \xffa\t\xc0b \x80 ", "\xffa \xc0b \x80"},
		{"NUL", " a\x00b\t c ", "a\x00b c"},
		{"only ASCII whitespace", strings.Repeat(" \t\n", 1024), ""},
		{"only whitespace", strings.Repeat(" \t\n\u3000", 1024), ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := store.Record{Seq: 7, At: at, Kind: kinds.Report, Lane: "desk", Text: tt.text}
			stamp := render.Stamp(at, at)
			want := "#7 " + stamp + " REPORT desk " + tt.want
			if got := render.Line(r, at); got != want {
				t.Fatalf("Line = %q, want %q", got, want)
			}
			if got := render.Brief(r, at, 0); got != want {
				t.Fatalf("Brief = %q, want %q", got, want)
			}
		})
	}
	path := strings.Repeat("p/", 64) + "path"
	for _, text := range []string{"ready", strings.TrimSpace(strings.Repeat("only, no non-test ", 20)), "α β γ δ"} {
		t.Run(text, func(t *testing.T) {
			r := store.Record{
				Seq: 10000, Num: "#10000", At: at, Kind: kinds.Report, Lane: "desk", Topic: "tracking", Text: text,
				Refs: store.Refs{PRs: []store.PR{{Number: 42}}, Stacks: []string{"stack/env"}, Targets: []string{"target"}, CCN: "note", Builds: []string{"green"}, Board: "board", Path: path},
			}
			stamp := render.Stamp(at, at)
			rest := " [tracking] " + text + " pr#42 stack:stack/env target:target ccn:note build:green board:board " + path
			want := "#10000 " + stamp + " REPORT desk" + rest
			if got := render.Line(r, at); got != want {
				t.Fatalf("Line = %q, want %q", got, want)
			}
			if got := render.Brief(r, at, 0); got != want {
				t.Fatalf("Brief = %q, want %q", got, want)
			}
		})
	}
}

var renderedTextCost string

func BenchmarkRenderedText(b *testing.B) {
	host := time.Local
	zone, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		b.Fatal(err)
	}
	time.Local = zone
	b.Cleanup(func() { time.Local = host })
	at := time.Date(2026, 11, 1, 6, 22, 0, 0, time.UTC)
	for _, tt := range []struct {
		name, text string
	}{
		{"Empty", ""},
		{"SingleWord", "ready"},
		{"LongWord", strings.Repeat("x", store.MaxText)},
		{"ASCII", strings.Repeat("only, no non-test ", 32)},
		{"ASCIIWhitespace", strings.Repeat(" \t\n", 1024)},
		{"Unicode", strings.Repeat("only,\u00a0no\u3000non-test\u0085", 32)},
		{"Whitespace", strings.Repeat(" \t\n\u3000", 1024)},
	} {
		for _, n := range []int{32, 10000} {
			b.Run(fmt.Sprintf("%s/N%d", tt.name, n), func(b *testing.B) {
				records := make([]store.Record, n)
				for i := range records {
					records[i] = store.Record{Seq: int64(i + 10000), Num: fmt.Sprintf("#%d", i+10000), At: at, Kind: kinds.Report, Lane: "desk", Text: tt.text}
				}
				for _, mode := range []string{"Record", "Aggregate"} {
					b.Run(mode, func(b *testing.B) {
						for _, r := range records {
							renderedTextCost = render.Line(r, at)
						}
						b.ReportAllocs()
						if mode == "Aggregate" {
							b.SetBytes(int64(n * len(tt.text)))
							for b.Loop() {
								for _, r := range records {
									renderedTextCost = render.Line(r, at)
								}
							}
							return
						}
						b.SetBytes(int64(len(tt.text)))
						i := 0
						for b.Loop() {
							renderedTextCost = render.Line(records[i], at)
							i++
							if i == len(records) {
								i = 0
							}
						}
					})
				}
			})
		}
	}
}
