package render

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	DefaultBudget  = 6144
	MaxBudget      = 16000
	DefaultWidth   = 400
	trailerReserve = 96
)

func Clamp(budget int) int {
	if budget <= 0 {
		return DefaultBudget
	}
	return min(budget, MaxBudget)
}

func Stamp(t, now time.Time) string {
	local := t.In(time.Local)
	n := now.In(time.Local)
	if local.Year() == n.Year() && local.YearDay() == n.YearDay() {
		return local.Format("3:04 PM")
	}
	return local.Format("Jan 2 3:04 PM")
}

func Line(r store.Record, now time.Time) string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%d %s %s %s", r.Seq, Stamp(r.At, now), strings.ToUpper(string(r.Kind)), r.Lane)
	if len(r.To) > 0 {
		fmt.Fprintf(&b, " -> %s", strings.Join(r.To, ","))
	}
	if r.Re != 0 {
		fmt.Fprintf(&b, " re #%d", r.Re)
	}
	if r.Topic != "" {
		fmt.Fprintf(&b, " [%s]", r.Topic)
	}
	b.WriteString(" ")
	b.WriteString(strings.Join(strings.Fields(r.Text), " "))
	for _, pr := range r.Refs.PRs {
		fmt.Fprintf(&b, " pr#%d", pr)
	}
	for _, stack := range r.Refs.Stacks {
		fmt.Fprintf(&b, " stack:%s", stack)
	}
	for _, target := range r.Refs.Targets {
		fmt.Fprintf(&b, " target:%s", target)
	}
	if r.Resolves != 0 {
		fmt.Fprintf(&b, " resolves #%d", r.Resolves)
	}
	if r.Refs.CCN != "" {
		fmt.Fprintf(&b, " ccn:%s", r.Refs.CCN)
	}
	for _, build := range r.Refs.Builds {
		fmt.Fprintf(&b, " build:%s", build)
	}
	if r.Refs.Board != "" {
		fmt.Fprintf(&b, " board:%s", r.Refs.Board)
	}
	if r.Refs.URL != "" {
		fmt.Fprintf(&b, " %s", r.Refs.URL)
	}
	if r.Refs.Path != "" {
		fmt.Fprintf(&b, " %s", r.Refs.Path)
	}
	return b.String()
}

func JSON(r store.Record) string {
	out, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("marshal record #%d: %v", r.Seq, err))
	}
	return string(out)
}

type Budget struct {
	w     io.Writer
	left  int
	full  bool
	wrote int
}

func NewBudget(w io.Writer, budget int) *Budget {
	return &Budget{w: w, left: Clamp(budget) - trailerReserve}
}

func Clip(s string, width int) string {
	r := []rune(s)
	if width <= 0 || len(r) <= width {
		return s
	}
	return string(r[:width-1]) + "…"
}

func (b *Budget) Line(s string) bool {
	if b.full {
		return false
	}
	if len(s)+1 > b.left {
		b.full = true
		return false
	}
	b.left -= len(s) + 1
	b.wrote++
	_, _ = fmt.Fprintln(b.w, s)
	return true
}

func (b *Budget) Full() bool {
	return b.full
}

func (b *Budget) Wrote() int {
	return b.wrote
}

func (b *Budget) Trailer(s string) {
	_, _ = fmt.Fprintln(b.w, s)
}
