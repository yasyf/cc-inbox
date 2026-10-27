package render

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	DefaultBudget   = 6144
	FoldWidth       = 160
	trailerReserve  = 96
	shownRecipients = 8
)

func Stamp(t, now time.Time) string {
	local := t.In(time.Local)
	n := now.In(time.Local)
	if local.Year() == n.Year() && local.YearDay() == n.YearDay() {
		return local.Format("3:04 PM")
	}
	return local.Format("Jan 2 3:04 PM")
}

func Line(r store.Record, now time.Time) string {
	return header(r, now) + rest(r, false, false)
}

func Brief(r store.Record, now time.Time, width int) string {
	return fit(header(r, now), r, width)
}

func Untagged(r store.Record, now time.Time, width int) string {
	return fit(Stamp(r.At, now), r, width)
}

func fit(head string, r store.Record, width int) string {
	if s := head + rest(r, true, true); width <= 0 || utf8.RuneCountInString(s) <= width {
		return s
	}
	return Clip(head+rest(r, true, false), width)
}

func header(r store.Record, now time.Time) string {
	return fmt.Sprintf("#%d %s %s %s", r.Seq, Stamp(r.At, now), strings.ToUpper(string(r.Kind)), r.Lane)
}

func rest(r store.Record, brief, elide bool) string {
	var words []string
	if elide {
		words = strings.FieldsFunc(r.Text, func(c rune) bool { return unicode.IsSpace(c) || strings.ContainsRune(",;()[]", c) })
		for i, w := range words {
			words[i] = strings.TrimRight(w, ".:!?")
		}
	}
	var b strings.Builder
	if r.Origin != "" {
		fmt.Fprintf(&b, " origin:%s", r.Origin)
	}
	to := recipients(r.To)
	if brief && len(to) > shownRecipients {
		to = append(to[:shownRecipients:shownRecipients], fmt.Sprintf("+%d more", len(to)-shownRecipients))
	}
	if len(to) > 0 {
		fmt.Fprintf(&b, " -> %s", strings.Join(to, ","))
	}
	if r.Re != 0 {
		fmt.Fprintf(&b, " re #%d", r.Re)
	}
	if r.Topic != "" && !slices.Contains(words, r.Topic) {
		fmt.Fprintf(&b, " [%s]", r.Topic)
	}
	b.WriteString(" ")
	b.WriteString(strings.Join(strings.Fields(r.Text), " "))
	for _, pr := range r.Refs.PRs {
		if slices.Contains(words, "#"+pr.String()) || pr.Repo != "" && slices.Contains(words, pr.String()) {
			continue
		}
		if pr.Repo == "" {
			fmt.Fprintf(&b, " pr#%d", pr.Number)
		} else {
			fmt.Fprintf(&b, " pr:%s", pr.String())
		}
	}
	for _, stack := range r.Refs.Stacks {
		fmt.Fprintf(&b, " stack:%s", stack)
	}
	for _, target := range r.Refs.Targets {
		fmt.Fprintf(&b, " target:%s", target)
	}
	for i, seq := range r.Closes() {
		if i == 0 {
			fmt.Fprintf(&b, " resolves #%d", seq)
		} else {
			fmt.Fprintf(&b, ",#%d", seq)
		}
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

// WithBody appends the record's JSON body, indented, on the lines after line.
func WithBody(line string, r store.Record) string {
	if len(r.Body) == 0 {
		return line
	}
	var b bytes.Buffer
	if err := json.Indent(&b, r.Body, "", "  "); err != nil {
		panic(fmt.Sprintf("indent body of #%d: %v", r.Seq, err))
	}
	return line + "\n" + b.String()
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
	if budget <= 0 {
		budget = DefaultBudget
	}
	return &Budget{w: w, left: budget - trailerReserve}
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
	if b.wrote > 0 && len(s)+1 > b.left {
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

func recipients(to []string) []string {
	out := make([]string, len(to))
	for i, t := range to {
		if target, ok := strings.CutPrefix(t, "peer:"); ok {
			node, rest := store.SplitPeer(target)
			session, agent, _ := strings.Cut(rest, "/")
			if len(session) > 8 {
				session = session[:8]
			}
			t = "peer:" + session
			if agent != "" {
				t += "/" + agent
			}
			if node != "" {
				t = store.PeerAddressOn(node, strings.TrimPrefix(t, "peer:"))
			}
		}
		out[i] = t
	}
	return out
}
