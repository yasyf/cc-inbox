package render

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/pacific"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	DefaultBudget   = 6144
	FoldWidth       = 160
	trailerReserve  = 96
	shownRecipients = 8
)

func Stamp(t, now time.Time) string {
	at, n := t.In(pacific.Zone), now.In(pacific.Zone)
	if at.Year() == n.Year() && at.YearDay() == n.YearDay() {
		return at.Format("3:04 PM")
	}
	return at.Format("Jan 2 3:04 PM")
}

func Line(r store.Record, now time.Time) string {
	return header(r, now) + rest(r, false, false)
}

func Brief(r store.Record, now time.Time, width int) string {
	head := header(r, now)
	if s := head + rest(r, true, true); width <= 0 || utf8.RuneCountInString(s) <= width {
		return s
	}
	return Clip(head+rest(r, true, false), width)
}

func header(r store.Record, now time.Time) string {
	return fmt.Sprintf("%s %s %s %s", r.Name(), Stamp(r.At, now), strings.ToUpper(string(r.Kind)), sender(r))
}

func sender(r store.Record) string {
	lane := cmp.Or(r.Stem, r.Lane)
	for _, to := range r.To {
		if _, drive, cross := strings.Cut(to, "@"); cross && drive != r.Drive {
			return lane + "@" + r.Drive
		}
	}
	return lane
}

func rest(r store.Record, brief, elide bool) string {
	var words []string
	if elide && (r.Topic != "" || len(r.Refs.PRs) > 0) {
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
		to = []string{fmt.Sprintf("%d lanes", len(to))}
	}
	if len(to) > 0 {
		fmt.Fprintf(&b, " -> %s", strings.Join(to, ","))
	}
	if r.Re != 0 || r.ReNum != "" {
		fmt.Fprintf(&b, " re %s", r.ReName())
	}
	if r.Topic != "" && !slices.Contains(words, r.Topic) {
		fmt.Fprintf(&b, " [%s]", r.Topic)
	}
	b.WriteString(" ")
	text := strings.TrimSpace(r.Text)
	hasSpace := strings.Contains(text, " ")
	if text == "" || hasSpace && !strings.Contains(text, "  ") && strings.IndexFunc(text, func(c rune) bool { return c != ' ' && unicode.IsSpace(c) }) < 0 {
		if hasSpace {
			b.Grow(len(text))
		}
		b.WriteString(text)
	} else {
		textWords := strings.Fields(text)
		if len(textWords) < 2 {
			b.WriteString(strings.Join(textWords, " "))
		} else {
			textSize := len(textWords) - 1
			for _, word := range textWords {
				textSize += len(word)
			}
			b.Grow(textSize)
			b.WriteString(textWords[0])
			for _, word := range textWords[1:] {
				b.WriteByte(' ')
				b.WriteString(word)
			}
		}
	}
	for _, pr := range r.Refs.PRs {
		if slices.Contains(words, "#"+pr.String()) || pr.Repo != "" && slices.Contains(words, pr.String()) || r.Fields.Attrs[store.HeadAttr+pr.String()] != "" {
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
	if closes := r.CloseNames(); len(closes) > 0 {
		fmt.Fprintf(&b, " resolves %s", strings.Join(closes, ","))
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

// WithBody appends the record's JSON body, indented, on the lines after line,
// or, for a body longer than store.MaxText, its size and the command that
// prints it, as a long text leaves only its head and blob path.
func WithBody(line string, r store.Record) string {
	if len(r.Body) == 0 {
		return line
	}
	var b bytes.Buffer
	if err := json.Indent(&b, r.Body, "", "  "); err != nil {
		panic(fmt.Sprintf("indent body of #%d: %v", r.Seq, err))
	}
	if b.Len() > store.MaxText {
		return fmt.Sprintf("%s\n(body: %d bytes; cci show %d prints it)", line, len(r.Body), r.Seq)
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
