package importer

import (
	"bufio"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

type Clock struct {
	Hour, Minute int
	UTC          bool
}

type Entry struct {
	Start   string
	Lane    string
	Topic   string
	Kind    kinds.Kind
	Text    string
	To      []string
	PRs     []int
	Builds  []string
	Stacks  []string
	Targets []string
	Clock   *Clock
}

var (
	timeParen = regexp.MustCompile(`\(([^()]*?)\b(\d{1,2}):(\d[\dx])(?::\d{2})?\s*([AaPp][Mm])?\s*(PT|PDT|PST|Z|UTC)?\b([^()]*)\)`)
	bareClock = regexp.MustCompile(`\b(\d{1,2}):(\d[\dx])\s*([AaPp][Mm])\b(?:\s*(?:PT|PDT|PST)\b)?`)
	zulu      = regexp.MustCompile(`\b(\d{1,2}):(\d{2})Z\b`)
	runner    = regexp.MustCompile(`^(\d{1,2}):(\d{2})(Z)?\s+([A-Z][A-Z0-9_-]+)\s+(.*)$`)
	dispatch  = regexp.MustCompile(`\b([a-z][a-z0-9]*(?:-[a-z0-9]+)+)=ctx_`)
	ruling    = regexp.MustCompile(`^R\d+[a-z]?$`)
	kindWord  = regexp.MustCompile(`^[A-Z][A-Z0-9]*(?:[-_][A-Z0-9]+)*:?$`)
	laneWord  = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*:?$`)
	prRef     = regexp.MustCompile(`(?:#|/pull/)(\d{4,6})\b`)
	buildRef  = regexp.MustCompile(`https://buildkite\.com/[\w.-]+/[\w.-]+/builds/\d+`)
	stackTag  = regexp.MustCompile(`\bstack:([a-z][a-z0-9-]*/[a-z0-9][a-z0-9-]*)`)
	targetTag = regexp.MustCompile(`\btarget:([a-z][a-z0-9-]*)`)
	stackRef  = regexp.MustCompile(`\b[a-z][a-z0-9-]*/[a-z]+-(?:us|eu|ap|ca|sa|me|af)[a-z]+\d-[a-z0-9]+\b`)
	bullet    = regexp.MustCompile(`^(?:[-*]\s+|#{1,4}\s+)`)
)

func Parse(r io.Reader, fallbackLane string) ([]Entry, error) {
	var (
		out  []Entry
		cur  *Entry
		scan = bufio.NewScanner(r)
	)
	scan.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	flush := func() {
		if cur != nil {
			cur.Text = strings.Join(strings.Fields(cur.Text), " ")
			cur.refs(cur.Start + " " + cur.Text)
			out = append(out, *cur)
			cur = nil
		}
	}
	for scan.Scan() {
		line := strings.TrimRight(scan.Text(), " \t\r")
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		if e, ok := start(line, fallbackLane); ok {
			flush()
			cur = &e
			continue
		}
		if cur == nil {
			e := plain(line, fallbackLane)
			cur = &e
			continue
		}
		cur.Text += " " + strings.TrimSpace(line)
	}
	flush()
	return out, scan.Err()
}

func plain(line, lane string) Entry {
	body := strings.TrimSpace(line)
	return Entry{Start: line, Lane: lane, Kind: kinds.Note, Text: body}
}

func start(line, fallbackLane string) (Entry, bool) {
	marked := bullet.MatchString(line)
	body := strings.TrimSpace(bullet.ReplaceAllString(line, ""))
	if m := runner.FindStringSubmatch(body); m != nil && !strings.EqualFold(m[4], "AM") && !strings.EqualFold(m[4], "PM") {
		return runnerEntry(line, m, fallbackLane), true
	}
	toks := strings.Fields(body)
	if len(toks) > 0 && ruling.MatchString(toks[0]) {
		return rulingEntry(line, body, toks), true
	}
	e := Entry{Start: line, Lane: fallbackLane, Kind: kinds.Note}
	var head, tail []string
	parenLane := ""
	timed, promoted := false, false
	if loc := timeParen.FindStringSubmatchIndex(body); loc != nil && len(strings.Fields(body[:loc[0]])) <= 3 {
		m := timeParen.FindStringSubmatch(body)
		e.Clock = clock(m[2], m[3], m[4], m[5])
		head = strings.Fields(body[:loc[0]])
		tail = strings.Fields(body[loc[1]:])
		parenLane = strings.TrimRight(firstField(m[1]), ",:")
		timed = true
	} else if loc := bareClock.FindStringSubmatchIndex(body); loc != nil && len(strings.Fields(body[:loc[0]])) <= 1 {
		m := bareClock.FindStringSubmatch(body)
		e.Clock = clock(m[1], m[2], m[3], "")
		head = strings.Fields(body[:loc[0]])
		tail = strings.Fields(body[loc[1]:])
		if len(head) == 0 {
			head, tail = firstN(tail, 2), tail[min(2, len(tail)):]
			promoted = true
		}
		timed = true
	} else {
		if m := zulu.FindStringSubmatch(body); m != nil {
			e.Clock = clock(m[1], m[2], "", "Z")
		}
		head = firstN(toks, 3)
		tail = toks[len(head):]
	}
	kindFound, laneFound := false, false
	var rest []string
	for _, tok := range head {
		switch {
		case !kindFound && isKind(tok):
			e.Kind, _ = kinds.Lookup(strings.TrimSuffix(tok, ":"))
			kindFound = true
		case !laneFound && (!promoted || e.Kind != kinds.Owner) && isLane(tok, !promoted && (timed || kindFound)):
			e.Lane = strings.TrimSuffix(tok, ":")
			laneFound = true
		default:
			rest = append(rest, tok)
		}
	}
	if !laneFound && parenLane != "" && isLane(parenLane, true) {
		e.Lane = parenLane
		laneFound = true
	}
	for timed && len(tail) > 0 {
		if !kindFound && isKind(tail[0]) {
			e.Kind, _ = kinds.Lookup(strings.TrimSuffix(tail[0], ":"))
			kindFound = true
		} else if !laneFound && e.Kind != kinds.Owner && namesLane(tail) {
			e.Lane = strings.TrimSuffix(tail[0], ":")
			laneFound = true
		} else {
			break
		}
		tail = tail[1:]
	}
	if !laneFound && e.Kind == kinds.Owner {
		e.Lane = "root"
	}
	e.Text = strings.Join(append(rest, tail...), " ")
	if e.Text == "" {
		e.Text = body
	}
	return e, marked || kindFound || timed || laneFound && strings.HasSuffix(firstField(body), ":")
}

func isKind(tok string) bool {
	if !kindWord.MatchString(tok) {
		return false
	}
	_, ok := kinds.Lookup(strings.TrimSuffix(tok, ":"))
	return ok
}

func isLane(tok string, loose bool) bool {
	if !laneWord.MatchString(tok) || len(strings.TrimSuffix(tok, ":")) < 3 {
		return false
	}
	return loose || strings.HasSuffix(tok, ":") || strings.Contains(tok, "-")
}

func namesLane(toks []string) bool {
	tok := toks[0]
	if !isLane(tok, false) || !strings.Contains(tok, "-") {
		return false
	}
	return strings.HasSuffix(tok, ":") || len(toks) > 1 && kindWord.MatchString(toks[1])
}

func runnerEntry(line string, m []string, fallbackLane string) Entry {
	e := Entry{Start: line, Lane: fallbackLane, Kind: kinds.Note, Clock: clock(m[1], m[2], "", m[3])}
	if k, ok := kinds.Lookup(m[4]); ok {
		e.Kind = k
		e.Text = m[5]
	} else {
		e.Text = m[4] + " " + m[5]
	}
	toks := strings.Fields(m[5])
	for i, tok := range firstN(toks, 3) {
		if !strings.HasSuffix(tok, ":") || !laneWord.MatchString(tok) {
			continue
		}
		subject := strings.TrimSuffix(tok, ":")
		if i == 0 || strings.HasPrefix(toks[0], "msg_") {
			e.Lane = subject
		} else {
			e.Lane, e.Topic = "runner", subject
		}
		break
	}
	if e.Topic == "runner" {
		e.Topic = ""
		if d := dispatch.FindStringSubmatch(m[5]); d != nil {
			e.Topic = d[1]
		}
	}
	return e
}

func rulingEntry(line, body string, fields []string) Entry {
	e := Entry{Start: line, Lane: "root", Kind: kinds.Go, Text: body}
	if m := timeParen.FindStringSubmatch(body); m != nil {
		e.Clock = clock(m[2], m[3], m[4], m[5])
	}
	for _, tok := range firstN(fields[1:], 5) {
		if strings.HasSuffix(tok, ":") && laneWord.MatchString(tok) {
			e.To = []string{strings.TrimSuffix(tok, ":")}
			break
		}
	}
	return e
}

func firstField(s string) string {
	f := strings.Fields(s)
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

func firstN(s []string, n int) []string {
	return s[:min(n, len(s))]
}

func (e *Entry) refs(body string) {
	for _, m := range prRef.FindAllStringSubmatch(body, -1) {
		n, _ := strconv.Atoi(m[1])
		if !slices.Contains(e.PRs, n) {
			e.PRs = append(e.PRs, n)
		}
	}
	for _, b := range buildRef.FindAllString(body, -1) {
		if !slices.Contains(e.Builds, b) {
			e.Builds = append(e.Builds, b)
		}
	}
	for _, st := range stackRef.FindAllString(body, -1) {
		if !slices.Contains(e.Stacks, st) {
			e.Stacks = append(e.Stacks, st)
		}
	}
	for _, m := range stackTag.FindAllStringSubmatch(body, -1) {
		if !slices.Contains(e.Stacks, m[1]) {
			e.Stacks = append(e.Stacks, m[1])
		}
	}
	for _, m := range targetTag.FindAllStringSubmatch(body, -1) {
		if !slices.Contains(e.Targets, m[1]) {
			e.Targets = append(e.Targets, m[1])
		}
	}
}

func clock(h, m, ampm, zone string) *Clock {
	hour, _ := strconv.Atoi(h)
	minute, _ := strconv.Atoi(strings.ReplaceAll(m, "x", "0"))
	switch strings.ToUpper(ampm) {
	case "PM":
		if hour < 12 {
			hour += 12
		}
	case "AM":
		if hour == 12 {
			hour = 0
		}
	}
	if hour > 23 || minute > 59 {
		return nil
	}
	return &Clock{Hour: hour, Minute: minute, UTC: zone == "Z" || zone == "UTC"}
}

func Date(entries []Entry, end time.Time) []time.Time {
	out := make([]time.Time, len(entries))
	cur := end
	for i := len(entries) - 1; i >= 0; i-- {
		c := entries[i].Clock
		if c == nil {
			out[i] = cur
			continue
		}
		loc := time.Local
		if c.UTC {
			loc = time.UTC
		}
		ref := cur.In(loc)
		t := time.Date(ref.Year(), ref.Month(), ref.Day(), c.Hour, c.Minute, 0, 0, loc)
		latest := end.Add(5 * time.Minute)
		switch {
		case t.Sub(cur) > 12*time.Hour || t.After(latest):
			t = t.AddDate(0, 0, -1)
		case cur.Sub(t) > 12*time.Hour && !t.AddDate(0, 0, 1).After(latest):
			t = t.AddDate(0, 0, 1)
		}
		out[i] = t
		if t.Before(cur) {
			cur = t
		}
	}
	return out
}
