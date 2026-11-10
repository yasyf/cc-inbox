package render

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/store"
)

var leadingStamp = regexp.MustCompile(`^(?:R\d+ \()?\d{1,2}:\d\d ?[AP]M\)?:? ?`)

func Compact(r store.Record, width int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", strings.TrimPrefix(r.Name(), "#"), r.Kind)
	if r.Status == "closed" {
		b.WriteString(" closed")
	}
	b.WriteString(" " + sender(r))
	if to := recipients(r.To); len(to) > 0 {
		for i, t := range to {
			to[i] = strings.TrimSuffix(t, "@"+r.Drive)
		}
		b.WriteString(">" + strings.Join(to, ","))
	}
	if r.Re != 0 {
		b.WriteString(" ^" + strings.TrimPrefix(r.ReName(), "#"))
	}
	for _, closed := range r.CloseNames() {
		b.WriteString(" x" + strings.TrimPrefix(closed, "#"))
	}
	text := strings.Join(strings.Fields(leadingStamp.ReplaceAllString(strings.TrimSpace(r.Text), "")), " ")
	text = strings.TrimPrefix(strings.TrimPrefix(text, r.Lane+": "), r.Lane+" ")
	for _, pr := range r.Refs.PRs {
		short := fmt.Sprintf("#%d", pr.Number)
		if strings.Contains(text, short) {
			continue
		}
		b.WriteString(" " + pr.Repo[strings.LastIndex(pr.Repo, "/")+1:] + short)
	}
	if r.Topic != "" && !strings.Contains(text, r.Topic) {
		b.WriteString(" t:" + r.Topic)
	}
	for _, ref := range slices.Concat(prefixed("stack:", r.Refs.Stacks), prefixed("target:", r.Refs.Targets), prefixed("build:", r.Refs.Builds)) {
		b.WriteString(" " + ref)
	}
	if r.Refs.CCN != "" {
		b.WriteString(" ccn:" + r.Refs.CCN)
	}
	b.WriteString(Attrs(r))
	if r.Refs.URL != "" {
		b.WriteString(" " + r.Refs.URL)
	}
	if r.Refs.Path != "" || len(r.Body) > 0 {
		b.WriteString(" +")
	}
	if width > 0 && utf8.RuneCountInString(text) > width {
		text = Clip(text, width)
	}
	b.WriteString(" " + text)
	return b.String()
}

func prefixed(prefix string, values []string) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = prefix + v
	}
	return out
}

type Wire struct {
	Now    time.Time
	minute string
}

func (w *Wire) Line(r store.Record, width int) string {
	return w.Headed(r.At, Compact(r, width))
}

func (w *Wire) Headed(at time.Time, line string) string {
	if minute := Stamp(at, w.Now); minute != w.minute {
		w.minute = minute
		return "@" + minute + "\n" + line
	}
	return line
}
