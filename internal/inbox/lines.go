package inbox

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

var replyMarks = map[kinds.Kind]string{
	kinds.Answer:   "ANSWERED",
	kinds.Withdraw: "WITHDRAWN",
	kinds.Go:       "GO",
	kinds.Lift:     "LIFTED",
	kinds.Done:     "DONE",
	kinds.FixLive:  "FIX-LIVE",
}

type openerReader interface {
	OpenAmong(context.Context, string, []int64) ([]store.Record, error)
}

func Annotate(ctx context.Context, st openerReader, records []store.Record) error {
	tracked := map[string][]int64{}
	for _, r := range records {
		if r.Kind.Opens() && r.Tracked() {
			tracked[r.Drive] = append(tracked[r.Drive], r.Seq)
		}
	}
	live := map[int64]bool{}
	for _, drive := range slices.Sorted(maps.Keys(tracked)) {
		open, err := st.OpenAmong(ctx, drive, tracked[drive])
		if err != nil {
			return err
		}
		for _, o := range open {
			live[o.Seq] = true
		}
	}
	for i, r := range records {
		switch {
		case !r.Kind.Opens(), r.Seq < 1:
		case !r.Tracked():
			records[i].Status = "imported"
		case live[r.Seq]:
			records[i].Status = "open"
		default:
			records[i].Status = "closed"
		}
	}
	return nil
}

type view struct {
	JSON     bool
	Compact  bool
	Reader   string
	Width    int
	Fold     int
	Untagged bool
	Whole    bool
	Now      time.Time
	Unnamed  bool
}

func lines(ctx context.Context, st *store.Store, records []store.Record, v view) ([]string, []bool, error) {
	out := make([]string, len(records))
	folded := make([]bool, len(records))
	if v.JSON {
		if err := Annotate(ctx, st, records); err != nil {
			return nil, nil, err
		}
		for i, r := range records {
			out[i] = render.JSON(r)
		}
		return out, folded, nil
	}
	if len(records) == 0 {
		return out, folded, nil
	}
	seqs := make([]int64, len(records))
	for i, r := range records {
		seqs[i] = r.Seq
	}
	replies, err := st.Replies(ctx, seqs)
	if err != nil {
		return nil, nil, err
	}
	now := v.Now
	if now.IsZero() {
		now = st.Now()
	}
	out, folded = textLines(records, replies, v, now)
	return out, folded, nil
}

func textLines(records []store.Record, replies map[int64][]store.Record, v view, now time.Time) ([]string, []bool) {
	out := make([]string, len(records))
	folded := make([]bool, len(records))
	wire := render.Wire{Now: now}
	line := func(r store.Record, width int) string {
		switch {
		case v.Whole:
			return render.Clip(render.Line(r, now), width)
		case v.Untagged:
			return render.Untagged(r, now, width)
		}
		return render.Brief(r, now, width)
	}
	for i, r := range records {
		shown := r
		if v.Unnamed || v.Compact && v.Reader != "" {
			shown.To = nil
		}
		if v.Compact {
			width := compactWidth(r, v)
			if v.Fold > 0 && resolved(r.Seq, replies[r.Seq]) {
				width, folded[i] = v.Fold, true
			}
			out[i] = wire.Line(shown, width) + marks(r.Seq, replies[r.Seq])
			continue
		}
		text := line(shown, v.Width)
		if v.Fold > 0 && resolved(r.Seq, replies[r.Seq]) && utf8.RuneCountInString(text) > v.Fold {
			text, folded[i] = line(shown, v.Fold), true
		}
		out[i] = text + marks(r.Seq, replies[r.Seq])
	}
	return out, folded
}

func resolved(seq int64, replies []store.Record) bool {
	return slices.ContainsFunc(replies, func(r store.Record) bool { return slices.Contains(r.Closes(), seq) })
}

func marks(seq int64, replies []store.Record) string {
	var b strings.Builder
	for _, r := range replies {
		if slices.Contains(r.Closes(), seq) {
			fmt.Fprintf(&b, " [RESOLVED %s]", r.Name())
			continue
		}
		label, ok := replyMarks[r.Kind]
		if !ok {
			label = "RE"
		}
		fmt.Fprintf(&b, " [%s %s]", label, r.Name())
	}
	return b.String()
}

func compactWidth(r store.Record, v view) int {
	switch {
	case v.Reader == "":
		return v.Width
	case slices.Contains(r.To, v.Reader):
		return 0
	}
	return render.FoldWidth
}
