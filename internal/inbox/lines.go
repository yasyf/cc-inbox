package inbox

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
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

func Annotate(ctx context.Context, st *store.Store, records []store.Record) error {
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
		case !r.Kind.Opens():
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
	Width    int
	Fold     int
	Untagged bool
	Whole    bool
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
	line := render.Brief
	switch {
	case v.Whole:
		line = render.Line
	case v.Untagged:
		line = render.Untagged
	}
	now := st.Now()
	for i, r := range records {
		text := render.Clip(line(r, now), v.Width)
		if v.Fold > 0 && resolved(r.Seq, replies[r.Seq]) && utf8.RuneCountInString(text) > v.Fold {
			text, folded[i] = render.Clip(text, v.Fold), true
		}
		out[i] = text + marks(r.Seq, replies[r.Seq])
	}
	return out, folded, nil
}

func resolved(seq int64, replies []store.Record) bool {
	return slices.ContainsFunc(replies, func(r store.Record) bool { return slices.Contains(r.Closes(), seq) })
}

func marks(seq int64, replies []store.Record) string {
	var b strings.Builder
	for _, r := range replies {
		if slices.Contains(r.Closes(), seq) {
			fmt.Fprintf(&b, " [RESOLVED #%d]", r.Seq)
			continue
		}
		label, ok := replyMarks[r.Kind]
		if !ok {
			label = "RE"
		}
		fmt.Fprintf(&b, " [%s #%d]", label, r.Seq)
	}
	return b.String()
}
