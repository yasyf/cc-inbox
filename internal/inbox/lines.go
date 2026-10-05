package inbox

import (
	"context"
	"fmt"
	"slices"
	"strings"

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
	if !slices.ContainsFunc(records, func(r store.Record) bool { return r.Kind.Opens() }) {
		return nil
	}
	open, err := st.OpenItems(ctx, records[0].Drive)
	if err != nil {
		return err
	}
	live := map[int64]bool{}
	for _, o := range open {
		live[o.Seq] = true
	}
	for i, r := range records {
		switch {
		case !r.Kind.Opens():
		case strings.HasPrefix(r.Source, "import:"):
			records[i].Status = "imported"
		case live[r.Seq]:
			records[i].Status = "open"
		default:
			records[i].Status = "closed"
		}
	}
	return nil
}

func lines(ctx context.Context, st *store.Store, records []store.Record, asJSON bool) ([]string, error) {
	out := make([]string, len(records))
	if asJSON {
		if err := Annotate(ctx, st, records); err != nil {
			return nil, err
		}
		for i, r := range records {
			out[i] = render.JSON(r)
		}
		return out, nil
	}
	if len(records) == 0 {
		return out, nil
	}
	seqs := make([]int64, len(records))
	for i, r := range records {
		seqs[i] = r.Seq
	}
	replies, err := st.Replies(ctx, records[0].Drive, seqs)
	if err != nil {
		return nil, err
	}
	now := st.Now()
	for i, r := range records {
		out[i] = render.Clip(render.Line(r, now)) + marks(r.Seq, replies[r.Seq])
	}
	return out, nil
}

func marks(seq int64, replies []store.Record) string {
	var b strings.Builder
	for _, r := range replies {
		if r.Resolves == seq {
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
