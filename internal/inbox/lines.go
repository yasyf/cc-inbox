package inbox

import (
	"context"
	"fmt"
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

func lines(ctx context.Context, st *store.Store, records []store.Record, asJSON bool) ([]string, error) {
	out := make([]string, len(records))
	if asJSON {
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
		out[i] = render.Line(r, now) + marks(replies[r.Seq])
	}
	return out, nil
}

func marks(replies []store.Record) string {
	var b strings.Builder
	for _, r := range replies {
		label, ok := replyMarks[r.Kind]
		if !ok {
			label = "RE"
		}
		fmt.Fprintf(&b, " [%s #%d]", label, r.Seq)
	}
	return b.String()
}
