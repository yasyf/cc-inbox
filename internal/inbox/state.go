package inbox

import (
	"cmp"
	"context"
	"io"
	"maps"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

func State(ctx context.Context, st *store.Store, f store.Filter) ([]store.Record, error) {
	allowed := []kinds.Kind{kinds.Head, kinds.Contract, kinds.State}
	if len(f.Kinds) > 0 {
		allowed = slices.DeleteFunc(slices.Clone(allowed), func(k kinds.Kind) bool { return !slices.Contains(f.Kinds, k) })
	}
	f.Kinds = allowed
	f.IncludeExpired = true
	found, err := st.Query(ctx, f)
	if err != nil {
		return nil, err
	}
	records := slices.DeleteFunc(found, func(r store.Record) bool { return !slices.Contains(allowed, r.Kind) })
	seqs := make([]int64, len(records))
	for i, r := range records {
		seqs[i] = r.Seq
	}
	replies, err := st.Replies(ctx, f.Drive, seqs)
	if err != nil {
		return nil, err
	}
	type key struct {
		kind        kinds.Kind
		lane, topic string
	}
	latest := map[key]store.Record{}
	for _, r := range records {
		if !withdrawn(replies[r.Seq]) {
			latest[key{r.Kind, r.Lane, r.Topic}] = r
		}
	}
	return slices.SortedFunc(maps.Values(latest), func(a, b store.Record) int {
		return cmp.Or(cmp.Compare(a.Lane, b.Lane), cmp.Compare(a.Topic, b.Topic), cmp.Compare(a.Kind, b.Kind))
	}), nil
}

func withdrawn(replies []store.Record) bool {
	return slices.ContainsFunc(replies, func(r store.Record) bool { return r.Kind == kinds.Withdraw })
}

func WriteState(w io.Writer, records []store.Record, now time.Time, budget int, asJSON bool) {
	b := render.NewBudget(w, budget)
	for _, r := range records {
		line := render.JSON(r)
		if !asJSON {
			line = render.Line(r, now)
		}
		if !b.Line(line) {
			if !asJSON {
				b.Trailer("... state truncated; narrow with --lane or --topic")
			}
			return
		}
	}
}
