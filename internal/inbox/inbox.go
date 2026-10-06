package inbox

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"time"

	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	pageRows     = 500
	DigestWindow = 24 * time.Hour
	FreshCursor  = time.Hour
)

type TailOptions struct {
	Filter store.Filter
	Cursor string
	Limit  int
	Budget int
	Width  int
	JSON   bool
	Resume string
}

type TailResult struct {
	Printed int
	Last    int64
	More    int
}

func Tail(ctx context.Context, st *store.Store, opts TailOptions, w io.Writer) (TailResult, error) {
	f := opts.Filter
	if opts.Cursor != "" {
		seq, ok, err := st.Cursor(ctx, opts.Cursor, f.Drive)
		if err != nil {
			return TailResult{}, err
		}
		f.After = seq
		if !ok {
			f.Since = st.Now().Add(-FreshCursor)
		}
	}
	f.Limit = pageRows
	if opts.Limit > 0 {
		f.Limit = opts.Limit
		f.Descending = true
	}
	records, err := st.Query(ctx, f)
	if err != nil {
		return TailResult{}, err
	}
	if f.Descending {
		slices.Reverse(records)
	}
	rendered, err := lines(ctx, st, records, opts.JSON, opts.Width)
	if err != nil {
		return TailResult{}, err
	}
	b := render.NewBudget(w, opts.Budget)
	res := TailResult{Last: f.After}
	for i, r := range records {
		if !b.Line(rendered[i]) {
			break
		}
		res.Printed++
		res.Last = r.Seq
	}
	rest := f
	rest.Limit = 0
	rest.Descending = false
	rest.After = res.Last
	if res.More, err = st.Count(ctx, rest); err != nil {
		return TailResult{}, err
	}
	if res.More > 0 && !opts.JSON {
		resume := fmt.Sprintf("--since %d", res.Last)
		if opts.Cursor != "" {
			resume = cmp.Or(opts.Resume, "cci tail")
		}
		b.Trailer(fmt.Sprintf("... %d more; resume with %s", res.More, resume))
	}
	if opts.Cursor != "" && res.Printed > 0 {
		if err := st.SetCursor(ctx, opts.Cursor, f.Drive, res.Last); err != nil {
			return TailResult{}, err
		}
	}
	return res, nil
}

func Grep(ctx context.Context, st *store.Store, f store.Filter, pattern *regexp.Regexp, limit, budget int, asJSON bool, w io.Writer) error {
	b := render.NewBudget(w, budget)
	matched, printed := 0, 0
	f.Descending = true
	f.Limit = pageRows
	now := st.Now()
	for {
		records, err := st.Query(ctx, f)
		if err != nil {
			return err
		}
		var hits []store.Record
		for _, r := range records {
			if pattern.MatchString(render.Line(r, now)) {
				hits = append(hits, r)
			}
		}
		rendered, err := lines(ctx, st, hits, asJSON, 0)
		if err != nil {
			return err
		}
		for _, line := range rendered {
			matched++
			if (limit == 0 || printed < limit) && b.Line(line) {
				printed++
			}
		}
		if len(records) < pageRows {
			break
		}
		f.Before = records[len(records)-1].Seq
	}
	if asJSON {
		return nil
	}
	if matched == 0 {
		head, err := st.MaxSeq(ctx)
		if err != nil {
			return err
		}
		b.Trailer(fmt.Sprintf("no records on %s match; store head #%d", f.Drive, head))
	}
	if matched > printed {
		b.Trailer(fmt.Sprintf("... %d more matches; narrow with --kind, --lane or --since", matched-printed))
	}
	return nil
}
