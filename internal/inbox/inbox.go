package inbox

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
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
	Open   bool
	Fold   int
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
		if f.Peer != "" {
			if f.PeerAfter, err = st.CursorPeak(ctx, opts.Cursor); err != nil {
				return TailResult{}, err
			}
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
	rendered, folded, err := lines(ctx, st, records, view{JSON: opts.JSON, Width: opts.Width, Fold: opts.Fold})
	if err != nil {
		return TailResult{}, err
	}
	closed, err := closedOf(ctx, st, records, opts.Open)
	if err != nil {
		return TailResult{}, err
	}
	b := render.NewBudget(w, opts.Budget)
	res := TailResult{Last: f.After}
	shortened := 0
	for i, r := range records {
		if closed[r.Seq] {
			res.Last = r.Seq
			continue
		}
		if !b.Line(rendered[i]) {
			break
		}
		if folded[i] {
			shortened++
		}
		res.Printed++
		res.Last = r.Seq
	}
	if shortened > 0 {
		b.Trailer(fmt.Sprintf("... %d resolved records cut to %d characters; cci show <seq> prints one whole, --all prints them whole", shortened, opts.Fold))
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
		upTo := ""
		if opts.Open {
			upTo = "up to "
		}
		b.Trailer(fmt.Sprintf("... %s%d more; resume with %s", upTo, res.More, resume))
	}
	if opts.Cursor != "" && res.Last != f.After {
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
		rendered, _, err := lines(ctx, st, hits, view{JSON: asJSON, Whole: true})
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

func Show(ctx context.Context, st *store.Store, seqs []int64, asJSON bool, w io.Writer) error {
	records := make([]store.Record, len(seqs))
	for i, seq := range seqs {
		r, err := st.Get(ctx, seq)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("show: no record #%d", seq)
		}
		if err != nil {
			return err
		}
		records[i] = r
	}
	rendered, _, err := lines(ctx, st, records, view{JSON: asJSON, Whole: true})
	if err != nil {
		return err
	}
	for i, line := range rendered {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
		path := records[i].Refs.Path
		if asJSON || path == "" {
			continue
		}
		body, err := os.ReadFile(path) //nolint:gosec // G304: the path is the one the record names.
		if errors.Is(err, os.ErrNotExist) {
			body = []byte(path + " is not on this machine")
		} else if err != nil {
			return fmt.Errorf("show #%d: %w", records[i].Seq, err)
		}
		if !bytes.HasSuffix(body, []byte("\n")) {
			body = append(body, '\n')
		}
		if _, err := w.Write(body); err != nil {
			return err
		}
	}
	return nil
}

func closedOf(ctx context.Context, st *store.Store, records []store.Record, open bool) (map[int64]bool, error) {
	if !open {
		return nil, nil
	}
	openers := map[string][]int64{}
	closed := map[int64]bool{}
	for _, r := range records {
		if r.Kind.Opens() && r.Tracked() {
			openers[r.Drive] = append(openers[r.Drive], r.Seq)
			closed[r.Seq] = true
		}
	}
	for drive, seqs := range openers {
		stillOpen, err := st.OpenAmong(ctx, drive, seqs)
		if err != nil {
			return nil, err
		}
		for _, r := range stillOpen {
			delete(closed, r.Seq)
		}
	}
	return closed, nil
}
