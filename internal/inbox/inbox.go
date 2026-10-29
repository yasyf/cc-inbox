package inbox

import (
	"bytes"
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"maps"
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
	Notes  io.Writer
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
	ahead, err := addressedAhead(ctx, st, f, opts)
	if err != nil {
		return TailResult{}, err
	}
	all := slices.Clone(records)
	for _, r := range ahead.pending {
		if len(records) == 0 || r.Seq > records[len(records)-1].Seq {
			all = append(all, r)
		}
	}
	rendered, folded, err := lines(ctx, st, all, view{JSON: opts.JSON, Width: opts.Width, Fold: opts.Fold})
	if err != nil {
		return TailResult{}, err
	}
	skipped, err := skippedOf(ctx, st, records, opts.Open)
	if err != nil {
		return TailResult{}, err
	}
	if skipped == nil {
		skipped = map[int64]bool{}
	}
	maps.Copy(skipped, ahead.delivered)
	index := make(map[int64]int, len(all))
	for i, r := range all {
		index[r.Seq] = i
	}
	run := func(first []store.Record) (*bytes.Buffer, *render.Budget, tailRun) {
		var out bytes.Buffer
		b := render.NewBudget(&out, opts.Budget)
		t := tailRun{res: TailResult{Last: f.After}, printed: map[int64]bool{}}
		for _, r := range first {
			if !b.Line(rendered[index[r.Seq]]) {
				break
			}
			t.mark(r, folded[index[r.Seq]])
			t.sent = r.Seq
			t.first++
		}
		t.done = true
		for _, r := range records {
			if skipped[r.Seq] || t.printed[r.Seq] {
				t.res.Last = r.Seq
				continue
			}
			if !b.Line(rendered[index[r.Seq]]) {
				t.done = false
				break
			}
			t.mark(r, folded[index[r.Seq]])
			t.res.Last = r.Seq
		}
		return &out, b, t
	}
	out, b, t := run(nil)
	if len(ahead.pending) > 0 && (!t.done || len(records) == f.Limit) {
		out, b, t = run(ahead.pending)
	}
	res, shortened := t.res, t.shortened
	var notes []string
	trailer := func(s string) {
		if opts.JSON {
			notes = append(notes, s)
			return
		}
		b.Trailer(s)
	}
	if t.first > 0 && !opts.JSON {
		trailer(fmt.Sprintf("... %d to %s first", t.first, f.For))
	}
	if shortened > 0 {
		trailer(fmt.Sprintf("... %d resolved records cut to %d characters; cci show <seq> prints one whole, --all prints them whole", shortened, opts.Fold))
	}
	rest := f
	rest.Limit = 0
	rest.Descending = false
	rest.After = res.Last
	if res.More, err = st.Count(ctx, rest); err != nil {
		return TailResult{}, err
	}
	if sent := max(ahead.sent, t.sent); sent > res.Last {
		early := ahead.filter
		early.After, early.Before = res.Last, sent+1
		n, err := st.Count(ctx, early)
		if err != nil {
			return TailResult{}, err
		}
		res.More -= n
	}
	if res.More > 0 {
		resume := fmt.Sprintf("--since %d", res.Last)
		if opts.Cursor != "" {
			resume = cmp.Or(opts.Resume, "cci tail")
		}
		upTo := ""
		if opts.Open {
			upTo = "up to "
		}
		trailer(fmt.Sprintf("... %s%d more; resume with %s", upTo, res.More, resume))
	}
	if opts.Cursor != "" && res.Last != f.After {
		if err := st.SetCursor(ctx, opts.Cursor, f.Drive, res.Last); err != nil {
			return TailResult{}, err
		}
	}
	if t.sent > ahead.sent {
		if err := st.SetCursor(ctx, addressedCursor(opts.Cursor), f.Drive, t.sent); err != nil {
			return TailResult{}, err
		}
	}
	if _, err := out.WriteTo(w); err != nil {
		return TailResult{}, fmt.Errorf("write tail: %w", err)
	}
	for _, note := range notes {
		if _, err := fmt.Fprintln(opts.Notes, note); err != nil {
			return TailResult{}, fmt.Errorf("write tail: %w", err)
		}
	}
	return res, nil
}

type tailRun struct {
	res       TailResult
	printed   map[int64]bool
	sent      int64
	first     int
	shortened int
	done      bool
}

func (t *tailRun) mark(r store.Record, folded bool) {
	t.printed[r.Seq] = true
	t.res.Printed++
	if folded {
		t.shortened++
	}
}

func addressedCursor(cursor string) string {
	return cursor + "#addressed"
}

type ahead struct {
	filter    store.Filter
	sent      int64
	pending   []store.Record
	delivered map[int64]bool
}

func addressedAhead(ctx context.Context, st *store.Store, f store.Filter, opts TailOptions) (ahead, error) {
	if f.For == "" || f.Descending || opts.Cursor == "" {
		return ahead{}, nil
	}
	a := ahead{filter: f, delivered: map[int64]bool{}}
	a.filter.For, a.filter.Addressed = "", f.For
	a.filter.Kinds, a.filter.Lanes, a.filter.Topics = nil, nil, nil
	a.filter.Peer, a.filter.PeerAfter = "", 0
	a.filter.Limit = 0
	sent, _, err := st.Cursor(ctx, addressedCursor(opts.Cursor), f.Drive)
	if err != nil {
		return ahead{}, err
	}
	a.sent = sent
	if a.sent > f.After {
		done := a.filter
		done.Before = a.sent + 1
		records, err := st.Query(ctx, done)
		if err != nil {
			return ahead{}, err
		}
		for _, r := range records {
			a.delivered[r.Seq] = true
		}
	}
	waiting := a.filter
	waiting.After = max(f.After, a.sent)
	waiting.Limit = pageRows
	pending, err := st.Query(ctx, waiting)
	if err != nil {
		return ahead{}, err
	}
	a.pending = pending
	return a, nil
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

func skippedOf(ctx context.Context, st *store.Store, records []store.Record, open bool) (map[int64]bool, error) {
	if !open {
		return nil, nil
	}
	openers := map[string][]int64{}
	latest := map[string][]int64{}
	skipped := map[int64]bool{}
	for _, r := range records {
		if r.Kind.Opens() && r.Tracked() {
			openers[r.Drive] = append(openers[r.Drive], r.Seq)
			skipped[r.Seq] = true
		}
		if r.Kind.Spec().Latest && r.Topic != "" {
			latest[r.Drive] = append(latest[r.Drive], r.Seq)
		}
	}
	for drive, seqs := range openers {
		stillOpen, err := st.OpenAmong(ctx, drive, seqs)
		if err != nil {
			return nil, err
		}
		for _, r := range stillOpen {
			delete(skipped, r.Seq)
		}
	}
	for drive, seqs := range latest {
		replaced, err := st.ReplacedAmong(ctx, drive, seqs)
		if err != nil {
			return nil, err
		}
		maps.Copy(skipped, replaced)
	}
	return skipped, nil
}
