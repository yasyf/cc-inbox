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
	"strings"
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
	Filter  store.Filter
	Cursor  string
	Seed    string
	Limit   int
	Budget  int
	Width   int
	JSON    bool
	Resume  string
	Open    bool
	Oldest  bool
	Fold    int
	Notes   io.Writer
	Defer   bool
	Compact bool
	Reader  string
}

// TailResult reports what a Tail printed. Ack holds the cursor moves a
// Tail with Defer set left for its caller to apply once the output reached the
// reader; a Tail that does not defer applies them after it writes.
type TailResult struct {
	Printed int
	Last    int64
	More    int
	Ack     []store.CursorMove
}

func Tail(ctx context.Context, st *store.Store, opts TailOptions, w io.Writer) (TailResult, error) {
	f := opts.Filter
	seeded := false
	if opts.Cursor != "" {
		seq, ok, err := st.Cursor(ctx, opts.Cursor, f.Drive)
		if err != nil {
			return TailResult{}, err
		}
		f.After = seq
		switch {
		case ok:
		case opts.Seed != "":
			if f.After, err = seedOf(ctx, st, opts.Seed, f.Drive); err != nil {
				return TailResult{}, err
			}
			seeded = true
		default:
			f.Since = st.Now().Add(-FreshCursor)
		}
		if f.Peer != "" {
			if f.PeerAfter, err = st.CursorPeak(ctx, opts.Cursor); err != nil {
				return TailResult{}, err
			}
		}
	}
	newest := opts.Cursor == "" && !opts.Oldest && !opts.JSON
	f.Limit = pageRows
	if opts.Limit > 0 {
		f.Limit = opts.Limit
	}
	f.Descending = newest || opts.Limit > 0
	v := view{JSON: opts.JSON, Width: opts.Width, Fold: opts.Fold, Compact: opts.Compact, Reader: opts.Reader}
	records, rendered, folded, paged, err := tailPages(ctx, st, f, v, opts.Budget, opts.Limit > 0)
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
	cut := 0
	if newest && len(records) > 0 {
		fits := render.NewBudget(io.Discard, opts.Budget)
		first := len(records)
		for first > 0 && (skipped[records[first-1].Seq] || fits.Line(rendered[first-1])) {
			first--
		}
		records, rendered, folded, cut = records[first:], rendered[first:], folded[first:], first
		if opts.Limit == 0 {
			older := f
			older.Limit, older.Descending, older.Before = 0, false, records[0].Seq
			if cut, err = st.Count(ctx, older); err != nil {
				return TailResult{}, err
			}
		}
	}
	ahead, err := addressedAhead(ctx, st, f, opts, records, paged)
	if err != nil {
		return TailResult{}, err
	}
	all := slices.Clone(records)
	var later []store.Record
	for _, r := range ahead.pending {
		if len(records) == 0 || r.Seq > records[len(records)-1].Seq {
			later = append(later, r)
		}
	}
	laterLines, laterFolded, err := lines(ctx, st, later, v)
	if err != nil {
		return TailResult{}, err
	}
	all, rendered, folded = append(all, later...), append(rendered, laterLines...), append(folded, laterFolded...)
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
	if len(ahead.pending) > 0 && (!t.done || paged) {
		out, b, t = run(ahead.pending)
	}
	res, shortened := t.res, t.shortened
	var notes []string
	trailer := func(s string, cut bool) {
		if opts.JSON || cut && opts.Notes != nil {
			notes = append(notes, s)
		}
		if !opts.JSON {
			b.Trailer(s)
		}
	}
	if t.first > 0 && !opts.JSON {
		first := "to " + f.For
		if f.For == "" {
			first = "from root"
		}
		trailer(fmt.Sprintf("... %d %s first", t.first, first), false)
	}
	if cut > 0 {
		trailer(fmt.Sprintf("... CUT %d older records before %s; --oldest reads them first", cut, records[0].Name()), true)
	}
	if shortened > 0 {
		trailer(fmt.Sprintf("... %d resolved records cut to %d characters; cci show <seq> prints one whole, --all prints them whole", shortened, opts.Fold), false)
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
		trailer(fmt.Sprintf("... %s%d more; resume with %s", upTo, res.More, resume), true)
	}
	if opts.Cursor != "" && (res.Last != f.After || seeded) {
		res.Ack = append(res.Ack, store.CursorMove{Name: opts.Cursor, Drive: f.Drive, Seq: res.Last})
	}
	if t.sent > ahead.sent {
		res.Ack = append(res.Ack, store.CursorMove{Name: addressedCursor(opts.Cursor), Drive: f.Drive, Seq: t.sent})
	}
	if _, err := out.WriteTo(w); err != nil {
		return TailResult{}, fmt.Errorf("write tail: %w", err)
	}
	for _, note := range notes {
		if _, err := fmt.Fprintln(opts.Notes, note); err != nil {
			return TailResult{}, fmt.Errorf("write tail: %w", err)
		}
	}
	if opts.Defer {
		return res, nil
	}
	if err := st.MoveCursors(ctx, res.Ack); err != nil {
		return TailResult{}, err
	}
	res.Ack = nil
	return res, nil
}

type tailPage struct {
	records  []store.Record
	rendered []string
	folded   []bool
}

func joinTailPages(pages []tailPage) ([]store.Record, []string, []bool) {
	if len(pages) == 1 {
		return pages[0].records, pages[0].rendered, pages[0].folded
	}
	rows := 0
	for _, page := range pages {
		rows += len(page.records)
	}
	records := make([]store.Record, 0, rows)
	rendered := make([]string, 0, rows)
	folded := make([]bool, 0, rows)
	for i := len(pages) - 1; i >= 0; i-- {
		records = append(records, pages[i].records...)
		rendered = append(rendered, pages[i].rendered...)
		folded = append(folded, pages[i].folded...)
	}
	return records, rendered, folded
}

func tailPages(ctx context.Context, st *store.Store, f store.Filter, v view, budget int, limited bool) ([]store.Record, []string, []bool, bool, error) {
	left := cmp.Or(max(budget, 0), render.DefaultBudget)
	var records []store.Record
	var rendered []string
	var folded []bool
	var pages []tailPage
	query := st.Query
	if !v.JSON {
		query = st.QueryText
	}
	for {
		page, err := query(ctx, f)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if f.Descending {
			slices.Reverse(page)
		}
		out, fold, err := lines(ctx, st, page, v)
		if err != nil {
			return nil, nil, nil, false, err
		}
		for _, line := range out {
			left -= len(line) + 1
		}
		full := len(page) == f.Limit
		if f.Descending {
			pages = append(pages, tailPage{records: page, rendered: out, folded: fold})
		} else {
			records, rendered, folded = append(records, page...), append(rendered, out...), append(folded, fold...)
		}
		if limited || !full || left < 0 {
			if f.Descending {
				records, rendered, folded = joinTailPages(pages)
			}
			return records, rendered, folded, full, nil
		}
		if f.Descending {
			f.Before = page[0].Seq
			continue
		}
		f.After = page[len(page)-1].Seq
	}
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

func seedOf(ctx context.Context, st *store.Store, seed, drive string) (int64, error) {
	seq, ok, err := st.Cursor(ctx, seed, drive)
	if err != nil || ok {
		return seq, err
	}
	return st.MaxSeq(ctx)
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

func addressedAhead(ctx context.Context, st *store.Store, f store.Filter, opts TailOptions, page []store.Record, paged bool) (ahead, error) {
	laneMail := f.For == "" && f.To != "" && !store.RootName(f.To)
	if f.For == "" && !laneMail || f.Descending || opts.Cursor == "" {
		return ahead{}, nil
	}
	a := ahead{filter: f, delivered: map[int64]bool{}}
	if laneMail {
		a.filter.Lanes = []string{"root", "main"}
	} else {
		a.filter.For, a.filter.Addressed = "", f.For
		a.filter.Kinds, a.filter.Lanes, a.filter.Topics = nil, nil, nil
	}
	a.filter.Peer, a.filter.PeerAfter = "", 0
	a.filter.Limit = 0
	sent, _, err := st.Cursor(ctx, addressedCursor(opts.Cursor), f.Drive)
	if err != nil {
		return ahead{}, err
	}
	a.sent = sent
	if laneMail && !paged {
		for _, r := range page {
			if !slices.Contains(a.filter.Lanes, r.Lane) {
				continue
			}
			if r.Seq <= a.sent {
				a.delivered[r.Seq] = true
			} else {
				a.pending = append(a.pending, r)
			}
		}
		return a, nil
	}
	if a.sent > f.After {
		done := a.filter
		done.Before = a.sent + 1
		records, err := st.QueryText(ctx, done)
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

func Grep(ctx context.Context, st *store.Store, f store.Filter, pattern *regexp.Regexp, limit, budget, width int, asJSON bool, w, notes io.Writer) error {
	query := st.QueryText
	if asJSON {
		query = st.Query
	}
	b := render.NewBudget(w, budget)
	candidateBudget := render.NewBudget(io.Discard, budget)
	matched, printed, kept := 0, 0, 0
	var selected [][]store.Record
	var readErr error
	f.Descending = true
	f.Limit = pageRows
	now := st.Now()
	for {
		records, err := query(ctx, f)
		if err != nil {
			readErr = err
			break
		}
		var hits []store.Record
		for _, r := range records {
			line := render.Line(r, now)
			if !pattern.MatchString(line) {
				continue
			}
			matched++
			if candidateBudget.Full() || limit != 0 && kept >= limit {
				continue
			}
			if asJSON {
				line = render.JSON(r)
			}
			if candidateBudget.Line(line) {
				hits = append(hits, r)
				kept++
			}
		}
		if len(hits) > 0 {
			selected = append(selected, hits)
		}
		if len(records) < pageRows {
			break
		}
		f.Before = records[len(records)-1].Seq
	}
	for _, page := range grepPages(ctx, st, selected, asJSON, width, now) {
		if b.Full() {
			break
		}
		if page.err != nil {
			return page.err
		}
		for _, line := range page.lines {
			if !b.Line(line) {
				break
			}
			printed++
		}
	}
	if readErr != nil {
		return readErr
	}
	if asJSON {
		if matched > printed && limit >= 0 && notes != nil {
			if _, err := fmt.Fprintf(notes, "... %d more matches; narrow with --kind, --lane or --since\n", matched-printed); err != nil {
				return fmt.Errorf("write grep: %w", err)
			}
		}
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
		more := fmt.Sprintf("... %d more matches; narrow with --kind, --lane or --since", matched-printed)
		b.Trailer(more)
		if notes != nil {
			if _, err := fmt.Fprintln(notes, more); err != nil {
				return fmt.Errorf("write grep: %w", err)
			}
		}
	}
	return nil
}

type grepPage struct {
	lines []string
	err   error
}

func grepPages(ctx context.Context, st *store.Store, selected [][]store.Record, asJSON bool, width int, now time.Time) []grepPage {
	pages := make([]grepPage, len(selected))
	if len(selected) == 0 {
		return pages
	}
	if asJSON {
		drives := map[string][]store.Record{}
		first := map[string]int{}
		for page, records := range selected {
			for _, r := range records {
				if _, seen := first[r.Drive]; !seen && r.Kind.Opens() && r.Tracked() {
					first[r.Drive] = page
				}
				drives[r.Drive] = append(drives[r.Drive], r)
			}
		}
		statuses := map[int64]string{}
		for _, drive := range slices.Sorted(maps.Keys(drives)) {
			if err := Annotate(ctx, st, drives[drive]); err != nil {
				if pages[first[drive]].err == nil {
					pages[first[drive]].err = err
				}
				continue
			}
			for _, r := range drives[drive] {
				statuses[r.Seq] = r.Status
			}
		}
		for page, records := range selected {
			for _, r := range records {
				r.Status = statuses[r.Seq]
				pages[page].lines = append(pages[page].lines, render.JSON(r))
			}
		}
		return pages
	}
	seqs := make([][]int64, len(selected))
	for page, records := range selected {
		for _, r := range records {
			seqs[page] = append(seqs[page], r.Seq)
		}
	}
	replies, err := st.ReplyPages(ctx, seqs)
	if err != nil {
		pages[0].err = err
		return pages
	}
	for page, records := range selected {
		pages[page].err = replies[page].Err
		if replies[page].Err == nil {
			pages[page].lines, _ = textLines(records, replies[page].Replies, view{Whole: true, Width: width}, now)
		}
	}
	return pages
}

func Show(ctx context.Context, st *store.Store, seqs []int64, drive string, budget int, asJSON bool, w io.Writer) error {
	records := make([]store.Record, len(seqs))
	for i, seq := range seqs {
		r, err := st.Get(ctx, seq)
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("show: no record #%d", seq)
		}
		if err != nil {
			return err
		}
		if drive != "" && r.Drive != drive {
			return fmt.Errorf("show: #%d is on drive %s, not %s", seq, r.Drive, drive)
		}
		records[i] = r
	}
	rendered, _, err := lines(ctx, st, records, view{JSON: asJSON, Whole: true})
	if err != nil {
		return err
	}
	share := 0
	if budget > 0 {
		share = max(budget/len(records), 1)
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
		body = BodyAfterText(body, records[i].Text)
		if share > 0 && len(body) > share {
			body = fmt.Appendf(body[:share:share], "\n... cut %d bytes of #%d; cci show %d reads it whole", len(body)-share, records[i].Seq, records[i].Seq)
		}
		if len(body) == 0 {
			continue
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

func BodyAfterText(body []byte, text string) []byte {
	cut, spilled := strings.CutSuffix(text, "…")
	rest, ok := bytes.CutPrefix(body, []byte(cut))
	if !ok || cut == "" {
		return body
	}
	if spilled {
		return append([]byte("…"), rest...)
	}
	return bytes.TrimPrefix(rest, []byte("\n"))
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
