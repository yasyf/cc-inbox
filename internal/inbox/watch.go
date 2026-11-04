package inbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const feedRetry = 10 * time.Second

var errWatchStopped = errors.New("watch stopped")

type WatchOptions struct {
	Filter   store.Filter
	Cursor   string
	Interval time.Duration
	For      time.Duration
	Width    int
	JSON     bool
	Untagged bool
}

// Watch emits each record opts.Filter passes as it commits. With a feed on st
// it reads the store only when the feed says a record passes; without one, or
// while the feed is away, it looks for commits itself every opts.Interval.
func Watch(ctx context.Context, st *store.Store, opts WatchOptions, emit func(store.Record, string) error) error {
	f := opts.Filter
	if f.After == 0 && f.Since.IsZero() {
		start, err := watchStart(ctx, st, opts)
		if err != nil {
			return err
		}
		f.After = start
	}
	w := &watcher{st: st, opts: opts, filter: f, emit: emit}
	if opts.For > 0 {
		w.deadline = time.Now().Add(opts.For)
	}
	if feed := st.Feed(); feed != nil {
		for {
			done, err := w.follow(ctx, feed)
			if done {
				return err
			}
			if done, err = w.poll(ctx, time.Now().Add(feedRetry)); done {
				return err
			}
		}
	}
	_, err := w.poll(ctx, time.Time{})
	return err
}

type watcher struct {
	st       *store.Store
	opts     WatchOptions
	filter   store.Filter
	deadline time.Time
	emit     func(store.Record, string) error
}

func (w *watcher) follow(ctx context.Context, feed store.Feed) (bool, error) {
	followed := ctx
	if !w.deadline.IsZero() {
		var cancel context.CancelFunc
		followed, cancel = context.WithDeadline(ctx, w.deadline)
		defer cancel()
	}
	err := feed.Follow(followed, w.filter, func(wake store.Wake) error {
		w.filter.After = max(w.filter.After, wake.First-1)
		w.filter.Before = wake.Head + 1
		defer func() { w.filter.Before = 0 }()
		for {
			n, err := w.page(followed)
			if err != nil {
				return err
			}
			if n < pageRows {
				return nil
			}
		}
	})
	switch {
	case followed.Err() != nil, errors.Is(err, errWatchStopped):
		return true, nil
	case errors.Is(err, store.ErrUnrouted):
		return false, nil
	}
	return true, err
}

func (w *watcher) poll(ctx context.Context, until time.Time) (bool, error) {
	changes, err := w.st.OpenChangeVersion(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return true, nil
		}
		return true, err
	}
	defer func() { _ = changes.Close() }()
	var previous int64
	idle := false
	for {
		version, err := changes.Read(ctx)
		if ctx.Err() != nil {
			return true, nil
		}
		if err != nil {
			return true, err
		}
		if !idle || version != previous {
			n, err := w.page(ctx)
			if errors.Is(err, errWatchStopped) {
				return true, nil
			}
			if err != nil {
				return true, err
			}
			previous, idle = version, n < pageRows
			if n == pageRows {
				continue
			}
		}
		now := time.Now()
		if !w.deadline.IsZero() && now.After(w.deadline) {
			return true, nil
		}
		if !until.IsZero() && now.After(until) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return true, nil
		case <-time.After(w.opts.Interval):
		}
	}
}

func (w *watcher) page(ctx context.Context) (int, error) {
	f := &w.filter
	f.Limit = pageRows
	records, err := w.st.Query(ctx, *f)
	if ctx.Err() != nil {
		return 0, errWatchStopped
	}
	if err != nil {
		return 0, err
	}
	rendered, _, err := lines(ctx, w.st, records, view{JSON: w.opts.JSON, Width: w.opts.Width, Untagged: w.opts.Untagged, Unnamed: f.For != "" || f.To != ""})
	if ctx.Err() != nil {
		return 0, errWatchStopped
	}
	if err != nil {
		return 0, err
	}
	from := f.After
	for i, r := range records {
		if err = w.emit(r, rendered[i]); err != nil {
			break
		}
		f.After = r.Seq
	}
	if f.After > from && w.opts.Cursor != "" {
		if cerr := w.st.SetCursor(context.WithoutCancel(ctx), w.opts.Cursor, f.Drive, f.After); cerr != nil {
			return 0, errors.Join(err, cerr)
		}
	}
	if err != nil {
		return 0, err
	}
	return len(records), nil
}

// WriteLines prints each watched line, with a text line's record body under it.
func WriteLines(w io.Writer, asJSON bool) func(store.Record, string) error {
	return func(r store.Record, line string) error {
		if !asJSON {
			line = render.WithBody(line, r)
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return fmt.Errorf("write watch line: %w", err)
		}
		return nil
	}
}

func watchStart(ctx context.Context, st *store.Store, opts WatchOptions) (int64, error) {
	if opts.Cursor != "" {
		seq, ok, err := st.Cursor(ctx, opts.Cursor, opts.Filter.Drive)
		if err != nil {
			return 0, err
		}
		if ok {
			return seq, nil
		}
	}
	return st.MaxSeq(ctx)
}
