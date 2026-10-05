package inbox

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

type WatchOptions struct {
	Filter   store.Filter
	Cursor   string
	Interval time.Duration
	For      time.Duration
	JSON     bool
}

func Watch(ctx context.Context, st *store.Store, opts WatchOptions, w io.Writer) error {
	f := opts.Filter
	if f.After == 0 && f.Since.IsZero() {
		start, err := watchStart(ctx, st, opts)
		if err != nil {
			return err
		}
		f.After = start
	}
	deadline := time.Now().Add(opts.For)
	for {
		f.Limit = pageRows
		records, err := st.Query(ctx, f)
		if err != nil {
			return err
		}
		now := st.Now()
		for _, r := range records {
			line := clip(render.Line(r, now), 600)
			if opts.JSON {
				line = render.JSON(r)
			}
			if _, err := fmt.Fprintln(w, line); err != nil {
				return fmt.Errorf("write watch line: %w", err)
			}
			f.After = r.Seq
		}
		if len(records) > 0 && opts.Cursor != "" {
			if err := st.SetCursor(ctx, opts.Cursor, f.Drive, f.After); err != nil {
				return err
			}
		}
		if len(records) == pageRows {
			continue
		}
		if time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.Interval):
		}
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
