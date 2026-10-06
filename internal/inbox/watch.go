package inbox

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

type WatchOptions struct {
	Filter   store.Filter
	Cursor   string
	Interval time.Duration
	For      time.Duration
	Width    int
	JSON     bool
}

func Watch(ctx context.Context, st *store.Store, opts WatchOptions, emit func(store.Record, string) error) error {
	f := opts.Filter
	if f.After == 0 && f.Since.IsZero() {
		start, err := watchStart(ctx, st, opts)
		if err != nil {
			return err
		}
		f.After = start
	}
	var deadline time.Time
	if opts.For > 0 {
		deadline = time.Now().Add(opts.For)
	}
	for {
		f.Limit = pageRows
		records, err := st.Query(ctx, f)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		rendered, err := lines(ctx, st, records, opts.JSON, opts.Width)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			return err
		}
		from := f.After
		for i, r := range records {
			if err = emit(r, rendered[i]); err != nil {
				break
			}
			f.After = r.Seq
		}
		if f.After > from && opts.Cursor != "" {
			if cerr := st.SetCursor(context.WithoutCancel(ctx), opts.Cursor, f.Drive, f.After); cerr != nil {
				return errors.Join(err, cerr)
			}
		}
		if err != nil {
			return err
		}
		if len(records) == pageRows {
			continue
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.Interval):
		}
	}
}

func WriteLines(w io.Writer) func(store.Record, string) error {
	return func(_ store.Record, line string) error {
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
