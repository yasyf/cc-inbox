package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	embedPageRows = 500
	embedSlow     = 50 * time.Millisecond
)

// EmbedCall is one read an embedder asks of a store.
type EmbedCall struct {
	Op     string    `json:"op"`
	Drive  string    `json:"drive,omitempty"`
	After  int64     `json:"after,omitempty"`
	Since  time.Time `json:"since,omitzero"`
	Kinds  []string  `json:"kinds,omitempty"`
	Lanes  []string  `json:"lanes,omitempty"`
	PR     string    `json:"pr,omitempty"`
	Seqs   []int64   `json:"seqs,omitempty"`
	Name   string    `json:"name,omitempty"`
	Reader string    `json:"reader,omitempty"`
}

// EmbedRecord carries what a record's JSON leaves out, so an embedder's record
// names the records it answers as the store's does.
type EmbedRecord struct {
	store.Record
	ReNum     string   `json:"re_num,omitempty"`
	CloseNums []string `json:"close_nums,omitempty"`
}

// EmbedRecords wraps records for the wire.
func EmbedRecords(records []store.Record) []EmbedRecord {
	if records == nil {
		return nil
	}
	out := make([]EmbedRecord, len(records))
	for i, r := range records {
		out[i] = EmbedRecord{Record: r, ReNum: r.ReNum, CloseNums: r.CloseNums}
	}
	return out
}

// Unwrapped is the record an embedder gets.
func (e EmbedRecord) Unwrapped() store.Record {
	r := e.Record
	r.ReNum, r.CloseNums = e.ReNum, e.CloseNums
	return r
}

// EmbedRecordsFrom reads the records c selects, oldest first, with Status set on
// each opening record as cci tail --json sets it.
func EmbedRecordsFrom(ctx context.Context, st *store.Store, c EmbedCall) ([]store.Record, error) {
	switch c.Op {
	case "tail":
		filter, err := embedTailFilter(ctx, st, c)
		if err != nil {
			return nil, err
		}
		var records []store.Record
		for {
			page, err := st.Query(ctx, filter)
			if err != nil {
				return nil, err
			}
			records = append(records, page...)
			if len(page) < embedPageRows {
				return records, inbox.Annotate(ctx, st, records)
			}
			filter.After = page[len(page)-1].Seq
		}
	case "among":
		records, err := st.RecordsAmong(ctx, c.Drive, c.Seqs)
		if err != nil {
			return nil, err
		}
		return records, inbox.Annotate(ctx, st, records)
	case "open":
		selected, err := kinds.ParseAll(c.Kinds)
		if err != nil {
			return nil, err
		}
		openers, err := st.Query(ctx, store.Filter{Drive: c.Drive, Kinds: selected, IncludeExpired: true})
		if err != nil {
			return nil, err
		}
		seqs := make([]int64, 0, len(openers))
		named := make(map[int64]store.Record, len(openers))
		for _, r := range openers {
			seqs = append(seqs, r.Seq)
			named[r.Seq] = r
		}
		open, err := st.OpenAmong(ctx, c.Drive, seqs)
		for i, r := range open {
			open[i].Num, open[i].ReNum, open[i].CloseNums = named[r.Seq].Num, named[r.Seq].ReNum, named[r.Seq].CloseNums
		}
		return open, err
	}
	return nil, fmt.Errorf("no embedded read %q", c.Op)
}

func embedAnswer(ctx context.Context, st *store.Store, c EmbedCall) (any, error) {
	switch c.Op {
	case "reach":
		return st.Reach(ctx, c.Drive, c.Name)
	case "generations":
		return st.Generations(ctx, c.Drive, c.Name)
	case "head":
		return st.MaxSeq(ctx)
	}
	records, err := EmbedRecordsFrom(ctx, st, c)
	if err != nil {
		return nil, err
	}
	return EmbedRecords(records), nil
}

type embedWriter struct {
	out   io.Writer
	wrote bool
	line  bytes.Buffer
}

func (w *embedWriter) records(records []store.Record) error {
	for _, r := range records {
		w.line.Reset()
		if w.wrote {
			w.line.WriteByte(',')
		} else {
			w.line.WriteByte('[')
		}
		w.wrote = true
		encoded, err := json.Marshal(EmbedRecord{Record: r, ReNum: r.ReNum, CloseNums: r.CloseNums})
		if err != nil {
			return err
		}
		w.line.Write(encoded)
		if _, err := w.out.Write(w.line.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

func (w *embedWriter) close() error {
	closing := "]\n"
	if !w.wrote {
		closing = "null\n"
	}
	_, err := io.WriteString(w.out, closing)
	return err
}

// embedTailTo writes what embedAnswer answers for a tail, a page of records at a
// time, so a long answer holds one page in memory rather than the whole answer.
func embedTailTo(ctx context.Context, st *store.Store, c EmbedCall, out io.Writer) error {
	filter, err := embedTailFilter(ctx, st, c)
	if err != nil {
		return err
	}
	writer := embedWriter{out: out}
	for {
		page, err := st.Query(ctx, filter)
		if err != nil {
			return err
		}
		if err := inbox.Annotate(ctx, st, page); err != nil {
			return err
		}
		if err := writer.records(page); err != nil {
			return err
		}
		if len(page) < embedPageRows {
			return writer.close()
		}
		filter.After = page[len(page)-1].Seq
	}
}

func embedTailFilter(ctx context.Context, st *store.Store, c EmbedCall) (store.Filter, error) {
	selected, err := kinds.ParseAll(c.Kinds)
	if err != nil {
		return store.Filter{}, err
	}
	var pr store.PR
	if c.PR != "" {
		if pr, err = store.ParsePR(c.PR); err != nil {
			return store.Filter{}, err
		}
	}
	return st.BoundSince(ctx, store.Filter{Drive: c.Drive, Kinds: selected, Lanes: c.Lanes, PR: pr, After: c.After, Since: c.Since, Limit: embedPageRows})
}

// EmbedFollow calls emit with each record c selects as the store takes it.
func EmbedFollow(ctx context.Context, st *store.Store, c EmbedCall, peer string, emit func(store.Record) error) error {
	selected, err := kinds.ParseAll(c.Kinds)
	if err != nil {
		return err
	}
	filter := store.Filter{Drive: c.Drive, For: c.Reader, Peer: peer, Kinds: selected, After: c.After}
	return inbox.Watch(ctx, st, inbox.WatchOptions{Filter: filter, Interval: time.Second, JSON: true}, func(r store.Record, _ string) error {
		return emit(r)
	})
}

func embedderPID(ctx context.Context) int {
	if inv := invoked(ctx); inv != nil {
		return inv.pid
	}
	return os.Getpid()
}

func newEmbedCmd() *cobra.Command {
	read := func(in io.Reader) (EmbedCall, error) {
		var c EmbedCall
		if err := json.NewDecoder(in).Decode(&c); err != nil {
			return c, fmt.Errorf("read an embedded call: %w", err)
		}
		return c, nil
	}
	return &cobra.Command{
		Use:    "embed",
		Short:  "Answer one call of the client package",
		Hidden: true,
		Args:   cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			c, err := read(cmd.InOrStdin())
			if err != nil {
				return err
			}
			captureEmbedded(cmd.Context(), st.HomeDir(), args[0], c)
			out := json.NewEncoder(cmd.OutOrStdout())
			if args[0] == "follow" {
				peer := SelfPeer(cmd.Context()).String()
				return EmbedFollow(cmd.Context(), st, c, peer, func(r store.Record) error {
					return out.Encode(EmbedRecord{Record: r, ReNum: r.ReNum, CloseNums: r.CloseNums})
				})
			}
			started := time.Now()
			var answer any
			if c.Op == "tail" {
				err = embedTailTo(cmd.Context(), st, c, cmd.OutOrStdout())
			} else {
				answer, err = embedAnswer(cmd.Context(), st, c)
			}
			if err != nil {
				return err
			}
			if took := time.Since(started); took >= embedSlow {
				slog.Info("slow embedded call", "op", c.Op, "pid", embedderPID(cmd.Context()), "kinds", c.Kinds, "lanes", c.Lanes, "after", c.After, "since", c.Since, "pr", c.PR, "seqs", len(c.Seqs), "took", took)
			}
			if c.Op == "tail" {
				return nil
			}
			return out.Encode(answer)
		}),
	}
}
