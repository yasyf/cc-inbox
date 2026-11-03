package store

import (
	"context"
	"encoding/json"
	"fmt"
)

// Unstored returns rows this store does not hold as the records it would print
// for them, numbered the way every node numbers them, with each body written
// to this node's blobs. They take seqs below 1 in the order given, and a re or
// resolves naming another of the rows points at that row's seq.
func (s *Store) Unstored(ctx context.Context, rows []Row) ([]Record, error) {
	owners, err := s.owners(ctx)
	if err != nil {
		return nil, err
	}
	seqs := make(map[Pointer]int64, len(rows))
	for i, row := range rows {
		seqs[Pointer{Origin: row.Origin, Seq: row.OriginSeq}] = int64(i - len(rows))
	}
	name := func(p Pointer) string { return number(owners, p.Origin, p.Seq) }
	out := make([]Record, len(rows))
	for i, row := range rows {
		refs := row.Refs
		if row.PathBody != "" {
			if refs.Path, err = s.Blob(row.PathBody); err != nil {
				return nil, fmt.Errorf("unstored %s/%d: %w", row.Origin, row.OriginSeq, err)
			}
		}
		r := Record{
			Seq: int64(i - len(rows)), Origin: s.local(row.Origin), OriginSeq: row.OriginSeq, ID: row.ID,
			Drive: row.Drive, Lane: row.Lane, Kind: row.Kind, At: row.At, Text: row.Text, Topic: row.Topic, To: row.To,
			Refs: refs, Fields: row.Fields, ExpiresAt: row.ExpiresAt, Source: row.Source, Body: row.Body,
			Num: name(Pointer{Origin: row.Origin, Seq: row.OriginSeq}),
		}
		if row.Re != nil {
			r.Re, r.ReNum = seqs[*row.Re], name(*row.Re)
		}
		if row.Resolves != nil {
			r.Resolves, r.CloseNums = seqs[*row.Resolves], []string{name(*row.Resolves)}
			for _, p := range row.AlsoResolves {
				r.AlsoResolves, r.CloseNums = append(r.AlsoResolves, seqs[p]), append(r.CloseNums, name(p))
			}
		}
		out[i] = r
	}
	return out, nil
}

// Oldest returns the lowest origin_seq among origin's records this store holds
// on drive, or 0 when it holds none.
func (s *Store) Oldest(ctx context.Context, drive, origin string) (int64, error) {
	var seq int64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MIN(origin_seq), 0) FROM records WHERE origin = ? AND drive = ?", origin, drive).Scan(&seq); err != nil {
		return 0, fmt.Errorf("oldest record of %s on %s: %w", origin, drive, err)
	}
	return seq, nil
}

const unstoredRecords = `WITH records AS (SELECT value->>'seq' AS seq, value->>'drive' AS drive, value->>'lane' AS lane, value->>'kind' AS kind,
value->>'at' AS at, value->>'topic' AS topic, value->'recipients' AS recipients, value->>'re' AS re, value->'refs' AS refs,
value->>'expires_at' AS expires_at, value->>'source' AS source, value->>'origin' AS origin FROM json_each(?))
SELECT seq FROM records WHERE `

// Selected returns the records among unstored, which Unstored made, that f
// selects, in order, by the same predicate Query applies to stored records.
func (s *Store) Selected(ctx context.Context, f Filter, unstored []Record) ([]Record, error) {
	type row struct {
		Seq        int64    `json:"seq"`
		Drive      string   `json:"drive"`
		Lane       string   `json:"lane"`
		Kind       string   `json:"kind"`
		At         int64    `json:"at"`
		Topic      string   `json:"topic"`
		Recipients []string `json:"recipients"`
		Re         int64    `json:"re"`
		Refs       Refs     `json:"refs"`
		ExpiresAt  *int64   `json:"expires_at"`
		Source     string   `json:"source"`
		Origin     string   `json:"origin"`
	}
	rows := make([]row, len(unstored))
	for i, r := range unstored {
		rows[i] = row{
			Seq: r.Seq, Drive: r.Drive, Lane: r.Lane, Kind: string(r.Kind), At: r.At.UnixMilli(), Topic: r.Topic,
			Recipients: append([]string{}, r.To...), Re: r.Re, Refs: r.Refs, Source: r.Source, Origin: r.Origin,
		}
		if r.ExpiresAt != nil {
			ms := r.ExpiresAt.UnixMilli()
			rows[i].ExpiresAt = &ms
		}
	}
	raw, err := json.Marshal(rows)
	if err != nil {
		return nil, fmt.Errorf("encode unstored records: %w", err)
	}
	f, err = s.aliased(ctx, f)
	if err != nil {
		return nil, err
	}
	where, params := f.where(s.Self(), s.Now())
	selected, err := s.db.QueryContext(ctx, unstoredRecords+where, append([]any{string(raw)}, params...)...)
	if err != nil {
		return nil, fmt.Errorf("select unstored records: %w", err)
	}
	defer func() { _ = selected.Close() }()
	keep := map[int64]bool{}
	for selected.Next() {
		var seq int64
		if err := selected.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan unstored record: %w", err)
		}
		keep[seq] = true
	}
	if err := selected.Err(); err != nil {
		return nil, err
	}
	var out []Record
	for _, r := range unstored {
		if keep[r.Seq] {
			out = append(out, r)
		}
	}
	return out, nil
}
