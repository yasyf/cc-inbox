package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

// Pointer names a record by its identity on every node: the node that wrote
// it and that node's seq for it.
type Pointer struct {
	Origin string `json:"origin"`
	Seq    int64  `json:"seq"`
}

// Row is a record as it travels between nodes. Its re and resolves are
// pointers; Apply rewrites them to the receiving store's seqs.
type Row struct {
	Origin    string          `json:"origin"`
	OriginSeq int64           `json:"origin_seq"`
	ID        string          `json:"id,omitempty"`
	Drive     string          `json:"drive"`
	Lane      string          `json:"lane"`
	Kind      kinds.Kind      `json:"kind"`
	At        time.Time       `json:"at"`
	Text      string          `json:"text"`
	Topic     string          `json:"topic,omitempty"`
	To        []string        `json:"to,omitempty"`
	Re        *Pointer        `json:"re,omitempty"`
	Resolves  *Pointer        `json:"resolves,omitempty"`
	Refs      Refs            `json:"refs"`
	Fields    Fields          `json:"fields"`
	ExpiresAt *time.Time      `json:"expires_at,omitempty"`
	Source    string          `json:"source"`
	Body      json.RawMessage `json:"body,omitempty"`
}

// Receipt is the last change a transport channel applied completely from
// one origin, stored in the same transaction as that change's rows.
type Receipt struct {
	Channel  string `json:"channel"`
	Origin   string `json:"origin"`
	ChangeID string `json:"change_id"`
	Revision string `json:"revision"`
	Digest   string `json:"digest"`
}

// Applied counts what one Apply did: rows inserted, rows already present,
// replies held for a target that has not arrived, and held replies released.
type Applied struct {
	Inserted  int `json:"inserted"`
	Duplicate int `json:"duplicate"`
	Buffered  int `json:"buffered"`
	Released  int `json:"released"`
}

const exportColumns = columns + ", re_origin, re_origin_seq, resolves_origin, resolves_origin_seq"

// Export returns origin's rows with origin_seq above since, oldest first,
// leaving out the digests compaction writes; limit 0 returns them all.
func (s *Store) Export(ctx context.Context, origin string, since int64, limit int) ([]Row, error) {
	q := "SELECT " + exportColumns + " FROM records WHERE origin = ? AND origin_seq > ? AND kind != ? ORDER BY origin_seq"
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.QueryContext(ctx, q, origin, since, string(kinds.Digest))
	if err != nil {
		return nil, fmt.Errorf("export %s: %w", origin, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Row
	for rows.Next() {
		var (
			re, resolves Pointer
			extra        exportScanner
		)
		extra.row, extra.tail = rows, []any{&re.Origin, &re.Seq, &resolves.Origin, &resolves.Seq}
		r, err := s.scan(extra)
		if err != nil {
			return nil, err
		}
		out = append(out, Row{
			Origin: s.origin(r.Origin), OriginSeq: originSeq(r), ID: r.ID,
			Drive: r.Drive, Lane: r.Lane, Kind: r.Kind, At: r.At, Text: r.Text, Topic: r.Topic, To: r.To,
			Re: pointer(r.Re, re), Resolves: pointer(r.Resolves, resolves),
			Refs: r.Refs, Fields: r.Fields, ExpiresAt: r.ExpiresAt, Source: r.Source, Body: r.Body,
		})
	}
	return out, rows.Err()
}

type exportScanner struct {
	row  scanner
	tail []any
}

func (e exportScanner) Scan(dest ...any) error {
	return e.row.Scan(append(dest, e.tail...)...)
}

func originSeq(r Record) int64 {
	if r.Origin == "" {
		return r.Seq
	}
	return r.OriginSeq
}

func pointer(seq int64, p Pointer) *Pointer {
	if seq == 0 {
		return nil
	}
	return &p
}

// Apply ingests rows from other nodes in one transaction. A row whose
// (origin, origin_seq) is already stored, or whose origin is this node, is a
// duplicate. A row whose re or resolves target has not arrived waits in
// pending until it does. The caller has already verified each row's
// provenance; Apply stores what it is given, keeping its time and source.
func (s *Store) Apply(ctx context.Context, rows []Row, receipt *Receipt) (Applied, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Applied{}, fmt.Errorf("begin apply: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var a Applied
	for _, row := range rows {
		if err := s.apply(ctx, tx, row, &a, false); err != nil {
			return Applied{}, err
		}
	}
	if receipt != nil {
		if _, err := tx.ExecContext(ctx, `INSERT INTO receipts (channel, origin, change_id, revision, digest) VALUES (?, ?, ?, ?, ?)
ON CONFLICT (channel, origin) DO UPDATE SET change_id = excluded.change_id, revision = excluded.revision, digest = excluded.digest`,
			receipt.Channel, receipt.Origin, receipt.ChangeID, receipt.Revision, receipt.Digest); err != nil {
			return Applied{}, fmt.Errorf("store receipt: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return Applied{}, fmt.Errorf("commit apply: %w", err)
	}
	return a, nil
}

func (s *Store) apply(ctx context.Context, tx *sql.Tx, row Row, a *Applied, released bool) error {
	if row.Origin == "" || row.OriginSeq <= 0 {
		return fmt.Errorf("apply: row has no origin identity (%q, %d)", row.Origin, row.OriginSeq)
	}
	if row.Origin == s.Self() {
		a.Duplicate++
		return nil
	}
	var known int
	if err := tx.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM records WHERE origin = ? AND origin_seq = ?) + (SELECT COUNT(*) FROM pending WHERE origin = ? AND origin_seq = ?) + (SELECT COUNT(*) FROM aliases WHERE origin = ? AND origin_seq = ?)",
		row.Origin, row.OriginSeq, row.Origin, row.OriginSeq, row.Origin, row.OriginSeq).Scan(&known); err != nil {
		return fmt.Errorf("apply %s/%d: %w", row.Origin, row.OriginSeq, err)
	}
	if known > 0 {
		a.Duplicate++
		return nil
	}
	if row.ID != "" {
		var same int64
		err := tx.QueryRowContext(ctx, "SELECT seq FROM records WHERE drive = ? AND id = ?", row.Drive, row.ID).Scan(&same)
		switch {
		case err == nil:
			if _, err := tx.ExecContext(ctx, "INSERT INTO aliases (origin, origin_seq, seq) VALUES (?, ?, ?)", row.Origin, row.OriginSeq, same); err != nil {
				return fmt.Errorf("alias %s/%d: %w", row.Origin, row.OriginSeq, err)
			}
			a.Duplicate++
			return s.release(ctx, tx, Pointer{Origin: row.Origin, Seq: row.OriginSeq}, a)
		case !errors.Is(err, sql.ErrNoRows):
			return fmt.Errorf("apply %s/%d: %w", row.Origin, row.OriginSeq, err)
		}
	}
	re, waiting, err := s.target(ctx, tx, row.Re)
	if err != nil {
		return err
	}
	resolves, waitingResolves, err := s.target(ctx, tx, row.Resolves)
	if err != nil {
		return err
	}
	if waiting == nil {
		waiting = waitingResolves
	}
	if waiting != nil {
		raw, _ := json.Marshal(row)
		if _, err := tx.ExecContext(ctx, "INSERT INTO pending (origin, origin_seq, wait_origin, wait_seq, row) VALUES (?, ?, ?, ?, ?)",
			row.Origin, row.OriginSeq, waiting.Origin, waiting.Seq, string(raw)); err != nil {
			return fmt.Errorf("buffer %s/%d: %w", row.Origin, row.OriginSeq, err)
		}
		if !released {
			a.Buffered++
		}
		return nil
	}
	r := Record{
		Origin: row.Origin, OriginSeq: row.OriginSeq, ID: row.ID,
		Drive: row.Drive, Lane: row.Lane, Kind: row.Kind, At: row.At, Text: row.Text, Topic: row.Topic, To: row.To,
		Re: re, Resolves: resolves, Refs: row.Refs, Fields: row.Fields, ExpiresAt: row.ExpiresAt, Source: row.Source, Body: row.Body,
	}
	seq, err := s.insert(ctx, tx, r, Hash(r), nil)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE records SET re_origin = ?, re_origin_seq = ?, resolves_origin = ?, resolves_origin_seq = ? WHERE seq = ?",
		pointerOrigin(row.Re), pointerSeq(row.Re), pointerOrigin(row.Resolves), pointerSeq(row.Resolves), seq); err != nil {
		return fmt.Errorf("apply pointers of %s/%d: %w", row.Origin, row.OriginSeq, err)
	}
	if released {
		a.Released++
	} else {
		a.Inserted++
	}
	return s.release(ctx, tx, Pointer{Origin: row.Origin, Seq: row.OriginSeq}, a)
}

func (s *Store) release(ctx context.Context, tx *sql.Tx, arrived Pointer, a *Applied) error {
	rows, err := tx.QueryContext(ctx, "DELETE FROM pending WHERE wait_origin = ? AND wait_seq = ? RETURNING row", arrived.Origin, arrived.Seq)
	if err != nil {
		return fmt.Errorf("release replies to %s/%d: %w", arrived.Origin, arrived.Seq, err)
	}
	var held []Row
	for rows.Next() {
		var raw string
		if err := rows.Scan(&raw); err != nil {
			_ = rows.Close()
			return fmt.Errorf("release replies to %s/%d: %w", arrived.Origin, arrived.Seq, err)
		}
		var row Row
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			_ = rows.Close()
			return fmt.Errorf("decode held reply: %w", err)
		}
		held = append(held, row)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("release replies to %s/%d: %w", arrived.Origin, arrived.Seq, err)
	}
	for _, row := range held {
		if err := s.apply(ctx, tx, row, a, true); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) target(ctx context.Context, tx *sql.Tx, p *Pointer) (int64, *Pointer, error) {
	if p == nil {
		return 0, nil, nil
	}
	if p.Origin == s.Self() {
		return p.Seq, nil, nil
	}
	var seq int64
	err := tx.QueryRowContext(ctx, "SELECT seq FROM records WHERE origin = ? AND origin_seq = ? UNION ALL SELECT seq FROM aliases WHERE origin = ? AND origin_seq = ? LIMIT 1", p.Origin, p.Seq, p.Origin, p.Seq).Scan(&seq)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, p, nil
	case err != nil:
		return 0, nil, fmt.Errorf("resolve %s/%d: %w", p.Origin, p.Seq, err)
	}
	return seq, nil, nil
}

func pointerOrigin(p *Pointer) string {
	if p == nil {
		return ""
	}
	return p.Origin
}

func pointerSeq(p *Pointer) int64 {
	if p == nil {
		return 0
	}
	return p.Seq
}

// Receipt reads the receipt channel last stored for origin.
func (s *Store) Receipt(ctx context.Context, channel, origin string) (Receipt, bool, error) {
	r := Receipt{Channel: channel, Origin: origin}
	err := s.db.QueryRowContext(ctx, "SELECT change_id, revision, digest FROM receipts WHERE channel = ? AND origin = ?", channel, origin).Scan(&r.ChangeID, &r.Revision, &r.Digest)
	if errors.Is(err, sql.ErrNoRows) {
		return Receipt{}, false, nil
	}
	if err != nil {
		return Receipt{}, false, fmt.Errorf("read receipt %s/%s: %w", channel, origin, err)
	}
	return r, true, nil
}

// Ack records that peer holds this node's rows through originSeq. A
// watermark only rises.
func (s *Store) Ack(ctx context.Context, peer string, originSeq int64) error {
	if _, err := s.db.ExecContext(ctx, `INSERT INTO watermarks (peer, origin_seq) VALUES (?, ?)
ON CONFLICT (peer) DO UPDATE SET origin_seq = MAX(origin_seq, excluded.origin_seq)`, peer, originSeq); err != nil {
		return fmt.Errorf("ack %s: %w", peer, err)
	}
	return nil
}

// Watermarks maps each peer to the highest of this node's origin_seqs it acked.
func (s *Store) Watermarks(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT peer, origin_seq FROM watermarks")
	if err != nil {
		return nil, fmt.Errorf("read watermarks: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int64{}
	for rows.Next() {
		var (
			peer string
			seq  int64
		)
		if err := rows.Scan(&peer, &seq); err != nil {
			return nil, fmt.Errorf("scan watermark: %w", err)
		}
		out[peer] = seq
	}
	return out, rows.Err()
}

// ForgetPeer drops what this node holds for a peer that left: its watermark,
// so compaction no longer waits on it, and its replicated sessions and claims.
func (s *Store) ForgetPeer(ctx context.Context, peer string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("forget %s: %w", peer, err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := forget(ctx, tx, peer); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("forget %s: %w", peer, err)
	}
	return nil
}

func forget(ctx context.Context, tx *sql.Tx, peer string) error {
	for _, q := range []string{"DELETE FROM watermarks WHERE peer = ?", "DELETE FROM sessions WHERE origin = ?", "DELETE FROM claims WHERE origin = ?"} {
		if _, err := tx.ExecContext(ctx, q, peer); err != nil {
			return fmt.Errorf("forget %s: %w", peer, err)
		}
	}
	return nil
}

// CompactedThrough is the highest of this node's origin_seqs compaction has
// folded; an Export from below it cannot be complete, so a peer that far
// behind needs a snapshot.
func (s *Store) CompactedThrough(ctx context.Context) (int64, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key = 'compacted_through'").Scan(&seq)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("read compacted-through: %w", err)
	}
	return seq, nil
}

func (s *Store) compactionFloor(ctx context.Context) (int64, bool, error) {
	var floor sql.NullInt64
	if err := s.db.QueryRowContext(ctx, "SELECT MIN(origin_seq) FROM watermarks").Scan(&floor); err != nil {
		return 0, false, fmt.Errorf("read compaction floor: %w", err)
	}
	return floor.Int64, floor.Valid, nil
}
