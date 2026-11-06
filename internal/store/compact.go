package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func (s *Store) OpenItems(ctx context.Context, drive string) ([]Record, error) {
	return s.open(ctx, drive, Record.Tracked)
}

// OpenTo returns, in seq order, the tracked openers of ks addressed to reader
// that no closer or resolver has closed, less what the thread k bound as
// reader holds as backfill: a record ApplyAll stored after k bound that is
// stamped before k started.
func (s *Store) OpenTo(ctx context.Context, k PeerKey, drive, reader string, ks []kinds.Kind) ([]Record, error) {
	openers, err := s.Query(ctx, Filter{Drive: drive, To: reader, LaneFloor: BroadcastCursor(k), Kinds: ks, IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	return s.unclosedAmong(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() || !slices.Contains(ks, o.Kind) }))
}

func (s *Store) UntrackedHolds(ctx context.Context, drive string) ([]Record, error) {
	return s.open(ctx, drive, func(r Record) bool { return r.Kind == kinds.Hold && !r.Tracked() })
}

// OpenAmong returns, in seq order, the tracked openers among seqs that sit in
// drive and that no closer or resolver has closed. It rereads those openers
// and the drive's closers and resolvers, so a seq that a reparse took out of
// the opener kinds or out of tracking, or that compaction folded, is not
// returned. seqs travels as one JSON array parameter, so its length is not
// bounded by SQLite's limit on bound variables.
func (s *Store) OpenAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	if m := s.reads.model.Load(); m != nil {
		if open, ok := m.openRecordsAmong(ctx, s, drive, seqs); ok {
			return open, nil
		}
	}
	openers, err := s.openersAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	return s.unclosedAmong(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() }))
}

// ReplacedAmong returns the seqs among seqs of a kind whose newest record on a
// topic replaces the earlier ones, that a later record of the same drive, lane,
// kind, topic and recipients has replaced. A record with no topic is never
// replaced.
func (s *Store) ReplacedAmong(ctx context.Context, drive string, seqs []int64) (map[int64]bool, error) {
	latest := kinds.Latests()
	ids, _ := json.Marshal(seqs)
	params := make([]any, 0, 2+len(latest))
	params = append(params, drive, string(ids))
	for _, k := range latest {
		params = append(params, string(k))
	}
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq FROM records r WHERE r.drive = ? AND r.seq IN (SELECT value FROM json_each(?)) AND r.topic != '' AND r.kind IN (`+placeholders(len(latest))+`)
AND EXISTS (SELECT 1 FROM records l WHERE l.drive = r.drive AND l.kind = r.kind AND l.seq > r.seq AND l.lane = r.lane AND l.topic = r.topic AND l.recipients = r.recipients)`, params...)
	if err != nil {
		return nil, fmt.Errorf("read replaced records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	replaced := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		replaced[seq] = true
	}
	return replaced, rows.Err()
}

// OpenSeqsAmong is the seqs of OpenAmong's answer, for a reader that needs no
// more than which are open.
func (s *Store) OpenSeqsAmong(ctx context.Context, drive string, seqs []int64) ([]int64, error) {
	if m := s.reads.model.Load(); m != nil {
		if open, ok := m.openAmong(ctx, s, drive, seqs); ok {
			return open, nil
		}
	}
	open, err := s.OpenAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	out := make([]int64, len(open))
	for i, o := range open {
		out[i] = o.Seq
	}
	return out, nil
}

func (s *Store) openersAmong(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	openers := kinds.Openers()
	ids, _ := json.Marshal(seqs)
	params := make([]any, 0, 2+len(openers))
	params = append(params, drive, string(ids))
	for _, k := range openers {
		params = append(params, string(k))
	}
	rows, err := s.db.QueryContext(ctx, "SELECT "+columns+" FROM records WHERE drive = ? AND seq IN (SELECT value FROM json_each(?)) AND kind IN ("+placeholders(len(openers))+") ORDER BY seq", params...)
	if err != nil {
		return nil, fmt.Errorf("read openers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) unclosed(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	if m := s.reads.model.Load(); m != nil {
		if open, ok := m.unclosed(ctx, s, drive, openers); ok {
			return open, nil
		}
	}
	openers, err := s.withoutIncidentUpdates(ctx, drive, openers)
	if err != nil {
		return nil, err
	}
	closers, err := s.closureRows(ctx, drive, kinds.Closers())
	if err != nil {
		return nil, err
	}
	resolved, err := s.Resolvers(ctx, drive)
	if err != nil {
		return nil, err
	}
	ix := indexClosers(closers)
	var open []Record
	for _, o := range openers {
		if by, ok := resolved[o.Seq]; ok && by > o.Seq {
			continue
		}
		if !ix.closed(o) {
			open = append(open, o)
		}
	}
	return s.heardAsks(ctx, s.db, drive, open)
}

func (s *Store) withoutIncidentUpdates(ctx context.Context, drive string, openers []Record) ([]Record, error) {
	var targets []int64
	for _, o := range openers {
		if o.Kind == kinds.Incident {
			targets = append(targets, incidentTargets(o)...)
		}
	}
	if len(targets) == 0 {
		return openers, nil
	}
	named, err := s.openersAmong(ctx, drive, targets)
	if err != nil {
		return nil, err
	}
	incidents := map[int64]bool{}
	for _, r := range named {
		incidents[r.Seq] = r.Kind == kinds.Incident
	}
	return slices.DeleteFunc(openers, func(o Record) bool {
		return o.Kind == kinds.Incident && slices.ContainsFunc(incidentTargets(o), func(seq int64) bool { return incidents[seq] })
	}), nil
}

func incidentTargets(r Record) []int64 {
	return slices.DeleteFunc(append([]int64{r.Re, r.Resolves}, r.AlsoResolves...), func(seq int64) bool { return seq == 0 })
}

func (s *Store) open(ctx context.Context, drive string, keep func(Record) bool) ([]Record, error) {
	openers, err := s.servedClosureRows(ctx, drive, kinds.Openers())
	if err != nil {
		return nil, err
	}
	open, err := s.unclosed(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !keep(o) }))
	if err != nil {
		return nil, err
	}
	seqs := make([]int64, len(open))
	for i, o := range open {
		seqs[i] = o.Seq
	}
	return s.RecordsAmong(ctx, drive, seqs)
}

// OpenSince is what a digest shows of drive's open items: the whole records
// opened since since, tracked and untracked holds apart, and how many tracked
// ones are older, read from one pass over the drive's openers and closers.
func (s *Store) OpenSince(ctx context.Context, drive string, since time.Time) (open, untracked []Record, older int, err error) {
	openers, err := s.servedClosureRows(ctx, drive, kinds.Openers())
	if err != nil {
		return nil, nil, 0, err
	}
	unclosed, err := s.unclosed(ctx, drive, slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() && o.Kind != kinds.Hold }))
	if err != nil {
		return nil, nil, 0, err
	}
	var seqs []int64
	for _, o := range unclosed {
		switch {
		case !o.At.Before(since):
			seqs = append(seqs, o.Seq)
		case o.Tracked():
			older++
		}
	}
	recent, err := s.RecordsAmong(ctx, drive, seqs)
	if err != nil {
		return nil, nil, 0, err
	}
	for _, r := range recent {
		if r.Tracked() {
			open = append(open, r)
		} else {
			untracked = append(untracked, r)
		}
	}
	return open, untracked, older, nil
}

const closureColumns = "seq, lane, kind, at, topic, re, resolves, COALESCE(also_resolves, ''), refs, source"

// closureRows reads drive's records of ks in seq order, with only the columns
// that open or close an item, for reads that touch every opener and closer.
func (s *Store) servedClosureRows(ctx context.Context, drive string, ks []kinds.Kind) ([]Record, error) {
	if m := s.reads.model.Load(); m != nil {
		if rows, ok := m.closureRows(ctx, drive, ks); ok {
			return rows, nil
		}
	}
	return s.closureRows(ctx, drive, ks)
}

func (s *Store) closureRows(ctx context.Context, drive string, ks []kinds.Kind) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT "+closureColumns+" FROM records WHERE drive = ? AND kind IN ("+placeholders(len(ks))+") ORDER BY seq", append([]any{drive}, kindArgs(ks)...)...)
	if err != nil {
		return nil, fmt.Errorf("read closure rows: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		var (
			r               Record
			kind, also, ref string
			at              int64
		)
		if err := rows.Scan(&r.Seq, &r.Lane, &kind, &at, &r.Topic, &r.Re, &r.Resolves, &also, &ref, &r.Source); err != nil {
			return nil, fmt.Errorf("scan closure row: %w", err)
		}
		r.Kind = kinds.Kind(kind)
		r.At = time.UnixMilli(at).UTC()
		if also != "" {
			if err := json.Unmarshal([]byte(also), &r.AlsoResolves); err != nil {
				return nil, fmt.Errorf("decode also_resolves of #%d: %w", r.Seq, err)
			}
		}
		if strings.Contains(ref, `"stacks"`) || strings.Contains(ref, `"targets"`) {
			if err := json.Unmarshal([]byte(ref), &r.Refs); err != nil {
				return nil, fmt.Errorf("decode refs of #%d: %w", r.Seq, err)
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type closerIndex struct {
	byRe              seqBuckets[int64]
	byTopic           seqBuckets[string]
	byStack           stampBuckets
	byTarget          stampBuckets
	byUnstackedTarget stampBuckets
}

func indexClosers(closers []Record) closerIndex {
	ix := closerIndex{
		byRe:              seqBuckets[int64]{},
		byTopic:           seqBuckets[string]{},
		byStack:           stampBuckets{},
		byTarget:          stampBuckets{},
		byUnstackedTarget: stampBuckets{},
	}
	for _, c := range closers {
		ix.byRe.add(c.Re, closer{kind: c.Kind}, c.Seq)
		paired := closer{kind: c.Kind, replyLane: replyLane(c)}
		if c.Topic != "" {
			ix.byTopic.add(c.Topic, paired, c.Seq)
		}
		for _, stack := range c.Refs.Stacks {
			ix.byStack.add(stack, paired, c)
		}
		for _, target := range c.Refs.Targets {
			ix.byTarget.add(target, paired, c)
			if len(c.Refs.Stacks) == 0 {
				ix.byUnstackedTarget.add(target, paired, c)
			}
		}
	}
	return ix
}

type closer struct {
	kind      kinds.Kind
	replyLane string
}

func replyLane(c Record) string {
	if c.Kind == kinds.Answer || c.Kind == kinds.Go {
		return c.Lane
	}
	return ""
}

func (c closer) pairs(opener Record) bool {
	return c.kind.Closes(opener.Kind) && (c.replyLane == "" || !SameLane(c.replyLane, opener.Lane))
}

func (ix closerIndex) closed(opener Record) bool {
	if ix.byRe.closes(opener.Seq, opener) || ix.byTopic.closes(opener.Topic, opener) {
		return true
	}
	byTarget := ix.byTarget
	if len(opener.Refs.Stacks) > 0 {
		if ix.byStack.closes(opener.Refs.Stacks, opener) {
			return true
		}
		byTarget = ix.byUnstackedTarget
	}
	return byTarget.closes(opener.Refs.Targets, opener)
}

type seqBuckets[K comparable] map[K]map[closer]int64

func (b seqBuckets[K]) add(key K, c closer, seq int64) {
	latest, ok := b[key]
	if !ok {
		latest = map[closer]int64{}
		b[key] = latest
	}
	if cur, ok := latest[c]; !ok || seq > cur {
		latest[c] = seq
	}
}

func (b seqBuckets[K]) closes(key K, opener Record) bool {
	for c, seq := range b[key] {
		if seq > opener.Seq && c.pairs(opener) {
			return true
		}
	}
	return false
}

type stamp struct {
	at  time.Time
	seq int64
}

func (a stamp) after(b stamp) bool {
	return a.at.After(b.at) || a.at.Equal(b.at) && a.seq > b.seq
}

type stampBuckets map[string]map[closer]stamp

func (b stampBuckets) add(key string, c closer, r Record) {
	latest, ok := b[key]
	if !ok {
		latest = map[closer]stamp{}
		b[key] = latest
	}
	at := stamp{r.At, r.Seq}
	if cur, ok := latest[c]; !ok || at.after(cur) {
		latest[c] = at
	}
}

func (b stampBuckets) closes(keys []string, opener Record) bool {
	since := stamp{opener.At, opener.Seq}
	for _, key := range keys {
		for c, at := range b[key] {
			if at.after(since) && pairsByRef(c, opener) {
				return true
			}
		}
	}
	return false
}

func pairsByRef(c closer, opener Record) bool {
	return c.pairs(opener) || c.kind.ClosesByRef() && strings.HasPrefix(opener.Source, "import:")
}

type CompactResult struct {
	Folded  int `json:"folded"`
	Digests int `json:"digests"`
}

func (s *Store) Compact(ctx context.Context, drive string, keep time.Duration) (CompactResult, error) {
	cutoff := s.Now().Add(-keep)
	open, err := s.OpenItems(ctx, drive)
	if err != nil {
		return CompactResult{}, err
	}
	keepSeq := map[int64]bool{}
	for _, o := range open {
		keepSeq[o.Seq] = true
	}
	old, err := s.Query(ctx, Filter{Drive: drive, Until: cutoff, IncludeExpired: true})
	if err != nil {
		return CompactResult{}, err
	}
	floor, held, err := s.compactionFloor(ctx)
	if err != nil {
		return CompactResult{}, err
	}
	elsewhere, err := s.foreignResolvers(ctx, drive)
	if err != nil {
		return CompactResult{}, err
	}
	for _, r := range old {
		if held && r.Origin == "" && r.Seq > floor {
			keepSeq[r.Seq] = true
		}
	}
	days := map[string][]Record{}
	for _, r := range old {
		if r.Kind == kinds.Digest || keepSeq[r.Seq] || elsewhere[r.Seq] || keepSeq[r.Re] || slices.ContainsFunc(r.Closes(), func(seq int64) bool { return keepSeq[seq] }) {
			continue
		}
		day := r.At.In(time.Local).Format(time.DateOnly)
		days[day] = append(days[day], r)
	}
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return CompactResult{}, fmt.Errorf("begin compact: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	var res CompactResult
	for _, day := range slices.Sorted(maps.Keys(days)) {
		if err := s.fold(ctx, tx, drive, day, days[day]); err != nil {
			return CompactResult{}, err
		}
		res.Folded += len(days[day])
		res.Digests++
	}
	if err := tx.Commit(); err != nil {
		return CompactResult{}, fmt.Errorf("commit compact: %w", err)
	}
	return res, nil
}

func (s *Store) foreignResolvers(ctx context.Context, drive string) (map[int64]bool, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq FROM records r JOIN records t ON t.seq = r.resolves WHERE r.drive = ? AND t.drive != ?
UNION SELECT r.seq FROM records r, json_each(r.also_resolves) j JOIN records t ON t.seq = j.value WHERE r.drive = ? AND t.drive != ?`, drive, drive, drive, drive)
	if err != nil {
		return nil, fmt.Errorf("read cross-drive resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, fmt.Errorf("scan cross-drive resolver: %w", err)
		}
		out[seq] = true
	}
	return out, rows.Err()
}

func (s *Store) fold(ctx context.Context, tx *sql.Tx, drive, day string, records []Record) error {
	topic := "digest:" + day
	counts := map[string]int{}
	var existing int64
	var raw string
	err := tx.QueryRowContext(ctx, "SELECT seq, fields FROM records WHERE drive = ? AND kind = ? AND topic = ?", drive, string(kinds.Digest), topic).Scan(&existing, &raw)
	switch {
	case err == nil:
		var f Fields
		if err := json.Unmarshal([]byte(raw), &f); err != nil {
			return fmt.Errorf("decode digest %s: %w", topic, err)
		}
		maps.Copy(counts, f.Counts)
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("read digest %s: %w", topic, err)
	}
	last := records[len(records)-1].At
	for _, r := range records {
		counts["kind:"+string(r.Kind)]++
		counts["lane:"+r.Lane]++
		counts["total"]++
	}
	d := Record{
		Drive:  drive,
		Lane:   "cci",
		Kind:   kinds.Digest,
		At:     last,
		Text:   digestText(day, counts),
		Topic:  topic,
		Fields: Fields{Counts: counts},
		Source: "compact",
	}
	if existing != 0 {
		fields, _ := json.Marshal(d.Fields)
		if _, err := tx.ExecContext(ctx, "UPDATE records SET text = ?, fields = ?, at = MAX(at, ?) WHERE seq = ?", d.Text, string(fields), last.UnixMilli(), existing); err != nil {
			return fmt.Errorf("update digest %s: %w", topic, err)
		}
	} else if _, err := s.insert(ctx, tx, d, Hash(d), nil); err != nil {
		return err
	}
	seqs := make([]any, len(records))
	var through int64
	for i, r := range records {
		seqs[i] = r.Seq
		if r.Origin == "" {
			through = max(through, r.Seq)
			continue
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO folded (origin, origin_seq) VALUES (?, ?) ON CONFLICT DO NOTHING", r.Origin, r.OriginSeq); err != nil {
			return fmt.Errorf("record folded %s/%d: %w", r.Origin, r.OriginSeq, err)
		}
	}
	if through > 0 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES ('compacted_through', ?)
ON CONFLICT (key) DO UPDATE SET value = MAX(value, excluded.value)`, through); err != nil {
			return fmt.Errorf("record compacted-through: %w", err)
		}
	}
	for chunk := range slices.Chunk(seqs, 500) {
		if _, err := tx.ExecContext(ctx, "DELETE FROM records WHERE seq IN ("+placeholders(len(chunk))+")", chunk...); err != nil {
			return fmt.Errorf("delete folded records: %w", err)
		}
	}
	return countRecordsDeleted(ctx, tx)
}

func digestText(day string, counts map[string]int) string {
	return Clip(fmt.Sprintf("%s: %d records. kinds: %s. lanes: %s", day, counts["total"], top(counts, "kind:", 8), top(counts, "lane:", 8)))
}

func top(counts map[string]int, prefix string, n int) string {
	type kv struct {
		k string
		v int
	}
	var all []kv
	for k, v := range counts {
		if name, ok := strings.CutPrefix(k, prefix); ok {
			all = append(all, kv{name, v})
		}
	}
	slices.SortFunc(all, func(a, b kv) int {
		return cmp.Or(cmp.Compare(b.v, a.v), cmp.Compare(a.k, b.k))
	})
	parts := make([]string, 0, n)
	for i, e := range all {
		if i == n {
			parts = append(parts, fmt.Sprintf("+%d more", len(all)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%s %d", e.k, e.v))
	}
	return strings.Join(parts, ", ")
}
