package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	readModelFile = "model.on"
	// ReadModelBytes caps what a read model holds: every record's selection
	// columns, and the whole text of the newest records that fit.
	ReadModelBytes     = 150 << 20
	readModelSpan      = 36 * time.Hour
	readModelBatch     = 1000
	readModelRowBytes  = 300
	readModelTextBytes = 800
	readModelTrimRows  = 1000
	readModelTrimAge   = 10 * time.Minute
	readModelMetaHeld  = 1 << 10
	modelNoExpiry      = math.MinInt64
	readModelColumns   = "seq, origin, origin_seq, COALESCE(id, ''), drive, lane, kind, at, text, topic, recipients, re, resolves, refs, fields, expires_at, source, COALESCE(body, ''), COALESCE(also_resolves, ''), re_origin_seq, resolves_origin_seq"
)

var (
	modelClosers = kindSet(kinds.Closers())
	modelOpeners = kindSet(kinds.Openers())
)

var errReadModelStale = errors.New("a stored record changed under the read model")

// ReadModelOn reports whether home's switch asks the daemon for a read model.
func ReadModelOn(home string) bool {
	_, err := os.Stat(filepath.Join(home, readModelFile))
	return err == nil
}

// SetReadModel flips home's switch; a running daemon follows within a second.
func SetReadModel(home string, on bool) error {
	path := filepath.Join(home, readModelFile)
	if !on {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("turn the read model off: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		return fmt.Errorf("turn the read model on: %w", err)
	}
	return nil
}

type modelAddressees struct {
	raw   string
	to    []string
	all   bool
	wrong bool
}

type modelRefs struct {
	ints    []int64
	texts   []string
	stacks  []string
	targets []string
	wrong   bool
}

type modelText struct {
	id, text, refs, fields, body, also string
	reOriginSeq, resolvesOriginSeq     sql.NullInt64
	meta                               atomic.Pointer[modelMeta]
}

type modelMeta struct {
	refs   Refs
	fields Fields
}

type modelRow struct {
	seq, at, expires, re, resolves, originSeq int64
	drive, lane, kind, topic, source, from    string
	addressees                                *modelAddressees
	refs                                      *modelRefs
	also                                      []int64
	text                                      *modelText
}

// ReadModel answers record selections from memory: every record's selection
// columns, indexed, and the whole text of the newest ones. A read it cannot
// answer exactly goes to the store, so whatever it returns is what the store
// would have.
type ReadModel struct {
	source   *Store
	observer *ChangeVersion
	limit    int64

	mu         sync.RWMutex
	version    int64
	head       int64
	mutations  int64
	rows       []modelRow
	atHigh     []int64
	byDrive    map[string][]int32
	byLane     map[string][]int32
	byKind     map[string][]int32
	byAddress  map[string][]int32
	byPR       map[string][]int32
	closers    map[string][]int32
	resolvedBy map[int64]int64
	foreign    map[int64][]int32
	owners     map[string]bool
	wrong      []int32
	judged     []int64
	interned   map[string]string
	addressees map[string]*modelAddressees
	textFrom   int
	coldAt     int64
	bytes      int64
	inserted   int
	trimmed    time.Time

	stale    atomic.Bool
	hotFloor atomic.Int64
	hotAt    atomic.Int64
	held     atomic.Int64
	hits     atomic.Int64
	misses   atomic.Int64
	failures atomic.Int64
}

// ReadModelStats is what a read model holds and how often it has answered.
type ReadModelStats struct {
	Rows     int
	HotFloor int64
	Head     int64
	Bytes    int64
	Hits     int64
	Misses   int64
	Failures int64
}

// BuildReadModel fills a read model from the store, in batches that pace calls
// between, so readers keep the store while it fills.
func (s *Store) BuildReadModel(ctx context.Context, pace func(context.Context) error) (*ReadModel, error) {
	return s.BuildReadModelUnder(ctx, pace, ReadModelBytes)
}

// BuildReadModelUnder is BuildReadModel with its own cap on the bytes it holds.
func (s *Store) BuildReadModelUnder(ctx context.Context, pace func(context.Context) error, limit int64) (*ReadModel, error) {
	tip, err := s.RecordTip(ctx)
	if err != nil {
		return nil, err
	}
	if !tip.Exact {
		return nil, errors.New("read model: the store does not count changes to its records yet")
	}
	observer, err := s.OpenChangeVersion(ctx)
	if err != nil {
		return nil, err
	}
	m := &ReadModel{
		source: s, observer: observer, limit: limit, mutations: tip.Mutations,
		byDrive: map[string][]int32{}, byLane: map[string][]int32{}, byKind: map[string][]int32{}, byAddress: map[string][]int32{}, byPR: map[string][]int32{}, closers: map[string][]int32{}, resolvedBy: map[int64]int64{}, foreign: map[int64][]int32{},
		interned: map[string]string{}, addressees: map[string]*modelAddressees{},
	}
	for {
		m.mu.Lock()
		more, err := m.catchUpBatch(ctx)
		if err == nil {
			err = m.trim()
		}
		m.mu.Unlock()
		if err != nil {
			return nil, errors.Join(err, m.Close())
		}
		if !more {
			break
		}
		if err := pace(ctx); err != nil {
			return nil, errors.Join(err, m.Close())
		}
	}
	if err := m.catchUp(ctx); err != nil {
		return nil, errors.Join(err, m.Close())
	}
	return m, nil
}

// ServeReadModel has s's reads try m first; nil returns them to the store alone.
func (s *Store) ServeReadModel(m *ReadModel) {
	s.window.model.Store(m)
}

// ServedReadModel is the read model s's reads try first, nil when there is none.
func (s *Store) ServedReadModel() *ReadModel {
	return s.window.model.Load()
}

// Close frees the model; a read still on it fails and goes to the store.
func (m *ReadModel) Close() error {
	m.stale.Store(true)
	return m.observer.Close()
}

// Stale reports that a stored record changed under the model, which must be rebuilt.
func (m *ReadModel) Stale() bool {
	return m.stale.Load()
}

// Stats reports what the model holds and how often it answered.
func (m *ReadModel) Stats() ReadModelStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return ReadModelStats{Rows: len(m.rows), HotFloor: m.hotFloor.Load(), Head: m.head, Bytes: m.held.Load(), Hits: m.hits.Load(), Misses: m.misses.Load(), Failures: m.failures.Load()}
}

// Bounds is the lowest seq whose whole text the model holds, and its newest seq.
func (m *ReadModel) Bounds() (floor, head int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.hotFloor.Load(), m.head
}

// CatchUp copies what the store committed since the last call.
func (m *ReadModel) CatchUp(ctx context.Context) error {
	return m.catchUp(ctx)
}

func (m *ReadModel) catchUp(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	version, err := m.observer.Read(ctx)
	if err != nil {
		return err
	}
	if version == m.version && m.version != 0 {
		return nil
	}
	for {
		more, err := m.catchUpBatch(ctx)
		if err != nil {
			return err
		}
		if err := m.trim(); err != nil {
			m.stale.Store(true)
			return err
		}
		if !more {
			break
		}
	}
	m.version = version
	return nil
}

func (m *ReadModel) catchUpBatch(ctx context.Context) (bool, error) {
	snapshot, err := m.observer.connection.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return false, fmt.Errorf("begin read model read: %w", err)
	}
	defer func() { _ = snapshot.Rollback() }()
	var mutations int64
	if err := snapshot.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM meta WHERE key = ?), 0)", recordMutationsKey).Scan(&mutations); err != nil {
		return false, fmt.Errorf("read record mutations: %w", err)
	}
	if mutations != m.mutations {
		m.stale.Store(true)
		return false, errReadModelStale
	}
	if m.owners, err = readOwners(ctx, snapshot, m.source); err != nil {
		return false, err
	}
	rows, err := snapshot.QueryContext(ctx, "SELECT "+readModelColumns+" FROM records WHERE seq > ? ORDER BY seq LIMIT ?", m.head, readModelBatch)
	if err != nil {
		return false, fmt.Errorf("read records for the read model: %w", err)
	}
	defer func() { _ = rows.Close() }()
	cold := m.source.Now().Add(-readModelSpan).UnixMilli()
	copied := 0
	for rows.Next() {
		if err := m.ingest(rows, cold); err != nil {
			return false, err
		}
		copied++
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read records for the read model: %w", err)
	}
	m.inserted += copied
	return copied == readModelBatch, nil
}

func (m *ReadModel) intern(s string) string {
	if held, ok := m.interned[s]; ok {
		return held
	}
	s = strings.Clone(s)
	m.interned[s] = s
	m.bytes += int64(len(s)) + 32
	return s
}

func (m *ReadModel) ingest(rows *sql.Rows, cold int64) error {
	var (
		text                  modelText
		seq, at, re, resolves int64
		originSeq             int64
		origin, recipient     string
		drive, lane, kind     string
		topic, source         string
		expires               sql.NullInt64
	)
	if err := rows.Scan(&seq, &origin, &originSeq, &text.id, &drive, &lane, &kind, &at, &text.text, &topic, &recipient, &re, &resolves, &text.refs, &text.fields, &expires, &source, &text.body, &text.also, &text.reOriginSeq, &text.resolvesOriginSeq); err != nil {
		return fmt.Errorf("scan a record for the read model: %w", err)
	}
	row := modelRow{expires: modelNoExpiry, seq: seq, at: at, re: re, resolves: resolves, originSeq: originSeq, drive: m.intern(drive), lane: m.intern(lane), kind: m.intern(kind), topic: m.intern(topic), source: m.intern(source), from: m.intern(origin)}
	if expires.Valid {
		row.expires = expires.Int64
	}
	row.addressees = m.parsedAddressees(recipient)
	row.refs = parsedModelRefs(text.refs)
	wrong := row.addressees.wrong || row.refs != nil && row.refs.wrong || !text.decodes()
	if text.also != "" {
		wrong = json.Unmarshal([]byte(text.also), &row.also) != nil || wrong
		row.also = slices.Clip(row.also)
		m.bytes += int64(len(row.also)) * 8
	}
	index := int32(len(m.rows)) //nolint:gosec // G115: a store never holds 2^31 records.
	if at >= cold || len(m.rows) > m.textFrom {
		row.text = &text
		m.bytes += text.size()
	} else {
		m.textFrom = len(m.rows) + 1
		m.coldAt = max(m.coldAt, at)
		m.hotFloor.Store(seq + 1)
		m.hotAt.Store(m.coldAt)
	}
	m.bytes += readModelRowBytes
	m.rows = append(m.rows, row)
	if len(m.atHigh) > 0 {
		at = max(at, m.atHigh[len(m.atHigh)-1])
	}
	m.atHigh = append(m.atHigh, at)
	m.head = seq
	key := row.drive + "\x00"
	m.byDrive[row.drive] = append(m.byDrive[row.drive], index)
	m.byLane[key+row.lane] = append(m.byLane[key+row.lane], index)
	m.byKind[key+row.kind] = append(m.byKind[key+row.kind], index)
	for _, address := range row.addressees.to {
		m.byAddress[address] = appendRowOnce(m.byAddress[address], index)
	}
	if row.refs != nil {
		m.bytes += row.refs.size()
		for _, number := range row.refs.numbers() {
			m.byPR[key+number] = appendRowOnce(m.byPR[key+number], index)
		}
	}
	if wrong {
		m.wrong = append(m.wrong, index)
		m.judged = append(m.judged, seq)
	}
	m.indexClosure(&row, index)
	m.held.Store(m.bytes)
	return nil
}

func (m *ReadModel) indexClosure(row *modelRow, index int32) {
	if row.resolves != 0 {
		m.resolvedBy[row.resolves] = row.seq
	}
	for _, target := range row.also {
		m.resolvedBy[target] = row.seq
	}
	if row.from != m.source.Self() {
		m.foreign[row.originSeq] = append(m.foreign[row.originSeq], index)
	}
	if !modelClosers[row.kind] {
		return
	}
	key := row.drive + "\x00"
	if row.re != 0 {
		add := key + "re\x00" + strconv.FormatInt(row.re, 10)
		m.closers[add] = append(m.closers[add], index)
	}
	if row.topic != "" {
		add := key + "topic\x00" + row.topic
		m.closers[add] = append(m.closers[add], index)
	}
	if row.refs == nil {
		return
	}
	for _, ref := range slices.Concat(row.refs.stacks, row.refs.targets) {
		add := key + "ref\x00" + ref
		m.closers[add] = appendRowOnce(m.closers[add], index)
	}
}

func appendRowOnce(rows []int32, index int32) []int32 {
	if len(rows) > 0 && rows[len(rows)-1] == index {
		return rows
	}
	return append(rows, index)
}

func (r *modelRefs) size() int64 {
	total := int64(96)
	for _, values := range [...][]string{r.texts, r.stacks, r.targets} {
		for _, value := range values {
			total += int64(len(value)) + 16
		}
	}
	return total + int64(len(r.ints))*8
}

func (t *modelText) size() int64 {
	return int64(len(t.id)+len(t.text)+len(t.refs)+len(t.fields)+len(t.body)+len(t.also)) + readModelTextBytes
}

func (m *ReadModel) parsedAddressees(raw string) *modelAddressees {
	if held, ok := m.addressees[raw]; ok {
		return held
	}
	parsed := &modelAddressees{raw: strings.Clone(raw)}
	var values []any
	if err := json.Unmarshal([]byte(raw), &values); err != nil {
		parsed.wrong = true
	}
	for _, value := range values {
		address, ok := value.(string)
		if !ok {
			parsed.wrong = true
			continue
		}
		address = m.intern(address)
		parsed.to = append(parsed.to, address)
		parsed.all = parsed.all || address == "all"
	}
	parsed.to = slices.Clip(parsed.to)
	m.addressees[parsed.raw] = parsed
	m.bytes += int64(len(raw)) + int64(len(parsed.to))*16 + 64
	return parsed
}

func parsedModelRefs(raw string) *modelRefs {
	if raw == "{}" {
		return nil
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &keys); err != nil || strings.Contains(strings.ToLower(raw), `\ud`) {
		return &modelRefs{wrong: true}
	}
	for _, name := range [...]string{`"prs"`, `"stacks"`, `"targets"`} {
		if strings.Count(raw, name) > 1 {
			return &modelRefs{wrong: true}
		}
	}
	refs := &modelRefs{}
	held := false
	for key, value := range keys {
		switch {
		case key == "prs":
			held = true
			refs.wrong = !refs.parsePRs(value) || refs.wrong
		case key == "stacks":
			held = true
			refs.wrong = json.Unmarshal(value, &refs.stacks) != nil || refs.wrong
		case key == "targets":
			held = true
			refs.wrong = json.Unmarshal(value, &refs.targets) != nil || refs.wrong
		case strings.EqualFold(key, "prs"), strings.EqualFold(key, "stacks"), strings.EqualFold(key, "targets"):
			refs.wrong = true
		}
	}
	if !held && !refs.wrong {
		return nil
	}
	return refs
}

func (r *modelRefs) parsePRs(raw json.RawMessage) bool {
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return false
	}
	for _, value := range values {
		if number, err := strconv.ParseInt(string(value), 10, 64); err == nil {
			r.ints = append(r.ints, number)
			continue
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil {
			return false
		}
		r.texts = append(r.texts, text)
	}
	return true
}

func (r *modelRefs) numbers() []string {
	out := make([]string, 0, len(r.ints)+len(r.texts))
	for _, number := range r.ints {
		out = append(out, strconv.FormatInt(number, 10))
	}
	for _, text := range r.texts {
		if at := strings.LastIndexByte(text, '#'); at >= 0 {
			out = append(out, text[at+1:])
		}
	}
	return out
}

func (m *ReadModel) trim() error {
	now := m.source.Now()
	if m.inserted < readModelTrimRows && !m.trimmed.IsZero() && now.Sub(m.trimmed) < readModelTrimAge && m.bytes <= m.limit {
		return nil
	}
	m.inserted, m.trimmed = 0, now
	cold := now.Add(-readModelSpan).UnixMilli()
	for m.textFrom < len(m.rows) && (m.rows[m.textFrom].at < cold || m.bytes > m.limit) {
		row := &m.rows[m.textFrom]
		if row.text != nil {
			m.bytes -= row.text.size()
			row.text = nil
		}
		m.coldAt = max(m.coldAt, row.at)
		m.textFrom++
		m.hotFloor.Store(row.seq + 1)
	}
	m.hotAt.Store(m.coldAt)
	m.held.Store(m.bytes)
	if m.bytes > m.limit {
		return fmt.Errorf("read model: %d rows hold %d bytes with no text left to drop, over its cap of %d", len(m.rows), m.bytes, m.limit)
	}
	return nil
}

func (m *ReadModel) prepared(ctx context.Context, s *Store, f Filter) (Filter, bool) {
	if f.BroadcastFloor != "" {
		after, _, err := s.Cursor(ctx, f.BroadcastFloor, f.Drive)
		if err != nil {
			m.failures.Add(1)
			return f, false
		}
		f.broadcastAfter = &after
		if f.broadcastSince, _, err = s.Cursor(ctx, broadcastStart(f.BroadcastFloor), f.Drive); err != nil {
			m.failures.Add(1)
			return f, false
		}
	}
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return f, false
	}
	return f, true
}

func (m *ReadModel) answer(ctx context.Context, s *Store, f Filter, projection string, now time.Time) ([]Record, bool) {
	if projection != columns && projection != textColumns {
		m.misses.Add(1)
		return nil, false
	}
	f, ok := m.prepared(ctx, s, f)
	if !ok {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || !f.modelable() {
		m.misses.Add(1)
		return nil, false
	}
	selecting := newModelSelection(m, s, f, now)
	var chosen []*modelRow
	proved := selecting.walk(func(row *modelRow) bool {
		chosen = append(chosen, row)
		return f.Limit <= 0 || len(chosen) < f.Limit
	})
	if !proved {
		m.misses.Add(1)
		return nil, false
	}
	out, err := m.hydrated(ctx, s, chosen, projection)
	if err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.hits.Add(1)
	return out, true
}

func (m *ReadModel) count(ctx context.Context, s *Store, f Filter, now time.Time) (int, bool) {
	f, ok := m.prepared(ctx, s, f)
	if !ok {
		return 0, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || !f.modelable() {
		m.misses.Add(1)
		return 0, false
	}
	n := 0
	if !newModelSelection(m, s, f, now).walk(func(*modelRow) bool { n++; return true }) {
		m.misses.Add(1)
		return 0, false
	}
	m.hits.Add(1)
	return n, true
}

// Answer is what the model returns for f, with whether it could answer: the
// parity harness compares it to the store's own read.
func (m *ReadModel) Answer(ctx context.Context, s *Store, f Filter) ([]Record, bool, error) {
	f, err := s.aliased(ctx, f)
	if err != nil {
		return nil, false, err
	}
	out, ok := m.answer(ctx, s, f, columns, s.Now())
	if !ok {
		return nil, false, nil
	}
	return out, true, s.numbered(ctx, out)
}

func (m *ReadModel) firstSince(at int64) sql.NullInt64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() {
		return sql.NullInt64{}
	}
	first, _ := slices.BinarySearch(m.atHigh, at)
	if first == len(m.rows) {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: m.rows[first].seq, Valid: true}
}

func (f Filter) modelable() bool {
	if f.Trigrams != "" {
		return false
	}
	for _, word := range f.Contains {
		if strings.ContainsAny(word, "%_") {
			return false
		}
	}
	return true
}

type modelSelection struct {
	m                             *ReadModel
	f                             Filter
	now                           int64
	forNames, forHooks            []string
	addressed, to                 []string
	peers, readers                []string
	kindSet, muteSet, toKinds     map[string]bool
	firstOnly                     map[string]bool
	orders                        map[string]bool
	wordsLower                    [][]byte
	wordsNumeric                  []bool
	since, until                  int64
	broadcastAfter, broadcastFrom int64
	broadcastBound                bool
}

func stringsOf(values []any) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = value.(string)
	}
	return out
}

func kindSet(ks []kinds.Kind) map[string]bool {
	if len(ks) == 0 {
		return nil
	}
	out := make(map[string]bool, len(ks))
	for _, k := range ks {
		out[string(k)] = true
	}
	return out
}

func newModelSelection(m *ReadModel, s *Store, f Filter, now time.Time) *modelSelection {
	sel := &modelSelection{m: m, f: f, now: now.UnixMilli(), kindSet: kindSet(f.Kinds), muteSet: kindSet(f.Mute), toKinds: kindSet(f.ToKinds), firstOnly: kindSet(f.FirstOnly), orders: kindSet(kinds.Orders())}
	if f.For != "" {
		sel.forNames = stringsOf(f.addressees(f.For))
		sel.forHooks = stringsOf(hookSources(f.addressees(f.For)))
	}
	if f.Addressed != "" {
		sel.addressed = stringsOf(f.addressees(f.Addressed))
	}
	if f.To != "" {
		sel.to = stringsOf(f.addressees(f.To))
	}
	sel.peers, sel.readers = stringsOf(f.peerAddresses(s.Self())), stringsOf(f.readerAddresses())
	for _, word := range f.Contains {
		sel.wordsLower = append(sel.wordsLower, lowerASCII(word))
		sel.wordsNumeric = append(sel.wordsNumeric, strings.Trim(word, "0123456789") == "")
	}
	if !f.Since.IsZero() {
		sel.since = f.Since.UnixMilli()
	}
	if !f.Until.IsZero() {
		sel.until = f.Until.UnixMilli()
	}
	if f.broadcastAfter != nil {
		sel.broadcastBound, sel.broadcastAfter, sel.broadcastFrom = true, *f.broadcastAfter, f.broadcastSince
	}
	return sel
}

func (sel *modelSelection) candidates() [][]int32 {
	m, f := sel.m, sel.f
	key := f.Drive + "\x00"
	var lists [][]int32
	switch {
	case f.To != "":
		for _, address := range append(slices.Clone(sel.to), "all") {
			lists = append(lists, m.byAddress[address])
		}
	case f.PR.Number != 0:
		lists = append(lists, m.byPR[key+strconv.Itoa(f.PR.Number)])
	case f.For == "" && len(f.Lanes) > 0:
		for _, lane := range f.Lanes {
			lists = append(lists, m.byLane[key+lane])
		}
	case f.For == "" && len(f.Kinds) > 0:
		for _, k := range f.Kinds {
			lists = append(lists, m.byKind[key+string(k)])
		}
	default:
		lists = append(lists, m.byDrive[f.Drive])
	}
	for _, address := range slices.Concat(sel.peers, sel.readers) {
		lists = append(lists, m.byAddress[address])
	}
	for _, follow := range f.Follow {
		for _, k := range follow.Kinds {
			lists = append(lists, m.byKind[follow.Drive+"\x00"+string(k)])
		}
	}
	return append(lists, m.wrong)
}

// walk visits the rows f selects in f's order until visit declines one. It
// reports false when a row it met can only be judged by the store.
func (sel *modelSelection) walk(visit func(*modelRow) bool) bool {
	m, f := sel.m, sel.f
	from, to := 0, len(m.rows)
	if f.After > 0 {
		at, _ := slices.BinarySearchFunc(m.rows, f.After+1, func(r modelRow, seq int64) int { return cmp.Compare(r.seq, seq) })
		from = at
	}
	if !f.Since.IsZero() {
		at, _ := slices.BinarySearch(m.atHigh, sel.since)
		from = max(from, at)
	}
	if f.Before > 0 {
		at, _ := slices.BinarySearchFunc(m.rows, f.Before, func(r modelRow, seq int64) int { return cmp.Compare(r.seq, seq) })
		to = at
	}
	lists := sel.candidates()
	low := make([]int, len(lists))
	high := make([]int, len(lists))
	for i, list := range lists {
		low[i], high[i] = rowAtOrAfter(list, from), rowAtOrAfter(list, to)
	}
	for {
		best := int32(-1)
		for i, list := range lists {
			if low[i] >= high[i] {
				continue
			}
			var head int32
			if f.Descending {
				head = list[high[i]-1]
				if best < 0 || head > best {
					best = head
				}
			} else {
				head = list[low[i]]
				if best < 0 || head < best {
					best = head
				}
			}
		}
		if best < 0 {
			return true
		}
		for i, list := range lists {
			if low[i] >= high[i] {
				continue
			}
			if f.Descending && list[high[i]-1] == best {
				high[i]--
			} else if !f.Descending && list[low[i]] == best {
				low[i]++
			}
		}
		row := &m.rows[best]
		selected, proved := sel.matches(row)
		if !proved {
			return false
		}
		if selected && !visit(row) {
			return true
		}
	}
}

func rowAtOrAfter(list []int32, row int) int {
	at, _ := slices.BinarySearchFunc(list, row, func(held int32, row int) int { return cmp.Compare(int(held), row) })
	return at
}

func (sel *modelSelection) matches(r *modelRow) (selected, proved bool) {
	f := sel.f
	if r.addressees.wrong || r.refs != nil && r.refs.wrong {
		return false, false
	}
	branch := sel.scoped(r) || sel.peerBranch(r) || sel.readerBranch(r) || sel.followBranch(r)
	if !branch {
		return false, true
	}
	if f.Origin != "" && r.from != f.Origin {
		return false, true
	}
	if f.Stack != "" && (r.refs == nil || !slices.Contains(r.refs.stacks, f.Stack)) {
		return false, true
	}
	if f.Target != "" && (r.refs == nil || !slices.Contains(r.refs.targets, f.Target)) {
		return false, true
	}
	if f.PR.Number != 0 && !sel.namesPR(r) {
		return false, true
	}
	if sel.muteSet[r.kind] {
		return false, true
	}
	if sel.firstOnly[r.kind] && f.To != "" && (len(r.addressees.to) == 0 || !slices.Contains(sel.to, r.addressees.to[0])) {
		return false, true
	}
	if f.RepeatWindow > 0 && r.kind == string(kinds.Urgent) && r.topic != "" && sel.repeats(r) {
		return false, true
	}
	if f.After > 0 && r.seq <= f.After || f.Before > 0 && r.seq >= f.Before {
		return false, true
	}
	if !f.Since.IsZero() && r.at < sel.since || !f.Until.IsZero() && r.at >= sel.until {
		return false, true
	}
	if !f.IncludeExpired && r.expires != modelNoExpiry && r.expires <= sel.now {
		return false, true
	}
	if len(sel.wordsLower) == 0 {
		return true, true
	}
	if r.text == nil {
		return false, false
	}
	return sel.holdsAWord(r), true
}

func (sel *modelSelection) selectors(r *modelRow) bool {
	f := sel.f
	return (len(f.Kinds) == 0 || sel.kindSet[r.kind]) &&
		(len(f.Lanes) == 0 || slices.Contains(f.Lanes, r.lane)) &&
		(len(f.Topics) == 0 || slices.Contains(f.Topics, r.topic))
}

func (sel *modelSelection) addressedKinds(r *modelRow) bool {
	return len(sel.f.ToKinds) == 0 || sel.toKinds[r.kind]
}

func anyOf(values, names []string) bool {
	for _, value := range values {
		if slices.Contains(names, value) {
			return true
		}
	}
	return false
}

func (sel *modelSelection) addressedTo(r *modelRow, names []string) bool {
	if anyOf(r.addressees.to, names) {
		return true
	}
	if r.re == 0 {
		return false
	}
	at, found := slices.BinarySearchFunc(sel.m.rows, r.re, func(row modelRow, seq int64) int { return cmp.Compare(row.seq, seq) })
	return found && slices.Contains(names, sel.m.rows[at].lane) && r.lane != sel.m.rows[at].lane
}

func (sel *modelSelection) fresh(r *modelRow) bool {
	return r.at >= sel.f.aliasSince || r.at >= sel.f.aliasOrders && sel.orders[r.kind]
}

func (sel *modelSelection) scoped(r *modelRow) bool {
	f := sel.f
	if r.drive != f.Drive {
		return false
	}
	if f.For != "" {
		addressed := sel.addressedTo(r, sel.forNames) && sel.addressedKinds(r)
		broadcast := (r.addressees.raw == "[]" || r.addressees.all) &&
			!slices.Contains(sel.forNames, r.lane) && !slices.Contains(sel.forHooks, r.source) && sel.selectors(r)
		if !addressed && !broadcast {
			return false
		}
	} else if !sel.selectors(r) {
		return false
	}
	if f.Addressed != "" {
		if f.aliasSince > 0 && len(sel.addressed) > 1 {
			if !sel.addressedTo(r, sel.addressed[:1]) && (!sel.fresh(r) || !sel.addressedTo(r, sel.addressed[1:])) {
				return false
			}
		} else if !sel.addressedTo(r, sel.addressed) {
			return false
		}
	}
	if f.To != "" && !sel.toMatches(r) {
		return false
	}
	return true
}

func (sel *modelSelection) toMatches(r *modelRow) bool {
	f := sel.f
	var recipient bool
	if f.aliasSince == 0 || len(sel.to) < 2 {
		recipient = anyOf(r.addressees.to, sel.to)
	} else {
		recipient = anyOf(r.addressees.to, sel.to[:1]) || sel.fresh(r) && anyOf(r.addressees.to, sel.to[1:])
	}
	all := r.addressees.all && !slices.Contains(sel.to, r.lane)
	if all && sel.broadcastBound {
		all = r.seq > sel.broadcastAfter && r.at >= sel.broadcastFrom
	}
	return (recipient || all) && sel.addressedKinds(r)
}

func (sel *modelSelection) peerBranch(r *modelRow) bool {
	return len(sel.peers) > 0 && anyOf(r.addressees.to, sel.peers) && (sel.f.PeerAfter <= 0 || r.seq > sel.f.PeerAfter) && sel.addressedKinds(r)
}

func (sel *modelSelection) readerBranch(r *modelRow) bool {
	if len(sel.readers) == 0 || !anyOf(r.addressees.to, sel.readers) {
		return false
	}
	if sel.f.For == "" && sel.f.LaneAliases && !sel.selectors(r) {
		return false
	}
	return sel.addressedKinds(r)
}

func (sel *modelSelection) followBranch(r *modelRow) bool {
	for _, follow := range sel.f.Follow {
		if r.drive == follow.Drive && (r.addressees.raw == "[]" || r.addressees.all) && slices.Contains(follow.Kinds, kinds.Kind(r.kind)) {
			return true
		}
	}
	return false
}

func (sel *modelSelection) namesPR(r *modelRow) bool {
	if r.refs == nil {
		return false
	}
	pr := sel.f.PR
	if slices.Contains(r.refs.ints, int64(pr.Number)) {
		return true
	}
	if pr.Repo != "" {
		return slices.Contains(r.refs.texts, pr.String())
	}
	suffix := "#" + strconv.Itoa(pr.Number)
	return slices.ContainsFunc(r.refs.texts, func(text string) bool { return strings.HasSuffix(text, suffix) })
}

func (sel *modelSelection) repeats(r *modelRow) bool {
	window := sel.f.RepeatWindow.Milliseconds()
	for _, index := range sel.m.byKind[r.drive+"\x00"+r.kind] {
		prior := &sel.m.rows[index]
		if prior.seq >= r.seq {
			return false
		}
		if prior.lane == r.lane && prior.topic == r.topic && prior.at > r.at-window {
			return true
		}
	}
	return false
}

func (sel *modelSelection) holdsAWord(r *modelRow) bool {
	columns := [...]string{r.text.text, r.lane, r.topic, r.kind, r.addressees.raw, r.text.refs, r.from}
	for i, word := range sel.wordsLower {
		for _, column := range columns {
			if containsFoldASCII(column, word) {
				return true
			}
		}
		if !sel.wordsNumeric[i] {
			continue
		}
		numbers := [...]string{
			strconv.FormatInt(r.seq, 10), strconv.FormatInt(r.originSeq, 10), nullableNumber(r.text.reOriginSeq), nullableNumber(r.text.resolvesOriginSeq), r.text.also,
		}
		for _, column := range numbers {
			if containsFoldASCII(column, word) {
				return true
			}
		}
	}
	return false
}

func nullableNumber(n sql.NullInt64) string {
	if !n.Valid {
		return ""
	}
	return strconv.FormatInt(n.Int64, 10)
}

func lowerASCII(s string) []byte {
	out := []byte(s)
	for i, c := range out {
		if 'A' <= c && c <= 'Z' {
			out[i] = c + 'a' - 'A'
		}
	}
	return out
}

func containsFoldASCII(hay string, lowerWord []byte) bool {
	if end := strings.IndexByte(hay, 0); end >= 0 {
		hay = hay[:end]
	}
	if len(lowerWord) == 0 {
		return true
	}
	first := lowerWord[0]
	for i := 0; i+len(lowerWord) <= len(hay); i++ {
		c := hay[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != first {
			continue
		}
		matched := true
		for j := 1; j < len(lowerWord); j++ {
			d := hay[i+j]
			if 'A' <= d && d <= 'Z' {
				d += 'a' - 'A'
			}
			if d != lowerWord[j] {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func (m *ReadModel) hydrated(ctx context.Context, s *Store, chosen []*modelRow, projection string) ([]Record, error) {
	out := make([]Record, len(chosen))
	var coldSeqs []int64
	for i, row := range chosen {
		if row.text == nil {
			coldSeqs = append(coldSeqs, row.seq)
			continue
		}
		record, err := s.modelRecord(row, projection)
		if err != nil {
			return nil, err
		}
		out[i] = record
	}
	if len(coldSeqs) == 0 {
		if len(out) == 0 {
			return nil, nil
		}
		return out, nil
	}
	cold, err := m.recordsBySeq(ctx, s, coldSeqs, projection)
	if err != nil {
		return nil, err
	}
	for i, row := range chosen {
		if row.text != nil {
			continue
		}
		record, ok := cold[row.seq]
		if !ok {
			return nil, fmt.Errorf("record #%d left the store under the read model", row.seq)
		}
		out[i] = record
	}
	return out, nil
}

func (m *ReadModel) recordsBySeq(ctx context.Context, s *Store, seqs []int64, projection string) (map[int64]Record, error) {
	wanted, err := json.Marshal(seqs)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin a read of older text: %w", err)
	}
	defer func() { _ = snapshot.Rollback() }()
	var mutations int64
	if err := snapshot.QueryRowContext(ctx, "SELECT COALESCE((SELECT value FROM meta WHERE key = ?), 0)", recordMutationsKey).Scan(&mutations); err != nil {
		return nil, fmt.Errorf("read record mutations: %w", err)
	}
	if mutations != m.mutations {
		return nil, errReadModelStale
	}
	rows, err := snapshot.QueryContext(ctx, "SELECT "+projection+" FROM records WHERE seq IN (SELECT value FROM json_each(?))", string(wanted))
	if err != nil {
		return nil, fmt.Errorf("read records by seq: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := make(map[int64]Record, len(seqs))
	for rows.Next() {
		record, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out[record.Seq] = record
	}
	return out, rows.Err()
}

func (s *Store) modelRecord(row *modelRow, projection string) (Record, error) {
	meta, err := row.text.decodedMeta(row.seq)
	if err != nil {
		return Record{}, err
	}
	r := Record{
		Seq: row.seq, Origin: s.local(row.from), OriginSeq: row.originSeq, ID: row.text.id, Drive: row.drive, Lane: row.lane, Kind: kinds.Kind(row.kind),
		At: time.UnixMilli(row.at).UTC(), Text: row.text.text, Topic: row.topic, To: row.addressees.to, Re: row.re, Resolves: row.resolves, AlsoResolves: row.also,
		Refs: meta.refs, Fields: meta.fields, Source: row.source,
	}
	if r.Origin == "" {
		r.OriginSeq = 0
	}
	if projection != textColumns && row.text.body != "" {
		r.Body = json.RawMessage(row.text.body)
	}
	if row.expires != modelNoExpiry {
		expires := time.UnixMilli(row.expires).UTC()
		r.ExpiresAt = &expires
	}
	return r, nil
}

func (t *modelText) decodes() bool {
	_, err := t.decode(0)
	return err == nil
}

func (t *modelText) decode(seq int64) (*modelMeta, error) {
	meta := &modelMeta{}
	if t.refs != "{}" {
		if err := json.Unmarshal([]byte(t.refs), &meta.refs); err != nil {
			return nil, fmt.Errorf("decode refs of #%d: %w", seq, err)
		}
	}
	if t.fields != "{}" {
		if err := json.Unmarshal([]byte(t.fields), &meta.fields); err != nil {
			return nil, fmt.Errorf("decode fields of #%d: %w", seq, err)
		}
	}
	clipRefs(&meta.refs)
	meta.fields.Envs = slices.Clip(meta.fields.Envs)
	return meta, nil
}

func clipRefs(refs *Refs) {
	refs.PRs, refs.Builds, refs.Stacks, refs.Targets, refs.Lanes = slices.Clip(refs.PRs), slices.Clip(refs.Builds), slices.Clip(refs.Stacks), slices.Clip(refs.Targets), slices.Clip(refs.Lanes)
}

func (t *modelText) decodedMeta(seq int64) (*modelMeta, error) {
	if meta := t.meta.Load(); meta != nil {
		return meta, nil
	}
	meta, err := t.decode(seq)
	if err != nil {
		return nil, err
	}
	if len(t.refs)+len(t.fields) <= readModelMetaHeld {
		t.meta.Store(meta)
	}
	return meta, nil
}

func (m *ReadModel) rowOf(seq int64) *modelRow {
	at, found := slices.BinarySearchFunc(m.rows, seq, func(r modelRow, seq int64) int { return cmp.Compare(r.seq, seq) })
	if !found {
		return nil
	}
	return &m.rows[at]
}

func (r *modelRow) closure() Record {
	record := Record{Seq: r.seq, Lane: r.lane, Kind: kinds.Kind(r.kind), At: time.UnixMilli(r.at).UTC(), Topic: r.topic, Re: r.re, Resolves: r.resolves, AlsoResolves: r.also, Source: r.source}
	if r.refs != nil {
		record.Refs.Stacks, record.Refs.Targets = r.refs.stacks, r.refs.targets
	}
	return record
}

// openRecordsAmong is OpenAmong's answer, unnumbered as the store's is.
func (m *ReadModel) openRecordsAmong(ctx context.Context, s *Store, drive string, seqs []int64) ([]Record, bool) {
	open, ok := m.openAmong(ctx, s, drive, seqs)
	if !ok {
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	chosen := make([]*modelRow, 0, len(open))
	for _, seq := range open {
		row := m.rowOf(seq)
		if row == nil {
			return nil, false
		}
		chosen = append(chosen, row)
	}
	out, err := m.hydrated(ctx, s, chosen, columns)
	if err != nil {
		m.failures.Add(1)
		return nil, false
	}
	return out, true
}

// openAmong is the seqs of OpenAmong's answer: it reads no record's text, and
// false leaves the question to the store.
func (m *ReadModel) openAmong(ctx context.Context, s *Store, drive string, seqs []int64) ([]int64, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || len(m.wrong) > 0 {
		m.misses.Add(1)
		return nil, false
	}
	seqs = slices.Compact(slices.Sorted(slices.Values(seqs)))
	openers := make([]Record, 0, len(seqs))
	for _, seq := range seqs {
		if row := m.rowOf(seq); row != nil && row.drive == drive && modelOpeners[row.kind] {
			if opener := row.closure(); opener.Tracked() {
				openers = append(openers, opener)
			}
		}
	}
	open, err := m.unclosedLocked(ctx, s, drive, openers)
	if err != nil {
		m.failures.Add(1)
		return nil, false
	}
	out := make([]int64, len(open))
	for i, o := range open {
		out[i] = o.Seq
	}
	m.hits.Add(1)
	return out, true
}

func (m *ReadModel) unclosedLocked(ctx context.Context, s *Store, drive string, openers []Record) ([]Record, error) {
	openers = slices.DeleteFunc(openers, func(o Record) bool {
		return o.Kind == kinds.Incident && slices.ContainsFunc(incidentTargets(o), func(seq int64) bool {
			row := m.rowOf(seq)
			return row != nil && row.drive == drive && row.kind == string(kinds.Incident)
		})
	})
	if len(openers) == 0 {
		return nil, nil
	}
	ix := indexClosers(m.closersOf(drive, openers))
	var open []Record
	for _, o := range openers {
		if by, ok := m.resolvedBy[o.Seq]; ok && by > o.Seq {
			continue
		}
		if !ix.closed(o) {
			open = append(open, o)
		}
	}
	return s.heardAsks(ctx, s.db, drive, open)
}

// unclosed is what stays open among openers of drive, false when only the store
// can judge.
func (m *ReadModel) unclosed(ctx context.Context, s *Store, drive string, openers []Record) ([]Record, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || len(m.wrong) > 0 {
		m.misses.Add(1)
		return nil, false
	}
	open, err := m.unclosedLocked(ctx, s, drive, openers)
	if err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.hits.Add(1)
	return open, true
}

// closureRows is the store's closureRows for ks, false when only the store can
// judge.
func (m *ReadModel) closureRows(ctx context.Context, drive string, ks []kinds.Kind) ([]Record, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || len(m.wrong) > 0 {
		m.misses.Add(1)
		return nil, false
	}
	var indexes []int32
	for _, k := range ks {
		indexes = append(indexes, m.byKind[drive+"\x00"+string(k)]...)
	}
	slices.Sort(indexes)
	out := make([]Record, len(indexes))
	for i, index := range indexes {
		out[i] = m.rows[index].closure()
	}
	m.hits.Add(1)
	return out, true
}

func (m *ReadModel) closersOf(drive string, openers []Record) []Record {
	key := drive + "\x00"
	seen := map[int32]bool{}
	var out []Record
	add := func(lookup string) {
		for _, index := range m.closers[lookup] {
			if !seen[index] {
				seen[index] = true
				out = append(out, m.rows[index].closure())
			}
		}
	}
	for _, o := range openers {
		add(key + "re\x00" + strconv.FormatInt(o.Seq, 10))
		if o.Topic != "" {
			add(key + "topic\x00" + o.Topic)
		}
		for _, ref := range slices.Concat(o.Refs.Stacks, o.Refs.Targets) {
			add(key + "ref\x00" + ref)
		}
	}
	return out
}

// otherNumbered is the record of another origin that #n also names in its
// drive, nil when none does; false when only the store can say which is first.
func (m *ReadModel) otherNumbered(ctx context.Context, n int64) (*modelRow, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() {
		return nil, false
	}
	own := m.rowOf(n)
	if own == nil {
		return nil, true
	}
	if own.from != m.source.Self() {
		return nil, false
	}
	var other *modelRow
	for _, index := range m.foreign[n] {
		row := &m.rows[index]
		if row.drive != own.drive || row.from == own.from {
			continue
		}
		if other != nil {
			return nil, false
		}
		other = row
	}
	return other, true
}

// namesOf is names answered from memory, false when a target is past what the
// model holds.
func (m *ReadModel) namesOf(ctx context.Context, s *Store, seqs []int64) (map[string]bool, map[int64]string, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || m.owners == nil {
		return nil, nil, false
	}
	owners := maps.Clone(m.owners)
	owners[s.Self()] = s.owner
	out := make(map[int64]string, len(seqs))
	for _, seq := range seqs {
		if seq == 0 {
			continue
		}
		if seq > m.head {
			return nil, nil, false
		}
		if row := m.rowOf(seq); row != nil {
			out[seq] = s.name(owners, Record{Seq: row.seq, Origin: s.local(row.from), OriginSeq: row.originSeq})
		}
	}
	return owners, out, true
}
