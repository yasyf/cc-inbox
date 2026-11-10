package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
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
	"syscall"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

const (
	readModelFile = "model.on"
	// ReadModelBytes caps what a read model holds in memory: every record's
	// selection columns. The whole text of every record sits in a file the kernel
	// pages in on demand and can drop under pressure, outside this cap.
	ReadModelBytes     = 150 << 20
	readModelSpan      = 36 * time.Hour
	readModelBatch     = 1000
	readModelRowBytes  = 170
	readModelKeyBytes  = 64
	readModelChunk     = 16 << 20
	readModelDiskFloor = 1 << 30
	readModelMetaHeld  = 1 << 10
	readModelMetaSlots = 1 << 13
	readModelMetaBytes = 12 << 20
	readModelMetaFloor = 64 << 20
	modelNoExpiry      = math.MinInt64
	modelNone          = -1
	readModelColumns   = "seq, origin, origin_seq, COALESCE(id, ''), drive, lane, kind, at, text, topic, recipients, re, resolves, refs, fields, expires_at, source, COALESCE(body, ''), COALESCE(also_resolves, ''), re_origin_seq, resolves_origin_seq"
)

const (
	closerRe = iota + 1
	closerTopic
	closerRef
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

type parsedRefs struct {
	ints    []int64
	texts   []string
	stacks  []string
	targets []string
	wrong   bool
}

type modelRefs struct {
	ints, texts, stacks, targets []int32
}

type modelText struct {
	id, text, refs, fields, body, also string
	reOriginSeq, resolvesOriginSeq     sql.NullInt64
}

type modelMeta struct {
	seq    int64
	refs   Refs
	fields Fields
}

// modelRow holds no pointers, so the collector never scans the rows.
type modelRow struct {
	seq, at, expires, re, resolves, originSeq int64
	textAt                                    int64
	drive, lane, kind, topic, source, from    int32
	addressees, refs, also                    int32
	wrong                                     bool
}

type modelView struct {
	row                                    *modelRow
	drive, lane, kind, topic, source, from string
	addressees                             *modelAddressees
}

type modelChunk struct {
	memory []byte
	offset int64
	used   int
}

// walChange is the part of SQLite's WAL index header that moves on every commit:
// iChange and mxFrame in the header's first copy.
const (
	walChangeOffset = 8
	walFrameOffset  = 16
	walHeaderBytes  = 48
)

type modelCloser struct {
	row, next int32
}

// ReadModel answers record selections from memory: every record's selection
// columns, indexed, and every record's whole text in a file mapped outside the
// heap. A read it cannot answer exactly goes to the store, so whatever it
// returns is what the store would have.
type ReadModel struct {
	source   *Store
	observer *ChangeVersion
	walIndex []byte
	limit    int64
	text     *os.File
	textEnd  int64
	textUsed int64

	window      modelWindow
	builtBytes  int64
	unnamed     []int64
	mu          sync.RWMutex
	head        int64
	mutations   int64
	rows        []modelRow
	atHigh      []int64
	byDrive     map[int32][]int32
	byLane      map[uint64][]int32
	byKind      map[uint64][]int32
	byAddress   map[int32][]int32
	byPR        map[uint64][]int32
	closerHead  map[modelCloserKey]int32
	closerLinks []modelCloser
	resolvedBy  map[int64]int64
	foreign     map[int64][]int32
	owners      map[string]bool
	wrong       []int32
	ids         map[string]int32
	strs        []string
	addrIDs     map[string]int32
	addrs       []*modelAddressees
	refPool     []int32
	alsoPool    []int64
	chunks      []modelChunk
	scratch     []byte
	keepMetas   bool
	bytes       int64
	metas       [readModelMetaSlots]atomic.Pointer[modelMeta]

	stale       atomic.Bool
	full        atomic.Bool
	seen        atomic.Uint64
	searchFloor atomic.Int64
	searchSince atomic.Int64
	held        atomic.Int64
	hits        atomic.Int64
	misses      atomic.Int64
	failures    atomic.Int64
}

// ReadModelStats is what a read model holds and how often it has answered.
type ReadModelStats struct {
	Rows        int
	Floor       int64
	SearchFloor int64
	TextBytes   int64
	Head        int64
	Bytes       int64
	Hits        int64
	Misses      int64
	Failures    int64
}

// BuildReadModel fills a read model from the store, in batches that pace calls
// between, so readers keep the store while it fills.
func (s *Store) BuildReadModel(ctx context.Context, pace func(context.Context) error) (*ReadModel, error) {
	return s.BuildReadModelUnder(ctx, pace, ReadModelBytes)
}

// BuildReadModelUnder is BuildReadModel with its own cap on the bytes it holds.
func (s *Store) BuildReadModelUnder(ctx context.Context, pace func(context.Context) error, limit int64) (*ReadModel, error) {
	return s.buildReadModel(ctx, pace, limit, readModelWindowRows(limit))
}

func (s *Store) buildReadModel(ctx context.Context, pace func(context.Context) error, limit, windowRows int64) (*ReadModel, error) {
	tip, err := s.RecordTip(ctx)
	if err != nil {
		return nil, err
	}
	if !tip.Exact {
		return nil, errors.New("read model: the store does not count changes to its records yet")
	}
	window, err := s.readModelWindow(ctx, windowRows)
	if err != nil {
		return nil, err
	}
	observer, err := s.OpenChangeVersion(ctx)
	if err != nil {
		return nil, err
	}
	walIndex, err := mapWALIndex(ctx, s)
	if err != nil {
		return nil, errors.Join(err, observer.Close())
	}
	textFile, err := openModelText(s.home)
	if err != nil {
		return nil, errors.Join(err, syscall.Munmap(walIndex), observer.Close())
	}
	m := &ReadModel{
		source: s, observer: observer, walIndex: walIndex, text: textFile, limit: limit, mutations: tip.Mutations, window: window, head: max(window.floor-1, 0),
		byDrive: map[int32][]int32{}, byLane: map[uint64][]int32{}, byKind: map[uint64][]int32{}, byAddress: map[int32][]int32{}, byPR: map[uint64][]int32{},
		closerHead: map[modelCloserKey]int32{}, resolvedBy: map[int64]int64{}, foreign: map[int64][]int32{},
		ids: map[string]int32{}, addrIDs: map[string]int32{},
	}
	m.keepMetas = limit >= readModelMetaFloor
	m.bytes = int64(unsafe.Sizeof(m.metas))
	m.bytes += window.bytes()
	if m.keepMetas {
		m.bytes += readModelMetaBytes
	}
	for {
		m.mu.Lock()
		more, err := m.catchUpBatch(ctx)
		if err == nil {
			err = m.withinCap()
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
	m.mu.Lock()
	m.builtBytes = m.bytes
	m.mu.Unlock()
	return m, nil
}

type servedModel struct {
	model   atomic.Pointer[ReadModel]
	aliases aliasMemo
}

// HeldFrom is the lowest seq a search scans in the model, 0 when no model
// serves; older records answer it from the store's trigram narrowing.
func (s *Store) HeldFrom() int64 {
	m := s.reads.model.Load()
	if m == nil || m.stale.Load() {
		return 0
	}
	return m.searchFloor.Load()
}

// Covers reports whether a search of f stays above the model's search floor,
// so f needs no narrowing that exists to spare the store a scan.
func (s *Store) Covers(f Filter) bool {
	m := s.reads.model.Load()
	return m != nil && !m.stale.Load() && (f.After >= m.searchFloor.Load()-1 || !f.Since.IsZero() && f.Since.UnixMilli() > m.searchSince.Load())
}

func mapWALIndex(ctx context.Context, s *Store) ([]byte, error) {
	var filename string
	if err := s.db.QueryRowContext(ctx, "SELECT file FROM pragma_database_list WHERE name='main'").Scan(&filename); err != nil {
		return nil, fmt.Errorf("find the store's file: %w", err)
	}
	file, err := os.Open(filename + "-shm") //nolint:gosec // G304: SQLite named this file.
	if err != nil {
		return nil, fmt.Errorf("open the WAL index: %w", err)
	}
	defer func() { _ = file.Close() }()
	mapped, err := syscall.Mmap(int(file.Fd()), 0, walHeaderBytes, syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: a descriptor fits an int.
	if err != nil {
		return nil, fmt.Errorf("map the WAL index: %w", err)
	}
	return mapped, nil
}

// committed moves whenever a commit reaches the store, from any process, and
// costs no system call. It reads what PRAGMA data_version reads, directly.
// mark is committed for a caller outside the model's lock: false once the
// model is closed, when its WAL index is gone.
func (m *ReadModel) mark() (uint64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.walIndex == nil {
		return 0, false
	}
	return m.committed(), true
}

func (m *ReadModel) committed() uint64 {
	change := atomic.LoadUint32((*uint32)(unsafe.Pointer(&m.walIndex[walChangeOffset]))) //nolint:gosec // G103: the header's fields are aligned, shared memory.
	frame := atomic.LoadUint32((*uint32)(unsafe.Pointer(&m.walIndex[walFrameOffset])))   //nolint:gosec // G103: the header's fields are aligned, shared memory.
	return uint64(change)<<32 | uint64(frame)
}

// ServeReadModel has s's reads try m first; nil returns them to the store alone.
func (s *Store) ServeReadModel(m *ReadModel) {
	s.reads.model.Store(m)
}

// ServedReadModel is the read model s's reads try first, nil when there is none.
func (s *Store) ServedReadModel() *ReadModel {
	return s.reads.model.Load()
}

// Close frees the model; a read still on it fails and goes to the store.
func (m *ReadModel) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stale.Store(true)
	var failed error
	for _, chunk := range m.chunks {
		failed = errors.Join(failed, syscall.Munmap(chunk.memory))
	}
	m.chunks = nil
	failed = errors.Join(failed, m.text.Close())
	if m.walIndex != nil {
		failed = errors.Join(failed, syscall.Munmap(m.walIndex))
		m.walIndex = nil
	}
	return errors.Join(failed, m.observer.Close())
}

// Full reports that the model has grown past its refill mark, so a model
// rebuilt on a newer window should replace it.
func (m *ReadModel) Full() bool {
	return m.full.Load()
}

// Stale reports that a stored record changed under the model, which must be rebuilt.
func (m *ReadModel) Stale() bool {
	return m.stale.Load()
}

// Stats reports what the model holds and how often it answered.
func (m *ReadModel) Stats() ReadModelStats {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return ReadModelStats{Rows: len(m.rows), Floor: m.window.floor, SearchFloor: m.searchFloor.Load(), TextBytes: m.textUsed, Head: m.head, Bytes: m.held.Load(), Hits: m.hits.Load(), Misses: m.misses.Load(), Failures: m.failures.Load()}
}

// Bounds is the model's search floor and its newest seq.
func (m *ReadModel) Bounds() (floor, head int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.searchFloor.Load(), m.head
}

// CatchUp copies what the store committed since the last call.
func (m *ReadModel) CatchUp(ctx context.Context) error {
	return m.catchUp(ctx)
}

func (m *ReadModel) catchUp(ctx context.Context) error {
	m.mu.RLock()
	stale := m.stale.Load()
	current := stale || m.committed() == m.seen.Load()
	m.mu.RUnlock()
	if stale {
		return errReadModelStale
	}
	if current {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stale.Load() {
		return errReadModelStale
	}
	committed := m.committed()
	if committed == m.seen.Load() {
		return nil
	}
	for {
		more, err := m.catchUpBatch(ctx)
		if err != nil {
			return err
		}
		if err := m.withinCap(); err != nil {
			m.stale.Store(true)
			return err
		}
		if !more {
			break
		}
	}
	m.seen.Store(committed)
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
	copied := 0
	for rows.Next() {
		if err := m.ingest(rows); err != nil {
			return false, err
		}
		copied++
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("read records for the read model: %w", err)
	}
	if err := m.nameReplyTargets(ctx, snapshot); err != nil {
		return false, err
	}
	return copied == readModelBatch, nil
}

func (m *ReadModel) intern(s string) int32 {
	if id, ok := m.ids[s]; ok {
		return id
	}
	s = strings.Clone(s)
	id := int32(len(m.strs)) //nolint:gosec // G115: fewer distinct strings than records, which never reach 2^31.
	m.ids[s] = id
	m.strs = append(m.strs, s)
	m.bytes += int64(len(s)) + readModelKeyBytes
	return id
}

func pairKey(a, b int32) uint64 {
	return uint64(uint32(a))<<32 | uint64(uint32(b)) //nolint:gosec // G115: the bits are the key.
}

type modelCloserKey struct {
	drive, axis int32
	value       int64
}

func (m *ReadModel) ingest(rows *sql.Rows) error {
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
	row := modelRow{
		seq: seq, at: at, re: re, resolves: resolves, originSeq: originSeq, expires: modelNoExpiry, refs: modelNone, also: modelNone,
		drive: m.intern(drive), lane: m.intern(lane), kind: m.intern(kind), topic: m.intern(topic), source: m.intern(source), from: m.intern(origin),
	}
	if expires.Valid {
		row.expires = expires.Int64
	}
	row.addressees = m.internAddressees(recipient)
	addressees := m.addrs[row.addressees]
	refs := parsedModelRefs(text.refs)
	row.wrong = addressees.wrong || text.refs != "{}" && json.Unmarshal([]byte(text.refs), new(Refs)) != nil
	if refs != nil {
		row.wrong = row.wrong || refs.wrong || !fitsRefPool(refs)
		if !row.wrong {
			row.refs = m.storeRefs(refs)
		}
	}
	var also []int64
	if text.also != "" {
		if err := json.Unmarshal([]byte(text.also), &also); err != nil {
			row.wrong = true
		} else {
			row.also = int32(len(m.alsoPool)) //nolint:gosec // G115: the pool never reaches 2^31 entries.
			m.alsoPool = append(append(m.alsoPool, int64(len(also))), also...)
			m.bytes += int64(len(also)+1) * 8
		}
	}
	index := int32(len(m.rows)) //nolint:gosec // G115: a store never holds 2^31 records.
	if err := m.place(&row, &text); err != nil {
		m.stale.Store(true)
		return err
	}
	m.bytes += readModelRowBytes
	m.rows = append(m.rows, row)
	if len(m.atHigh) > 0 {
		at = max(at, m.atHigh[len(m.atHigh)-1])
	}
	m.atHigh = append(m.atHigh, at)
	m.head = seq
	m.byDrive[row.drive] = append(m.byDrive[row.drive], index)
	m.byLane[pairKey(row.drive, row.lane)] = append(m.byLane[pairKey(row.drive, row.lane)], index)
	m.byKind[pairKey(row.drive, row.kind)] = append(m.byKind[pairKey(row.drive, row.kind)], index)
	for _, address := range addressees.to {
		id := m.intern(address)
		m.byAddress[id] = appendRowOnce(m.byAddress[id], index)
	}
	if refs != nil && row.refs != modelNone {
		for _, number := range refs.numbers() {
			key := pairKey(row.drive, number)
			if m.byPR[key] == nil {
				m.bytes += readModelKeyBytes
			}
			m.byPR[key] = appendRowOnce(m.byPR[key], index)
		}
	}
	if row.wrong {
		m.wrong = append(m.wrong, index)
	}
	if row.re != 0 && row.re < m.window.floor {
		if _, held := m.window.replyLanes[row.re]; !held {
			m.unnamed = append(m.unnamed, row.re)
		}
	}
	m.indexClosure(&row, kind, refs, also, index)
	m.held.Store(m.bytes)
	return nil
}

func (m *ReadModel) indexClosure(row *modelRow, kind string, refs *parsedRefs, also []int64, index int32) {
	if row.resolves != 0 {
		m.resolvedBy[row.resolves] = row.seq
		m.bytes += readModelKeyBytes
	}
	for _, target := range also {
		m.resolvedBy[target] = row.seq
		m.bytes += readModelKeyBytes
	}
	if m.strs[row.from] != m.source.Self() {
		m.foreign[row.originSeq] = append(m.foreign[row.originSeq], index)
		m.bytes += readModelKeyBytes
	}
	if !modelClosers[kind] {
		return
	}
	if row.re != 0 {
		m.linkCloser(modelCloserKey{row.drive, closerRe, row.re}, index)
	}
	if m.strs[row.topic] != "" {
		m.linkCloser(modelCloserKey{row.drive, closerTopic, int64(row.topic)}, index)
	}
	if refs == nil || row.refs == modelNone {
		return
	}
	for _, ref := range slices.Concat(refs.stacks, refs.targets) {
		m.linkCloser(modelCloserKey{row.drive, closerRef, int64(m.intern(ref))}, index)
	}
}

func (m *ReadModel) linkCloser(key modelCloserKey, index int32) {
	next, held := m.closerHead[key]
	if !held {
		next = modelNone
	}
	m.closerHead[key] = int32(len(m.closerLinks)) //nolint:gosec // G115: the links never reach 2^31 entries.
	m.closerLinks = append(m.closerLinks, modelCloser{row: index, next: next})
	m.bytes += readModelKeyBytes
}

func appendRowOnce(rows []int32, index int32) []int32 {
	if len(rows) > 0 && rows[len(rows)-1] == index {
		return rows
	}
	return append(rows, index)
}

func (m *ReadModel) internAddressees(raw string) int32 {
	if id, ok := m.addrIDs[raw]; ok {
		return id
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
		address = m.strs[m.intern(address)]
		parsed.to = append(parsed.to, address)
		parsed.all = parsed.all || address == "all"
	}
	parsed.to = slices.Clip(parsed.to)
	id := int32(len(m.addrs)) //nolint:gosec // G115: fewer distinct recipient lists than records.
	m.addrIDs[parsed.raw] = id
	m.addrs = append(m.addrs, parsed)
	m.bytes += int64(len(raw)) + int64(len(parsed.to))*16 + 2*readModelKeyBytes
	return id
}

func parsedModelRefs(raw string) *parsedRefs {
	if raw == "{}" {
		return nil
	}
	var keys map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &keys); err != nil || strings.Contains(strings.ToLower(raw), `\ud`) {
		return &parsedRefs{wrong: true}
	}
	for _, name := range [...]string{`"prs"`, `"stacks"`, `"targets"`} {
		if strings.Count(raw, name) > 1 {
			return &parsedRefs{wrong: true}
		}
	}
	refs := &parsedRefs{}
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

func (r *parsedRefs) parsePRs(raw json.RawMessage) bool {
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

func fitsRefPool(r *parsedRefs) bool {
	return !slices.ContainsFunc(r.ints, func(number int64) bool { return number < math.MinInt32 || number > math.MaxInt32 })
}

// numbers is every PR number the refs can be asked for, as canonical decimals
// a filter's number renders to.
func (r *parsedRefs) numbers() []int32 {
	out := make([]int32, 0, len(r.ints)+len(r.texts))
	for _, number := range r.ints {
		out = append(out, int32(number)) //nolint:gosec // G115: fitsRefPool held these to 32 bits.
	}
	for _, text := range r.texts {
		at := strings.LastIndexByte(text, '#')
		if at < 0 {
			continue
		}
		number, err := strconv.ParseInt(text[at+1:], 10, 32)
		if err == nil && strconv.FormatInt(number, 10) == text[at+1:] {
			out = append(out, int32(number))
		}
	}
	return out
}

func (m *ReadModel) storeRefs(r *parsedRefs) int32 {
	at := int32(len(m.refPool))                                                                                         //nolint:gosec // G115: the pool never reaches 2^31 entries.
	m.refPool = append(m.refPool, int32(len(r.ints)), int32(len(r.texts)), int32(len(r.stacks)), int32(len(r.targets))) //nolint:gosec // G115: refs hold far fewer than 2^31 entries.
	for _, number := range r.ints {
		m.refPool = append(m.refPool, int32(number)) //nolint:gosec // G115: fitsRefPool held these to 32 bits.
	}
	for _, list := range [...][]string{r.texts, r.stacks, r.targets} {
		for _, value := range list {
			m.refPool = append(m.refPool, m.intern(value))
		}
	}
	m.bytes += int64(4+len(r.ints)+len(r.texts)+len(r.stacks)+len(r.targets)) * 4
	return at
}

func (m *ReadModel) refsOf(r *modelRow) (modelRefs, bool) {
	if r.refs == modelNone {
		return modelRefs{}, false
	}
	pool := m.refPool[r.refs:]
	ints, texts, stacks, targets := pool[0], pool[1], pool[2], pool[3]
	pool = pool[4:]
	return modelRefs{
		ints:    pool[:ints:ints],
		texts:   pool[ints : ints+texts : ints+texts],
		stacks:  pool[ints+texts : ints+texts+stacks : ints+texts+stacks],
		targets: pool[ints+texts+stacks : ints+texts+stacks+targets : ints+texts+stacks+targets],
	}, true
}

func (m *ReadModel) alsoOf(r *modelRow) []int64 {
	if r.also == modelNone {
		return nil
	}
	n := m.alsoPool[r.also]
	return m.alsoPool[int64(r.also)+1 : int64(r.also)+1+n : int64(r.also)+1+n]
}

func (m *ReadModel) stringsOf(ids []int32) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = m.strs[id]
	}
	return out
}

func (m *ReadModel) view(r *modelRow) modelView {
	return modelView{row: r, drive: m.strs[r.drive], lane: m.strs[r.lane], kind: m.strs[r.kind], topic: m.strs[r.topic], source: m.strs[r.source], from: m.strs[r.from], addressees: m.addrs[r.addressees]}
}

func (m *ReadModel) place(row *modelRow, text *modelText) error {
	blob := m.scratch[:0]
	for _, field := range [...]string{text.id, text.text, text.refs, text.fields, text.body, text.also} {
		blob = append(binary.AppendUvarint(blob, uint64(len(field))), field...)
	}
	for _, number := range [...]sql.NullInt64{text.reOriginSeq, text.resolvesOriginSeq} {
		if number.Valid {
			blob = binary.AppendVarint(append(blob, 1), number.Int64)
		} else {
			blob = append(blob, 0)
		}
	}
	m.scratch = blob
	if len(m.chunks) == 0 || m.chunks[len(m.chunks)-1].used+len(blob) > len(m.chunks[len(m.chunks)-1].memory) {
		if err := m.growText(len(blob)); err != nil {
			return err
		}
	}
	chunk := &m.chunks[len(m.chunks)-1]
	if _, err := m.text.WriteAt(blob, chunk.offset+int64(chunk.used)); err != nil {
		return fmt.Errorf("write read model text: %w", err)
	}
	row.textAt = int64(len(m.chunks)-1)<<32 | int64(chunk.used)
	chunk.used += len(blob)
	m.textUsed += int64(len(blob))
	return nil
}

func (m *ReadModel) growText(blob int) error {
	var disk syscall.Statfs_t
	if err := syscall.Fstatfs(int(m.text.Fd()), &disk); err != nil { //nolint:gosec // G115: a file descriptor fits in an int.
		return fmt.Errorf("read the disk under read model text: %w", err)
	}
	if free := uint64(disk.Bavail) * uint64(disk.Bsize); free < readModelDiskFloor { //nolint:gosec,unconvert // G115: both fields are sizes; their types differ by platform.
		return fmt.Errorf("read model text: %d bytes free, under its floor of %d", free, readModelDiskFloor)
	}
	page := os.Getpagesize()
	size := (max(readModelChunk, blob) + page - 1) &^ (page - 1)
	if err := m.text.Truncate(m.textEnd + int64(size)); err != nil {
		return fmt.Errorf("grow read model text: %w", err)
	}
	memory, err := syscall.Mmap(int(m.text.Fd()), m.textEnd, size, syscall.PROT_READ, syscall.MAP_SHARED) //nolint:gosec // G115: a file descriptor fits in an int.
	if err != nil {
		return fmt.Errorf("map read model text: %w", err)
	}
	m.chunks = append(m.chunks, modelChunk{memory: memory, offset: m.textEnd})
	m.textEnd += int64(size)
	return nil
}

func openModelText(home string) (*os.File, error) {
	file, err := os.CreateTemp(home, "read-model-text-*")
	if err != nil {
		return nil, fmt.Errorf("create read model text: %w", err)
	}
	if err := os.Remove(file.Name()); err != nil {
		return nil, errors.Join(fmt.Errorf("unlink read model text: %w", err), file.Close())
	}
	return file, nil
}

func (m *ReadModel) textOf(r *modelRow) modelText {
	blob := m.chunks[r.textAt>>32].memory[r.textAt&math.MaxUint32:]
	field := func() string {
		n, read := binary.Uvarint(blob)
		value := unsafe.String(unsafe.SliceData(blob[read:]), n) //nolint:gosec // G103, G115: a view of this chunk, copied before a record leaves the model.
		blob = blob[uint64(read)+n:]                             //nolint:gosec // G115: read is a varint's length.
		return value
	}
	number := func() sql.NullInt64 {
		held := blob[0] == 1
		blob = blob[1:]
		if !held {
			return sql.NullInt64{}
		}
		value, read := binary.Varint(blob)
		blob = blob[read:]
		return sql.NullInt64{Int64: value, Valid: true}
	}
	text := modelText{id: field(), text: field(), refs: field(), fields: field(), body: field(), also: field()}
	text.reOriginSeq, text.resolvesOriginSeq = number(), number()
	return text
}

func (m *ReadModel) moveSearchFloor() {
	floor, since := m.window.floor, m.window.floorAt
	if first, _ := slices.BinarySearch(m.atHigh, m.source.Now().Add(-readModelSpan).UnixMilli()); first > 0 {
		floor, since = max(floor, m.rows[first-1].seq+1), max(since, m.atHigh[first-1])
	}
	m.searchFloor.Store(floor)
	m.searchSince.Store(since)
}

func (m *ReadModel) withinCap() error {
	m.moveSearchFloor()
	m.held.Store(m.bytes)
	if m.bytes-m.builtBytes > m.limit*(readModelRefillShare-readModelWindowShare)/100 {
		m.full.Store(true)
	}
	if m.bytes > m.limit {
		return fmt.Errorf("read model: %d rows hold %d bytes, over its cap of %d", len(m.rows), m.bytes, m.limit)
	}
	return nil
}

func (m *ReadModel) prepared(ctx context.Context, s *Store, f Filter) (Filter, bool) {
	if f.LaneFloor != "" {
		after, _, err := s.Cursor(ctx, f.LaneFloor, f.Drive)
		if err != nil {
			m.failures.Add(1)
			return f, false
		}
		f.broadcastAfter = &after
		if f.laneStart, _, err = s.Cursor(ctx, startCursor(f.LaneFloor), f.Drive); err != nil {
			m.failures.Add(1)
			return f, false
		}
		head, bound, err := s.Cursor(ctx, headCursor(f.LaneFloor), f.Drive)
		if err != nil {
			m.failures.Add(1)
			return f, false
		}
		f.laneHead = math.MaxInt64
		if bound {
			f.laneHead = head
			if f.backfills, f.backfillsAsOf, err = s.backfills(ctx, head); err != nil {
				m.failures.Add(1)
				return f, false
			}
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
	out, err := m.hydrated(s, chosen, projection)
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

// first is the seq of the first record f selects, 0 when there is none, false
// when only the store can judge.
func (m *ReadModel) first(ctx context.Context, s *Store, f Filter, now time.Time) (int64, bool) {
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
	var seq int64
	if !newModelSelection(m, s, f, now).walk(func(row *modelRow) bool {
		seq = row.seq
		return false
	}) {
		m.misses.Add(1)
		return 0, false
	}
	m.hits.Add(1)
	return seq, true
}

func (m *ReadModel) firstSince(at int64) sql.NullInt64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() || m.window.floor > 0 && at <= m.window.floorAt {
		return sql.NullInt64{}
	}
	first, _ := slices.BinarySearch(m.atHigh, at)
	if first == len(m.rows) {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: m.rows[first].seq, Valid: true}
}

func (f Filter) modelable() bool {
	if f.Trigrams != "" || f.PR.Number < math.MinInt32 || f.PR.Number > math.MaxInt32 {
		return false
	}
	for _, word := range f.Contains {
		if strings.Contains(word, "%") {
			return false
		}
	}
	return true
}

type modelSelection struct {
	m                         *ReadModel
	f                         Filter
	now                       int64
	forNames, forHooks        []string
	addressed, to             []string
	peers, readers            []string
	kindSet, muteSet, toKinds map[string]bool
	firstOnly                 map[string]bool
	orders                    map[string]bool
	wordsLower                [][]byte
	wordsNumeric              []bool
	since, until              int64
	broadcastAfter            int64
	laneStart, laneHead       int64
	laneBound                 bool
	urgent                    int32
	stack, target, repoPR     int32
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

func (m *ReadModel) idOf(s string) int32 {
	if id, ok := m.ids[s]; ok {
		return id
	}
	return modelNone
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
	sel.urgent, sel.stack, sel.target = m.idOf(string(kinds.Urgent)), m.idOf(f.Stack), m.idOf(f.Target)
	sel.repoPR = modelNone
	if f.PR.Number != 0 && f.PR.Repo != "" {
		sel.repoPR = m.idOf(f.PR.String())
	}
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
		sel.laneBound, sel.broadcastAfter, sel.laneStart, sel.laneHead = true, *f.broadcastAfter, f.laneStart, f.laneHead
	}
	return sel
}

func (sel *modelSelection) candidates() [][]int32 {
	m, f := sel.m, sel.f
	drive := m.idOf(f.Drive)
	byAddress := func(address string) []int32 { return m.byAddress[m.idOf(address)] }
	var lists [][]int32
	switch {
	case f.To != "":
		for _, address := range append(slices.Clone(sel.to), "all") {
			lists = append(lists, byAddress(address))
		}
	case f.PR.Number != 0:
		lists = append(lists, m.byPR[pairKey(drive, int32(f.PR.Number))]) //nolint:gosec // G115: modelable declines the rest.
	case f.For == "" && len(f.Lanes) > 0:
		for _, lane := range f.Lanes {
			lists = append(lists, m.byLane[pairKey(drive, m.idOf(lane))])
		}
	case f.For == "" && len(f.Kinds) > 0:
		for _, k := range f.Kinds {
			lists = append(lists, m.byKind[pairKey(drive, m.idOf(string(k)))])
		}
	default:
		lists = append(lists, m.byDrive[drive])
	}
	for _, address := range slices.Concat(sel.peers, sel.readers) {
		lists = append(lists, byAddress(address))
	}
	for _, follow := range f.Follow {
		followed := m.idOf(follow.Drive)
		for _, k := range follow.Kinds {
			lists = append(lists, m.byKind[pairKey(followed, m.idOf(string(k)))])
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
	complete := m.window.holdsFrom(f, sel.since)
	if !complete && !f.Descending {
		return false
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
			return complete
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
	if r.wrong {
		return false, false
	}
	v := sel.m.view(r)
	if !sel.scoped(&v) && !sel.peerBranch(&v) && !sel.readerBranch(&v) && !sel.followBranch(&v) {
		return false, true
	}
	if f.Origin != "" && v.from != f.Origin {
		return false, true
	}
	if f.Stack != "" || f.Target != "" || f.PR.Number != 0 {
		refs, held := sel.m.refsOf(r)
		if f.Stack != "" && (!held || !slices.Contains(refs.stacks, sel.stack)) {
			return false, true
		}
		if f.Target != "" && (!held || !slices.Contains(refs.targets, sel.target)) {
			return false, true
		}
		if f.PR.Number != 0 && (!held || !sel.namesPR(refs)) {
			return false, true
		}
	}
	if sel.muteSet[v.kind] {
		return false, true
	}
	if sel.firstOnly[v.kind] && f.To != "" && (len(v.addressees.to) == 0 || !slices.Contains(sel.to, v.addressees.to[0])) {
		refs, held := sel.m.refsOf(r)
		return false, v.kind != string(kinds.Review) || held && len(refs.ints)+len(refs.texts) == 0
	}
	if f.RepeatWindow > 0 && r.kind == sel.urgent && v.topic != "" {
		if sel.m.window.floor > 0 && r.at-f.RepeatWindow.Milliseconds() <= sel.m.window.floorAt {
			return false, false
		}
		if sel.repeats(r) {
			return false, true
		}
	}
	if sel.laneBound && r.seq > sel.laneHead && r.at < sel.laneStart {
		if r.seq > f.backfillsAsOf {
			return false, false
		}
		if slices.ContainsFunc(f.backfills, func(span [2]int64) bool { return r.seq > span[0] && r.seq <= span[1] }) {
			return false, true
		}
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
	if r.seq < sel.m.searchFloor.Load() {
		return false, false
	}
	text := sel.m.textOf(r)
	return sel.holdsAWord(&v, &text), true
}

func (sel *modelSelection) selectors(v *modelView) bool {
	f := sel.f
	return (len(f.Kinds) == 0 || sel.kindSet[v.kind]) &&
		(len(f.Lanes) == 0 || slices.Contains(f.Lanes, v.lane)) &&
		(len(f.Topics) == 0 || slices.Contains(f.Topics, v.topic))
}

func (sel *modelSelection) addressedKinds(v *modelView) bool {
	return len(sel.f.ToKinds) == 0 || sel.toKinds[v.kind]
}

func anyOf(values, names []string) bool {
	for _, value := range values {
		if slices.Contains(names, value) {
			return true
		}
	}
	return false
}

func (sel *modelSelection) addressedTo(v *modelView, names []string) bool {
	if anyOf(v.addressees.to, names) {
		return true
	}
	if v.row.re == 0 {
		return false
	}
	own := sel.m.rowOf(v.row.re)
	if own == nil {
		lane, held := sel.m.window.replyLanes[v.row.re]
		return held && slices.Contains(names, lane) && v.lane != lane
	}
	return slices.Contains(names, sel.m.strs[own.lane]) && v.row.lane != own.lane
}

func (sel *modelSelection) fresh(v *modelView) bool {
	return v.row.at >= sel.f.aliasSince || v.row.at >= sel.f.aliasOrders && sel.orders[v.kind]
}

func (sel *modelSelection) scoped(v *modelView) bool {
	f := sel.f
	if v.drive != f.Drive {
		return false
	}
	if f.For != "" {
		addressed := sel.addressedTo(v, sel.forNames) && sel.addressedKinds(v)
		broadcast := (v.addressees.raw == "[]" || v.addressees.all) &&
			!slices.Contains(sel.forNames, v.lane) && !slices.Contains(sel.forHooks, v.source) && sel.selectors(v)
		if !addressed && !broadcast {
			return false
		}
	} else if !sel.selectors(v) {
		return false
	}
	if f.Addressed != "" {
		if f.aliasSince > 0 && len(sel.addressed) > 1 {
			if !sel.addressedTo(v, sel.addressed[:1]) && (!sel.fresh(v) || !sel.addressedTo(v, sel.addressed[1:])) {
				return false
			}
		} else if !sel.addressedTo(v, sel.addressed) {
			return false
		}
	}
	return f.To == "" || sel.toMatches(v)
}

func (sel *modelSelection) toMatches(v *modelView) bool {
	f := sel.f
	var recipient bool
	if f.aliasSince == 0 || len(sel.to) < 2 {
		recipient = anyOf(v.addressees.to, sel.to)
	} else {
		recipient = anyOf(v.addressees.to, sel.to[:1]) || sel.fresh(v) && anyOf(v.addressees.to, sel.to[1:])
	}
	all := v.addressees.all && !slices.Contains(sel.to, v.lane)
	if all && sel.laneBound {
		all = v.row.seq > sel.broadcastAfter && v.row.at >= sel.laneStart
	}
	return (recipient || all) && sel.addressedKinds(v)
}

func (sel *modelSelection) peerBranch(v *modelView) bool {
	return len(sel.peers) > 0 && anyOf(v.addressees.to, sel.peers) && (sel.f.PeerAfter <= 0 || v.row.seq > sel.f.PeerAfter) && sel.addressedKinds(v)
}

func (sel *modelSelection) readerBranch(v *modelView) bool {
	if len(sel.readers) == 0 || !anyOf(v.addressees.to, sel.readers) {
		return false
	}
	if sel.f.For == "" && sel.f.LaneAliases && !sel.selectors(v) {
		return false
	}
	return sel.addressedKinds(v)
}

func (sel *modelSelection) followBranch(v *modelView) bool {
	for _, follow := range sel.f.Follow {
		if v.drive == follow.Drive && (v.addressees.raw == "[]" || v.addressees.all) && slices.Contains(follow.Kinds, kinds.Kind(v.kind)) {
			return true
		}
	}
	return false
}

func (sel *modelSelection) namesPR(refs modelRefs) bool {
	pr := sel.f.PR
	if slices.Contains(refs.ints, int32(pr.Number)) { //nolint:gosec // G115: modelable declines the rest.
		return true
	}
	if pr.Repo != "" {
		return slices.Contains(refs.texts, sel.repoPR)
	}
	suffix := "#" + strconv.Itoa(pr.Number)
	return slices.ContainsFunc(refs.texts, func(id int32) bool { return strings.HasSuffix(sel.m.strs[id], suffix) })
}

func (sel *modelSelection) repeats(r *modelRow) bool {
	window := sel.f.RepeatWindow.Milliseconds()
	for _, index := range sel.m.byKind[pairKey(r.drive, r.kind)] {
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

func (sel *modelSelection) holdsAWord(v *modelView, text *modelText) bool {
	columns := [...]string{text.text, v.lane, v.topic, v.kind, v.addressees.raw, text.refs, v.from}
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
			strconv.FormatInt(v.row.seq, 10), strconv.FormatInt(v.row.originSeq, 10), nullableNumber(text.reOriginSeq), nullableNumber(text.resolvesOriginSeq), text.also,
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
	wildFirst := lowerWord[0] == '_'
	for i := range len(hay) {
		if wildFirst && hay[i]&0xC0 == 0x80 {
			continue
		}
		if matchesFoldASCII(hay[i:], lowerWord) {
			return true
		}
	}
	return false
}

// matchesFoldASCII reports whether hay starts with word as LIKE reads it: ASCII
// folds, and _ takes one UTF-8 character.
func matchesFoldASCII(hay string, word []byte) bool {
	at := 0
	for _, want := range word {
		if at >= len(hay) {
			return false
		}
		if want == '_' {
			_, size := utf8.DecodeRuneInString(hay[at:])
			at += size
			continue
		}
		c := hay[at]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c != want {
			return false
		}
		at++
	}
	return true
}

func (m *ReadModel) hydrated(s *Store, chosen []*modelRow, projection string) ([]Record, error) {
	if len(chosen) == 0 {
		return nil, nil
	}
	out := make([]Record, len(chosen))
	for i, row := range chosen {
		text := m.textOf(row)
		record, err := m.record(s, row, &text, projection)
		if err != nil {
			return nil, err
		}
		out[i] = record
	}
	return out, nil
}

func (m *ReadModel) record(s *Store, row *modelRow, text *modelText, projection string) (Record, error) {
	meta, err := m.metaOf(row.seq, text)
	if err != nil {
		return Record{}, err
	}
	v := m.view(row)
	r := Record{
		Seq: row.seq, Origin: s.local(v.from), OriginSeq: row.originSeq, ID: strings.Clone(text.id), Drive: v.drive, Lane: v.lane, Kind: kinds.Kind(v.kind),
		At: time.UnixMilli(row.at).UTC(), Text: strings.Clone(text.text), Topic: v.topic, To: v.addressees.to, Re: row.re, Resolves: row.resolves, AlsoResolves: m.alsoOf(row),
		Refs: meta.refs, Fields: meta.fields, Source: v.source,
	}
	if r.Origin == "" {
		r.OriginSeq = 0
	}
	if projection != textColumns && text.body != "" {
		r.Body = json.RawMessage(text.body)
	}
	if row.expires != modelNoExpiry {
		expires := time.UnixMilli(row.expires).UTC()
		r.ExpiresAt = &expires
	}
	return r, nil
}

func (t *modelText) decode(seq int64) (*modelMeta, error) {
	meta := &modelMeta{seq: seq}
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
	refs := &meta.refs
	refs.PRs, refs.Builds, refs.Stacks, refs.Targets, refs.Lanes = slices.Clip(refs.PRs), slices.Clip(refs.Builds), slices.Clip(refs.Stacks), slices.Clip(refs.Targets), slices.Clip(refs.Lanes)
	meta.fields.Envs = slices.Clip(meta.fields.Envs)
	return meta, nil
}

func (m *ReadModel) metaOf(seq int64, text *modelText) (*modelMeta, error) {
	slot := &m.metas[uint64(seq)%readModelMetaSlots] //nolint:gosec // G115: seqs are positive.
	if meta := slot.Load(); meta != nil && meta.seq == seq {
		return meta, nil
	}
	meta, err := text.decode(seq)
	if err != nil {
		return nil, err
	}
	if m.keepMetas && len(text.refs)+len(text.fields) <= readModelMetaHeld {
		slot.Store(meta)
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

func (m *ReadModel) closure(r *modelRow) Record {
	record := Record{Seq: r.seq, Lane: m.strs[r.lane], Kind: kinds.Kind(m.strs[r.kind]), At: time.UnixMilli(r.at).UTC(), Topic: m.strs[r.topic], Re: r.re, Resolves: r.resolves, AlsoResolves: m.alsoOf(r), Source: m.strs[r.source]}
	if refs, held := m.refsOf(r); held {
		record.Refs.Stacks, record.Refs.Targets = m.stringsOf(refs.stacks), m.stringsOf(refs.targets)
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
	if m.stale.Load() {
		return nil, false
	}
	chosen := make([]*modelRow, 0, len(open))
	for _, seq := range open {
		row := m.rowOf(seq)
		if row == nil {
			return nil, false
		}
		chosen = append(chosen, row)
	}
	out, err := m.hydrated(s, chosen, columns)
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
	if m.stale.Load() || len(m.wrong) > 0 || m.window.wrong {
		m.misses.Add(1)
		return nil, false
	}
	seqs = slices.Compact(slices.Sorted(slices.Values(seqs)))
	openers := make([]Record, 0, len(seqs))
	for _, seq := range seqs {
		if seq < m.window.floor {
			if opener, held := m.window.pinned[seq]; held && m.window.pinnedDrive[seq] == drive && opener.Tracked() {
				openers = append(openers, opener)
			}
			continue
		}
		if row := m.rowOf(seq); row != nil && m.strs[row.drive] == drive && modelOpeners[m.strs[row.kind]] {
			if opener := m.closure(row); opener.Tracked() {
				openers = append(openers, opener)
			}
		}
	}
	open, err := m.unclosedLocked(ctx, s, drive, openers)
	if errors.Is(err, errReadModelBelow) {
		m.misses.Add(1)
		return nil, false
	}
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
	openers, err := m.window.heldOpeners(drive, openers)
	if err != nil {
		return nil, err
	}
	openers = slices.DeleteFunc(openers, func(o Record) bool {
		return o.Kind == kinds.Incident && slices.ContainsFunc(incidentTargets(o), func(seq int64) bool {
			if seq < m.window.floor {
				return m.window.incidents[seq] == drive
			}
			row := m.rowOf(seq)
			return row != nil && m.strs[row.drive] == drive && m.strs[row.kind] == string(kinds.Incident)
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
	if m.stale.Load() || len(m.wrong) > 0 || m.window.wrong {
		m.misses.Add(1)
		return nil, false
	}
	open, err := m.unclosedLocked(ctx, s, drive, openers)
	if errors.Is(err, errReadModelBelow) {
		m.misses.Add(1)
		return nil, false
	}
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
	if m.stale.Load() || len(m.wrong) > 0 || m.window.wrong || m.window.floor > 0 && !m.window.sameOpeners(ks) {
		m.misses.Add(1)
		return nil, false
	}
	var indexes []int32
	held := m.idOf(drive)
	for _, k := range ks {
		indexes = append(indexes, m.byKind[pairKey(held, m.idOf(string(k)))]...)
	}
	slices.Sort(indexes)
	out := m.window.pinnedIn(drive)
	for _, index := range indexes {
		out = append(out, m.closure(&m.rows[index]))
	}
	m.hits.Add(1)
	return out, true
}

func (m *ReadModel) closersOf(drive string, openers []Record) []Record {
	held := m.idOf(drive)
	seen := map[int32]bool{}
	var out []Record
	add := func(key modelCloserKey) {
		link, ok := m.closerHead[key]
		for ok && link != modelNone {
			entry := m.closerLinks[link]
			if !seen[entry.row] {
				seen[entry.row] = true
				out = append(out, m.closure(&m.rows[entry.row]))
			}
			link = entry.next
		}
	}
	for _, o := range openers {
		add(modelCloserKey{held, closerRe, o.Seq})
		if o.Topic != "" {
			if topic := m.idOf(o.Topic); topic != modelNone {
				add(modelCloserKey{held, closerTopic, int64(topic)})
			}
		}
		for _, ref := range slices.Concat(o.Refs.Stacks, o.Refs.Targets) {
			if id := m.idOf(ref); id != modelNone {
				add(modelCloserKey{held, closerRef, int64(id)})
			}
		}
	}
	return out
}

// otherNumbered is the record of another origin that #n also names in its
// drive, nil when none does; false when only the store can say which is first.
func (m *ReadModel) otherNumbered(ctx context.Context, n int64) (*modelView, bool) {
	if err := m.catchUp(ctx); err != nil {
		m.failures.Add(1)
		return nil, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stale.Load() {
		return nil, false
	}
	if m.window.floor > 0 && (n < m.window.floor || m.window.foreignNumbers[n]) {
		return nil, false
	}
	own := m.rowOf(n)
	if own == nil {
		return nil, true
	}
	if m.strs[own.from] != m.source.Self() {
		return nil, false
	}
	var other *modelView
	for _, index := range m.foreign[n] {
		row := &m.rows[index]
		if row.drive != own.drive || row.from == own.from {
			continue
		}
		if other != nil {
			return nil, false
		}
		v := m.view(row)
		other = &v
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
		if seq > m.head || seq < m.window.floor {
			return nil, nil, false
		}
		if row := m.rowOf(seq); row != nil {
			out[seq] = s.name(owners, Record{Seq: row.seq, Origin: s.local(m.strs[row.from]), OriginSeq: row.originSeq})
		}
	}
	return owners, out, true
}
