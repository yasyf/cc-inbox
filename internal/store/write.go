package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// WritePage is how many records a paged write stores, folds or settles in
// one transaction, and how many a writer's batch gathers before it commits.
const WritePage = 64

// Write is the mutations one caller commits together, applied in field
// order: touches, retirements, the lane binding, records, a page of rows from
// other nodes, a backfill page, a settle page, a page of imported lines, a
// reparse page, a compaction fold, then cursor moves. A Write whose turn
// comes after StartBy applies nothing and fails with ErrExpired.
type Write struct {
	Touch    []PeerKey     `json:"touch,omitempty"`
	Retire   []PeerKey     `json:"retire,omitempty"`
	Bind     *LaneBinding  `json:"bind,omitempty"`
	Records  []Record      `json:"records,omitempty"`
	TTL      time.Duration `json:"ttl,omitempty"`
	At       time.Time     `json:"at,omitzero"`
	Apply    []Row         `json:"apply,omitempty"`
	Receipt  *Receipt      `json:"receipt,omitempty"`
	Backfill []Row         `json:"backfill,omitempty"`
	Settle   *SettlePage   `json:"settle,omitempty"`
	Ingest   []Ingestion   `json:"ingest,omitempty"`
	Reparse  *ReparsePage  `json:"reparse,omitempty"`
	Fold     *FoldPage     `json:"fold,omitempty"`
	Cursors  []CursorMove  `json:"cursors,omitempty"`
	StartBy  time.Time     `json:"start_by,omitzero"`
}

// SettlePage releases a page of the rows pending holds for Waiting, storing a
// pointer to a record in Absent unresolved.
type SettlePage struct {
	Waiting []Pointer `json:"waiting"`
	Absent  []Pointer `json:"absent"`
}

// ReparsePage rewrites a page of Items, carrying From across pages. More says
// Items stops short of the input: a page that would end there while a split
// line may still follow applies nothing and consumes no item, so the caller
// sends a longer page.
type ReparsePage struct {
	Items []Ingestion `json:"items"`
	Fresh bool        `json:"fresh,omitempty"`
	More  bool        `json:"more,omitempty"`
	From  Reparsing   `json:"from"`
}

// FoldPage folds the Records of Day still stored on Drive into that day's
// digest.
type FoldPage struct {
	Drive   string   `json:"drive"`
	Day     string   `json:"day"`
	Records []Record `json:"records"`
}

// Written is what one Write stored: each record it posted, or the stored
// record that record duplicated, and what its pages applied: rows Applied,
// pointers still Waiting after a settle page, lines Ingested, the Reparsed
// progress and the Items it consumed, and the records Folded.
type Written struct {
	Posted   []Posted   `json:"posted,omitempty"`
	Applied  Applied    `json:"applied,omitzero"`
	Waiting  []Pointer  `json:"waiting,omitempty"`
	Ingested int        `json:"ingested,omitempty"`
	Reparsed *Reparsing `json:"reparsed,omitempty"`
	Items    int        `json:"items,omitempty"`
	Folded   int        `json:"folded,omitempty"`
}

// Rows is how many records w stores, folds or settles at most, the weight a
// writer's batch counts toward WritePage.
func (w Write) Rows() int {
	n := len(w.Records) + len(w.Apply) + len(w.Backfill) + len(w.Ingest)
	if w.Settle != nil {
		n += WritePage
	}
	if w.Reparse != nil {
		n += len(w.Reparse.Items)
	}
	if w.Fold != nil {
		n += len(w.Fold.Records)
	}
	return n
}

// Writer commits a Write in the process that owns this store's writes.
type Writer interface {
	Commit(ctx context.Context, w Write) (Written, error)
}

var (
	// ErrUnrouted reports a Writer that applied nothing and cannot take the
	// write, so the store applies it itself.
	ErrUnrouted = errors.New("cci write reached no writer")
	// ErrExpired reports a Write whose turn came after its StartBy: none of it
	// was applied.
	ErrExpired = fmt.Errorf("%w: the write's turn came after its deadline", ErrBusy)
	// ErrUnconfirmed reports a Writer that cannot tell whether it applied the
	// write: it may land, so a retry must be one that deduplicates.
	ErrUnconfirmed = errors.New("cci write was not confirmed")
)

// WriteThrough sends every later Commit to w; a write w answers ErrUnrouted
// is applied here instead.
func (s *Store) WriteThrough(w Writer) {
	s.writer = w
}

// Dedicated opens another handle on s's database for one writer, whose lock
// waits give up after busy so that CommitAll retries them under the caller's
// context instead of SQLite's busy timeout.
func (s *Store) Dedicated(busy time.Duration) (*Store, error) {
	db, err := sql.Open("sqlite", storeDSN(s.home, int(busy.Milliseconds())))
	if err != nil {
		return nil, fmt.Errorf("open writer: %w", err)
	}
	return &Store{db: db, rw: db, home: s.home, node: s.node, owner: s.owner, Now: s.Now, floors: s.floors, reads: s.reads}, nil
}

// Commit writes the bodies of w's relayed rows to this node's blobs, then
// applies w in one transaction, through the store's Writer when it has one,
// within ctx's deadline or ClientBudget when ctx has none.
func (s *Store) Commit(ctx context.Context, w Write) (Written, error) {
	if err := w.check(); err != nil {
		return Written{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, ClientBudget)
		defer cancel()
	}
	var err error
	if w.Apply, err = s.blobbed(w.Apply); err != nil {
		return Written{}, err
	}
	if w.Backfill, err = s.blobbed(w.Backfill); err != nil {
		return Written{}, err
	}
	if s.writer != nil {
		done, err := s.writer.Commit(ctx, w)
		if !errors.Is(err, ErrUnrouted) {
			return done, err
		}
	}
	done, errs := s.CommitAll(ctx, []Write{w})
	return done[0], errs[0]
}

// CommitAll applies ws here, in one transaction, each under a savepoint of
// its own: a write that fails a check, names a record that is not there, or
// breaks a constraint rolls back alone with its own error while the others
// commit. Any other failure, the commit's included, fails every write and
// applies none.
func (s *Store) CommitAll(ctx context.Context, ws []Write) ([]Written, []error) {
	done := make([]Written, len(ws))
	errs := make([]error, len(ws))
	failAll := func(err error) ([]Written, []error) {
		for i := range errs {
			errs[i] = err
		}
		return make([]Written, len(ws)), errs
	}
	tx, err := s.rw.BeginTx(ctx, nil)
	for Busy(err) && ctx.Err() == nil {
		tx, err = s.rw.BeginTx(ctx, nil)
	}
	if err != nil {
		if ctx.Err() != nil {
			return failAll(fmt.Errorf("%w: begin write: %w", ErrBusy, err))
		}
		return failAll(fmt.Errorf("begin write: %w", err))
	}
	defer func() { _ = tx.Rollback() }()
	for i, w := range ws {
		if !w.StartBy.IsZero() && time.Now().After(w.StartBy) {
			errs[i] = ErrExpired
			continue
		}
		if errs[i] = w.check(); errs[i] != nil {
			continue
		}
		if _, err := tx.ExecContext(ctx, "SAVEPOINT write"); err != nil {
			return failAll(fmt.Errorf("begin write %d: %w", i, err))
		}
		if done[i], errs[i] = s.write(ctx, tx, w); errs[i] != nil {
			if !contained(errs[i]) {
				return failAll(errs[i])
			}
			done[i] = Written{}
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO write"); err != nil {
				return failAll(fmt.Errorf("roll back write %d: %w", i, err))
			}
		}
		if _, err := tx.ExecContext(ctx, "RELEASE write"); err != nil {
			return failAll(fmt.Errorf("end write %d: %w", i, err))
		}
	}
	if err := tx.Commit(); err != nil {
		return failAll(fmt.Errorf("commit write: %w", err))
	}
	return done, errs
}

// contained reports whether ROLLBACK TO undoes err alone: SQLite may end the
// whole transaction on any of its own errors but a constraint violation.
func contained(err error) bool {
	var failed *sqlite.Error
	return !errors.As(err, &failed) || failed.Code()&0xff == sqlite3.SQLITE_CONSTRAINT
}

func (w Write) check() error {
	for _, r := range w.Records {
		if r.Kind.Spec().Internal {
			return fmt.Errorf("kind %s is written only by cci itself", r.Kind)
		}
		if err := Validate(r); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) write(ctx context.Context, tx *sql.Tx, w Write) (Written, error) {
	now := s.Now().UTC().UnixMilli()
	for _, k := range w.Touch {
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET seen_at = ? WHERE session = ? AND agent = ?", now, k.Session, k.Agent); err != nil {
			return Written{}, fmt.Errorf("touch %s: %w", k, err)
		}
	}
	for _, k := range w.Retire {
		if _, err := tx.ExecContext(ctx, "UPDATE sessions SET superseded = 1, seen_at = ? WHERE session = ? AND agent = ?", now, k.Session, k.Agent); err != nil {
			return Written{}, fmt.Errorf("retire %s: %w", k, err)
		}
	}
	if b := w.Bind; b != nil {
		if err := s.bindLane(ctx, tx, *b); err != nil {
			return Written{}, err
		}
	}
	var done Written
	for _, r := range w.Records {
		posted, duplicate, err := s.post(ctx, tx, r, w.TTL, w.At)
		if err != nil {
			return Written{}, err
		}
		done.Posted = append(done.Posted, Posted{Record: posted, Duplicate: duplicate})
	}
	if len(w.Apply) > 0 || w.Receipt != nil {
		applied, err := s.applyRows(ctx, tx, w.Apply, w.Receipt)
		if err != nil {
			return Written{}, err
		}
		done.Applied.add(applied)
	}
	if len(w.Backfill) > 0 {
		applied, span, err := s.backfillRows(ctx, tx, w.Backfill)
		if err != nil {
			return Written{}, err
		}
		done.Applied.add(applied)
		w.Cursors = append(slices.Clone(w.Cursors), span...)
	}
	if w.Settle != nil {
		absent := make(map[Pointer]bool, len(w.Settle.Absent))
		for _, p := range w.Settle.Absent {
			absent[p] = true
		}
		waiting, err := s.settlePage(ctx, tx, slices.Clone(w.Settle.Waiting), absent, &done.Applied)
		if err != nil {
			return Written{}, err
		}
		done.Waiting = waiting
	}
	if len(w.Ingest) > 0 {
		ingested, err := s.ingestPage(ctx, tx, w.Ingest)
		if err != nil {
			return Written{}, err
		}
		done.Ingested = ingested
	}
	if p := w.Reparse; p != nil {
		if _, err := tx.ExecContext(ctx, "SAVEPOINT reparse"); err != nil {
			return Written{}, fmt.Errorf("begin reparse page: %w", err)
		}
		reparsed := p.From
		items, err := s.reparsePage(ctx, tx, p.Items, p.Fresh, &reparsed)
		if err != nil {
			return Written{}, err
		}
		if p.More && items == len(p.Items) && !p.Fresh && reparsed.ParentText != "" {
			if _, err := tx.ExecContext(ctx, "ROLLBACK TO reparse"); err != nil {
				return Written{}, fmt.Errorf("roll back reparse page: %w", err)
			}
			items, reparsed = 0, p.From
		}
		if _, err := tx.ExecContext(ctx, "RELEASE reparse"); err != nil {
			return Written{}, fmt.Errorf("end reparse page: %w", err)
		}
		done.Reparsed, done.Items = &reparsed, items
	}
	if p := w.Fold; p != nil {
		folded, err := s.fold(ctx, tx, p.Drive, p.Day, p.Records)
		if err != nil {
			return Written{}, err
		}
		done.Folded = folded
	}
	for _, m := range w.Cursors {
		set := "MAX(seq, excluded.seq)"
		if m.Exact {
			set = "excluded.seq"
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) VALUES (?, ?, ?)
ON CONFLICT (name, drive) DO UPDATE SET seq = `+set, m.Name, m.Drive, m.Seq); err != nil {
			return Written{}, fmt.Errorf("move cursor %s: %w", m.Name, err)
		}
	}
	return done, nil
}
