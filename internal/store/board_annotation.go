package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
)

const boardColdEffectsSQL = `INSERT INTO board_query_effects(scope,source_seq,drive,axis,key)
SELECT ?1,source_seq,drive,axis,key FROM record_closer_keys WHERE source_seq>?2 AND source_seq<=?3
UNION SELECT ?1,seq,'','self',CAST(seq AS TEXT) FROM records WHERE seq>?2 AND seq<=?3
UNION SELECT ?1,seq,'','incident',CAST(seq AS TEXT) FROM records WHERE seq>?2 AND seq<=?3
UNION SELECT ?1,seq,'','resolve',CAST(resolves AS TEXT) FROM records WHERE seq>?2 AND seq<=?3 AND resolves!=0
UNION SELECT ?1,source_seq,'','resolve',CAST(target AS TEXT) FROM record_also_resolves WHERE source_seq>?2 AND source_seq<=?3`

func boardOpenerKeys(ctx context.Context, tx *sql.Tx, scope *boardScope, category int, record Record) error {
	if !record.Kind.Opens() || !record.Tracked() {
		return nil
	}
	keys := map[[3]string]bool{}
	add := func(drive, axis, key string) { keys[[3]string{drive, axis, key}] = true }
	add("", "self", fmt.Sprint(record.Seq))
	add("", "resolve", fmt.Sprint(record.Seq))
	add(record.Drive, "re", fmt.Sprint(record.Seq))
	if record.Topic != "" {
		add(record.Drive, "topic", fmt.Sprintf("%X", []byte(record.Topic)))
	}
	for _, key := range record.Refs.Stacks {
		add(record.Drive, "ref", key)
	}
	for _, key := range record.Refs.Targets {
		add(record.Drive, "ref", key)
	}
	if len(record.Refs.Stacks) != 0 || len(record.Refs.Targets) != 0 {
		add(record.Drive, "ref-ambiguous", "")
	}
	if string(record.Kind) == "incident" {
		for _, target := range incidentTargets(record) {
			add("", "incident", fmt.Sprint(target))
		}
	}
	for key := range keys {
		if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_openers(scope,category,seq,drive,axis,key) VALUES(?,?,?,?,?,?)", scope.id, category, record.Seq, key[0], key[1], key[2]); err != nil {
			return err
		}
	}
	return nil
}

func boardChangedEffects(ctx context.Context, tx *sql.Tx, scope *boardScope, seq int64, affected map[[2]int64]bool) error {
	collect := func() error {
		rows, err := tx.QueryContext(ctx, "SELECT DISTINCT o.category,o.seq FROM board_query_effects e CROSS JOIN board_query_openers o ON o.scope=e.scope AND o.drive=e.drive AND o.axis=e.axis AND o.key=e.key WHERE e.scope=? AND e.source_seq=?", scope.id, seq)
		if err != nil {
			return err
		}
		for rows.Next() {
			var category, opener int64
			if err := rows.Scan(&category, &opener); err != nil {
				_ = rows.Close()
				return err
			}
			affected[[2]int64{category, opener}] = true
		}
		return errors.Join(rows.Err(), rows.Close())
	}
	if err := collect(); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM board_query_effects WHERE scope=? AND source_seq=?", scope.id, seq); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO board_query_effects(scope,source_seq,drive,axis,key)
SELECT ?,source_seq,drive,axis,key FROM record_closer_keys WHERE source_seq=?
UNION SELECT ?,seq,'','self',CAST(seq AS TEXT) FROM records WHERE seq=?
UNION SELECT ?,seq,'','incident',CAST(seq AS TEXT) FROM records WHERE seq=?
UNION SELECT ?,seq,'','resolve',CAST(resolves AS TEXT) FROM records WHERE seq=? AND resolves!=0
UNION SELECT ?,source_seq,'','resolve',CAST(target AS TEXT) FROM record_also_resolves WHERE source_seq=?`, scope.id, seq, scope.id, seq, scope.id, seq, scope.id, seq, scope.id, seq); err != nil {
		return err
	}
	return collect()
}

func boardAnnotateErrors(ctx context.Context, tx *sql.Tx, st *Store, scope *boardScope, affected map[[2]int64]bool) error {
	keys := make([][2]int64, 0, len(affected))
	for key := range affected {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b [2]int64) int {
		if a[0] != b[0] {
			return int(a[0] - b[0])
		}
		if a[1] < b[1] {
			return -1
		}
		if a[1] > b[1] {
			return 1
		}
		return 0
	})
	snapshot := &BoardSnapshot{store: st, tx: tx, now: st.Now()}
	for _, key := range keys {
		var drive string
		err := tx.QueryRowContext(ctx, "SELECT r.drive FROM board_query_rows b JOIN records r ON r.seq=b.seq WHERE b.scope=? AND b.category=? AND b.seq=? AND b.error=''", scope.id, key[0], key[1]).Scan(&drive)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return err
		}
		_, annotationErr := snapshot.OpenAmong(ctx, drive, []int64{key[1]})
		message := ""
		if annotationErr != nil {
			message = annotationErr.Error()
		}
		if _, err := tx.ExecContext(ctx, "UPDATE board_query_rows SET annotation_error=? WHERE scope=? AND category=? AND seq=?", message, scope.id, key[0], key[1]); err != nil {
			return err
		}
	}
	return nil
}

// boardColdMatches returns nil when a malformed row fails the scan, so the
// per-record probes pin that failure to its own row.
func boardColdMatches(ctx context.Context, tx *sql.Tx, st *Store, filter Filter, after, last int64) map[int64]bool {
	filter.IncludeExpired = true
	where, params := filter.where(st.Self(), st.Now())
	rows, err := tx.QueryContext(ctx, "SELECT seq FROM records WHERE seq>? AND seq<=? AND ("+where+")", append([]any{after, last}, params...)...)
	if err != nil {
		return nil
	}
	matches := map[int64]bool{}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			_ = rows.Close()
			return nil
		}
		matches[seq] = true
	}
	if errors.Join(rows.Err(), rows.Close()) != nil {
		return nil
	}
	return matches
}

// boardAnnotateColdErrors reads one drive's closure at once. A batch that
// succeeds proves every row in it, since each row reads a subset of the batch.
func boardAnnotateColdErrors(ctx context.Context, tx *sql.Tx, st *Store, scope *boardScope, opened map[string][]int64) error {
	snapshot := &BoardSnapshot{store: st, tx: tx, now: st.Now()}
	for drive, seqs := range opened {
		if _, err := snapshot.OpenAmong(ctx, drive, seqs); err == nil {
			continue
		}
		for _, seq := range seqs {
			_, annotationErr := snapshot.OpenAmong(ctx, drive, []int64{seq})
			if annotationErr == nil {
				continue
			}
			if _, err := tx.ExecContext(ctx, "UPDATE board_query_rows SET annotation_error=? WHERE scope=? AND "+everyBoardCategory+" AND seq=? AND error=''", annotationErr.Error(), scope.id, seq); err != nil {
				return err
			}
		}
	}
	return nil
}

func boardRootEligible(scope *boardScope, record Record) bool {
	if record.Drive != scope.spec.Drive {
		return false
	}
	switch string(record.Kind) {
	case "go", "decide", "decision", "answer":
	default:
		return false
	}
	return boardDelegated(record.Lane, append([]string{"root"}, scope.spec.GoLanes...))
}

func boardRefreshClock(ctx context.Context, tx *sql.Tx, scope string, now int64) (bool, error) {
	var before int64
	if err := tx.QueryRowContext(ctx, "SELECT clock_ms FROM board_query_scopes WHERE id=?", scope).Scan(&before); err != nil {
		return false, err
	}
	if before == now {
		return false, nil
	}
	low, high := min(before, now), max(before, now)
	rows, err := tx.QueryContext(ctx, "SELECT category,seq FROM board_query_rows WHERE scope=? AND expires_at>? AND expires_at<=? ORDER BY expires_at,category,seq", scope, low, high)
	if err != nil {
		return false, err
	}
	var changed [][2]int64
	for rows.Next() {
		var key [2]int64
		if err := rows.Scan(&key[0], &key[1]); err != nil {
			_ = rows.Close()
			return false, err
		}
		changed = append(changed, key)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return false, err
	}
	if len(changed) == 0 {
		return false, nil
	}
	for _, key := range changed {
		if _, err := tx.ExecContext(ctx, "UPDATE board_query_rows SET active=(expires_at>?) WHERE scope=? AND category=? AND seq=?", now, scope, key[0], key[1]); err != nil {
			return false, err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE board_query_scopes SET clock_ms=? WHERE id=?", now, scope)
	return true, err
}
