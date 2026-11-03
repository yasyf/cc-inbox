package store

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func boardFixture(t *testing.T) (*Store, *BoardIndex, BoardSpec, string) {
	t.Helper()
	st, err := Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.InstallBoardProjection(t.Context()); err != nil {
		t.Fatal(err)
	}
	index := NewBoardIndex(st)
	t.Cleanup(func() { _ = index.Close() })
	peer := "board-session/agent"
	spec := BoardSpec{Drive: "d", Repo: "owner/repo", CWD: t.TempDir(), GoLanes: []string{"go-lane"}, Filters: [3]Filter{
		{Drive: "d", Peer: peer, Kinds: []kinds.Kind{kinds.Go, kinds.Decide, kinds.Decision, kinds.Answer}},
		{Drive: "d", Peer: peer, Kinds: []kinds.Kind{kinds.Head, kinds.Review}},
		{Drive: "d", Peer: peer, Kinds: []kinds.Kind{kinds.Withdraw}, Lanes: []string{"root"}},
	}}
	return st, index, spec, peer
}

func TestBoardProjectionBodyAndCrossDriveWithdrawal(t *testing.T) {
	st, index, spec, peer := boardFixture(t)
	path := filepath.Join(spec.CWD, "ruling")
	if err := os.WriteFile(path, []byte("close #123\r\nmerge-row-1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	ruling, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "short #123", Refs: Refs{Path: path}, To: []string{PeerAddress(peer)}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	withdrawn, _, err := st.Post(t.Context(), Record{Drive: "other", Lane: "root", Kind: kinds.Withdraw, Text: "withdraw", Resolves: ruling.Seq, To: []string{PeerAddress(peer)}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}, RowIDs: []string{"merge-row-123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
		if result.Rulings.Error != nil || len(result.Rulings.Records) != 1 || result.Rulings.Records[0].Seq != ruling.Seq {
			t.Fatalf("rulings=%+v", result.Rulings)
		}
		if string(result.Rulings.Bodies[ruling.Seq]) != "close #123\r\nmerge-row-1234" {
			t.Fatal("raw body changed")
		}
		if len(result.Withdrawals.Records) != 1 || result.Withdrawals.Records[0].Seq != withdrawn.Seq || result.Withdrawals.Records[0].Drive != "other" {
			t.Fatalf("withdrawals=%+v", result.Withdrawals)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoardColdProvisionMatchesIncrementalProjection(t *testing.T) {
	st, index, spec, peer := boardFixture(t)
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(spec.CWD, "ruling")
	if err := os.WriteFile(path, []byte("close #123\r\nmerge-row-1234"), 0o600); err != nil {
		t.Fatal(err)
	}
	post := func(record Record) Record {
		t.Helper()
		posted, _, err := st.Post(t.Context(), record, 0)
		if err != nil {
			t.Fatal(err)
		}
		return posted
	}
	ruling := post(Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "short #123", Refs: Refs{Path: path}, To: []string{PeerAddress(peer)}})
	opener := post(Record{Drive: "d", Lane: "root", Kind: kinds.Decide, Text: "decide #124", Refs: Refs{Targets: []string{"service"}}})
	post(Record{Drive: "d", Lane: "root", Kind: kinds.Decide, Text: "healthy #125", Topic: "healthy"})
	closer := post(Record{Drive: "d", Lane: "root", Kind: kinds.Answer, Text: "answer", Re: opener.Seq})
	post(Record{Drive: "d", Lane: "builder", Kind: kinds.Head, Text: "head", Refs: Refs{PRs: []PR{{Repo: "owner/repo", Number: 123}}}})
	post(Record{Drive: "foreign", Lane: "root", Kind: kinds.Decide, Text: "foreign #126", To: []string{PeerAddress(peer)}})
	post(Record{Drive: "other", Lane: "root", Kind: kinds.Withdraw, Text: "withdraw", Resolves: ruling.Seq, To: []string{PeerAddress(peer)}})
	post(Record{Drive: "d", Lane: "noise", Kind: kinds.Note, Text: "unselected #127"})
	malformed := post(Record{Drive: "d", Lane: "noise", Kind: kinds.Note, Text: "malformed recipients"})
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=?", closer.Seq); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET recipients='[' WHERE seq=?", malformed.Seq); err != nil {
		t.Fatal(err)
	}
	if err := index.Read(t.Context(), st, spec, BoardSelection{}, func(*BoardSnapshot, *BoardResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	incremental := boardProjectionRows(t, st)
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	cold := boardProjectionRows(t, st)
	if !slices.Equal(cold, incremental) {
		t.Fatalf("cold projection differs:\ncold=%q\nincremental=%q", cold, incremental)
	}
	var malformedRows, annotatedRows int
	if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM board_query_rows WHERE seq=? AND error!=''", malformed.Seq).Scan(&malformedRows); err != nil {
		t.Fatal(err)
	}
	if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM board_query_rows WHERE annotation_error!=''").Scan(&annotatedRows); err != nil {
		t.Fatal(err)
	}
	if malformedRows != len(spec.Filters) || annotatedRows != 1 {
		t.Fatalf("malformed rows=%d annotated rows=%d", malformedRows, annotatedRows)
	}
}

func boardProjectionRows(t *testing.T, st *Store) []string {
	t.Helper()
	var out []string
	for _, query := range []string{
		"SELECT category||'|'||seq||'|'||path||'|'||lane||'|'||IFNULL(expires_at,'')||'|'||error||'|'||annotation_error||'|'||active FROM board_query_rows ORDER BY category,seq",
		"SELECT category||'|'||axis||'|'||key||'|'||seq FROM board_query_keys ORDER BY category,axis,key,seq",
		"SELECT category||'|'||seq||'|'||drive||'|'||axis||'|'||key FROM board_query_openers ORDER BY category,seq,drive,axis,key",
		"SELECT source_seq||'|'||drive||'|'||axis||'|'||key FROM board_query_effects ORDER BY source_seq,drive,axis,key",
		"SELECT lane||'|'||n FROM board_query_lanes ORDER BY lane",
		"SELECT path||'|'||regular||'|'||error||'|'||HEX(body) FROM board_query_bodies ORDER BY path",
	} {
		rows, err := st.db.QueryContext(t.Context(), query)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var row string
			if err := rows.Scan(&row); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			out = append(out, row)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func TestBoardProjectionClosingCutRejectsBodyMutation(t *testing.T) {
	st, index, spec, _ := boardFixture(t)
	path := filepath.Join(spec.CWD, "ruling")
	if err := os.WriteFile(path, []byte("go #123"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "go #123", Refs: Refs{Path: path}}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	err := index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, _ *BoardResult) error { return os.WriteFile(path, []byte("closed #123"), 0o600) })
	if err == nil {
		t.Fatal("mixed snapshot returned success")
	}
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
		if len(result.Rulings.Records) != 1 || string(result.Rulings.Bodies[result.Rulings.Records[0].Seq]) != "closed #123" {
			t.Fatal("closing invalidation was lost")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoardProjectionUnselectedOpenerError(t *testing.T) {
	st, index, spec, _ := boardFixture(t)
	opener, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Decide, Text: "unselected #999", Refs: Refs{Targets: []string{"service"}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	closer, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "root", Kind: kinds.Answer, Text: "answer", Re: opener.Seq}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs='{' WHERE seq=?", closer.Seq); err != nil {
		t.Fatal(err)
	}
	if _, err := st.OpenAmong(t.Context(), "d", []int64{opener.Seq}); err == nil {
		t.Fatal("negative control did not reach original closure error")
	}
	err = index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
		if result.Rulings.Error == nil {
			t.Fatal("unselected opener error disappeared")
		}
		if result.Reviews.Error != nil {
			t.Fatal("ruling error contaminated independent reviews")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoardProjectionSnapshotExcludesLateCanonicalCommit(t *testing.T) {
	st, index, spec, _ := boardFixture(t)
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	err := index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(snapshot *BoardSnapshot, _ *BoardResult) error {
		late, _, err := st.Post(context.Background(), Record{Drive: "d", Lane: "root", Kind: kinds.Go, Text: "late #123"}, 0)
		if err != nil {
			return err
		}
		selected, err := snapshot.Records(t.Context(), []int64{late.Seq})
		if err != nil {
			return err
		}
		if len(selected) != 0 {
			t.Fatal("late canonical commit entered pinned snapshot")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBoardQuietReadDoesNotPublishDatabaseChanges(t *testing.T) {
	st, index, spec, _ := boardFixture(t)
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	observer, err := st.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = observer.Close() }()
	var before, after int64
	if err := observer.QueryRowContext(t.Context(), "PRAGMA data_version").Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err := index.Read(t.Context(), st, spec, BoardSelection{}, func(*BoardSnapshot, *BoardResult) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := observer.QueryRowContext(t.Context(), "PRAGMA data_version").Scan(&after); err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("quiet board read wrote database: %d -> %d", before, after)
	}
}

func TestBoardReviewLaneExpiryAndClockReversal(t *testing.T) {
	st, index, spec, _ := boardFixture(t)
	now := st.Now()
	st.Now = func() time.Time { return now }
	expiry := now.Add(time.Minute)
	if _, _, err := st.Post(t.Context(), Record{Drive: "d", Lane: "reviewer-2", Kind: kinds.Head, Text: "head #123"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := index.Provision(t.Context(), spec); err != nil {
		t.Fatal(err)
	}
	check := func(want bool) {
		t.Helper()
		err := index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"123"}}, func(_ *BoardSnapshot, result *BoardResult) error {
			if slices.Contains(result.Reviews.Lanes, "reviewer-2") != want {
				t.Fatalf("lanes=%v want=%v", result.Reviews.Lanes, want)
			}
			if (len(result.Reviews.Records) == 1) != want {
				t.Fatalf("reviews=%v want=%v", result.Reviews.Records, want)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	check(true)
	now = expiry.Add(time.Second)
	check(false)
	now = expiry.Add(-time.Second)
	check(true)
}
