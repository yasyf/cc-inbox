package store

import (
	"context"
	"strconv"
	"testing"

	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestRepliesSelectionExceedsVariableLimit(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	st.db.SetMaxOpenConns(1)
	opener, _, err := st.Post(ctx, Record{Drive: "d", Lane: "reader", Kind: kinds.Ask, Text: "question"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	reply, _, err := st.Post(ctx, Record{Drive: "other", Lane: "writer", Kind: kinds.Answer, Text: "answer", Re: opener.Seq}, 0)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := st.db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlite.Limit(conn, sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 64); err != nil {
		t.Fatal(err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	seqs := make([]int64, 1000)
	for i := range seqs {
		seqs[i] = int64(i + 1)
	}
	replies, err := st.Replies(ctx, seqs)
	if err != nil {
		t.Fatal(err)
	}
	if len(replies[opener.Seq]) != 1 || replies[opener.Seq][0].Seq != reply.Seq {
		t.Fatalf("wide selection replies = %v, want reply #%d for opener #%d", replies, reply.Seq, opener.Seq)
	}
	t.Logf("selected_sequences=%d sqlite_variable_limit=64 reply_count=%d", len(seqs), len(replies[opener.Seq]))
}

func TestAReplyResolvingManyRecordsJoinsOnlyTheSelectedOnes(t *testing.T) {
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	blockers := make([]int64, 300)
	for i := range blockers {
		r, _, err := st.Post(ctx, Record{Drive: "d", Lane: "runner", Kind: kinds.Blocker, Text: "red build " + strconv.Itoa(i)}, 0)
		if err != nil {
			t.Fatal(err)
		}
		blockers[i] = r.Seq
	}
	sweep, _, err := st.Post(ctx, Record{Drive: "d", Lane: "runner", Kind: kinds.Note, Text: "all stale", Resolves: blockers[0], AlsoResolves: blockers[1:]}, 0)
	if err != nil {
		t.Fatal(err)
	}
	selected := blockers[100:110]
	replies, err := st.Replies(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	if len(replies) != len(selected) {
		t.Fatalf("Replies(%d of %d resolved records) keyed %d records, want only the selected ones", len(selected), len(blockers), len(replies))
	}
	for _, seq := range selected {
		got := replies[seq]
		if len(got) != 1 || got[0].Seq != sweep.Seq || len(got[0].CloseNums) != len(blockers) {
			t.Fatalf("replies to #%d = %+v, want the one numbered sweep #%d naming all %d records", seq, got, sweep.Seq, len(blockers))
		}
	}
}
