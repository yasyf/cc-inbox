package store

import (
	"context"
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
