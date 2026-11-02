package store_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

type writerFunc func(context.Context, store.Write) (store.Written, error)

func (f writerFunc) Commit(ctx context.Context, w store.Write) (store.Written, error) {
	return f(ctx, w)
}

func TestCommitAllRollsBackOnlyTheWritesThatFail(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	if err := st.Bind(ctx, store.PeerKey{Session: "s"}, "d", false); err != nil {
		t.Fatal(err)
	}
	note := func(text string) store.Record {
		return store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Note, Text: text}
	}
	writes := []store.Write{
		{Records: []store.Record{note("first")}, At: st.Now()},
		{Records: []store.Record{note("rolled back"), {Drive: "d", Lane: "lane-a", Kind: kinds.Withdraw, Text: "takes back nothing", Re: 999}}, At: st.Now()},
		{Touch: []store.PeerKey{{Session: "s"}}, Records: []store.Record{note("third")}, At: st.Now(), Cursors: []store.CursorMove{{Name: "c", Drive: "d", Seq: 7}}},
		{Records: []store.Record{note("expired")}, At: st.Now(), StartBy: time.Now().Add(-time.Second)},
		{Records: []store.Record{{Drive: "d", Lane: "lane-a", Kind: kinds.Digest, Text: "internal"}}, At: st.Now()},
	}
	done, errs := st.CommitAll(ctx, writes)
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("errors of the writes that should commit = %v, %v", errs[0], errs[2])
	}
	if errs[1] == nil || !strings.Contains(errs[1].Error(), "names no record") {
		t.Fatalf("error of the write naming a missing record = %v", errs[1])
	}
	if !errors.Is(errs[3], store.ErrExpired) || !store.Busy(errs[3]) {
		t.Fatalf("error of the write past its StartBy = %v, want ErrExpired, which is busy", errs[3])
	}
	if errs[4] == nil || !strings.Contains(errs[4].Error(), "written only by cci itself") {
		t.Fatalf("error of the write posting an internal kind = %v", errs[4])
	}
	if len(done[1].Posted) != 0 || len(done[3].Posted) != 0 {
		t.Fatalf("records reported for refused writes = %+v, %+v", done[1], done[3])
	}
	first, third := done[0].Posted[0].Record, done[2].Posted[0].Record
	if first.Seq == 0 || third.Seq <= first.Seq {
		t.Fatalf("seqs = %d then %d, want them in commit order", first.Seq, third.Seq)
	}
	records, err := st.Query(ctx, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	var texts []string
	for _, r := range records {
		texts = append(texts, r.Text)
	}
	slices.Sort(texts)
	if !slices.Equal(texts, []string{"first", "third"}) {
		t.Fatalf("stored texts = %q, want only the committed writes'", texts)
	}
	if seq, ok, err := st.Cursor(ctx, "c", "d"); err != nil || !ok || seq != 7 {
		t.Fatalf("cursor c = %d, %v, %v, want 7 from the third write", seq, ok, err)
	}
	peer, err := st.Peer(ctx, store.PeerKey{Session: "s"})
	if err != nil || !peer.SeenAt.Equal(st.Now().Truncate(time.Millisecond)) {
		t.Fatalf("touched peer = %+v, %v, want seen at %v", peer, err, st.Now())
	}
}

func TestCommitAppliesHereOnlyWhatItsWriterDidNotTake(t *testing.T) {
	note := store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Note, Text: "routed"}
	for _, tt := range []struct {
		name   string
		answer error
		stored int
		want   error
	}{
		{"unrouted", store.ErrUnrouted, 1, nil},
		{"unconfirmed", store.ErrUnconfirmed, 0, store.ErrUnconfirmed},
		{"refused", errors.New("refused"), 0, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, _ := testutil.Store(t)
			ctx := context.Background()
			calls := 0
			st.WriteThrough(writerFunc(func(_ context.Context, w store.Write) (store.Written, error) {
				calls++
				if len(w.Records) != 1 || w.Records[0].Text != "routed" {
					t.Errorf("write the writer got = %+v", w)
				}
				return store.Written{}, tt.answer
			}))
			_, _, err := st.Post(ctx, note, 0)
			switch {
			case tt.answer == store.ErrUnrouted && err != nil:
				t.Fatalf("Post() through an unrouted writer = %v, want it applied here", err)
			case tt.answer != store.ErrUnrouted && err == nil:
				t.Fatalf("Post() through a writer that answered %v succeeded", tt.answer)
			case tt.want != nil && !errors.Is(err, tt.want):
				t.Fatalf("Post() = %v, want %v", err, tt.want)
			}
			if calls != 1 {
				t.Fatalf("writer calls = %d, want 1", calls)
			}
			n, err := st.Count(ctx, store.Filter{Drive: "d"})
			if err != nil || n != tt.stored {
				t.Fatalf("records stored here = %d, %v, want %d", n, err, tt.stored)
			}
		})
	}
}

func TestCommitChecksAWriteBeforeItsWriterSeesIt(t *testing.T) {
	st, _ := testutil.Store(t)
	st.WriteThrough(writerFunc(func(context.Context, store.Write) (store.Written, error) {
		t.Error("the writer got a write that fails its checks")
		return store.Written{}, nil
	}))
	_, _, err := st.Post(context.Background(), store.Record{Drive: "d", Lane: "lane-a", Kind: kinds.Note, Text: strings.Repeat("x", store.MaxText+1)}, 0)
	if !errors.Is(err, store.ErrTextTooLong) {
		t.Fatalf("Post() of an overlong record = %v, want ErrTextTooLong", err)
	}
}
