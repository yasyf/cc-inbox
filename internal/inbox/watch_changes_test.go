package inbox_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestWatchDrainsPagesAndKeepsDeliveryFrontier(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st, _ := testutil.Store(t)
		db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		_, err = db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<1002)
INSERT INTO records(drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT 'd','worker','note',1,'backlog','post','backlog-'||n,'[]',?,n,'backlog-'||n FROM history`, st.Self())
		if err != nil {
			t.Fatal(err)
		}
		filter := (store.Subscription{Drive: "d", Session: "missing", Reader: "root", Kinds: []kinds.Kind{kinds.Note}, ToKinds: store.DefaultToKinds("root")}).Filter()
		filter.After = 1
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		delivered := make(chan int64, 1003)
		refused := errors.New("notification refused")
		started := time.Now()
		go func() {
			done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: filter, Cursor: "root-channel", Interval: time.Hour}, func(record store.Record, _ string) error {
				if record.Text == "refused notification" {
					return refused
				}
				delivered <- record.Seq
				return nil
			})
		}()
		synctest.Wait()
		if len(delivered) != 1001 || time.Since(started) != 0 {
			t.Fatalf("backlog delivered=%d elapsed=%s, want 1001 without waiting", len(delivered), time.Since(started))
		}
		for i := range 1001 {
			seq := <-delivered
			if seq != int64(i+2) {
				t.Fatalf("delivered[%d] = %d, want %d", i, seq, i+2)
			}
		}
		post := func(text string) store.Record {
			t.Helper()
			record, _, err := st.Post(t.Context(), store.Record{Drive: "d", Lane: "worker", Kind: kinds.Note, Text: text}, 0)
			if err != nil {
				t.Fatal(err)
			}
			return record
		}
		late := post("committed after quiet page")
		time.Sleep(time.Hour)
		synctest.Wait()
		if len(delivered) != 1 {
			t.Fatalf("late deliveries = %d, want 1", len(delivered))
		}
		if seq := <-delivered; seq != late.Seq {
			t.Fatalf("late delivery = %d, want seq %d after backlog", seq, late.Seq)
		}
		post("refused notification")
		time.Sleep(time.Hour)
		synctest.Wait()
		if err := <-done; !errors.Is(err, refused) {
			t.Fatalf("watch error = %v, want %v", err, refused)
		}
		cursor, ok, err := st.Cursor(t.Context(), "root-channel", "d")
		if err != nil || !ok || cursor != late.Seq {
			t.Fatalf("cursor = %d, %v, %v; want %d, true, nil", cursor, ok, err, late.Seq)
		}
	})
}

func TestWatchCancelledBeforeFirstRead(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d", After: 1}}, func(store.Record, string) error {
		t.Fatal("cancelled watch emitted a record")
		return nil
	})
	if err != nil {
		t.Fatalf("cancelled watch error = %v", err)
	}
}

func TestWatchCommitDuringDeliveryWithoutCursor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st, _ := testutil.Store(t)
		anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before cursor"})
		first := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "first delivery"})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		delivered := make(chan int64, 2)
		var late store.Record
		go func() {
			done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d", After: anchor.Seq}, Interval: time.Second}, func(record store.Record, _ string) error {
				delivered <- record.Seq
				if record.Seq == first.Seq {
					var err error
					late, _, err = st.Post(ctx, store.Record{Drive: "d", Lane: "worker", Kind: kinds.Note, Text: "committed during delivery"}, 0)
					return err
				}
				return nil
			})
		}()
		synctest.Wait()
		if len(delivered) != 1 || late.Seq == 0 {
			t.Fatalf("first deliveries = %d, late = %+v", len(delivered), late)
		}
		if seq := <-delivered; seq != first.Seq {
			t.Fatalf("first delivery = %d, want %d", seq, first.Seq)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(delivered) != 1 {
			t.Fatalf("late deliveries = %d, want 1", len(delivered))
		}
		if seq := <-delivered; seq != late.Seq {
			t.Fatalf("late delivery = %d, want %d", seq, late.Seq)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestWatchObservesMutationOfUnmatchedSequence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st, _ := testutil.Store(t)
		anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before cursor"})
		unmatched := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "initially unmatched"})
		db, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = db.Close() }()
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		delivered := make(chan store.Record, 1)
		go func() {
			done <- inbox.Watch(ctx, st, inbox.WatchOptions{Filter: store.Filter{Drive: "d", After: anchor.Seq, Kinds: []kinds.Kind{kinds.Hold}}, Interval: time.Second}, func(record store.Record, _ string) error {
				delivered <- record
				return nil
			})
		}()
		synctest.Wait()
		if len(delivered) != 0 {
			t.Fatalf("unmatched deliveries = %d", len(delivered))
		}
		if _, err := db.ExecContext(t.Context(), "UPDATE records SET kind='hold' WHERE seq=?", unmatched.Seq); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if len(delivered) != 1 {
			t.Fatalf("mutated deliveries = %d, want 1", len(delivered))
		}
		if record := <-delivered; record.Seq != unmatched.Seq || record.Kind != kinds.Hold || record.Text != unmatched.Text {
			t.Fatalf("mutated delivery = %+v, want hold seq %d text %q", record, unmatched.Seq, unmatched.Text)
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}
