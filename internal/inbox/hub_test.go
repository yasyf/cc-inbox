package inbox_test

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func awaitWake(t *testing.T, f *inbox.Follower) store.Wake {
	t.Helper()
	select {
	case <-f.Ready():
		return f.Take()
	case <-time.After(time.Minute):
		t.Fatal("no wake within a minute")
		return store.Wake{}
	}
}

func TestHubWakesOnlyTheFollowerItsFilterPasses(t *testing.T) {
	st, _ := testutil.Store(t)
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	hub := inbox.NewHub(st, time.Millisecond)
	defer hub.Close()
	addressed := hub.Follow(store.Filter{Drive: "d", To: "reader", After: anchor.Seq})
	defer addressed.Leave()
	quiet := hub.Follow(store.Filter{Drive: "d", To: "other", After: anchor.Seq})
	defer quiet.Leave()
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "to nobody here"})
	first := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "first", To: []string{"reader"}})
	wake := awaitWake(t, addressed)
	if wake.First != first.Seq || wake.Head < first.Seq {
		t.Fatalf("wake = %+v, want first %d", wake, first.Seq)
	}
	second := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "second", To: []string{"reader"}})
	if wake = awaitWake(t, addressed); wake.First != second.Seq {
		t.Fatalf("wake = %+v, want first %d", wake, second.Seq)
	}
	select {
	case <-quiet.Ready():
		t.Fatalf("a follower no record passes woke: %+v", quiet.Take())
	default:
	}
}

func TestHubRereadsARecordThatChanged(t *testing.T) {
	st, clock := testutil.Store(t)
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	item := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Note, Text: "not a hold yet", Source: "import:/private.md", At: clock.Now()}, LineHash: "hub-import", Line: "not a hold yet"}
	if n, err := st.IngestAll(t.Context(), []store.Ingestion{item}); err != nil || n != 1 {
		t.Fatalf("IngestAll = %d, %v", n, err)
	}
	head, err := st.MaxSeq(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	hub := inbox.NewHub(st, time.Millisecond)
	defer hub.Close()
	holds := hub.Follow(store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Hold}, After: anchor.Seq, IncludeExpired: true})
	defer holds.Leave()
	idle, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	select {
	case <-holds.Ready():
		t.Fatalf("a follower nothing passes woke: %+v", holds.Take())
	case <-idle.Done():
	}
	item.Record.Kind = kinds.Hold
	if n, _, err := st.Reparse(t.Context(), []store.Ingestion{item}, false); err != nil || n != 1 {
		t.Fatalf("Reparse = %d, %v", n, err)
	}
	if wake := awaitWake(t, holds); wake.First != head {
		t.Fatalf("wake = %+v, want first %d", wake, head)
	}
}

func TestHubFirstReadSeesARecordChangedAfterTheReadersOwn(t *testing.T) {
	st, clock := testutil.Store(t)
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	item := store.Ingestion{Record: store.Record{Drive: "d", Lane: "importer", Kind: kinds.Note, Text: "not a hold yet", Source: "import:/private.md", At: clock.Now()}, LineHash: "hub-import", Line: "not a hold yet"}
	if n, err := st.IngestAll(t.Context(), []store.Ingestion{item}); err != nil || n != 1 {
		t.Fatalf("IngestAll = %d, %v", n, err)
	}
	head, err := st.MaxSeq(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	item.Record.Kind = kinds.Hold
	if n, _, err := st.Reparse(t.Context(), []store.Ingestion{item}, false); err != nil || n != 1 {
		t.Fatalf("Reparse = %d, %v", n, err)
	}
	hub := inbox.NewHub(st, time.Millisecond)
	defer hub.Close()
	holds := hub.Follow(store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Hold}, After: anchor.Seq, IncludeExpired: true})
	defer holds.Leave()
	if wake := awaitWake(t, holds); wake.First != head {
		t.Fatalf("wake = %+v, want first %d", wake, head)
	}
}

func TestHubWakesTheRunningLaneForAStemAFinishedFallbackWasHanded(t *testing.T) {
	st, _ := testutil.Store(t)
	for _, lane := range []string{"bugs-1", "bugs-2", "bugs-2-opus"} {
		testutil.Post(t, st, store.Record{Lane: lane, Kind: kinds.Note, Text: lane + " registers"})
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.StandDown, Text: "bugs-2-opus stands down", To: []string{"bugs-2-opus"}})
	anchor := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.StandDown, Text: "bugs-1 stands down", To: []string{"bugs-1"}, Fields: store.Fields{Attrs: map[string]string{"successor": "bugs-2-opus"}}})
	hub := inbox.NewHub(st, time.Millisecond)
	defer hub.Close()
	running := hub.Follow(store.Filter{Drive: "d", To: "bugs-2", After: anchor.Seq})
	defer running.Leave()
	mail := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Note, Text: "for the stem", To: []string{"bugs"}})
	if wake := awaitWake(t, running); wake.First > mail.Seq {
		t.Fatalf("wake = %+v, want first at or before %d", wake, mail.Seq)
	}
}

func TestHubReadsAgainWhenAFollowerGainsAnAlias(t *testing.T) {
	st, _ := testutil.Store(t)
	anchor := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "anchor"})
	testutil.Post(t, st, store.Record{Lane: "worker-2", Kind: kinds.Note, Text: "worker-2 registers"})
	mail := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "for the predecessor", To: []string{"old-name"}})
	hub := inbox.NewHub(st, time.Millisecond)
	defer hub.Close()
	successor := hub.Follow(store.Filter{Drive: "d", To: "worker-2", After: anchor.Seq})
	defer successor.Leave()
	testutil.Post(t, st, store.Record{Lane: "old-name", Kind: kinds.Note, Text: "old-name registers"})
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.StandDown, Text: "old-name stands down", To: []string{"old-name"}, Fields: store.Fields{Attrs: map[string]string{"successor": "worker-2"}}})
	if wake := awaitWake(t, successor); wake.First > mail.Seq {
		t.Fatalf("wake = %+v, want first at or before %d", wake, mail.Seq)
	}
}
