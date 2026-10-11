package inbox_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestResolveAcrossTwoStores(t *testing.T) {
	ctx := context.Background()
	a, _ := testutil.Store(t)
	b, _ := testutil.Store(t)
	testutil.Post(t, b, store.Record{Lane: "root", Kind: kinds.Note, Text: "b was here first"})
	ask := testutil.Post(t, a, store.Record{Lane: "root", Kind: kinds.Ask, Text: "take #34363?"})
	replicate(t, a, b)
	asked, err := b.Query(ctx, store.Filter{Drive: "d", Origin: a.Self()})
	if err != nil || len(asked) != 1 || asked[0].Seq == ask.Seq {
		t.Fatalf("b holds %+v, %v; want a's ask under a b-local seq", asked, err)
	}
	answer := testutil.Post(t, b, store.Record{Lane: "api", Kind: kinds.Answer, Text: "yes", Resolves: asked[0].Seq})
	replicate(t, b, a)
	for _, st := range []*store.Store{a, b} {
		var out bytes.Buffer
		if _, err := inbox.Tail(ctx, st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}}, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "take #34363? [RESOLVED #") {
			t.Fatalf("tail on %s = %s", st.Self(), out.String())
		}
		if open, err := st.OpenItems(ctx, "d"); err != nil || len(open) != 0 {
			t.Fatalf("open on %s = %+v, %v", st.Self(), open, err)
		}
	}
	var out bytes.Buffer
	if _, err := inbox.Tail(ctx, a, inbox.TailOptions{Filter: store.Filter{Drive: "d"}}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), fmt.Sprintf("ANSWER api origin:%s yes resolves #%d", b.Self(), ask.Seq)) {
		t.Fatalf("a renders b's answer as %s; want origin:%s resolving a's #%d (b's #%d)", out.String(), b.Self(), ask.Seq, answer.Seq)
	}
}

func replicate(t *testing.T, from, to *store.Store) {
	t.Helper()
	rows, err := from.Export(context.Background(), from.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := to.Apply(context.Background(), rows, nil); err != nil {
		t.Fatal(err)
	}
}
