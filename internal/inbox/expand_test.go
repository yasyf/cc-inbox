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

func TestTailFollowsAnOwnerTokenWithHisWordsOnce(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Drive: "d", Lane: "owner-relay", Kind: kinds.Owner, ID: "owner:m47", Text: "keep the nightly report"})
	testutil.Post(t, st, store.Record{Drive: "d", Lane: "owner-relay", Kind: kinds.Owner, ID: "slack:1793159602.693499", Text: "approve 43807"})
	testutil.Post(t, st, store.Record{Drive: "d", Lane: "owner-relay", Kind: kinds.Ask, To: []string{"root"}, Text: "owner:m47 keep it; per owner:m47 and slack:1793159602.693499, not owner:m9"})
	var out bytes.Buffer
	if _, err := inbox.Tail(context.Background(), st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}, Cursor: "root", Budget: 4000}, &out); err != nil {
		t.Fatal(err)
	}
	want := `owner:m47 "keep the nightly report" keep it; per owner:m47 and slack:1793159602.693499 "approve 43807", not owner:m9`
	if !strings.Contains(out.String(), want) {
		t.Fatalf("tail does not carry the owner's words once:\n%s", out.String())
	}
	if strings.Contains(out.String(), `"keep the nightly report" keep the nightly report`) || strings.Count(out.String(), "keep the nightly report") != 2 {
		t.Fatalf("the owner record itself or a second mention was expanded:\n%s", out.String())
	}
}

type countedTexts struct {
	st    *store.Store
	calls int
}

func (c *countedTexts) TextsByID(ctx context.Context, drive string, ids []string) (map[string]string, error) {
	c.calls++
	return c.st.TextsByID(ctx, drive, ids)
}

func TestExpandReadsOnceForOneRecordAndForAThousand(t *testing.T) {
	for _, n := range []int{1, 1000} {
		st, _ := testutil.Store(t)
		testutil.Post(t, st, store.Record{Drive: "d", Kind: kinds.Owner, ID: "owner:m1", Text: "release everything as it merges"})
		records := make([]store.Record, n)
		for i := range records {
			records[i] = store.Record{Drive: "d", Kind: kinds.Ask, Text: fmt.Sprintf("owner:m1 and owner:m%d", i+2)}
		}
		counted := &countedTexts{st: st}
		if err := inbox.Expand(context.Background(), counted, records); err != nil {
			t.Fatal(err)
		}
		if counted.calls != 1 {
			t.Fatalf("n=%d: %d id reads, want 1", n, counted.calls)
		}
		if got := records[n-1].Text; got != fmt.Sprintf(`owner:m1 "release everything as it merges" and owner:m%d`, n+1) {
			t.Fatalf("n=%d: text = %q", n, got)
		}
	}
}
