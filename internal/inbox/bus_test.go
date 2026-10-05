package inbox_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestReaderDelivery(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	toMe := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, Text: "ship it", To: []string{"desk"}})
	toOther := testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, Text: "not yours", To: []string{"other"}})
	broadcastContract := testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Contract, Topic: "schema", Text: "v2 shape"})
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Head, Topic: "#30001", Text: "abc"})
	testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Contract, Topic: "schema", Text: "my own broadcast"})
	tests := []struct {
		name string
		f    store.Filter
		want []int64
	}{
		{"everything for me", store.Filter{Drive: "d", For: "desk"}, []int64{toMe.Seq, broadcastContract.Seq, broadcastContract.Seq + 1}},
		{"subscribed to contracts", store.Filter{Drive: "d", For: "desk", Kinds: []kinds.Kind{kinds.Contract}}, []int64{toMe.Seq, broadcastContract.Seq}},
		{"subscribed to a topic", store.Filter{Drive: "d", For: "desk", Topics: []string{"#30001"}}, []int64{toMe.Seq, broadcastContract.Seq + 1}},
		{"plain topic filter", store.Filter{Drive: "d", Topics: []string{"schema"}}, []int64{broadcastContract.Seq, broadcastContract.Seq + 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := st.Query(ctx, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			seqs := make([]int64, len(got))
			for i, r := range got {
				seqs[i] = r.Seq
			}
			if fmt.Sprint(seqs) != fmt.Sprint(tt.want) {
				t.Fatalf("seqs = %v, want %v (toOther #%d must never appear)", seqs, tt.want, toOther.Seq)
			}
		})
	}
}

func TestTailMarksAnsweredAndWithdrawn(t *testing.T) {
	st, _ := testutil.Store(t)
	ask := testutil.Post(t, st, store.Record{Lane: "pr-plans", Kind: kinds.Ask, Text: "can my stack land first?", To: []string{"iam"}})
	answer := testutil.Post(t, st, store.Record{Lane: "iam", Kind: kinds.Answer, Re: ask.Seq, Text: "yes"})
	decision := testutil.Post(t, st, store.Record{Lane: "artifact", Kind: kinds.Decision, Text: "reads are declared"})
	withdraw := testutil.Post(t, st, store.Record{Lane: "artifact", Kind: kinds.Withdraw, Re: decision.Seq, Text: "wrong run"})
	var out bytes.Buffer
	if _, err := inbox.Tail(context.Background(), st, inbox.TailOptions{Filter: store.Filter{Drive: "d"}}, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{
		fmt.Sprintf("can my stack land first? [ANSWERED #%d]", answer.Seq),
		fmt.Sprintf("reads are declared [WITHDRAWN #%d]", withdraw.Seq),
	} {
		if !strings.Contains(got, want) {
			t.Errorf("tail missing %q:\n%s", want, got)
		}
	}
}

func TestStateFold(t *testing.T) {
	st, c := testutil.Store(t)
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Head, Topic: "#1", Text: "aaa"})
	c.Advance(time.Minute)
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Head, Topic: "#1", Text: "bbb"})
	contract := testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Contract, Topic: "schema", Text: "v1"})
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Contract, Topic: "schema", Text: "v2"})
	v2 := contract.Seq + 1
	testutil.Post(t, st, store.Record{Lane: "api", Kind: kinds.Withdraw, Re: v2, Text: "v2 retracted"})
	testutil.Post(t, st, store.Record{Lane: "web", Kind: kinds.Head, Topic: "#2", Text: "ccc"})
	got, err := inbox.State(context.Background(), st, store.Filter{Drive: "d"})
	if err != nil {
		t.Fatal(err)
	}
	lines := make([]string, 0, len(got))
	for _, r := range got {
		lines = append(lines, fmt.Sprintf("%s/%s/%s=%s", r.Lane, r.Topic, r.Kind, r.Text))
	}
	if want := "api/#1/head=bbb api/schema/contract=v1 web/#2/head=ccc"; strings.Join(lines, " ") != want {
		t.Fatalf("state = %v, want %s", lines, want)
	}
	testutil.Post(t, st, store.Record{Lane: "root", Kind: kinds.Go, Text: "addressed", To: []string{"desk"}})
	contracts, err := inbox.State(context.Background(), st, store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Contract}, For: "desk"})
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 || contracts[0].Text != "v1" {
		t.Fatalf("state --kind contract --reader desk = %+v", contracts)
	}
}

func TestDigestOpenBlockers(t *testing.T) {
	st, _ := testutil.Store(t)
	blocker := testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Blocker, Topic: "#30001", To: []string{"api"}, Text: "red lint on #30001"})
	testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Blocker, Topic: "#30002", Text: "cleared"})
	testutil.Post(t, st, store.Record{Lane: "desk", Kind: kinds.Withdraw, Topic: "#30002", Text: "green now"})
	v, err := inbox.Digest(context.Background(), st, "d", st.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(v.Blockers) != 1 || v.Blockers[0].Seq != blocker.Seq || len(v.Asks) != 0 {
		t.Fatalf("blockers = %+v asks = %+v", v.Blockers, v.Asks)
	}
}
