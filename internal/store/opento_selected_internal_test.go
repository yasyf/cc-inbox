package store

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestOpenToSelectedClosureMatchesAllAddressedOpeners(t *testing.T) {
	st, _ := openStore(t)
	ctx := t.Context()
	ks := []kinds.Kind{kinds.Ask, kinds.Blocked, kinds.Decide}
	for i := range 1001 {
		selectedPost(t, st, Record{Kind: kinds.Ask, Text: fmt.Sprint(i), To: []string{"api"}, Topic: fmt.Sprintf("topic-%d", i)})
	}
	selectedPost(t, st, Record{Kind: kinds.Answer, Text: "answer", Re: 1})
	selectedPost(t, st, Record{Kind: kinds.Answer, Text: "topic", Topic: "topic-1"})
	if _, err := st.db.ExecContext(ctx, `INSERT INTO records(drive,lane,kind,at,text,also_resolves,source,hash) VALUES('foreign','resolver','note',1,'resolve','[3]','post','resolve')`); err != nil {
		t.Fatal(err)
	}
	openers, err := st.Query(ctx, Filter{Drive: "d", To: "api", Kinds: ks, IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	openers = slices.DeleteFunc(openers, func(o Record) bool { return !o.Tracked() || !slices.Contains(ks, o.Kind) })
	old, err := st.unclosed(ctx, "d", openers)
	if err != nil {
		t.Fatal(err)
	}
	current, err := st.OpenTo(ctx, "d", "api", ks)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := json.Marshal(old)
	b, _ := json.Marshal(current)
	if string(a) != string(b) || len(current) != 998 {
		t.Fatalf("all-addressed output changed: old=%d current=%d equal=%t", len(old), len(current), string(a) == string(b))
	}
}

func TestOpenToSelectedErrorsAndUnrelatedPrivateCorruption(t *testing.T) {
	for _, field := range []string{"refs", "fields", "recipients", "also_resolves"} {
		for _, scope := range []string{"unrelated", "closer", "opener"} {
			t.Run(field+"/"+scope, func(t *testing.T) {
				st, _ := openStore(t)
				ctx := t.Context()
				opener := selectedPost(t, st, Record{Kind: kinds.Ask, Text: "selected", To: []string{"api"}})
				closer := selectedPost(t, st, Record{Kind: kinds.Answer, Text: "private", Re: 100000})
				if scope == "closer" {
					if _, err := st.db.ExecContext(ctx, "UPDATE records SET re=? WHERE seq=?", opener.Seq, closer.Seq); err != nil {
						t.Fatal(err)
					}
				}
				seq := closer.Seq
				if scope == "opener" {
					seq = opener.Seq
				}
				if _, err := st.db.ExecContext(ctx, "UPDATE records SET "+field+"='[' WHERE seq=?", seq); err != nil {
					t.Fatal(err)
				}
				current, err := st.OpenTo(ctx, "d", "api", []kinds.Kind{kinds.Ask})
				if scope == "unrelated" {
					if err != nil || len(current) != 1 {
						t.Fatalf("unrelated corruption selected: %+v/%v", current, err)
					}
				} else if err == nil {
					t.Fatal("selected decode error disappeared")
				}
			})
		}
	}
}
