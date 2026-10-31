package store

import "testing"

func TestSelectedClosureProjectionDeduplicatesRefAtomsAcrossPaths(t *testing.T) {
	st, _ := openStore(t)
	opener := selectedPost(t, st, Record{Kind: "ask", Text: "selected", Refs: Refs{Stacks: []string{"a/api"}}})
	closer := selectedPost(t, st, Record{Kind: "answer", Text: "closer", Topic: "shared", Refs: Refs{Stacks: []string{"a/api", "a/api"}, Targets: []string{"a/api"}}})
	assertSelectedOpen(t, st, opener.Seq, false)
	assertKeys := func() {
		t.Helper()
		var keys int
		if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM record_closer_keys WHERE source_seq=?", closer.Seq).Scan(&keys); err != nil || keys != 3 {
			t.Fatalf("duplicate ref atom keys=%d, error=%v; want re/topic/one ref", keys, err)
		}
	}
	assertKeys()
	if _, err := st.db.ExecContext(t.Context(), "UPDATE records SET refs=? WHERE seq=?", `{"stacks":["b/next","b/next"],"targets":["b/next"],"unknown":["b/next"]}`, closer.Seq); err != nil {
		t.Fatal(err)
	}
	assertKeys()
	assertSelectedOpen(t, st, opener.Seq, true)
	for i := len(closerMemberships) - 1; i >= 0; i-- {
		object := closerMemberships[i]
		if _, err := st.db.ExecContext(t.Context(), "DROP "+object.kind+" "+object.name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := MaintainIndexes(t.Context(), st.HomeDir()); err != nil {
		t.Fatal(err)
	}
	assertKeys()
	assertSelectedOpen(t, st, opener.Seq, true)
}
