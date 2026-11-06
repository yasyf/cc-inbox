package store

import (
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestABoardColdRebuildHoldsTheWriteLockAPageAtATime(t *testing.T) {
	largest := map[int]int{}
	for _, pages := range []int{1, 100} {
		st, writes := pagedStore(t)
		if err := st.InstallBoardProjection(t.Context()); err != nil {
			t.Fatal(err)
		}
		population := pages * writePage
		if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE history(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM history WHERE n<?)
INSERT INTO records(seq,drive,lane,kind,at,text,source,hash,recipients,origin,origin_seq,id)
SELECT n,'d','root','decide',?,'decide #'||n,'post','board-page-'||n,'[]',?,n,'board-page-'||n FROM history`, population, st.Now().UnixMilli(), st.Self()); err != nil {
			t.Fatal(err)
		}
		index := NewBoardIndex(st)
		t.Cleanup(func() { _ = index.Close() })
		spec := BoardSpec{Drive: "d", Repo: "owner/repo", CWD: t.TempDir(), Filters: [3]Filter{
			{Drive: "d", Kinds: []kinds.Kind{kinds.Go, kinds.Decide, kinds.Decision, kinds.Answer}},
			{Drive: "d", Kinds: []kinds.Kind{kinds.Head, kinds.Review}},
			{Drive: "d", Kinds: []kinds.Kind{kinds.Withdraw}, Lanes: []string{"root"}},
		}}
		writes.reset()
		var id string
		for range 2 {
			var err error
			if id, err = index.Provision(t.Context(), spec); err != nil {
				t.Fatal(err)
			}
		}
		largest[pages] = writes.of("board_query_rows")
		var projected int
		if err := st.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM board_query_rows WHERE scope=? AND category=0 AND error='' AND annotation_error=''", id).Scan(&projected); err != nil {
			t.Fatal(err)
		}
		if projected != population {
			t.Fatalf("pages=%d projected rows=%d want %d", pages, projected, population)
		}
		err := index.Read(t.Context(), st, spec, BoardSelection{PRs: []string{"1"}}, func(_ *BoardSnapshot, result *BoardResult) error {
			if result.Rulings.Error != nil || len(result.Rulings.Records) != 1 || result.Rulings.Records[0].Seq != 1 {
				t.Fatalf("pages=%d rulings=%+v", pages, result.Rulings)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if largest[1] == 0 || largest[100] != largest[1] {
		t.Fatalf("largest board_query_rows write per transaction: 1 page=%d, 100 pages=%d", largest[1], largest[100])
	}
}
