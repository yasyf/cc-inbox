package store

import "testing"

func TestAnIncidentScopesFirstPublishHoldsTheWriteLockAPageAtATime(t *testing.T) {
	type largest struct{ records, versions int }
	byPages := map[int]largest{}
	for _, pages := range []int{1, 100} {
		st, writes := pagedStore(t)
		if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','seed','incident',x,'incident #'||x,'post','paged-'||x FROM n`, pages*writePage); err != nil {
			t.Fatal(err)
		}
		writes.reset()
		report, err := st.ProvisionIncidentScope(t.Context(), "d")
		if err != nil || !report.Ready || report.Records != int64(pages*writePage) {
			t.Fatalf("pages=%d provision=%+v, %v", pages, report, err)
		}
		byPages[pages] = largest{records: writes.of("incident_records"), versions: writes.of("incident_versions")}
	}
	if byPages[1].records == 0 || byPages[1].versions == 0 {
		t.Fatalf("fixture counted no incident writes: %+v", byPages[1])
	}
	if byPages[1] != byPages[100] {
		t.Fatalf("largest incident write transaction at 1 page=%+v, at 100 pages=%+v", byPages[1], byPages[100])
	}
	t.Logf("records=%d..%d largest_transaction=%+v", writePage, 100*writePage, byPages[100])
}
