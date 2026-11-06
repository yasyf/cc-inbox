package store

import "testing"

func TestAnIncidentScopesFirstPublishHoldsTheWriteLockAPageAtATime(t *testing.T) {
	type largest struct{ records, versions int }
	byPages := map[int]largest{}
	for _, pages := range []int{1, 100} {
		st, writes := pagedStore(t)
		if _, err := st.db.ExecContext(t.Context(), `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','seed','incident',x,'incident #'||x,'post','paged-'||x FROM n`, pages*WritePage); err != nil {
			t.Fatal(err)
		}
		writes.reset()
		report, err := st.ProvisionIncidentScope(t.Context(), "d")
		if err != nil || !report.Ready || report.Records != int64(pages*WritePage) {
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
	t.Logf("records=%d..%d largest_transaction=%+v", WritePage, 100*WritePage, byPages[100])
}

func TestAnIncidentChangeQueuedBehindAPublishedPageIsCapturedNext(t *testing.T) {
	ctx := t.Context()
	st, _ := pagedStore(t)
	if _, err := st.db.ExecContext(ctx, `WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','seed','incident',x,'incident #'||x,'post','queued-'||x FROM n`, WritePage+1); err != nil {
		t.Fatal(err)
	}
	if report, err := st.ProvisionIncidentScope(ctx, "d"); err != nil || !report.Ready {
		t.Fatalf("provision = %+v, %v", report, err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET text = text || ' moved' WHERE kind = 'incident'"); err != nil {
		t.Fatal(err)
	}
	first, err := st.captureIncidentChanges(ctx)
	if err != nil || len(first) != WritePage || first[0].Seq != 1 {
		t.Fatalf("first capture = %d changes from seq %v, %v; want a page from seq 1", len(first), first, err)
	}
	if err := st.publishIncidentChanges(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := st.db.ExecContext(ctx, "UPDATE records SET text = 'incident #1 moved back' WHERE seq = 1"); err != nil {
		t.Fatal(err)
	}
	next, err := st.captureIncidentChanges(ctx)
	if err != nil {
		t.Fatal(err)
	}
	captured := map[int64]bool{}
	for _, change := range next {
		captured[change.Seq] = true
	}
	if !captured[1] || !captured[int64(WritePage+1)] {
		t.Fatalf("the capture after a published page holds seqs %v; want seq 1, queued again behind the page, and seq %d", captured, WritePage+1)
	}
}
