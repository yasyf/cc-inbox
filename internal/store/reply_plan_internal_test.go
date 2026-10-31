package store

import (
	"context"
	"strings"
	"testing"
)

func TestSelectedReplyPlanRequiresIndexedSeeks(t *testing.T) {
	ctx:=context.Background()
	st,err:=Open(ctx,t.TempDir())
	if err!=nil {t.Fatal(err)}
	t.Cleanup(func(){_=st.Close()})
	rows,err:=st.db.QueryContext(ctx,"EXPLAIN QUERY PLAN "+replyPagesQuery(false),"[1,2]")
	if err!=nil {t.Fatal(err)}
	var details []string
	for rows.Next() {
		var id,parent,unused int
		var detail string
		if err:=rows.Scan(&id,&parent,&unused,&detail);err!=nil {_=rows.Close();t.Fatal(err)}
		details=append(details,detail)
	}
	if err:=rows.Err();err!=nil {t.Fatal(err)}
	if err:=rows.Close();err!=nil {t.Fatal(err)}
	plan:=strings.Join(details,"\n")
	for _,want:=range []string{"records_re (re=?)","records_resolves_target (resolves=?)","record_also_resolves_target (target=?)","record_json_errors_field (field=?)","INTEGER PRIMARY KEY (rowid=?)"} {
		if !strings.Contains(plan,want) {t.Fatalf("missing indexed seek %s: %s",want,plan)}
	}
	if strings.Contains(plan,"SCAN records") || strings.Contains(plan,"USE TEMP B-TREE FOR ORDER BY") {t.Fatalf("global history scan or reordered error frontier: %s",plan)}
	t.Log(plan)
}

func TestSelectedReplyZeroOwnerPreservesDefaultResolvePredicate(t *testing.T) {
	ctx:=t.Context()
	st:=openMembershipStore(ctx,t,t.TempDir())
	if _,err:=st.db.ExecContext(ctx,`INSERT INTO records(drive,lane,kind,at,text,source,hash,re,fields) VALUES('foreign','reply','note',1,'reply','post','zero',9,'{')`);err!=nil {t.Fatal(err)}
	pages,err:=st.ReplyPages(ctx,[][]int64{{0}})
	if err!=nil || len(pages)!=1 || pages[0].Err==nil || !strings.Contains(pages[0].Err.Error(),"decode fields") {t.Fatalf("zero-owner predicate changed: pages=%+v error=%v",pages,err)}
}
