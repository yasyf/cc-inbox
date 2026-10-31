package cli_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"modernc.org/sqlite"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestGrepSelectedRepliesDoNotReadUnrelatedRecords(t *testing.T) {
	for _, population := range []int{32,10000} {
		for _, selected := range []int{1,1001} {
			for _, budget := range []int{6144,4<<20} {
				t.Run(fmt.Sprintf("noise=%d/selected=%d/budget=%d",population,selected,budget),func(t *testing.T) {
					ctx := context.Background()
					var reads atomic.Int64
					name := fmt.Sprintf("cci_selected_grep_%d",grepCounterID.Add(1))
					if err := sqlite.RegisterDeterministicScalarFunction(name,1,func(_ *sqlite.FunctionContext,args []driver.Value)(driver.Value,error){reads.Add(1);return args[0],nil});err!=nil {t.Fatal(err)}
					home:=t.TempDir()
					t.Setenv("CCI_HOME",home)
					t.Setenv("CLAUDE_CODE_SESSION_ID","")
					t.Setenv("CODEX_THREAD_ID","")
					t.Setenv("CCI_AGENT_ID","")
					st,err:=store.Open(ctx,home)
					if err!=nil {t.Fatal(err)}
					if err:=st.Close();err!=nil {t.Fatal(err)}
					db,err:=sql.Open("sqlite",filepath.Join(home,store.DBFile))
					if err!=nil {t.Fatal(err)}
					t.Cleanup(func(){_=db.Close()})
					if _,err:=db.ExecContext(ctx,"CREATE INDEX IF NOT EXISTS records_re ON records(re)");err!=nil {t.Fatal(err)}
					if _,err:=db.ExecContext(ctx,`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash,also_resolves)
SELECT 'foreign','noise','note',x,'irrelevant','post','noise-'||x,'[90000000]' FROM n`,population);err!=nil {t.Fatal(err)}
					if _,err:=db.ExecContext(ctx,`WITH RECURSIVE n(x) AS(VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<?)
INSERT INTO records(drive,lane,kind,at,text,source,hash)
SELECT 'd','reviewer','review',x,'REVIEW selected '||x,'post','selected-'||x FROM n`,selected);err!=nil {t.Fatal(err)}
					if _,err:=db.ExecContext(ctx,`INSERT INTO records(drive,lane,kind,at,text,source,hash,re,also_resolves) VALUES('foreign','reply','note',1,'selected reply','post','reply',?,'[0]')`,population+selected);err!=nil {t.Fatal(err)}
					args:=[]string{"grep","REVIEW","--drive","d","--kind","review","--budget",fmt.Sprint(budget)}
					if old:=os.Getenv("CCI_GREP_CURRENT_BASELINE");old!="" {
						command:=exec.CommandContext(ctx,old,args...)
						want,err:=command.CombinedOutput()
						if err!=nil {t.Fatalf("old CLI: %v: %s",err,want)}
						got,err:=run(t,args...)
						if err!=nil || got!=string(want) {t.Fatalf("CLI bytes differ: current=%q/%v old=%q",got,err,want)}
					}
					for _,statement:=range []string{"ALTER TABLE records RENAME COLUMN also_resolves TO counted_also_resolves","ALTER TABLE records ADD COLUMN also_resolves TEXT GENERATED ALWAYS AS ("+name+"(counted_also_resolves)) VIRTUAL"} {
						if _,err:=db.ExecContext(ctx,statement);err!=nil {t.Fatal(err)}
					}
					reads.Store(0)
					out,err:=run(t,args...)
					if err!=nil || !strings.Contains(out,"selected reply") {t.Fatalf("grep=%q, %v",out,err)}
					count:=reads.Load()
					if os.Getenv("CCI_GREP_EXPECT_GLOBAL_REPLY_SCAN")=="1" {
						if count<int64(population+2*selected) {t.Fatalf("baseline did not demonstrate history reads: %d",count)}
					} else if count<int64(selected) || count>int64(selected+3) {t.Fatalf("selected lookup reads=%d, candidate rows=%d",count,selected)}
					t.Logf("noise=%d selected_candidates=%d budget=%d actual_cli_also_column_reads=%d annotation_reads=%d",population,selected,budget,count,count-int64(selected))
				})
			}
		}
	}
}
