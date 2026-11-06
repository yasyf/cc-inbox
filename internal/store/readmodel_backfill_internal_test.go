package store

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func TestTheReadModelLeavesToTheStoreABackfillStoredAfterItReadTheSpans(t *testing.T) {
	ctx := t.Context()
	st := openMembershipStore(ctx, t, t.TempDir())
	k := PeerKey{Session: "s1"}
	started := st.Now()
	if err := st.BindLane(ctx, k, "d", "worker", started); err != nil {
		t.Fatal(err)
	}
	model, err := st.BuildReadModel(ctx, func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = model.Close() })
	f, err := st.aliased(ctx, LaneMail(k, Binding{Drive: "d", Lane: "worker"}))
	if err != nil {
		t.Fatal(err)
	}
	f, ok := model.prepared(ctx, st, f)
	if !ok {
		t.Fatal("the model could not prepare the lane's mail")
	}
	if _, err := st.Backfill(ctx, []Row{{Origin: "0123456789abcdef", OriginSeq: 1, Drive: "d", Lane: "root", Kind: kinds.Note, At: started.Add(-time.Hour), Text: "backfilled", To: []string{"worker"}, Source: "post"}}, nil); err != nil {
		t.Fatal(err)
	}
	if err := model.catchUp(ctx); err != nil {
		t.Fatal(err)
	}
	model.mu.RLock()
	defer model.mu.RUnlock()
	var chosen []int64
	proved := newModelSelection(model, st, f, st.Now()).walk(func(row *modelRow) bool {
		chosen = append(chosen, row.seq)
		return true
	})
	if proved && len(chosen) > 0 {
		t.Fatalf("the model delivered %v as lane mail, a record a seed stored after the model read the backfill spans", chosen)
	}
}
