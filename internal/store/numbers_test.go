package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestEveryNodeNamesARecordByItsOriginNumber(t *testing.T) {
	ctx := context.Background()
	mac, _ := testutil.Store(t)
	sprite, _ := testutil.Store(t)
	sprite.SetOwner(false)
	if err := sprite.PutMember(ctx, store.Member{Node: mac.Self(), PublicKey: mac.Node().PublicKey(), Tier: node.TierOwner, Via: "relay", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, mac, store.Record{Lane: "root", Kind: kinds.Note, Text: "warm-up"})
	ask := testutil.Post(t, mac, store.Record{Lane: "root", Kind: kinds.Ask, Text: "who owns the relay?"})
	note := testutil.Post(t, sprite, store.Record{Lane: "finder-desk", Kind: kinds.Note, Text: "sprite pass"})
	exchange := func(from, to *store.Store) {
		rows, err := from.Export(ctx, from.Self(), 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := to.Apply(ctx, rows, nil); err != nil {
			t.Fatal(err)
		}
	}
	exchange(mac, sprite)
	exchange(sprite, mac)
	tag := "#1@" + sprite.Self()[:8]

	for _, st := range []*store.Store{mac, sprite} {
		records, err := st.Query(ctx, store.Filter{Drive: "d"})
		if err != nil {
			t.Fatal(err)
		}
		names := map[string]string{}
		for _, r := range records {
			names[r.Text] = r.Num
		}
		if names["who owns the relay?"] != "#2" || names["sprite pass"] != tag {
			t.Fatalf("node %s names %v; want the Mac's ask #2 and the sprite's note %s on every node", st.Self()[:8], names, tag)
		}
	}

	onSprite, err := sprite.Resolve(ctx, "#2")
	if err != nil {
		t.Fatal(err)
	}
	reply := testutil.Post(t, sprite, store.Record{Lane: "finder-desk", Kind: kinds.Answer, Text: "the sprite does", Re: onSprite})
	got, err := sprite.Get(ctx, reply.Seq)
	if err != nil || got.ReNum != "#2" {
		t.Fatalf("sprite reply answers %q, %v; want the Mac's number #2", got.ReNum, err)
	}
	onMac, err := mac.Resolve(ctx, tag)
	if err != nil {
		t.Fatal(err)
	}
	if back, err := mac.Get(ctx, onMac); err != nil || back.Text != note.Text {
		t.Fatalf("mac resolves %s to %+v, %v; want the sprite's note", tag, back, err)
	}
	if seq, err := mac.Resolve(ctx, "#2"); err != nil || seq != ask.Seq {
		t.Fatalf("mac resolves #2 to %d, %v; want its own #%d", seq, err, ask.Seq)
	}
}

func TestAnOpenHoldALandingNodeReadsCarriesTheNumberTheMacResolvesToTheSameHold(t *testing.T) {
	ctx := context.Background()
	mac, _ := testutil.Store(t)
	landing, _ := testutil.Store(t)
	landing.SetOwner(false)
	if err := landing.PutMember(ctx, store.Member{Node: mac.Self(), PublicKey: mac.Node().PublicKey(), Tier: node.TierOwner, Via: "relay", By: "test", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	testutil.Post(t, landing, store.Record{Lane: "desk-runner", Kind: kinds.Note, Text: "landing pass 1"})
	testutil.Post(t, landing, store.Record{Lane: "desk-runner", Kind: kinds.Note, Text: "landing pass 2"})
	testutil.Post(t, mac, store.Record{Lane: "register-seed", Kind: kinds.Note, Text: "a register row"})
	hold := testutil.Post(t, mac, store.Record{Lane: "root", Kind: kinds.Hold, Text: "hold #48034 until its reader is live", To: []string{"root"}, Refs: store.Refs{PRs: []store.PR{{Repo: "Forge-AI/monorepo", Number: 48034}}}})
	rows, err := mac.Export(ctx, mac.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := landing.Apply(ctx, rows, nil); err != nil {
		t.Fatal(err)
	}
	held, err := landing.Query(ctx, store.Filter{Drive: "d", Kinds: []kinds.Kind{kinds.Hold}})
	if err != nil || len(held) != 1 {
		t.Fatalf("landing holds = %+v, %v", held, err)
	}
	open, err := landing.OpenAmong(ctx, "d", []int64{held[0].Seq})
	if err != nil || len(open) != 1 {
		t.Fatalf("landing open holds = %+v, %v", open, err)
	}
	if open[0].Seq == hold.Seq || open[0].Name() != hold.Name() {
		t.Fatalf("landing names its open hold %s at its own seq %d; want the Mac's %s", open[0].Name(), open[0].Seq, hold.Name())
	}
	seq, err := mac.Resolve(ctx, open[0].Name())
	if err != nil {
		t.Fatal(err)
	}
	if back, err := mac.Get(ctx, seq); err != nil || back.Text != hold.Text {
		t.Fatalf("the Mac resolves the landing node's %s to %+v, %v; want the hold", open[0].Name(), back, err)
	}
}

func TestABareNumberTwoNodesShareInADriveIsRefused(t *testing.T) {
	ctx := context.Background()
	mac, _ := testutil.Store(t)
	sprite, _ := testutil.Store(t)
	sprite.SetOwner(false)
	local := testutil.Post(t, mac, store.Record{Lane: "root", Kind: kinds.Decide, Text: "root decide"})
	testutil.Post(t, sprite, store.Record{Lane: "finder-desk", Kind: kinds.Review, Text: "finder: major"})
	rows, err := sprite.Export(ctx, sprite.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mac.Apply(ctx, rows, nil); err != nil {
		t.Fatal(err)
	}
	if seq, err := mac.Resolve(ctx, "#1"); err == nil {
		t.Fatalf("mac resolves a bare #1 to %d; want a refusal naming #1@%s", seq, sprite.Self()[:8])
	}
	if seq, err := mac.Resolve(ctx, "#1@"+mac.Self()[:8]); err != nil || seq != local.Seq {
		t.Fatalf("mac resolves its own tagged #1 to %d, %v; want #%d", seq, err, local.Seq)
	}
	remote, err := mac.Resolve(ctx, "#1@"+sprite.Self()[:8])
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mac.Get(ctx, remote); err != nil || got.Text != "finder: major" {
		t.Fatalf("mac resolves #1@%s to %+v, %v; want the sprite's review", sprite.Self()[:8], got, err)
	}
}
