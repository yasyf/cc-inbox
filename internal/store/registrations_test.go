package store_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestRegistrationOwnsTheSessionsSubscription(t *testing.T) {
	st, _ := testutil.Store(t)
	ctx := context.Background()
	w := store.Window{PID: 100, Started: 1}
	if _, err := st.Subscribe(ctx, store.Subscription{Session: "s1", Window: w, Drive: "d", Reader: "desk", Cursor: "channel-s1"}); err != nil {
		t.Fatal(err)
	}
	head := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before the alias"})
	coordinator := store.Registration{Subscription: store.Subscription{Alias: "release-demo-coordinator", Session: "s1", Window: w, Drive: "d", Reader: "root", Cursor: "coordinator"}, Command: "claude -n release-demo-coordinator", Notify: []string{"communicator"}}
	if _, err := st.Register(ctx, coordinator); err != nil {
		t.Fatal(err)
	}
	for name, read := range map[string]func() (store.Subscription, bool, error){
		"the window's":  func() (store.Subscription, bool, error) { return st.Subscription(ctx, w) },
		"the session's": func() (store.Subscription, bool, error) { return st.SessionSubscription(ctx, "s1") },
	} {
		sub, ok, err := read()
		if err != nil || !ok || sub.Alias != coordinator.Alias || sub.Reader != "root" || sub.Cursor != "coordinator" || sub.Window != w {
			t.Fatalf("%s subscription = %+v %v %v, want the alias's in window %+v", name, sub, ok, err, w)
		}
	}
	if seq, ok, err := st.Cursor(ctx, "coordinator", "d"); err != nil || !ok || seq != head.Seq {
		t.Fatalf("cursor coordinator = #%d %v %v, want seeded at the head #%d", seq, ok, err, head.Seq)
	}
	again := coordinator
	again.Kinds = []kinds.Kind{kinds.Incident}
	testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "after the alias"})
	if _, err := st.Register(ctx, again); err != nil {
		t.Fatal(err)
	}
	if sub, ok, err := st.Subscription(ctx, w); err != nil || !ok || !slices.Equal(sub.Kinds, again.Kinds) {
		t.Fatalf("subscription after registering again = %+v %v %v, want kinds %v", sub, ok, err, again.Kinds)
	}
	if seq, _, _ := st.Cursor(ctx, "coordinator", "d"); seq != head.Seq {
		t.Fatalf("registering again moved cursor coordinator to #%d, want it kept at #%d", seq, head.Seq)
	}
	for _, tt := range []struct {
		alias, session, want string
	}{
		{"second", "s1", "session s1 is registered as release-demo-coordinator"},
		{"two words", "s9", "is not a name"},
		{"../escape", "s9", "is not a name"},
	} {
		r := coordinator
		r.Alias, r.Session, r.Window = tt.alias, tt.session, store.Window{}
		if _, err := st.Register(ctx, r); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Fatalf("Register(%q, %q) = %v, want %q", tt.alias, tt.session, err, tt.want)
		}
	}
	all, err := st.Registrations(ctx)
	if err != nil || len(all) != 1 || all[0].Alias != coordinator.Alias || all[0].Command != coordinator.Command || !slices.Equal(all[0].Notify, coordinator.Notify) || all[0].Window != (store.Window{}) {
		t.Fatalf("Registrations = %+v %v, want the one alias, holding no window", all, err)
	}
	if got, ok, err := st.Registration(ctx, coordinator.Alias); err != nil || !ok || got.Session != "s1" || got.Command != coordinator.Command {
		t.Fatalf("Registration = %+v %v %v, want what the alias holds", got, ok, err)
	}
	if _, ok, err := st.Registration(ctx, "release-demo-meta"); err != nil || ok {
		t.Fatalf("Registration of an alias nobody registered = %v %v", ok, err)
	}
	removed, ok, err := st.Unregister(ctx, coordinator.Alias)
	if err != nil || !ok || removed.Session != "s1" {
		t.Fatalf("Unregister = %+v %v %v", removed, ok, err)
	}
	if _, ok, err := st.Subscription(ctx, w); err != nil || ok {
		t.Fatalf("the window kept a subscription after its alias was removed: %v %v", ok, err)
	}
	if _, ok, err := st.Unregister(ctx, coordinator.Alias); err != nil || ok {
		t.Fatalf("Unregister twice = %v %v, want nothing removed", ok, err)
	}
}
