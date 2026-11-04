package cli

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

var numberedLane = regexp.MustCompile(`^(.+)-([1-9]\d*)$`)

// refuseSecondBinder refuses self binding lane on drive while another live
// thread holds it, and tells root once by a defect record. A numbered
// successor binding beside its live predecessor binds, and the predecessor
// stands down for it.
func refuseSecondBinder(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window, drive, lane string, at time.Time) error {
	if b, ok, err := st.Binding(ctx, self); err != nil || ok && (b.Root || b.Lane != "" || b.Drive != drive) {
		return err
	}
	return refuseHeldLane(ctx, st, self, window, drive, lane, at)
}

// refuseHeldLane refuses self taking lane on drive from another live thread
// that never stood down, whatever self is bound to now, as refuseSecondBinder
// refuses an unbound thread's first post. A holder in self's own window is no
// other thread: a resumed session's MCP server keeps a session id of its own.
func refuseHeldLane(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window, drive, lane string, at time.Time) error {
	owner, _, _, err := st.Owner(ctx, self)
	if err != nil {
		return err
	}
	for _, held := range append([]string{lane}, predecessors(lane)...) {
		holder, ok, err := liveHolder(ctx, st, self, owner, window, drive, held)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		seen := st.Now().Sub(holder.SeenAt).Round(time.Minute)
		if held != lane {
			text := fmt.Sprintf("%s stood down for %s, which registered under %s while %s, thread %s %s, was live, seen %s ago: %s posts its handoff for %s and stops", held, lane, store.Stem(lane), held, holder.Name, holder.Key(), seen, held, lane)
			if _, _, err := st.PostAt(ctx, store.Record{ID: "replaced:" + held + ":" + lane, Drive: drive, Lane: peers.RegistryLane, Kind: kinds.StandDown, To: []string{held, lane}, Text: text, Fields: store.Fields{Attrs: map[string]string{"successor": lane}}, Source: "post"}, 0, at); err != nil {
				return err
			}
			continue
		}
		text := fmt.Sprintf("second binder of %s refused: thread %s tried to bind it while thread %s %s holds it live, seen %s ago, and it never stood down; stop the duplicate, or stand %s down before another thread takes it", lane, self, holder.Name, holder.Key(), seen, lane)
		defect, _, err := st.PostAt(ctx, store.Record{ID: "second-binder:" + held + ":" + self.String(), Drive: drive, Lane: lane, Kind: kinds.Defect, To: []string{"root"}, Text: text, Source: "post"}, 0, at)
		if err != nil {
			return err
		}
		return fmt.Errorf("lane %s is held by live thread %s %s, seen %s ago, and never stood down, so this thread did not bind it and posted nothing; stop and report to root, which heard defect #%d", lane, holder.Name, holder.Key(), seen, defect.Seq)
	}
	return nil
}

func liveHolder(ctx context.Context, st *store.Store, self, owner store.PeerKey, window store.Window, drive, lane string) (peers.View, bool, error) {
	if _, standDown, err := st.Successors(ctx, drive, lane); err != nil || standDown != 0 {
		return peers.View{}, false, err
	}
	views, err := peers.ResolveLanes(ctx, st, []string{lane})
	if err != nil {
		return peers.View{}, false, err
	}
	for _, v := range views {
		sameWindow := window.PID != 0 && v.PID == window.PID && v.PIDStarted == window.Started
		if v.Drive == drive && v.Lane == lane && v.Key() != self && v.Key() != owner && !sameWindow && st.Now().Sub(v.SeenAt) < peers.StaleAfter {
			return v, true, nil
		}
	}
	return peers.View{}, false, nil
}

func predecessors(lane string) []string {
	m := numberedLane.FindStringSubmatch(lane)
	if m == nil {
		return nil
	}
	n, _ := strconv.Atoi(m[2])
	switch n {
	case 1:
		return nil
	case 2:
		return []string{m[1] + "-1", m[1]}
	}
	return []string{m[1] + "-" + strconv.Itoa(n-1)}
}
