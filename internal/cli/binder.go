package cli

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

var numberedLane = regexp.MustCompile(`^(.+)-([1-9]\d*)$`)

// HeldLaneExit is the exit code of a post or announce refused because another
// live thread holds its lane, so a launcher's first command can stop on it.
const HeldLaneExit = 76

// HeldLane is the refusal of a thread taking a lane another live thread holds.
type HeldLane struct {
	text string
}

func (e *HeldLane) Error() string {
	return e.text
}

func refuseSecondBinder(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window, drive, lane string, at time.Time) (bool, error) {
	b, ok, err := st.Binding(ctx, self)
	switch {
	case err != nil:
		return false, err
	case ok && (b.Root || b.Drive != drive || b.Lane != "" && b.Lane != lane):
		return false, nil
	case ok && b.Lane == lane:
		return true, nil
	}
	return true, refuseHeldLane(ctx, st, self, window, drive, lane, at)
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
		name := strings.TrimSpace(holder.Name + " " + holder.Key().String())
		if holder.Origin != "" {
			name += " on node " + holder.Origin
		}
		if held != lane {
			text := fmt.Sprintf("%s stood down for %s, which registered under %s while %s, thread %s, was live, seen %s ago: %s posts its handoff for %s and stops", held, lane, store.Stem(lane), held, name, seen, held, lane)
			if _, _, err := st.PostAt(ctx, store.Record{ID: "replaced:" + held + ":" + lane, Drive: drive, Lane: peers.RegistryLane, Kind: kinds.StandDown, To: []string{held, lane}, Text: text, Fields: store.Fields{Attrs: map[string]string{"successor": lane}}, Source: "post"}, 0, at); err != nil {
				return err
			}
			continue
		}
		release := orphanRelease(holder, seen)
		text := fmt.Sprintf("second binder of %s refused: thread %s tried to bind it while thread %s holds it live, seen %s ago, and it never stood down; stop the duplicate, or stand %s down before another thread takes it%s", lane, self, name, seen, lane, release)
		excerpt, path, err := st.Fit(text)
		if err != nil {
			return err
		}
		defect, _, err := st.PostAt(ctx, store.Record{ID: "second-binder:" + held + ":" + self.String(), Drive: drive, Lane: lane, Kind: kinds.Defect, To: []string{"root"}, Text: excerpt, Refs: store.Refs{Path: path}, Source: "post"}, 0, at)
		if err != nil {
			return err
		}
		return &HeldLane{fmt.Sprintf("lane %s is held by live thread %s, seen %s ago, and never stood down, so this thread did not bind it and posted nothing; stop and report to root, which heard defect #%d. The name passes to another thread only after a stand-down: cci post --drive %s --lane root --kind stand-down --to %s --successor <lane> --text '<why>'%s", lane, name, seen, defect.Seq, drive, lane, release)}
	}
	return nil
}

func orphanRelease(holder peers.View, seen time.Duration) string {
	if holder.Agent == "" || holder.Origin != "" {
		return ""
	}
	return fmt.Sprintf(". Subagent %s of session %s last ran a tool %s ago; if the harness stopped it, as the tasks panel and Esc do without telling cci, free the lane in one step: echo '{\"session_id\":\"%s\",\"tool_input\":{\"task_id\":\"%s\"}}' | cci hook task-stop", holder.Agent, holder.Session, seen, holder.Session, holder.Agent)
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
		if v.Drive == drive && v.Lane == lane && v.Key() != self && v.Key() != owner && !sameWindow && holds(v, st.Now()) {
			return v, true, nil
		}
	}
	r, ok, err := st.RemoteBinder(ctx, drive, lane, st.Now().Add(-peers.StaleAfter))
	if err != nil || !ok || r.Kind == kinds.Done || r.Kind == kinds.Stopped {
		return peers.View{}, false, err
	}
	session, agent, _ := strings.Cut(r.Fields.Attrs[store.BinderAttr], "/")
	return peers.View{Peer: store.Peer{Session: session, Agent: agent, Origin: r.Origin, Drive: drive, Lane: lane, SeenAt: r.At}, Name: lane}, true, nil
}

func holds(v peers.View, now time.Time) bool {
	if v.Agent == "" {
		return now.Sub(v.SeenAt) < peers.StaleAfter
	}
	return v.Live
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
