package cli

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const launcherLaneEnv = "CLAUDE_LONG_RUNNING_LANE"

func aliasedLane(ctx context.Context, st *store.Store, drive, lane string) (string, error) {
	b, bound, err := st.Binding(ctx, SelfPeer(ctx))
	if err != nil || !bound || b.Drive != drive || b.Lane == "" || b.Lane == lane {
		return lane, err
	}
	reach, err := st.Reach(ctx, drive, lane)
	if err != nil || reach.Lane != b.Lane {
		return lane, err
	}
	return b.Lane, nil
}

func bindsCaller(ctx context.Context, lane string) bool {
	inv := invoked(ctx)
	return inv == nil || !inv.embedder || getenv(ctx, launcherLaneEnv) == lane
}

func registeredLane(ctx context.Context, st *store.Store, self store.PeerKey, window store.Window, drive, lane string) (string, error) {
	if store.RootName(lane) || lane == "owner" || getenv(ctx, launcherLaneEnv) == lane {
		return lane, nil
	}
	registered, err := st.LaneRegistered(ctx, drive, lane)
	if err != nil || registered {
		return lane, err
	}
	reach, err := st.Reach(ctx, drive, lane)
	if err != nil && !errors.Is(err, store.ErrNoLaneRegistry) {
		return "", err
	}
	if reach.Lane != "" && reach.Lane != lane {
		registered, err := st.LaneRegistered(ctx, drive, reach.Lane)
		if err != nil {
			return "", err
		}
		bound, err := st.LanePeers(ctx, []string{reach.Lane})
		if err != nil {
			return "", err
		}
		elsewhere := slices.ContainsFunc(bound, func(p store.Peer) bool {
			return p.Drive == drive && p.Key() != self && (p.PID != window.PID || p.PIDStarted != window.Started)
		})
		if registered && !elsewhere {
			return reach.Lane, nil
		}
	}
	return "", fmt.Errorf("lane %s has no registry row on drive %s, so nothing would supervise it: launch it through its launcher, or have the launcher post cci post --drive %s --lane <launcher> --kind spawn --lane-ref %s --attr brief=<absolute brief path> --attr model=<model> --text 'SPAWNED %s'", lane, drive, drive, lane, lane)
}

// refuseProseEnd refuses a lane's own end written as a sentence in a record the
// supervisor reads as progress, which is how a stopped lane got a successor.
func refuseProseEnd(k kinds.Kind, text, lane string) error {
	if k != kinds.State && k != kinds.Report && k != kinds.Note {
		return nil
	}
	name := regexp.QuoteMeta(lane)
	ended := regexp.MustCompile(`^\s*(?:` + clockLabel + `:?\s+)?(?:` + name + `:?\s+)?(?:(?:STOPPED|DONE|WITHDRAWN)\s+` + name + `\b|STOPPED:)`)
	if !ended.MatchString(text) {
		return nil
	}
	return fmt.Errorf("a lane's end is one typed record the supervisor reads, never a sentence in a %s record: post cci post --lane %s --kind stopped --text '<why>' when it ended on purpose, or --kind done when it delivered", k, lane)
}

func refuseStrayLift(ctx context.Context, st *store.Store, records []store.Record) error {
	var named []int64
	for _, r := range records {
		named = append(append(named, r.Re), r.Closes()...)
	}
	holds, err := st.OpenHoldsAmong(ctx, records[0].Drive, named)
	if err != nil {
		return err
	}
	open := map[int64]bool{}
	for _, h := range holds {
		open[h.Seq] = true
	}
	for _, r := range records {
		if !open[r.Re] && !slices.ContainsFunc(r.Closes(), func(seq int64) bool { return open[seq] }) {
			return strayLift(ctx, st, r)
		}
	}
	return nil
}

func strayLift(ctx context.Context, st *store.Store, r store.Record) error {
	named, err := st.RecordsAmong(ctx, r.Drive, append([]int64{r.Re}, r.Closes()...))
	if err != nil {
		return err
	}
	what, prs := "no record", slices.Clone(r.Refs.PRs)
	if len(named) > 0 {
		described := make([]string, len(named))
		for i, n := range named {
			described[i] = fmt.Sprintf("%s (%s)", n.Name(), n.Kind)
			prs = append(prs, n.Refs.PRs...)
		}
		what = strings.Join(described, ", ")
	}
	why := "a lift closes the open hold its --re or --resolves names, and this one names " + what
	var seqs []int64
	on := make([]string, 0, len(prs))
	for _, pr := range prs {
		if name := pr.Repo + "#" + strconv.Itoa(pr.Number); !slices.Contains(on, name) {
			on = append(on, name)
			held, err := st.Query(ctx, store.Filter{Drive: r.Drive, Kinds: []kinds.Kind{kinds.Hold}, PR: pr, IncludeExpired: true})
			if err != nil {
				return err
			}
			for _, h := range held {
				seqs = append(seqs, h.Seq)
			}
		}
	}
	if len(on) == 0 {
		return errors.New(why + ": name the hold's number, or add --pr to list the open holds on a pull request")
	}
	open, err := st.OpenHoldsAmong(ctx, r.Drive, seqs)
	if err != nil {
		return err
	}
	if len(open) == 0 {
		return fmt.Errorf("%s, and no hold on %s is open", why, strings.Join(on, ", "))
	}
	lines := make([]string, len(open))
	for i, h := range open {
		lines[i] = render.Compact(h, 120)
	}
	return fmt.Errorf("%s; the open holds on %s, one of which --resolves takes: %s", why, strings.Join(on, ", "), strings.Join(lines, "; "))
}
