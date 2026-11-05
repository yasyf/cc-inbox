package cli

import (
	"context"
	"fmt"
	"regexp"

	"github.com/yasyf/cc-inbox/internal/kinds"
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

// refuseUnregistered refuses a thread's first post as a lane nothing launched,
// which no supervisor would ever notice going quiet.
func refuseUnregistered(ctx context.Context, st *store.Store, drive, lane string) error {
	if store.RootName(lane) || lane == "owner" || getenv(ctx, launcherLaneEnv) == lane {
		return nil
	}
	registered, err := st.LaneRegistered(ctx, drive, lane)
	if err != nil || registered {
		return err
	}
	return fmt.Errorf("lane %s has no registry row on drive %s, so nothing would supervise it: launch it through its launcher, or have the launcher post cci post --drive %s --lane <launcher> --kind spawn --lane-ref %s --attr brief=<absolute brief path> --attr model=<model> --text 'SPAWNED %s'", lane, drive, drive, lane, lane)
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
