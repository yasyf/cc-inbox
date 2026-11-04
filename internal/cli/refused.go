package cli

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const launcherLaneEnv = "CLAUDE_LONG_RUNNING_LANE"

// reported is a refusal that already told its reader through a record of its own.
type reported struct{ error }

func (r reported) Unwrap() error { return r.error }

// refusalsBlock runs one post and turns its refusal into a blocker on the bus,
// so a caller that swallows stderr still leaves a record of what it could not say.
func refusalsBlock(drive, lane, kind, text *string, run func(*cobra.Command, *store.Store, []string) error) func(*cobra.Command, *store.Store, []string) error {
	return func(cmd *cobra.Command, st *store.Store, args []string) error {
		err := run(cmd, st, args)
		if err == nil || !isRefusal(err) {
			return err
		}
		return leaveBlocker(cmd.Context(), st, *drive, *lane, *kind, said(args, *text), err)
	}
}

// refusedBeforeStore records a refusal found while parsing the command line,
// which must not create a store: only an existing one takes the blocker.
func refusedBeforeStore(cmd *cobra.Command, drive, lane, kind string, args []string, text string, cause error) error {
	home, err := store.Home()
	if err != nil {
		return cause
	}
	if _, err := os.Stat(home); err != nil {
		return cause
	}
	st, err := openStore(cmd.Context())
	if err != nil {
		return cause
	}
	defer func() { _ = st.Close() }()
	return leaveBlocker(cmd.Context(), st, drive, lane, kind, said(args, text), cause)
}

func said(args []string, text string) string {
	if len(args) == 1 {
		return args[0]
	}
	return text
}

func isRefusal(err error) bool {
	var down *BusDown
	var told reported
	return !store.Busy(err) && !errors.Is(err, store.ErrUnconfirmed) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.As(err, &down) && !errors.As(err, &told)
}

func leaveBlocker(ctx context.Context, st *store.Store, drive, lane, kind, words string, cause error) error {
	if lane == "" {
		return cause
	}
	onDrive, err := resolveDrive(ctx, st, drive)
	if err != nil {
		return cause
	}
	open, err := st.OpenRefusals(ctx, onDrive, lane)
	if err != nil {
		return errors.Join(cause, err)
	}
	to, text := []string{lane}, fmt.Sprintf("post refused: %v. It said: %s %s", cause, kind, words)
	if first, again := store.Unescalated(open, lane); again {
		if to, err = st.Escalation(ctx, onDrive, lane); err != nil {
			return errors.Join(cause, err)
		}
		text = fmt.Sprintf("%s was refused again after it retried. First #%d: %s Then: %s", lane, first.Seq, render.Clip(first.Text, store.MaxText/2), text)
	}
	at := st.Now()
	key := fnv.New64a()
	_, _ = fmt.Fprintf(key, "%s\x00%s\x00%s\x00%d", cause, words, strings.Join(to, ","), at.Truncate(time.Hour).Unix())
	posted, _, err := st.PostAt(ctx, store.Record{
		ID:     fmt.Sprintf("refused:%s:%016x", lane, key.Sum64()),
		Drive:  onDrive,
		Lane:   lane,
		Kind:   kinds.Blocker,
		Topic:  store.RefusedPostTopic,
		To:     to,
		Text:   store.Clip(text),
		Source: "post",
	}, 0, at)
	if err != nil {
		return errors.Join(cause, err)
	}
	return fmt.Errorf("%w; blocker #%d went to %s, and %s's next stored post closes it", cause, posted.Seq, strings.Join(to, ","), lane)
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
