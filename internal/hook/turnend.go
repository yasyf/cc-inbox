package hook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	launchedPrefix = "launched-"
	turnEndPrefix  = "turn-end-"
)

var (
	standingDuty = regexp.MustCompile(`(?m)^Standing: *\S`)
	endsWork     = []kinds.Kind{kinds.Done, kinds.Stopped, kinds.StandDown, kinds.Handoff}
	waitsOnSome  = []kinds.Kind{kinds.Ask, kinds.Blocker, kinds.Blocked}
)

// launchedLane reports whether the thread k's lane owes a typed end to its
// turns: a record launched it and its brief names no standing duty. A standing
// lane idles by design, and a thread nothing launched has no work on the
// registry. The answer is read once per thread, so a stop stays one indexed read.
func launchedLane(ctx context.Context, st *store.Store, k store.PeerKey, b store.Binding) (bool, error) {
	if b.Root || b.Lane == "" || store.RootName(b.Lane) {
		return false, nil
	}
	name := launchedPrefix + k.String()
	if cached, ok, err := st.Cursor(ctx, name, b.Drive); err != nil || ok {
		return cached == 1, err
	}
	spawn, found, err := st.LaneSpawn(ctx, b.Drive, b.Lane)
	if err != nil {
		return false, err
	}
	launched := int64(0)
	if found && !standing(spawn.Fields.Attrs["brief"]) {
		launched = 1
	}
	return launched == 1, st.SetCursor(ctx, name, b.Drive, launched)
}

func landedClear(ctx context.Context, st *store.Store, r store.Record) (bool, error) {
	waits, err := st.Query(ctx, store.Filter{Drive: r.Drive, Lanes: []string{r.Lane}, Kinds: waitsOnSome, IncludeExpired: true})
	if err != nil {
		return false, err
	}
	seqs := make([]int64, len(waits))
	for i, w := range waits {
		seqs[i] = w.Seq
	}
	open, err := st.OpenAmong(ctx, r.Drive, seqs)
	if err != nil || len(open) > 0 {
		return false, err
	}
	pr, err := st.OpenPR(ctx, r.Drive, r.Lane)
	return !pr, err
}

func standing(brief string) bool {
	text, err := os.ReadFile(brief) //nolint:gosec // G304: the path is the brief the lane's own spawn record names.
	return err == nil && standingDuty.Match(text)
}

// settled reports whether r ends a turn: the lane finished, stopped or handed
// off, it waits on an ask, blocker or blocked record nobody closed yet, or it
// landed with no PR, ask or blocker of its own still open.
func settled(ctx context.Context, st *store.Store, r store.Record) (bool, error) {
	if slices.Contains(endsWork, r.Kind) {
		return true, nil
	}
	if r.Kind == kinds.Landed {
		return landedClear(ctx, st, r)
	}
	if !slices.Contains(waitsOnSome, r.Kind) {
		return false, nil
	}
	open, err := st.OpenAmong(ctx, r.Drive, []int64{r.Seq})
	return len(open) > 0, err
}

// endTurn lets a launched lane's thread stop only on a record that says who
// resumes its work. It refuses once per record with the commands that settle
// the turn; a second stop with nothing new gets the blocker written for it, so
// the turn never ends unrecorded. It reports whether it refused.
func endTurn(ctx context.Context, st *store.Store, p Payload, b store.Binding, w io.Writer) (bool, error) {
	launched, err := launchedLane(ctx, st, p.Key(), b)
	if err != nil || !launched {
		return false, err
	}
	if p.AgentID == "" {
		_, subscribed, err := st.SessionSubscription(ctx, p.SessionID)
		if err != nil || subscribed {
			return false, err
		}
	}
	newest, found, err := st.NewestOfLane(ctx, b.Drive, b.Lane)
	if err != nil {
		return false, err
	}
	if found {
		done, err := settled(ctx, st, newest)
		if err != nil || done {
			return false, err
		}
	}
	cursor := turnEndPrefix + p.Key().String()
	refused, seen, err := st.Cursor(ctx, cursor, b.Drive)
	if err != nil {
		return false, err
	}
	if seen && refused == newest.Seq {
		return false, unsettledBlocker(ctx, st, p, b, newest)
	}
	if err := st.SetCursor(ctx, cursor, b.Drive, newest.Seq); err != nil {
		return false, err
	}
	return true, json.NewEncoder(w).Encode(map[string]string{"decision": "block", "reason": openWork(b, newest, found)})
}

func openWork(b store.Binding, newest store.Record, found bool) string {
	post := fmt.Sprintf("cci post --drive %s --lane %s", b.Drive, b.Lane)
	lead := fmt.Sprintf("%s has posted no record yet, so nobody would resume this work.", b.Lane)
	if found {
		lead = fmt.Sprintf("%s's newest record, #%d %s, does not end a turn, so nobody would resume this work.", b.Lane, newest.Seq, strings.ToUpper(string(newest.Kind)))
	}
	if found && newest.Kind == kinds.Mechanism {
		lead = fmt.Sprintf("%s posted MECHANISM #%d and owes the fix: post `%s --kind opened --pr <n> --text 'OPENED #<n> <what it fixes>'`, or `%s --kind blocker --to <who> --text '<what stops the fix>'`.", b.Lane, newest.Seq, post, post)
	}
	return strings.Join([]string{
		lead + " Post the one record that is true, then stop:",
		post + " --kind done --text '<what it delivered>'   # finished; a PR still open keeps the lane parked until it lands",
		post + " --kind landed --pr <n> --text '<what merged>'   # merged; it ends the turn once no PR, ask or blocker of yours is open",
		post + " --kind stopped --text '<why>'   # ended on purpose; it gets no successor",
		post + " --kind blocked --pr <n> | --target <target> | --lane-ref <lane> --text '<what you wait on>'   # the supervisor resumes you when that lands, releases or ends",
		post + " --kind ask --to <who> --text '<question>'   # the answer resumes you",
		"Stop again with nothing new and cci writes the blocker for you.",
	}, "\n")
}

func unsettledBlocker(ctx context.Context, st *store.Store, p Payload, b store.Binding, newest store.Record) error {
	to, err := st.Escalation(ctx, b.Drive, b.Lane)
	if err != nil {
		return err
	}
	text := fmt.Sprintf("%s ended its turn twice with open work and no record saying who resumes it; its last words: %s", b.Lane, render.Clip(strings.Join(strings.Fields(p.LastMessage), " "), openWidth))
	_, _, err = st.PostAt(ctx, store.Record{ID: fmt.Sprintf("turn-end:%s:%d", b.Lane, newest.Seq), Drive: b.Drive, Lane: b.Lane, Kind: kinds.Blocker, To: to, Text: store.Clip(text), Source: "post"}, 0, st.Now())
	return err
}

// escalateRefusal hands the refusal that only its lane heard of to whoever hears
// the lane's blockers, once the lane ends its turn without the stored post that
// closes it.
func escalateRefusal(ctx context.Context, st *store.Store, b store.Binding) error {
	open, err := st.OpenRefusals(ctx, b.Drive, b.Lane)
	if err != nil {
		return err
	}
	first, quiet := store.Unescalated(open, b.Lane)
	if !quiet {
		return nil
	}
	to, err := st.Escalation(ctx, b.Drive, b.Lane)
	if err != nil {
		return err
	}
	text := fmt.Sprintf("%s ended its turn with no stored post after its refused post #%d: %s", b.Lane, first.Seq, first.Text)
	_, _, err = st.PostAt(ctx, store.Record{ID: fmt.Sprintf("refused-turn-end:%s:%d", b.Lane, first.Seq), Drive: b.Drive, Lane: b.Lane, Kind: kinds.Blocker, Topic: store.RefusedPostTopic, To: to, Text: store.Clip(text), Source: "post"}, 0, st.Now())
	return err
}

// TurnDied is the attr value of the blocker a turn killed by an API error leaves,
// which tells the supervisor to resume the lane that wrote it.
const TurnDied = "died"

// StopFailure records a launched lane's turn that an API error killed, where no
// Stop hook runs: the blocker carries turn=died, and the supervisor resumes it.
func StopFailure(ctx context.Context, st *store.Store, p Payload) error {
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok {
		return err
	}
	launched, err := launchedLane(ctx, st, p.Key(), b)
	if err != nil || !launched {
		return err
	}
	to, err := st.Escalation(ctx, b.Drive, b.Lane)
	if err != nil {
		return err
	}
	at := st.Now()
	text := fmt.Sprintf("%s's turn died on an API error: %s; its last words: %s", b.Lane, p.Error, render.Clip(strings.Join(strings.Fields(p.LastMessage), " "), openWidth))
	_, _, err = st.PostAt(ctx, store.Record{ID: fmt.Sprintf("turn-died:%s:%d", b.Lane, at.Unix()/60), Drive: b.Drive, Lane: b.Lane, Kind: kinds.Blocker, To: to, Text: store.Clip(text), Fields: store.Fields{Attrs: map[string]string{"turn": TurnDied}}, Source: "post"}, 0, at)
	return err
}
