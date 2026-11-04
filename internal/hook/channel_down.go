package hook

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/peers"
	"github.com/yasyf/cc-inbox/internal/store"
)

// ReconnectCommand is the slash command that reconnects the channel server in a
// live Claude Code session, the only route the harness offers: stdio servers
// get no automatic retry, and hooks and tools cannot run a slash command.
const ReconnectCommand = "/mcp reconnect plugin:cc-inbox:cci"

const (
	channelStale   = 2 * time.Minute
	channelGrace   = 2 * time.Minute
	reconnectEvery = 5 * time.Minute
	reconnectTries = 6
	idleWaitMS     = 30_000
	channelDownTo  = "meta-desk"
	channelTopic   = "cci-mcp-down"
)

// NoticeChannel checks that the session's channel server is running, once per
// tool call on the root thread of a lane. A server gone stale, or one that
// never started where the launch named it, opens one blocker from the lane and
// starts a typed reconnect through Orca; a server back up closes the blocker. It
// returns the line telling the session when the outage opens.
func NoticeChannel(ctx context.Context, st *store.Store, p Payload, wired func(pid int) bool, reconnect func(orcaDir, terminal string)) (string, error) {
	if p.AgentID != "" {
		return "", nil
	}
	b, ok, err := st.Binding(ctx, p.Key())
	if err != nil || !ok || b.Lane == "" {
		return "", err
	}
	peer, err := st.Peer(ctx, p.Key())
	if errors.Is(err, sql.ErrNoRows) || (err == nil && peer.PID == 0) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	window := store.Window{PID: peer.PID, Started: peer.PIDStarted}
	now := st.Now()
	beat, seen, err := st.ChannelBeat(window)
	if err != nil {
		return "", err
	}
	outage, open, err := st.ChannelOutage(window)
	if err != nil {
		return "", err
	}
	if seen && now.Sub(beat) < channelStale {
		if !open {
			return "", nil
		}
		return "", closeOutage(ctx, st, b, window, outage)
	}
	if !seen && (now.Sub(time.UnixMilli(window.Started)) < channelGrace || !wired(window.PID)) {
		return "", nil
	}
	note := ""
	if !open {
		r, _, err := st.Post(ctx, store.Record{
			ID: fmt.Sprintf("%s-%s-%d-%d", channelTopic, p.SessionID, window.Started, beat.Unix()), Drive: b.Drive, Lane: b.Lane,
			Kind: kinds.Blocker, To: []string{channelDownTo}, Topic: channelTopic, Text: outageText(b.Lane, beat, seen, now),
		}, 0)
		if err != nil {
			return "", err
		}
		outage.Blocker = r.Seq
		note = "The cci MCP server is down in this session, so its tools and channel delivery are gone; cci in Bash still posts and reads. Blocker #" + strconv.FormatInt(r.Seq, 10) + " went to " + channelDownTo + ". Reconnect with `" + ReconnectCommand + "`, typed at the prompt; a session inside Orca gets it typed at its next idle prompt.\n"
	}
	if terminal := peer.Env["ORCA_TERMINAL_HANDLE"]; terminal != "" && now.Sub(outage.TypedAt) >= reconnectEvery {
		reconnect(peer.Env[peers.OrcaBinDirEnv], terminal)
		outage.TypedAt = now
	}
	return note, st.SetChannelOutage(window, outage)
}

func outageText(lane string, beat time.Time, seen bool, now time.Time) string {
	if !seen {
		return "cci MCP down: " + lane + " was launched with the server and it never started. Reconnect: " + ReconnectCommand
	}
	return fmt.Sprintf("cci MCP down: %s's channel server last ran %s ago; its MCP tools and channel delivery are gone until it reconnects. Reconnect: %s", lane, now.Sub(beat).Round(time.Second), ReconnectCommand)
}

func closeOutage(ctx context.Context, st *store.Store, b store.Binding, window store.Window, outage store.ChannelOutage) error {
	_, _, err := st.Post(ctx, store.Record{
		Drive: b.Drive, Lane: b.Lane, Kind: kinds.Note, To: []string{channelDownTo}, Topic: channelTopic, Resolves: outage.Blocker,
		Text: "cci MCP is back in " + b.Lane + ".",
	}, 0)
	if err != nil {
		return err
	}
	return st.ClearChannelOutage(window)
}

// TypeReconnect waits until terminal sits idle with nothing in its input box,
// then types ReconnectCommand, so it never lands in a human's half-written
// prompt. It gives up after a few tries; a later tool call starts another.
func TypeReconnect(ctx context.Context, orca func(ctx context.Context, args ...string) (string, error), terminal string, pause func()) error {
	for range reconnectTries {
		typed, err := typeIfIdle(ctx, orca, terminal)
		if typed || err != nil {
			return err
		}
		pause()
	}
	return nil
}

func typeIfIdle(ctx context.Context, orca func(ctx context.Context, args ...string) (string, error), terminal string) (bool, error) {
	if _, err := orca(ctx, "terminal", "wait", "--terminal", terminal, "--for", "tui-idle", "--timeout-ms", strconv.Itoa(idleWaitMS)); err != nil {
		return false, nil //nolint:nilerr // a busy session times the wait out; the next try waits again.
	}
	out, err := orca(ctx, "terminal", "read", "--terminal", terminal, "--screen", "--json")
	if err != nil {
		return false, err
	}
	var read struct {
		Result struct {
			Terminal struct {
				Draft  json.RawMessage `json:"draft"`
				Source string          `json:"source"`
			} `json:"terminal"`
		} `json:"result"`
	}
	if err := json.Unmarshal([]byte(out), &read); err != nil {
		return false, fmt.Errorf("decode terminal read: %w", err)
	}
	if read.Result.Terminal.Source != "screen" || !noDraft(read.Result.Terminal.Draft) {
		return false, nil
	}
	_, err = orca(ctx, "terminal", "send", "--terminal", terminal, "--text", ReconnectCommand, "--enter")
	return err == nil, err
}

func noDraft(draft json.RawMessage) bool {
	switch string(draft) {
	case "", "null", "false", `""`:
		return true
	}
	return false
}
