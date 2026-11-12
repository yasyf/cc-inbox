package cli

import (
	"cmp"
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
)

func newSessionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Keep sessions under a cci alias that owns their channel subscription",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newSessionRegisterCmd(), newSessionListCmd(), newSessionRmCmd())
	return cmd
}

func newSessionRegisterCmd() *cobra.Command {
	var (
		flags        subscriptionFlags
		session      string
		cwd          string
		command      string
		resumePrompt string
		notify       []string
	)
	cmd := &cobra.Command{
		Use:   "register <alias>",
		Short: "Keep a session under <alias>, which owns its channel subscription and what runs it again",
		Long: `Keep a session under <alias>.

The alias owns the session's channel subscription, set by the flags cci
subscribe takes: the window the session runs in hears it at once, a resume in a
new window and a /clear in the same one keep it, and the session never runs cci
subscribe, which refuses it. Registering an alias again replaces what it holds
and keeps its cursor. --cwd, --command and --resume-prompt record how the
session runs again, and --notify the lanes told when it stops hearing. cci
session rm ends it.`,
		Args: cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			ctx := cmd.Context()
			window, session, err := registeredSession(ctx, session)
			if err != nil {
				return err
			}
			sub, err := flags.subscription(ctx, st, session, window, window.PID == 0 || channelsOn(window.PID), "channel-"+args[0])
			if err != nil {
				return err
			}
			sub.Alias = args[0]
			if cwd == "" {
				if cwd, err = getwd(ctx); err != nil {
					return fmt.Errorf("resolve cwd: %w", err)
				}
			}
			r := store.Registration{Subscription: sub, Cwd: cwd, Command: command, ResumePrompt: resumePrompt, Notify: notify}
			register := func() error {
				r, err = st.Register(ctx, r)
				return err
			}
			if window.PID != 0 {
				err = holdLane(ctx, st, sub, register)
			} else {
				err = register()
			}
			if err != nil {
				return err
			}
			from, _, err := st.Cursor(ctx, r.Cursor, r.Drive)
			if err != nil {
				return err
			}
			where := fmt.Sprintf("in window %d, which hears it now", window.PID)
			if window.PID == 0 {
				where = "not running: the next window it starts in hears it"
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s registered: session %s %s; %s, cursor %s at #%d\n", r.Alias, r.Session, where, describe(r.Subscription), r.Cursor, from)
			return err
		}),
	}
	flags.bind(cmd, "resume from and advance this cursor, used by no other reader (default: channel-<alias>, starting at the current head)")
	cmd.Flags().StringVar(&session, "session", "", "the Claude Code session id to keep (default: this session)")
	cmd.Flags().StringVar(&cwd, "cwd", "", "the directory the session runs in (default: this command's)")
	cmd.Flags().StringVar(&command, "command", "", "the command that starts the session")
	cmd.Flags().StringVar(&resumePrompt, "resume-prompt", "", "the prompt the session resumes with")
	cmd.Flags().StringSliceVar(&notify, "notify", nil, "lanes told when the session stops hearing (repeatable)")
	return cmd
}

func registeredSession(ctx context.Context, given string) (store.Window, string, error) {
	if given == "" {
		return thisSession(ctx)
	}
	dir, err := harness.Dir(ctx)
	if err != nil {
		return store.Window{}, "", err
	}
	running, err := harness.CurrentSessions(dir)
	if err != nil {
		return store.Window{}, "", err
	}
	h, ok := running[given]
	if !ok || !h.Alive(harness.Procs{}) {
		return store.Window{}, given, nil
	}
	window, err := windowOf(h.PID)
	return window, given, err
}

func newSessionListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered sessions: each alias, the session it keeps, whether it runs, and its cursor",
		Args:  cobra.NoArgs,
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, _ []string) error {
			ctx := cmd.Context()
			all, err := st.Registrations(ctx)
			if err != nil {
				return err
			}
			out := cmd.OutOrStdout()
			if len(all) == 0 {
				_, err := fmt.Fprintln(out, "no session is registered; cci session register <alias> keeps one")
				return err
			}
			dir, err := harness.Dir(ctx)
			if err != nil {
				return err
			}
			running, err := harness.CurrentSessions(dir)
			if err != nil {
				return err
			}
			procs := harness.Procs{}
			for _, r := range all {
				at, _, err := st.Cursor(ctx, r.Cursor, r.Drive)
				if err != nil {
					return err
				}
				state := "not running"
				if h, ok := running[r.Session]; ok && h.Alive(procs) {
					state = fmt.Sprintf("running in window %d", h.PID)
				}
				if _, err := fmt.Fprintf(out, "%s: session %s %s, drive %s as %s, cursor %s at #%d, notify %s\n", r.Alias, r.Session, state, r.Drive, r.Reader, r.Cursor, at, cmp.Or(strings.Join(r.Notify, ","), "none")); err != nil {
					return err
				}
				for _, detail := range [][2]string{{"cwd", r.Cwd}, {"command", r.Command}, {"resume prompt", r.ResumePrompt}} {
					if detail[1] == "" {
						continue
					}
					if _, err := fmt.Fprintf(out, "  %s %s\n", detail[0], detail[1]); err != nil {
						return err
					}
				}
			}
			return nil
		}),
	}
}

func newSessionRmCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "rm <alias>",
		Short: "Stop keeping the session registered as <alias> and drop the channel subscription it owned",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			r, ok, err := st.Unregister(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("no session is registered as %s; cci session list names each alias", args[0])
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s removed: session %s keeps no channel subscription until cci subscribe or cci session register gives it one\n", r.Alias, r.Session)
			return err
		}),
	}
}
