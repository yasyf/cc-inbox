package cli

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/supervisor"
)

func newSessionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "session",
		Short: "Keep sessions alive under a cci alias, each in its own tmux session, with the alias owning their channel subscription",
		Args:  cobra.NoArgs,
	}
	cmd.AddCommand(newSessionRegisterCmd(), newSessionListCmd(), newSessionRmCmd(), newSessionRunCmd(), newSessionAttachCmd(), newSessionTypeCmd())
	return cmd
}

func newSessionRegisterCmd() *cobra.Command {
	var (
		flags        subscriptionFlags
		session      string
		fresh        bool
		cwd          string
		command      string
		resumePrompt string
		notify       []string
	)
	cmd := &cobra.Command{
		Use:   "register <alias>",
		Short: "Keep a session alive under <alias> in the tmux session named <alias>, which owns its channel subscription",
		Long: `Keep a session alive under <alias>.

cci runs the session in the tmux session named <alias>, where cci session run
starts --command from --cwd and starts it again with --resume and
--resume-prompt each time Claude Code exits; Orca shows it with cci session
attach <alias>. A session not running now starts at once; one running in
another window moves there when that window exits.

The alias owns the session's channel subscription, set by the flags cci
subscribe takes: each window the session runs in hears it, and the session
never runs cci subscribe, which refuses it. --notify names the lanes told when
its channel disconnects. Registering an alias again replaces what it holds and
keeps its cursor; cci session rm ends it.`,
		Args: cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			ctx := cmd.Context()
			window, session, err := registeredSession(ctx, session, fresh)
			if err != nil {
				return err
			}
			channeled := slices.Contains(channelArgs(strings.Fields(command)), "plugin:cc-inbox@")
			if window.PID != 0 {
				channeled = channelsOn(window.PID)
			}
			sub, err := flags.subscription(ctx, st, session, window, channeled, "channel-"+args[0])
			if err != nil {
				return err
			}
			sub.Alias = args[0]
			if cwd == "" {
				if cwd, err = getwd(ctx); err != nil {
					return fmt.Errorf("resolve cwd: %w", err)
				}
			}
			shell := getenv(ctx, "SHELL")
			if shell == "" {
				return errors.New("SHELL is unset: cci session run starts --command in your login shell")
			}
			tmux, err := exec.LookPath("tmux")
			if err != nil {
				return fmt.Errorf("cci keeps a registered session in tmux, and none is on PATH: %w", err)
			}
			r := store.Registration{Subscription: sub, Cwd: cwd, Command: command, ResumePrompt: resumePrompt, Notify: notify, Shell: shell, Tmux: tmux}
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
			where := fmt.Sprintf("running in window %d, which hears it now; once that window exits it runs in tmux session %s", window.PID, r.Alias)
			if window.PID == 0 {
				launched, err := supervisor.Launch(ctx, r)
				if err != nil {
					return err
				}
				where = "running in tmux session " + r.Alias
				if launched {
					where = "started in tmux session " + r.Alias
				}
			}
			from, _, err := st.Cursor(ctx, r.Cursor, r.Drive)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s registered: session %s %s, which cci session attach %s shows; %s, cursor %s at #%d\n", r.Alias, r.Session, where, r.Alias, describe(r.Subscription), r.Cursor, from)
			return err
		}),
	}
	flags.bind(cmd, "resume from and advance this cursor, used by no other reader (default: channel-<alias>, starting at the current head)")
	cmd.Flags().StringVar(&session, "session", "", "the Claude Code session id to keep (default: this session)")
	cmd.Flags().BoolVar(&fresh, "new", false, "start a new session under the alias in place of keeping one")
	cmd.Flags().StringVar(&cwd, "cwd", "", "the directory the session runs in (default: this command's)")
	cmd.Flags().StringVar(&command, "command", "", "the command that starts the session, which cci runs with --session-id or --resume and the resume prompt after it")
	cmd.Flags().StringVar(&resumePrompt, "resume-prompt", "", "the prompt the session starts and resumes with")
	cmd.Flags().StringSliceVar(&notify, "notify", nil, "lanes told when the session's channel disconnects (repeatable; default: communicator)")
	cmd.MarkFlagsMutuallyExclusive("session", "new")
	_ = cmd.MarkFlagRequired("command")
	return cmd
}

func registeredSession(ctx context.Context, given string, fresh bool) (store.Window, string, error) {
	switch {
	case fresh:
		return store.Window{}, uuid.NewString(), nil
	case given == "":
		return thisSession(ctx)
	}
	h, running, err := harness.Running(ctx, given)
	if err != nil || !running {
		return store.Window{}, given, err
	}
	window, err := windowOf(h.PID)
	return window, given, err
}

func newSessionListCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List registered sessions: each alias, the session it keeps, where it runs, and its cursor",
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
			for _, r := range all {
				at, _, err := st.Cursor(ctx, r.Cursor, r.Drive)
				if err != nil {
					return err
				}
				h, running, err := harness.Running(ctx, r.Session)
				if err != nil {
					return err
				}
				up, err := supervisor.Up(ctx, r)
				if err != nil {
					return err
				}
				state := "not running"
				if running {
					state = fmt.Sprintf("running in window %d", h.PID)
				}
				held := "no tmux session"
				if up {
					held = "tmux session " + r.Alias
				}
				if _, err := fmt.Fprintf(out, "%s: session %s %s, %s, drive %s as %s, cursor %s at #%d, notify %s\n", r.Alias, r.Session, state, held, r.Drive, r.Reader, r.Cursor, at, cmp.Or(strings.Join(r.Notify, ","), "communicator")); err != nil {
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
		Short: "Stop keeping the session registered as <alias> alive and drop the channel subscription it owned",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			r, ok, err := st.Unregister(cmd.Context(), args[0])
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("no session is registered as %s; cci session list names each alias", args[0])
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "%s removed: session %s keeps no channel subscription, and once Claude Code exits nothing starts it again and tmux session %s ends\n", r.Alias, r.Session, r.Alias)
			return err
		}),
	}
}

func newSessionRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run <alias>",
		Short: "Run the session registered as <alias> on this terminal, starting it again each time Claude Code exits, until cci session rm",
		Long: `Run the session registered as <alias> on this terminal.

This is the command of the tmux session cci launches for <alias>: it starts the
registered command, waits for Claude Code to exit, and starts it again with
--resume and the resume prompt, until cci session rm <alias> removes the alias.
An interrupt reaches Claude Code and never stops this loop.`,
		Args: cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			signal.Notify(make(chan os.Signal, 1), os.Interrupt, syscall.SIGQUIT)
			return supervisor.Run(cmd.Context(), st, args[0], cmd.InOrStdin(), cmd.OutOrStdout(), cmd.ErrOrStderr())
		}),
	}
}

func newSessionAttachCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "attach <alias>",
		Short: "Attach this terminal, such as an Orca terminal, to the tmux session of <alias>, starting it if it is down",
		Args:  cobra.ExactArgs(1),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			ctx := cmd.Context()
			r, err := registered(ctx, st, args[0])
			if err != nil {
				return err
			}
			up, err := supervisor.Up(ctx, r)
			if err != nil {
				return err
			}
			if !up {
				h, running, err := harness.Running(ctx, r.Session)
				if err != nil {
					return err
				}
				if running {
					return fmt.Errorf("session %s of %s runs in window %d, outside tmux; it moves to tmux session %s once that window exits", r.Session, r.Alias, h.PID, r.Alias)
				}
				if _, err := supervisor.Launch(ctx, r); err != nil {
					return err
				}
			}
			return syscall.Exec(r.Tmux, []string{r.Tmux, "attach-session", "-t", "=" + r.Alias}, os.Environ()) //nolint:gosec // G204: the tmux the session was registered with.
		}),
	}
}

func newSessionTypeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "type <alias> <line>",
		Short: "Type <line> at the prompt of the session registered as <alias> and press Enter, as cci does to reconnect its channel",
		Args:  cobra.ExactArgs(2),
		RunE: withStore(func(cmd *cobra.Command, st *store.Store, args []string) error {
			r, err := registered(cmd.Context(), st, args[0])
			if err != nil {
				return err
			}
			if err := supervisor.Type(cmd.Context(), r, args[1]); err != nil {
				return err
			}
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "typed into tmux session %s: %s\n", r.Alias, args[1])
			return err
		}),
	}
}

func registered(ctx context.Context, st *store.Store, alias string) (store.Registration, error) {
	r, ok, err := st.Registration(ctx, alias)
	if err != nil {
		return store.Registration{}, err
	}
	if !ok {
		return store.Registration{}, fmt.Errorf("no session is registered as %s; cci session list names each alias", alias)
	}
	return r, nil
}
