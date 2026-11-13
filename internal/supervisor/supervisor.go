// Package supervisor keeps each registered session running in a tmux session
// named by its alias: it launches that tmux session, starts Claude Code in it
// again each time it exits, and types into it.
package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
)

const orcaTerminal = "ORCA_TERMINAL_HANDLE"

var restartPause = 2 * time.Second

// Launch starts the tmux session named by r's alias, running cci session run in
// r's login shell, unless it already exists, and reports whether it started it.
// Each client attaching to it then copies its ORCA_TERMINAL_HANDLE, the Orca
// terminal Orca names as the caller, into the tmux session's environment.
func Launch(ctx context.Context, r store.Registration) (bool, error) {
	up, err := Up(ctx, r)
	if err != nil || up {
		return false, err
	}
	if _, err := tmux(ctx, r, "new-session", "-d", "-s", r.Alias, "-c", r.Cwd, "--", r.Shell, "-l", "-c", "exec cci session run "+r.Alias, ";", "set-option", "-a", "-t", pane(r), "update-environment", orcaTerminal); err != nil {
		return false, err
	}
	return true, nil
}

// Up reports whether the tmux session named by r's alias exists.
func Up(ctx context.Context, r store.Registration) (bool, error) {
	_, err := tmux(ctx, r, "has-session", "-t", "="+r.Alias)
	if exit := (*exec.ExitError)(nil); errors.As(err, &exit) {
		return false, nil
	}
	return err == nil, err
}

// Type types line at the prompt of r's tmux session and presses Enter.
func Type(ctx context.Context, r store.Registration, line string) error {
	if _, err := tmux(ctx, r, "send-keys", "-t", pane(r), "-l", line); err != nil {
		return err
	}
	_, err := tmux(ctx, r, "send-keys", "-t", pane(r), "Enter")
	return err
}

func pane(r store.Registration) string {
	return "=" + r.Alias + ":"
}

// Keep launches the tmux session of each registered session that runs in no
// window, as after a reboot.
func Keep(ctx context.Context, st *store.Store) error {
	all, err := st.Registrations(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range all {
		_, running, err := harness.Running(ctx, r.Session)
		if err == nil && !running {
			_, err = Launch(ctx, r)
		}
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Run keeps alias's session running on this terminal until cci session rm
// removes alias: it starts the registered command in a login shell, as a
// top-level session with none of the Claude Code, cci lane or Orca pane
// variables of whatever started the tmux server but the ORCA_TERMINAL_HANDLE
// of the Orca terminal that last attached to it, waits for it to exit, and
// starts it again resuming the session.
func Run(ctx context.Context, st *store.Store, alias string, stdin io.Reader, stdout, stderr io.Writer) error {
	for again := false; ; again = true {
		r, ok, err := st.Registration(ctx, alias)
		if err != nil {
			return err
		}
		if !ok {
			_, err := fmt.Fprintf(stderr, "cci: %s is no longer registered, so nothing starts it again\n", alias)
			return err
		}
		w, running, err := harness.Running(ctx, r.Session)
		if err != nil {
			return err
		}
		if running {
			return fmt.Errorf("session %s of %s runs in window %d already; it starts here once that window exits", r.Session, alias, w.PID)
		}
		line, err := CommandLine(ctx, r)
		if err != nil {
			return err
		}
		if again {
			if _, err := fmt.Fprintf(stderr, "cci: starting %s again: %s\n", alias, line); err != nil {
				return err
			}
		}
		c := exec.CommandContext(ctx, r.Shell, "-l", "-c", line) //nolint:gosec // G204: the command the session was registered with.
		c.Dir, c.Stdin, c.Stdout, c.Stderr = r.Cwd, stdin, stdout, stderr
		c.Env = slices.DeleteFunc(os.Environ(), func(kv string) bool {
			name, _, _ := strings.Cut(kv, "=")
			return name != "CLAUDE_CONFIG_DIR" && (strings.HasPrefix(name, "CLAUDE") || strings.HasPrefix(name, "ORCA_") || name == "CCI_AGENT_ID")
		})
		if kv, err := tmux(ctx, r, "show-environment", "-t", "="+r.Alias, orcaTerminal); err == nil && bytes.HasPrefix(kv, []byte(orcaTerminal+"=")) {
			c.Env = append(c.Env, string(bytes.TrimSpace(kv)))
		}
		exited := c.Run()
		if ctx.Err() != nil {
			return nil
		}
		if _, err := fmt.Fprintf(stderr, "cci: %s exited (%v)\n", alias, exited); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(restartPause):
		}
	}
}

// CommandLine is the line that starts r's session: its command with
// --session-id until Claude Code has written the session's transcript, then
// with --resume, and its resume prompt.
func CommandLine(ctx context.Context, r store.Registration) (string, error) {
	flag := "--resume"
	if _, err := harness.Transcript(ctx, r.Session); errors.Is(err, harness.ErrNoTranscript) {
		flag = "--session-id"
	} else if err != nil {
		return "", err
	}
	line := r.Command + " " + flag + " " + r.Session
	if r.ResumePrompt != "" {
		line += " -- '" + strings.ReplaceAll(r.ResumePrompt, "'", `'\''`) + "'"
	}
	return line, nil
}

func tmux(ctx context.Context, r store.Registration, args ...string) ([]byte, error) {
	out, err := exec.CommandContext(ctx, r.Tmux, args...).CombinedOutput() //nolint:gosec // G204: the tmux the session was registered with.
	if err != nil {
		return out, fmt.Errorf("%s %s: %w: %s", r.Tmux, strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return out, nil
}
