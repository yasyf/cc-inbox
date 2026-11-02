package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	unsentEnv   = "CCI_UNSENT"
	unsentDir   = "unsent"
	refusedExt  = ".refused"
	drainBound  = 30 * time.Second
	BusDownExit = 75
)

var postPatience = 15 * time.Second

// BusDown is the error of a post the store did not take; cci exits with
// BusDownExit on it.
type BusDown struct {
	Cause error
	Path  string
}

func (e *BusDown) Error() string {
	if e.Path == "" {
		return fmt.Sprintf("the bus is down, this post was NOT stored: %v", e.Cause)
	}
	return fmt.Sprintf("the bus is down, this post was NOT stored yet: %v; it is kept at %s and the next cci post that reaches the bus stores it", e.Cause, e.Path)
}

func (e *BusDown) Unwrap() error { return e.Cause }

type givenFlag interface{ Given() []string }

type unsent struct {
	Args []string  `json:"args"`
	Dir  string    `json:"dir"`
	At   time.Time `json:"at"`
}

func untilStored(run func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx, cancel := context.WithCancel(cmd.Context())
		defer cancel()
		cmd.SetContext(ctx)
		done := make(chan error, 1)
		go func() { done <- run(cmd, args) }()
		var cause error
		select {
		case err := <-done:
			if err == nil {
				return drainUnsent(context.WithoutCancel(ctx), cmd)
			}
			if !store.Busy(err) {
				return err
			}
			cause = err
		case <-time.After(postPatience):
			cancel()
			cause = fmt.Errorf("the store did not answer within %s", postPatience)
		}
		return keepUnsent(cmd, args, cause)
	}
}

func keepUnsent(cmd *cobra.Command, args []string, cause error) error {
	if os.Getenv(unsentEnv) != "" {
		return &BusDown{Cause: cause}
	}
	for _, name := range []string{"lane", "drive"} {
		if !cmd.Flags().Changed(name) {
			return &BusDown{Cause: fmt.Errorf("%w; repost it with --lane and --drive, which a post kept for the bus to come back must name", cause)}
		}
	}
	if slices.Contains(args, "-") || slices.ContainsFunc([]string{"text", "path", "body-file"}, func(name string) bool {
		return slices.Contains([]string{"-", "/dev/stdin"}, cmd.Flags().Lookup(name).Value.String())
	}) {
		return &BusDown{Cause: fmt.Errorf("%w; a post read from stdin cannot be kept, so repost it once the bus answers", cause)}
	}
	home, err := store.Home()
	if err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	dir, err := os.Getwd()
	if err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	body, err := json.Marshal(unsent{Args: rebuiltArgs(cmd, args), Dir: dir, At: time.Now().UTC()})
	if err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	path := filepath.Join(home, unsentDir, fmt.Sprintf("%d.json", time.Now().UnixNano()))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	if err := os.WriteFile(path+".tmp", body, 0o600); err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		return errors.Join(&BusDown{Cause: cause}, err)
	}
	return &BusDown{Cause: cause, Path: path}
}

func rebuiltArgs(cmd *cobra.Command, args []string) []string {
	out := []string{cmd.Name()}
	cmd.Flags().Visit(func(f *pflag.Flag) {
		var values []string
		switch value := f.Value.(type) {
		case givenFlag:
			values = value.Given()
		case pflag.SliceValue:
			values = value.GetSlice()
		default:
			values = []string{strings.TrimSuffix(strings.TrimPrefix(f.Value.String(), "["), "]")}
			if f.Value.Type() != "stringToString" {
				values = []string{f.Value.String()}
			}
		}
		for _, v := range values {
			out = append(out, "--"+f.Name+"="+v)
		}
	})
	return append(append(out, "--"), args...)
}

func drainUnsent(ctx context.Context, cmd *cobra.Command) error {
	if os.Getenv(unsentEnv) != "" {
		return nil
	}
	home, err := store.Home()
	if err != nil {
		return err
	}
	kept, err := filepath.Glob(filepath.Join(home, unsentDir, "*.json"))
	if err != nil {
		return err
	}
	slices.Sort(kept)
	self, err := os.Executable()
	if err != nil {
		return err
	}
	stored := 0
drain:
	for _, path := range kept {
		raw, err := os.ReadFile(path) //nolint:gosec // G304: a post this cci home kept while its bus was down.
		if err != nil {
			return err
		}
		var post unsent
		if err := json.Unmarshal(raw, &post); err != nil {
			return fmt.Errorf("read the kept post %s: %w", path, err)
		}
		bounded, cancel := context.WithTimeout(ctx, drainBound)
		child := exec.CommandContext(bounded, self, post.Args...) //nolint:gosec // G204: this cci, rerunning a post its sender ran.
		child.Dir = post.Dir
		child.Env = append(os.Environ(), unsentEnv+"="+path, "CCI_HOME="+home)
		out, err := child.CombinedOutput()
		timedOut := bounded.Err() != nil
		cancel()
		var exit *exec.ExitError
		switch {
		case err == nil:
			if err := os.Remove(path); err != nil {
				return err
			}
			stored++
		case timedOut, errors.As(err, &exit) && exit.ExitCode() == BusDownExit:
			break drain
		default:
			if err := os.WriteFile(path+refusedExt, append(raw, "\n"+strings.TrimSpace(string(out))+"\n"...), 0o600); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			fmt.Fprintf(cmd.ErrOrStderr(), "cci: the bus refused a post kept since %s: %s; it is at %s\n", post.At.Local().Format(time.Kitchen), strings.TrimSpace(string(out)), path+refusedExt)
		}
	}
	if stored > 0 {
		fmt.Fprintf(cmd.ErrOrStderr(), "cci: stored %d post(s) kept while the bus was down\n", stored)
	}
	return nil
}
