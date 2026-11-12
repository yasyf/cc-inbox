package channel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"slices"
	"strconv"
	"sync"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
)

var (
	transcriptOf = harness.Transcript
	tagOpening   = regexp.MustCompile(`^<channel source="` + regexp.QuoteMeta(Plugin) + `[^"]*"([^>]*)>`)
	tagAttribute = regexp.MustCompile(`([a-z]+)="([^"]*)"`)
	tagMarker    = []byte(`<channel source=\"` + Plugin)
)

type tag struct {
	kind string
	seq  int64
}

type owed struct {
	tag   tag
	moves []store.CursorMove
	heard bool
}

type ledger struct {
	st      *store.Store
	session string
	path    string
	read    int64
	mu      sync.Mutex
	owed    []owed
	sent    map[string]int64
}

func newLedger(ctx context.Context, st *store.Store, session string) (*ledger, error) {
	path, err := transcriptOf(ctx, session)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat the session's transcript: %w", err)
	}
	return &ledger{st: st, session: session, path: path, read: info.Size(), sent: map[string]int64{}}, nil
}

func (l *ledger) listen(ctx context.Context, fn func(context.Context) error) error {
	f, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("open the session's transcript: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := f.Seek(l.read, io.SeekStart); err != nil {
		return fmt.Errorf("seek the session's transcript: %w", err)
	}
	listening, stop := context.WithCancel(ctx)
	defer stop()
	heard := make(chan error, 1)
	go func() {
		err := l.hearAll(listening, f)
		stop()
		heard <- err
	}()
	err = fn(listening)
	stop()
	return errors.Join(err, <-heard)
}

func (l *ledger) hearAll(ctx context.Context, transcript io.Reader) error {
	r := bufio.NewReader(transcript)
	var line []byte
	for {
		chunk, err := r.ReadBytes('\n')
		line = append(line, chunk...)
		switch {
		case err == nil:
			l.read += int64(len(line))
			if t, ok := heardTag(line); ok {
				if err := l.hear(ctx, t); err != nil {
					slog.Warn("cci channel: move the cursors of a heard tag", "seq", t.seq, "err", err)
				}
			}
			line = line[:0]
		case !errors.Is(err, io.EOF):
			return fmt.Errorf("read the session's transcript: %w", err)
		case ctx.Err() != nil:
			return nil
		default:
			sleep(ctx, interval)
		}
	}
}

func heardTag(line []byte) (tag, bool) {
	if !bytes.Contains(line, tagMarker) {
		return tag{}, false
	}
	var event struct {
		Type        string `json:"type"`
		IsSidechain bool   `json:"isSidechain"`
		Message     struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Attachment struct {
			Type   string          `json:"type"`
			Prompt json.RawMessage `json:"prompt"`
		} `json:"attachment"`
	}
	if json.Unmarshal(line, &event) != nil || event.IsSidechain {
		return tag{}, false
	}
	var prompt json.RawMessage
	switch {
	case event.Type == "user":
		prompt = event.Message.Content
	case event.Type == "attachment" && event.Attachment.Type == "queued_command":
		prompt = event.Attachment.Prompt
	default:
		return tag{}, false
	}
	opening := tagOpening.FindStringSubmatch(promptText(prompt))
	if opening == nil {
		return tag{}, false
	}
	attrs := map[string]string{}
	for _, a := range tagAttribute.FindAllStringSubmatch(opening[1], -1) {
		attrs[a[1]] = a[2]
	}
	seq, err := strconv.ParseInt(attrs["seq"], 10, 64)
	if err != nil || attrs["kind"] == "" {
		return tag{}, false
	}
	return tag{kind: attrs["kind"], seq: seq}, true
}

func promptText(raw json.RawMessage) string {
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return text
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil || len(blocks) == 0 || blocks[0].Type != "text" {
		return ""
	}
	return blocks[0].Text
}

func (l *ledger) position(ctx context.Context, name, drive string) (int64, error) {
	stored, _, err := l.st.Cursor(ctx, name, drive)
	l.mu.Lock()
	defer l.mu.Unlock()
	return max(stored, l.sent[name]), err
}

func (l *ledger) owe(t tag, moves ...store.CursorMove) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.note(moves)
	l.owed = append(l.owed, owed{tag: t, moves: moves})
}

func (l *ledger) pass(ctx context.Context, moves ...store.CursorMove) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.note(moves)
	l.owed = append(l.owed, owed{moves: moves, heard: true})
	return l.settle(ctx)
}

func (l *ledger) hear(ctx context.Context, t tag) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	i := slices.IndexFunc(l.owed, func(o owed) bool { return !o.heard && o.tag == t })
	if i < 0 {
		return nil
	}
	l.owed[i].heard = true
	return l.settle(ctx)
}

func (l *ledger) note(moves []store.CursorMove) {
	for _, m := range moves {
		l.sent[m.Name] = max(l.sent[m.Name], m.Seq)
	}
}

func (l *ledger) settle(ctx context.Context) error {
	n := 0
	var moves []store.CursorMove
	for ; n < len(l.owed) && l.owed[n].heard; n++ {
		moves = append(moves, l.owed[n].moves...)
	}
	if err := l.st.MoveCursors(context.WithoutCancel(ctx), moves); err != nil {
		return err
	}
	l.owed = l.owed[n:]
	return nil
}
