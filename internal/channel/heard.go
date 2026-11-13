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
	"time"

	"github.com/yasyf/cc-inbox/internal/harness"
	"github.com/yasyf/cc-inbox/internal/store"
)

var (
	deafAfter    = 30 * time.Second
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
	tag      tag
	moves    []store.CursorMove
	sent     time.Time
	received bool
	heard    bool
}

type ledger struct {
	st       *store.Store
	session  string
	window   store.Window
	path     string
	read     int64
	mu       sync.Mutex
	owed     []owed
	sent     map[string]int64
	received time.Time
	deaf     bool
}

func newLedger(ctx context.Context, st *store.Store, sub store.Subscription) (*ledger, error) {
	path, err := transcriptOf(ctx, sub.Session)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("stat the session's transcript: %w", err)
	}
	return &ledger{st: st, session: sub.Session, window: sub.Window, path: path, read: info.Size(), sent: map[string]int64{}}, nil
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
			if t, heard, ok := transcriptTag(line); ok {
				if err := l.receive(ctx, t, heard); err != nil {
					slog.Warn("cci channel: move the cursors of a heard tag", "seq", t.seq, "err", err)
				}
			}
			line = line[:0]
		case !errors.Is(err, io.EOF):
			return fmt.Errorf("read the session's transcript: %w", err)
		case ctx.Err() != nil:
			return nil
		default:
			if t, ok := l.deafened(); ok {
				if err := deafened(ctx, l.st, l.window, t); err != nil {
					slog.Warn("cci channel: tell the session's notify lanes it stopped hearing", "seq", t.seq, "err", err)
				}
			}
			sleep(ctx, interval)
		}
	}
}

func transcriptTag(line []byte) (tag, bool, bool) {
	if !bytes.Contains(line, tagMarker) {
		return tag{}, false, false
	}
	var event struct {
		Type        string `json:"type"`
		IsSidechain bool   `json:"isSidechain"`
		Content     string `json:"content"`
		Message     struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
		Attachment struct {
			Type   string          `json:"type"`
			Prompt json.RawMessage `json:"prompt"`
		} `json:"attachment"`
	}
	if json.Unmarshal(line, &event) != nil || event.IsSidechain {
		return tag{}, false, false
	}
	if event.Type == "queue-operation" {
		t, ok := openingTag(event.Content)
		return t, false, ok
	}
	var prompt json.RawMessage
	switch {
	case event.Type == "user":
		prompt = event.Message.Content
	case event.Type == "attachment" && event.Attachment.Type == "queued_command":
		prompt = event.Attachment.Prompt
	default:
		return tag{}, false, false
	}
	t, ok := openingTag(promptText(prompt))
	return t, ok, ok
}

func openingTag(text string) (tag, bool) {
	opening := tagOpening.FindStringSubmatch(text)
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
	l.owed = append(l.owed, owed{tag: t, moves: moves, sent: time.Now()})
}

func (l *ledger) pass(ctx context.Context, moves ...store.CursorMove) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.note(moves)
	l.owed = append(l.owed, owed{moves: moves, received: true, heard: true})
	return l.settle(ctx)
}

func (l *ledger) receive(ctx context.Context, t tag, heard bool) error {
	l.mu.Lock()
	i := slices.IndexFunc(l.owed, func(o owed) bool { return o.tag == t && !o.heard && (heard || !o.received) })
	if i < 0 {
		l.mu.Unlock()
		return nil
	}
	l.owed[i].received = true
	l.owed[i].heard = heard
	regained := l.received.IsZero() || l.deaf
	l.received, l.deaf = time.Now(), false
	err := l.settle(ctx)
	l.mu.Unlock()
	if regained {
		err = errors.Join(err, reconnected(context.WithoutCancel(ctx), l.st, l.window))
	}
	return err
}

func (l *ledger) deafened() (tag, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.deaf || time.Since(l.received) < deafAfter {
		return tag{}, false
	}
	i := slices.IndexFunc(l.owed, func(o owed) bool { return !o.received && time.Since(o.sent) >= deafAfter })
	if i < 0 {
		return tag{}, false
	}
	l.deaf = true
	return l.owed[i].tag, true
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
