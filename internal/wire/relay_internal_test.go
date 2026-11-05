package wire

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
)

type daemonEnd func(t *testing.T, r *bufio.Reader, w io.Writer)

func fakeDaemon(t *testing.T, end daemonEnd) io.ReadWriteCloser {
	t.Helper()
	client, daemon := net.Pipe()
	go func() {
		defer func() { _ = daemon.Close() }()
		r := bufio.NewReader(daemon)
		if kind, _, err := ReadFrame(r); err != nil || kind != FrameRequest {
			t.Errorf("first frame = %q, %v; want a request", kind, err)
			return
		}
		end(t, r, daemon)
	}()
	return client
}

func readStdin(t *testing.T, r *bufio.Reader, w io.Writer) []byte {
	t.Helper()
	if err := WriteFrame(w, FrameNeedIn, nil); err != nil {
		t.Errorf("ask for stdin: %v", err)
		return nil
	}
	for {
		kind, payload, err := ReadFrame(r)
		if err != nil {
			t.Errorf("read stdin: %v", err)
			return nil
		}
		if kind == FrameStdin {
			return payload
		}
	}
}

func exitWith(code int) daemonEnd {
	return func(t *testing.T, r *bufio.Reader, w io.Writer) {
		readStdin(t, r, w)
		if err := WriteFrame(w, FrameExit, ExitPayload(code)); err != nil {
			t.Errorf("exit: %v", err)
		}
	}
}

func TestAChannelADaemonEndsWhileClaudeHoldsStdinRunsItselfResumed(t *testing.T) {
	stdin, claude := io.Pipe()
	go func() { _, _ = claude.Write([]byte(`{"method":"notifications/initialized"}` + "\n")) }()
	pump := &stdinPump{in: stdin, needs: make(chan struct{}, 1)}
	go pump.run()
	request := Request{Argv: []string{"cci", "channel"}}
	code, ok, argv := relay(fakeDaemon(t, exitWith(0)), request, pump, func() io.ReadWriteCloser {
		t.Error("redialed after the daemon ended the channel")
		return nil
	}, io.Discard, io.Discard)
	if ok {
		t.Fatalf("relay = %d, true; want the channel handed back while Claude still holds stdin", code)
	}
	if !slices.Equal(argv, []string{"cci", "channel", "--resumed"}) {
		t.Errorf("argv = %v, want the channel to resume the handshake Claude already made", argv)
	}
}

func TestAChannelEndsWhenClaudeClosesStdin(t *testing.T) {
	pump := &stdinPump{in: strings.NewReader(""), needs: make(chan struct{}, 1)}
	go pump.run()
	request := Request{Argv: []string{"cci", "channel"}}
	code, ok, _ := relay(fakeDaemon(t, exitWith(0)), request, pump, func() io.ReadWriteCloser { return nil }, io.Discard, io.Discard)
	if !ok || code != 0 {
		t.Errorf("relay = %d, %v; want 0, true once Claude closed stdin", code, ok)
	}
}

func TestAChannelThatLosesItsDaemonResumesOnTheNext(t *testing.T) {
	stdin, claude := io.Pipe()
	go func() { _, _ = claude.Write([]byte("{}\n")) }()
	pump := &stdinPump{in: stdin, needs: make(chan struct{}, 1)}
	go pump.run()
	lost := fakeDaemon(t, func(t *testing.T, r *bufio.Reader, w io.Writer) { readStdin(t, r, w) })
	var resumedArgv []string
	next := func() io.ReadWriteCloser {
		client, daemon := net.Pipe()
		go func() {
			defer func() { _ = daemon.Close() }()
			r := bufio.NewReader(daemon)
			_, payload, err := ReadFrame(r)
			if err != nil {
				t.Errorf("resumed request: %v", err)
				return
			}
			resumedArgv = strings.Fields(string(payload))
			_ = claude.Close()
			if eof := readStdin(t, r, daemon); len(eof) != 0 {
				t.Errorf("stdin after close = %q, want EOF", eof)
			}
			_ = WriteFrame(daemon, FrameExit, ExitPayload(0))
		}()
		return client
	}
	code, ok, argv := relay(lost, Request{Argv: []string{"cci", "channel"}}, pump, next, io.Discard, io.Discard)
	if !ok || code != 0 {
		t.Errorf("relay = %d, %v; want 0, true", code, ok)
	}
	if !slices.Contains(argv, "--resumed") {
		t.Errorf("argv = %v, want the resumed channel", argv)
	}
	if !strings.Contains(strings.Join(resumedArgv, " "), "--resumed") {
		t.Errorf("resumed request = %v, want --resumed", resumedArgv)
	}
}

func TestAWatchThatLosesItsDaemonResumesFromItsCursorOnTheNext(t *testing.T) {
	pump := &stdinPump{in: strings.NewReader(""), needs: make(chan struct{}, 1)}
	go pump.run()
	argv := []string{"cci", "watch", "--drive", "d", "--cursor=watch-1"}
	lost := fakeDaemon(t, func(t *testing.T, r *bufio.Reader, w io.Writer) {
		if err := WriteFrame(w, FrameStdout, []byte("first\n")); err != nil {
			t.Errorf("stdout: %v", err)
			return
		}
		if kind, _, err := ReadFrame(r); err != nil || kind != FrameAck {
			t.Errorf("frame = %q, %v; want the client's ack", kind, err)
		}
	})
	var resumed Request
	next := func() io.ReadWriteCloser {
		client, daemon := net.Pipe()
		go func() {
			defer func() { _ = daemon.Close() }()
			_, payload, err := ReadFrame(bufio.NewReader(daemon))
			if err != nil {
				t.Errorf("resumed request: %v", err)
				return
			}
			if err := json.Unmarshal(payload, &resumed); err != nil {
				t.Errorf("resumed request: %v", err)
			}
			_ = WriteFrame(daemon, FrameExit, ExitPayload(0))
		}()
		return client
	}
	var stdout, stderr strings.Builder
	code, ok, _ := relay(lost, Request{Argv: argv}, pump, next, &stdout, &stderr)
	if !ok || code != 0 || stdout.String() != "first\n" || stderr.Len() != 0 {
		t.Errorf("relay = %d, %v, stdout %q, stderr %q; want the second daemon's exit with nothing said of the loss", code, ok, stdout.String(), stderr.String())
	}
	if !slices.Equal(resumed.Argv, argv) {
		t.Errorf("resumed argv = %v, want %v", resumed.Argv, argv)
	}
}

func TestThePumpNeverSendsAnEmptyReadAsEOF(t *testing.T) {
	client, daemon := net.Pipe()
	defer func() { _ = client.Close() }()
	pump := &stdinPump{in: &emptyThenData{}, needs: make(chan struct{}, 1)}
	go pump.run()
	if err := pump.attach(client); err != nil {
		t.Fatal(err)
	}
	pump.need()
	kind, payload, err := ReadFrame(bufio.NewReader(daemon))
	if err != nil || kind != FrameStdin || string(payload) != "x" {
		t.Errorf("frame = %q %q %v, want stdin x", kind, payload, err)
	}
	if pump.closed() {
		t.Error("pump reports stdin closed after an empty read")
	}
}

type emptyThenData struct{ reads int }

func (e *emptyThenData) Read(p []byte) (int, error) {
	e.reads++
	if e.reads == 1 {
		return 0, nil
	}
	return copy(p, "x"), nil
}
