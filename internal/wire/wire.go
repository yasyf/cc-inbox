// Package wire is the frame protocol between a cci process and the daemon that
// runs its command. It links nothing the daemon's store needs, so a client that
// only relays stays small.
package wire

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	SocketName = "cci.sock"

	FrameRequest  byte = 'q'
	FrameStdout   byte = 'o'
	FrameStderr   byte = 'e'
	FrameExit     byte = 'x'
	FrameFallback byte = 'f'
	FrameNeedIn   byte = 'r'
	FrameAck      byte = 'a'
	FrameStdin    byte = 'i'

	headerBytes   = 5
	maxFrameBytes = 16 << 20
	readBytes     = 64 << 10
	redialFor     = 30 * time.Second
	redialEvery   = 100 * time.Millisecond
	// LostExit is the exit code of a command whose daemon went away mid-reply.
	LostExit = 75
	// WatchCursorPrefix names the cursor a watch without one resumes from.
	WatchCursorPrefix = "watch-"
)

// Request is one command the daemon runs for a client.
type Request struct {
	Version        string   `json:"version"`
	Argv           []string `json:"argv"`
	Env            []string `json:"env"`
	Cwd            string   `json:"cwd"`
	PID            int      `json:"pid"`
	StdoutChar     bool     `json:"stdout_char"`
	StderrIsStdout bool     `json:"stderr_is_stdout,omitempty"`
	Embedder       bool     `json:"embedder,omitempty"`
}

// Outputs reports whether stdout is a terminal and whether stderr writes to the
// same file as stdout, as a shell's 2>&1 and the Claude Code Bash tool make it.
func Outputs(stdout, stderr *os.File) (terminal, shared bool, err error) {
	out, err := stdout.Stat()
	if err != nil {
		return false, false, err
	}
	errOut, errErr := stderr.Stat()
	return out.Mode()&os.ModeCharDevice != 0, errErr == nil && os.SameFile(out, errOut), nil
}

// Relayed reports whether argv is a command the daemon runs: a read or a post
// whose arguments name nothing only this process can open.
func Relayed(argv []string) bool {
	if len(argv) < 2 {
		return false
	}
	switch argv[1] {
	case "post", "tail", "grep", "show", "digest", "who", "watch", "channel", "mcp", "window", "embed":
	case "hook":
		if len(argv) < 3 || !slices.Contains([]string{"post-tool-use", "stop", "subagent-stop", "task-stop", "prompt"}, argv[2]) {
			return false
		}
	default:
		return false
	}
	for _, arg := range argv[2:] {
		_, value, _ := strings.Cut(arg, "=")
		if slices.ContainsFunc([]string{arg, value}, func(path string) bool {
			return strings.HasPrefix(path, "/dev/") || strings.HasPrefix(path, "/proc/")
		}) {
			return false
		}
	}
	return true
}

// WriteFrame writes one frame in one write.
func WriteFrame(w io.Writer, kind byte, payload []byte) error {
	frame := make([]byte, headerBytes+len(payload))
	frame[0] = kind
	binary.BigEndian.PutUint32(frame[1:headerBytes], uint32(len(payload))) //nolint:gosec // G115: a frame is bounded by maxFrameBytes.
	copy(frame[headerBytes:], payload)
	_, err := w.Write(frame)
	return err
}

// ReadFrame reads one frame.
func ReadFrame(r io.Reader) (byte, []byte, error) {
	var header [headerBytes]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	size := binary.BigEndian.Uint32(header[1:])
	if size > maxFrameBytes {
		return 0, nil, fmt.Errorf("frame of %d bytes", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return header[0], payload, nil
}

func dial(path string) (*os.File, error) {
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.Connect(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	return os.NewFile(uintptr(fd), "cci-daemon"), nil //nolint:gosec // G115: a file descriptor fits in uintptr.
}

// Stream reports whether argv runs until its client leaves, so any release's
// client may ask for it.
func Stream(argv []string) bool {
	return len(argv) > 2 && argv[1] == "embed" && argv[2] == "follow" || len(argv) > 1 && argv[1] == "watch" || Server(argv)
}

func forWhatIsLeft(argv []string, started time.Time) []string {
	left := func(given string) string {
		bound, err := time.ParseDuration(given)
		if err != nil {
			return given
		}
		return max(bound-time.Since(started), time.Millisecond).String()
	}
	out := slices.Clone(argv)
	for i, arg := range out {
		if given, ok := strings.CutPrefix(arg, "--for="); ok {
			out[i] = "--for=" + left(given)
		} else if arg == "--for" && i+1 < len(out) {
			out[i+1] = left(out[i+1])
		}
	}
	return out
}

// Relay runs this process's command in the daemon and reports its exit code. It
// returns false when the daemon did not take the command, so the caller runs
// os.Args itself; once any output has arrived the command is the daemon's. A
// stream that loses its daemon resumes on the next one, from its cursor.
func Relay(version string) (int, bool) {
	if !Relayed(os.Args) || os.Getenv("CCI_HOME") != "" || os.Getenv("CCI_UNSENT") != "" {
		return 0, false
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return 0, false
	}
	socket := filepath.Join(home, ".cc-inbox", SocketName)
	conn, err := dial(socket)
	if err != nil {
		return 0, false
	}
	cwd, err := os.Getwd()
	if err != nil {
		return 0, false
	}
	terminal, shared, err := Outputs(os.Stdout, os.Stderr)
	if err != nil {
		return 0, false
	}
	request := Request{
		Version: version, Argv: resumable(os.Args), Env: os.Environ(), Cwd: cwd, PID: os.Getpid(),
		StdoutChar: terminal, StderrIsStdout: shared,
	}
	pump := &stdinPump{in: os.Stdin, needs: make(chan struct{}, 1)}
	go pump.run()
	code, ok, argv := relay(conn, request, pump, func() io.ReadWriteCloser {
		if conn := redial(socket); conn != nil {
			return conn
		}
		return nil
	}, os.Stdout, os.Stderr)
	os.Args = argv
	return code, ok
}

// relay plays request on conn and on each connection redial returns after the
// last is lost, and returns the argv the caller runs itself when it reports
// false. A channel belongs to Claude Code until it closes stdin: a daemon that
// ends one first, as a daemon going away or failing to resume it does, hands
// the channel back to this process instead of ending the MCP session.
func relay(conn io.ReadWriteCloser, request Request, pump *stdinPump, redial func() io.ReadWriteCloser, stdout, stderr io.Writer) (int, bool, []string) {
	started, first := time.Now(), request.Argv
	for {
		code, ok, lost := exchange(conn, request, pump, stdout, stderr)
		_ = conn.Close()
		if Server(request.Argv) && ok && !lost && !pump.closed() {
			if pump.fed() {
				return 0, false, resumed(first)
			}
			return 0, false, request.Argv
		}
		if !lost || !Stream(request.Argv) {
			return code, ok, request.Argv
		}
		request.Argv = forWhatIsLeft(resumed(first), started)
		if conn = redial(); conn == nil {
			return 0, false, request.Argv
		}
	}
}

// Server reports whether argv runs one of cci's MCP servers, which belong to
// Claude Code until it closes their stdin.
func Server(argv []string) bool {
	return len(argv) > 1 && (argv[1] == "channel" || argv[1] == "mcp")
}

func resumable(argv []string) []string {
	if argv[1] != "watch" || !Stream(argv) || slices.ContainsFunc(argv, func(arg string) bool { return arg == "--cursor" || strings.HasPrefix(arg, "--cursor=") }) {
		return argv
	}
	return append(slices.Clone(argv), fmt.Sprintf("--cursor=%s%d-%d", WatchCursorPrefix, os.Getpid(), time.Now().UnixNano()))
}

func resumed(argv []string) []string {
	if !Server(argv) || slices.Contains(argv, "--resumed") {
		return argv
	}
	return append(slices.Clone(argv), "--resumed")
}

func redial(socket string) *os.File {
	for deadline := time.Now().Add(redialFor); time.Now().Before(deadline); time.Sleep(redialEvery) {
		if conn, err := dial(socket); err == nil {
			return conn
		}
	}
	return nil
}

// Exchange sends request over conn and plays the daemon's reply onto the streams.
func Exchange(conn io.ReadWriter, request Request, stdin io.Reader, stdout, stderr io.Writer) (int, bool) {
	code, ok, _ := ExchangeOutcome(conn, request, stdin, stdout, stderr)
	return code, ok
}

// ExchangeOutcome is Exchange that also says whether the connection was lost, so
// a caller can tell a daemon that declined the request from one that may have
// run it.
func ExchangeOutcome(conn io.ReadWriter, request Request, stdin io.Reader, stdout, stderr io.Writer) (code int, ran, lost bool) {
	pump := &stdinPump{in: stdin, needs: make(chan struct{}, 1)}
	go pump.run()
	defer close(pump.needs)
	return exchange(conn, request, pump, stdout, stderr)
}

type stdinPump struct {
	in      io.Reader
	needs   chan struct{}
	mu      sync.Mutex
	w       io.Writer
	reading bool
	held    [][]byte
	eof     bool
	sent    bool
}

// closed reports whether this process's stdin has ended.
func (p *stdinPump) closed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.eof
}

// fed reports whether any of stdin has gone to a daemon, so a command that
// takes it over must resume rather than start again.
func (p *stdinPump) fed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sent
}

// attach sends everything the last connection could not take down w, once w has
// carried its request.
func (p *stdinPump) attach(w io.Writer) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.w = w
	for len(p.held) > 0 {
		if err := WriteFrame(w, FrameStdin, p.held[0]); err != nil {
			return err
		}
		p.held = p.held[1:]
	}
	return nil
}

func (p *stdinPump) need() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.reading && len(p.held) == 0 {
		p.reading = true
		p.needs <- struct{}{}
	}
}

func (p *stdinPump) run() {
	buf := make([]byte, readBytes)
	for range p.needs {
		n, err := p.in.Read(buf)
		for n == 0 && err == nil {
			n, err = p.in.Read(buf)
		}
		p.mu.Lock()
		p.reading = false
		p.eof = n == 0
		p.sent = p.sent || n > 0
		if WriteFrame(p.w, FrameStdin, buf[:n]) != nil {
			p.held = append(p.held, slices.Clone(buf[:n]))
		}
		p.mu.Unlock()
	}
}

func (p *stdinPump) ack() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return WriteFrame(p.w, FrameAck, nil)
}

func exchange(conn io.ReadWriter, request Request, pump *stdinPump, stdout, stderr io.Writer) (code int, ok, lost bool) {
	body, err := json.Marshal(request)
	if err != nil || WriteFrame(conn, FrameRequest, body) != nil || pump.attach(conn) != nil {
		return 0, false, true
	}
	r := bufio.NewReaderSize(conn, readBytes)
	spoke := false
	for {
		kind, payload, err := ReadFrame(r)
		if err != nil {
			if !spoke {
				return 0, false, true
			}
			if Stream(request.Argv) {
				return 0, true, true
			}
			_, _ = fmt.Fprintln(stderr, "cci: the daemon went away:", err)
			return LostExit, true, true
		}
		switch kind {
		case FrameFallback:
			return 0, false, false
		case FrameExit:
			return int(binary.BigEndian.Uint32(payload)), true, false
		case FrameStderr:
			spoke = true
			_, _ = stderr.Write(payload)
		case FrameStdout:
			spoke = true
			if _, err := stdout.Write(payload); err != nil {
				_, _ = fmt.Fprintln(stderr, "cci:", err)
				return 1, true, false
			}
			if err := pump.ack(); err != nil {
				return LostExit, true, true
			}
		case FrameNeedIn:
			spoke = true
			pump.need()
		}
	}
}
