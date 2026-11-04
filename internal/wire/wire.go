// Package wire is the frame protocol between a cci process and the daemon that
// runs its command. It links nothing the daemon's store needs, so a client that
// only relays stays small.
package wire

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	// LostExit is the exit code of a command whose daemon went away mid-reply.
	LostExit = 75
)

// Request is one command the daemon runs for a client.
type Request struct {
	Version    string   `json:"version"`
	Argv       []string `json:"argv"`
	Env        []string `json:"env"`
	Cwd        string   `json:"cwd"`
	PID        int      `json:"pid"`
	StdoutChar bool     `json:"stdout_char"`
}

// Relayed reports whether argv is a command the daemon runs: a read or a post
// whose arguments name nothing only this process can open.
func Relayed(argv []string) bool {
	if len(argv) < 2 {
		return false
	}
	switch argv[1] {
	case "post", "tail", "grep", "show", "digest":
	default:
		return false
	}
	for _, arg := range argv[2:] {
		if _, value, flag := strings.Cut(arg, "="); flag && strings.HasPrefix(value, "/dev/") || strings.HasPrefix(arg, "/dev/") || strings.HasPrefix(arg, "/proc/") {
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

// Relay runs this process's command in the daemon and reports its exit code. It
// returns false when the daemon did not take the command, so the caller runs it
// itself; once any output has arrived the command is the daemon's.
func Relay(version string) (int, bool) {
	if !Relayed(os.Args) || os.Getenv("CCI_HOME") != "" || os.Getenv("CCI_UNSENT") != "" {
		return 0, false
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return 0, false
	}
	conn, err := dial(filepath.Join(home, ".cc-inbox", SocketName))
	if err != nil {
		return 0, false
	}
	defer func() { _ = conn.Close() }()
	cwd, err := os.Getwd()
	if err != nil {
		return 0, false
	}
	info, err := os.Stdout.Stat()
	if err != nil {
		return 0, false
	}
	return Exchange(conn, Request{
		Version: version, Argv: os.Args, Env: os.Environ(), Cwd: cwd, PID: os.Getpid(),
		StdoutChar: info.Mode()&os.ModeCharDevice != 0,
	}, os.Stdin, os.Stdout, os.Stderr)
}

// Exchange sends request over conn and plays the daemon's reply onto the streams.
func Exchange(conn io.ReadWriter, request Request, stdin io.Reader, stdout, stderr io.Writer) (int, bool) {
	body, err := json.Marshal(request)
	if err != nil || WriteFrame(conn, FrameRequest, body) != nil {
		return 0, false
	}
	r := bufio.NewReaderSize(conn, readBytes)
	w := conn
	spoke := false
	for {
		kind, payload, err := ReadFrame(r)
		if err != nil {
			if !spoke {
				return 0, false
			}
			_, _ = fmt.Fprintln(stderr, "cci: the daemon went away:", err)
			return LostExit, true
		}
		switch kind {
		case FrameFallback:
			return 0, false
		case FrameExit:
			return int(binary.BigEndian.Uint32(payload)), true
		case FrameStderr:
			spoke = true
			_, _ = stderr.Write(payload)
		case FrameStdout:
			spoke = true
			if _, err := stdout.Write(payload); err != nil {
				_, _ = fmt.Fprintln(stderr, "cci:", err)
				return 1, true
			}
			if err := WriteFrame(w, FrameAck, nil); err != nil {
				return LostExit, true
			}
		case FrameNeedIn:
			spoke = true
			buf := make([]byte, readBytes)
			n, err := stdin.Read(buf)
			if err != nil && !errors.Is(err, io.EOF) {
				_, _ = fmt.Fprintln(stderr, "cci:", err)
				return 1, true
			}
			if err := WriteFrame(w, FrameStdin, buf[:n]); err != nil {
				return LostExit, true
			}
		}
	}
}
