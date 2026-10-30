package cli

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/yasyf/cc-inbox/internal/store"
)

const nativeTailInputBytes = 8 << 20

type nativeTailRequest struct {
	Argv    []string `json:"argv"`
	Session *string  `json:"session"`
	Agent   *string  `json:"agent"`
	Home    *string  `json:"home"`
}

type tailProtocolError struct {
	status  int
	message string
}

func (e *tailProtocolError) Error() string { return e.message }

// TailHandler serves read-only native Tail requests against a borrowed daemon store.
func TailHandler(st *store.Store, home string) (http.Handler, error) {
	ownedHome, err := filepath.Abs(home)
	if err != nil {
		return nil, fmt.Errorf("resolve native tail home: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/tail", func(w http.ResponseWriter, r *http.Request) {
		request, protocol := decodeNativeTail(w, r)
		if protocol != nil {
			rejectNativeTail(w, protocol)
			return
		}
		if filepath.Clean(*request.Home) != ownedHome {
			rejectNativeTail(w, &tailProtocolError{http.StatusConflict, "native tail home mismatch"})
			return
		}
		var peer store.PeerKey
		if *request.Session != "" {
			peer = store.PeerKey{Session: *request.Session, Agent: *request.Agent}
		}
		cmd, flags := tailCommand(peer.String())
		root := rootCommand()
		root.AddCommand(cmd)
		root.SetArgs(request.Argv[1:])
		output := &nativeTailOutput{w: w}
		var stderr bytes.Buffer
		root.SetOut(output)
		root.SetErr(&stderr)
		validate := func(cmd *cobra.Command) *tailProtocolError {
			if !cmd.Flags().Changed("since") {
				return &tailProtocolError{http.StatusBadRequest, "native tail requires an explicit --since"}
			}
			if !cmd.Flags().Changed("drive") || flags.rf.drive == "" {
				return &tailProtocolError{http.StatusBadRequest, "native tail requires a nonempty explicit --drive"}
			}
			return nil
		}
		cmd.RunE = func(cmd *cobra.Command, _ []string) error {
			if protocol = validate(cmd); protocol != nil {
				return protocol
			}
			return flags.run(cmd, st, peer)
		}
		help := cmd.HelpFunc()
		cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
			if protocol = validate(cmd); protocol != nil {
				return
			}
			help(cmd, args)
		})
		err := root.ExecuteContext(r.Context())
		if protocol != nil {
			rejectNativeTail(w, protocol)
			return
		}
		code := 0
		if err != nil {
			_, _ = fmt.Fprintln(&stderr, "cci:", err)
			code = 1
		}
		if err := output.finish(stderr.Bytes(), code); err != nil {
			panic(http.ErrAbortHandler)
		}
	})
	return mux, nil
}

func decodeNativeTail(w http.ResponseWriter, r *http.Request) (nativeTailRequest, *tailProtocolError) {
	r.Body = http.MaxBytesReader(w, r.Body, nativeTailInputBytes)
	defer func() { _ = r.Body.Close() }()
	tooLarge := &tailProtocolError{http.StatusRequestEntityTooLarge, "native tail request too large"}
	invalid := &tailProtocolError{http.StatusBadRequest, "invalid native tail request"}
	if r.ContentLength > nativeTailInputBytes {
		return nativeTailRequest{}, tooLarge
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nativeTailRequest{}, invalid
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request nativeTailRequest
	if err := decoder.Decode(&request); err != nil {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			return nativeTailRequest{}, tooLarge
		}
		return nativeTailRequest{}, invalid
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		var max *http.MaxBytesError
		if errors.As(err, &max) {
			return nativeTailRequest{}, tooLarge
		}
		return nativeTailRequest{}, invalid
	}
	if len(request.Argv) < 2 || request.Argv[0] != "cci" || request.Argv[1] != "tail" || request.Session == nil || request.Agent == nil || request.Home == nil {
		return nativeTailRequest{}, invalid
	}
	for _, arg := range request.Argv {
		if strings.IndexByte(arg, 0) >= 0 {
			return nativeTailRequest{}, invalid
		}
	}
	if !filepath.IsAbs(*request.Home) || strings.IndexByte(*request.Home, 0) >= 0 {
		return nativeTailRequest{}, invalid
	}
	return request, nil
}

func rejectNativeTail(w http.ResponseWriter, protocol *tailProtocolError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(protocol.status)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": protocol.message}); err != nil {
		panic(http.ErrAbortHandler)
	}
}

type nativeTailOutput struct {
	w       http.ResponseWriter
	encoder io.WriteCloser
}

func (o *nativeTailOutput) start() error {
	if o.encoder != nil {
		return nil
	}
	o.w.Header().Set("Content-Type", "application/json")
	if _, err := io.WriteString(o.w, `{"stdout_b64":"`); err != nil {
		return err
	}
	o.encoder = base64.NewEncoder(base64.StdEncoding, o.w)
	return nil
}

func (o *nativeTailOutput) Write(p []byte) (int, error) {
	if err := o.start(); err != nil {
		return 0, err
	}
	return o.encoder.Write(p)
}

func (o *nativeTailOutput) finish(stderr []byte, code int) error {
	if err := o.start(); err != nil {
		return err
	}
	if err := o.encoder.Close(); err != nil {
		return err
	}
	if _, err := io.WriteString(o.w, `","stderr_b64":"`); err != nil {
		return err
	}
	encodedError := base64.NewEncoder(base64.StdEncoding, o.w)
	if _, err := encodedError.Write(stderr); err != nil {
		return err
	}
	if err := encodedError.Close(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(o.w, `","returncode":%d}`+"\n", code)
	return err
}
