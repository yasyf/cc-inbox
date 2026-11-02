package cli

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/pflag"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeBoardRequest struct {
	Contract     *string   `json:"contract"`
	Home         *string   `json:"home"`
	Session      *string   `json:"session"`
	Agent        *string   `json:"agent"`
	Drive        *string   `json:"drive"`
	Repo         *string   `json:"repo"`
	CWD          *string   `json:"cwd"`
	GoLanes      []*string `json:"go_lanes"`
	RulingArgv   []*string `json:"ruling_argv"`
	ReviewArgv   []*string `json:"review_argv"`
	WithdrawArgv []*string `json:"withdraw_argv"`
	PRs          []*string `json:"prs"`
	RowIDs       []*string `json:"row_ids"`
}

type BoardHTTP struct {
	http.Handler
	index *store.BoardIndex
}

func (h *BoardHTTP) Close() error { return h.index.Close() }

func BoardRecordsHandler(st *store.Store, home string) (*BoardHTTP, error) {
	ownedHome, err := filepath.Abs(home)
	if err != nil {
		return nil, err
	}
	view := *st
	index := store.NewBoardIndex(&view)
	mux := http.NewServeMux()
	handler := func(provision bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			request, err := decodeNativeBoard(w, r)
			if err != nil {
				rejectNativeTail(w, &tailProtocolError{http.StatusBadRequest, err.Error()})
				return
			}
			if filepath.Clean(*request.Home) != ownedHome {
				rejectNativeTail(w, &tailProtocolError{http.StatusConflict, "native board home mismatch"})
				return
			}
			requestView := view
			if err := configureStoreOwner(&requestView, ownedHome); err != nil {
				rejectNativeTail(w, &tailProtocolError{http.StatusConflict, err.Error()})
				return
			}
			spec, err := request.spec(r.Context(), &requestView)
			if err != nil {
				rejectNativeTail(w, &tailProtocolError{http.StatusBadRequest, err.Error()})
				return
			}
			if provision {
				id, err := index.Provision(r.Context(), spec)
				if err != nil {
					rejectNativeTail(w, &tailProtocolError{http.StatusConflict, err.Error()})
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(map[string]string{"contract": "board-query-v1", "scope_id": id}); err != nil {
					panic(http.ErrAbortHandler)
				}
				return
			}
			selection := store.BoardSelection{PRs: boardStrings(request.PRs), RowIDs: boardStrings(request.RowIDs)}
			var encoded []byte
			err = index.Read(r.Context(), &requestView, spec, selection, func(snapshot *store.BoardSnapshot, result *store.BoardResult) error {
				annotate := func(category *store.BoardCategory) {
					if category.Error == nil {
						category.Error = inbox.Annotate(r.Context(), snapshot, category.Records)
					}
				}
				annotate(&result.Rulings)
				annotate(&result.Withdrawals)
				annotate(&result.Reviews)
				var rulingError error
				if result.Rulings.Error != nil || result.Withdrawals.Error != nil {
					result.ErrorReplay = true
					rulingError = request.originalReadError(r.Context(), &requestView, 0)
					if rulingError == nil {
						rulingError = request.originalReadError(r.Context(), &requestView, 2)
					}
					if rulingError == nil {
						return errors.New("cached board ruling error did not reproduce in the original read")
					}
				}
				if result.Reviews.Error != nil {
					result.ErrorReplay = true
					result.Reviews.Error = request.originalReadError(r.Context(), &requestView, 1)
					if result.Reviews.Error == nil {
						return errors.New("cached board review error did not reproduce in the original read")
					}
				}
				rulings := []any{}
				withdrawals := []store.Record{}
				if rulingError == nil {
					for _, record := range result.Rulings.Records {
						rulings = append(rulings, map[string]any{"record": record, "body_b64": boardBase64(result.Rulings.Bodies[record.Seq])})
					}
					withdrawals = append(withdrawals, result.Withdrawals.Records...)
				}
				reviews := []store.Record{}
				lanes := []string{}
				bodies := map[int64]any{}
				if result.Reviews.Error == nil {
					reviews = append(reviews, result.Reviews.Records...)
					lanes = append(lanes, result.Reviews.Lanes...)
					for _, record := range reviews {
						bodies[record.Seq] = map[string]any{"body_b64": boardBase64(result.Reviews.Bodies[record.Seq]), "is_file": result.Reviews.BodyFiles[record.Seq], "error": result.Reviews.BodyErrors[record.Seq]}
					}
				}
				response := map[string]any{
					"contract": "board-query-v1", "scope_id": result.ScopeID, "source_revision": result.Revision, "clock_ms": result.Clock.UnixMilli(),
					"rulings_withdrawals": map[string]any{"error": boardError(rulingError), "rulings": rulings, "withdrawals": withdrawals},
					"reviews":             map[string]any{"error": boardError(result.Reviews.Error), "records": reviews, "lanes": lanes, "bodies": bodies},
				}
				var err error
				encoded, err = json.Marshal(response)
				return err
			})
			if err != nil {
				rejectNativeTail(w, &tailProtocolError{http.StatusConflict, err.Error()})
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if _, err := w.Write(append(encoded, '\n')); err != nil {
				panic(http.ErrAbortHandler)
			}
		}
	}
	mux.HandleFunc("POST /v1/board-records", handler(false))
	mux.HandleFunc("POST /v1/board-provision", handler(true))
	return &BoardHTTP{Handler: mux, index: index}, nil
}

func boardBase64(body []byte) any {
	if body == nil {
		return nil
	}
	return base64.StdEncoding.EncodeToString(body)
}

func boardError(err error) any {
	if err == nil {
		return nil
	}
	return err.Error()
}

func boardStrings(values []*string) []string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = *value
	}
	return out
}

func decodeNativeBoard(w http.ResponseWriter, r *http.Request) (nativeBoardRequest, error) {
	r.Body = http.MaxBytesReader(w, r.Body, nativeTailInputBytes)
	defer func() { _ = r.Body.Close() }()
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return nativeBoardRequest{}, errors.New("invalid native board content type")
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request nativeBoardRequest
	if err := decoder.Decode(&request); err != nil {
		return request, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return request, errors.New("native board requires exactly one request")
	}
	for _, value := range []*string{request.Contract, request.Home, request.Session, request.Agent, request.Drive, request.Repo, request.CWD} {
		if value == nil || strings.IndexByte(*value, 0) >= 0 {
			return request, errors.New("invalid native board string")
		}
	}
	if *request.Contract != "board-query-v1" || *request.Drive == "" || *request.Repo == "" || !filepath.IsAbs(*request.Home) || !filepath.IsAbs(*request.CWD) {
		return request, errors.New("invalid native board scope")
	}
	for _, values := range [][]*string{request.GoLanes, request.RulingArgv, request.ReviewArgv, request.WithdrawArgv, request.PRs, request.RowIDs} {
		if values == nil {
			return request, errors.New("missing native board array")
		}
		for _, value := range values {
			if value == nil || strings.IndexByte(*value, 0) >= 0 {
				return request, errors.New("invalid native board array member")
			}
		}
	}
	return request, nil
}

func (request nativeBoardRequest) spec(ctx context.Context, st *store.Store) (store.BoardSpec, error) {
	spec := store.BoardSpec{Drive: *request.Drive, Repo: *request.Repo, CWD: *request.CWD, GoLanes: boardStrings(request.GoLanes)}
	peer := store.PeerKey{}
	if *request.Session != "" {
		peer = store.PeerKey{Session: *request.Session, Agent: *request.Agent}
	}
	expected := [][]string{{"answer", "decide", "decision", "go"}, {"head", "review"}, {"withdraw"}}
	for category, values := range [][]*string{request.RulingArgv, request.ReviewArgv, request.WithdrawArgv} {
		argv := boardStrings(values)
		if len(argv) < 2 || argv[0] != "cci" || argv[1] != "tail" {
			return spec, errors.New("native board requires Tail argv")
		}
		cmd, flags := tailCommand(peer.String())
		if err := cmd.ParseFlags(argv[2:]); err != nil {
			return spec, err
		}
		if len(cmd.Flags().Args()) != 0 {
			return spec, errors.New("native board Tail takes no positional arguments")
		}
		var unsupported string
		cmd.Flags().Visit(func(flag *pflag.Flag) {
			switch flag.Name {
			case "drive", "since-seq", "json", "budget", "kind", "lane", "mute-kinds", "to-kind":
			default:
				unsupported = flag.Name
			}
		})
		if unsupported != "" {
			return spec, fmt.Errorf("native board Tail does not support --%s", unsupported)
		}
		if !cmd.Flags().Changed("drive") || flags.rf.drive != spec.Drive || !cmd.Flags().Changed("since-seq") || flags.rf.sinceSeq != 0 || !flags.rf.json || flags.rf.budget != 4000000 {
			return spec, errors.New("native board requires explicit drive/since-seq 0/json/budget 4000000")
		}
		names := slices.Clone(flags.rf.kinds)
		slices.Sort(names)
		names = slices.Compact(names)
		if !slices.Equal(names, expected[category]) {
			return spec, errors.New("native board Tail category kinds mismatch")
		}
		if category < 2 && len(flags.rf.lanes) != 0 || category == 2 && !slices.Equal(flags.rf.lanes, []string{"root"}) {
			return spec, errors.New("native board Tail category lanes mismatch")
		}
		filter, err := flags.rf.filterPeer(ctx, st, peer)
		if err != nil {
			return spec, err
		}
		spec.Filters[category] = filter
	}
	return spec, nil
}
