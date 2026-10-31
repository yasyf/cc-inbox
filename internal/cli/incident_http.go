package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/yasyf/cc-inbox/internal/store"
)

type incidentHoldsRequest struct {
	Home  *string    `json:"home"`
	Drive *string    `json:"drive"`
	PRs   *[]*string `json:"prs"`
}

type IncidentHTTP struct {
	store       *store.Store
	index       *store.IncidentIndex
	home        string
	unavailable error
}

func IncidentHoldsHandler(ctx context.Context, st *store.Store) (*IncidentHTTP, error) {
	home, err := filepath.Abs(st.HomeDir())
	if err != nil {
		return nil, err
	}
	index, unavailable := store.OpenIncidentIndex(ctx, st)
	return &IncidentHTTP{store: st, index: index, home: home, unavailable: unavailable}, nil
}

func (h *IncidentHTTP) Close() error {
	if h.index == nil {
		return nil
	}
	return h.index.Close()
}

func (h *IncidentHTTP) Stats() (store.IncidentIndexStats, error) {
	if h.unavailable != nil {
		return store.IncidentIndexStats{}, h.unavailable
	}
	return h.index.Stats(), nil
}

func (h *IncidentHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		rejectIncidentHolds(w, http.StatusMethodNotAllowed, "method_not_allowed", "incident holds requires POST")
		return
	}
	request, status, code := decodeIncidentHolds(w, r)
	if code != "" {
		rejectIncidentHolds(w, status, code, "invalid incident holds request")
		return
	}
	if filepath.Clean(*request.Home) != h.home {
		rejectIncidentHolds(w, http.StatusConflict, "home_mismatch", "incident holds home mismatch")
		return
	}
	prs := make([]string, len(*request.PRs))
	for i, pr := range *request.PRs {
		prs[i] = *pr
	}
	if h.unavailable != nil {
		code := "projection_unavailable"
		var typed *store.IncidentError
		if errors.As(h.unavailable, &typed) {
			code = typed.Code
		}
		rejectIncidentHolds(w, http.StatusServiceUnavailable, code, h.unavailable.Error())
		return
	}
	view := *h.store
	if err := configureStoreOwner(&view, h.home); err != nil {
		rejectIncidentHolds(w, http.StatusInternalServerError, "query_failed", err.Error())
		return
	}
	holds, err := h.index.Holds(r.Context(), &view, *request.Drive, prs)
	if err != nil {
		code, status := "query_failed", http.StatusInternalServerError
		var typed *store.IncidentError
		if errors.As(err, &typed) {
			code, status = typed.Code, http.StatusServiceUnavailable
		}
		rejectIncidentHolds(w, status, code, err.Error())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := writeIncidentHolds(w, prs, holds); err != nil {
		panic(http.ErrAbortHandler)
	}
}

func writeIncidentHolds(w io.Writer, prs []string, holds map[string]store.Record) error {
	if _, err := io.WriteString(w, "{"); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	comma := ""
	for _, pr := range prs {
		record, found := holds[pr]
		if !found {
			continue
		}
		if _, err := fmt.Fprintf(w, "%s%q:", comma, pr); err != nil {
			return err
		}
		if err := encoder.Encode(record); err != nil {
			return err
		}
		delete(holds, pr)
		comma = ","
	}
	_, err := io.WriteString(w, "}\n")
	return err
}

func decodeIncidentHolds(w http.ResponseWriter, r *http.Request) (incidentHoldsRequest, int, string) {
	r.Body = http.MaxBytesReader(w, r.Body, nativeTailInputBytes)
	defer func() { _ = r.Body.Close() }()
	if r.ContentLength > nativeTailInputBytes {
		return incidentHoldsRequest{}, http.StatusRequestEntityTooLarge, "request_too_large"
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return incidentHoldsRequest{}, http.StatusBadRequest, "invalid_request"
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var request incidentHoldsRequest
	err = decoder.Decode(&request)
	if err == nil {
		var trailing any
		if end := decoder.Decode(&trailing); !errors.Is(end, io.EOF) {
			if end == nil {
				end = errors.New("multiple JSON values")
			}
			err = end
		}
	}
	if err != nil {
		var maximum *http.MaxBytesError
		if errors.As(err, &maximum) {
			return incidentHoldsRequest{}, http.StatusRequestEntityTooLarge, "request_too_large"
		}
		return incidentHoldsRequest{}, http.StatusBadRequest, "invalid_request"
	}
	if request.Home == nil || request.Drive == nil || request.PRs == nil || !filepath.IsAbs(*request.Home) || *request.Drive == "" || strings.IndexByte(*request.Home, 0) >= 0 || strings.IndexByte(*request.Drive, 0) >= 0 {
		return incidentHoldsRequest{}, http.StatusBadRequest, "invalid_request"
	}
	for _, pr := range *request.PRs {
		if pr == nil || *pr == "" {
			return incidentHoldsRequest{}, http.StatusBadRequest, "invalid_request"
		}
		for _, digit := range *pr {
			if digit < '0' || digit > '9' {
				return incidentHoldsRequest{}, http.StatusBadRequest, "invalid_request"
			}
		}
	}
	return request, 0, ""
}

func rejectIncidentHolds(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"code": code, "error": message}); err != nil {
		panic(http.ErrAbortHandler)
	}
}
