package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func get(t *testing.T, srv *httptest.Server, path string, want int, into any) {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != want {
		t.Fatalf("GET %s = %d, want %d", path, resp.StatusCode, want)
	}
	if err := json.NewDecoder(resp.Body).Decode(into); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
}

func TestReadEndpoints(t *testing.T) {
	st, _ := testutil.Store(t)
	first := testutil.Post(t, st, store.Record{Kind: kinds.Opened, Text: "opened", Refs: store.Refs{PR: 30431}})
	testutil.Post(t, st, store.Record{Lane: "lane-b", Kind: kinds.Hold, Text: "hold", Topic: "#30431"})
	srv := httptest.NewServer(server.New(st, 10*time.Millisecond).Handler())
	defer srv.Close()

	var records []store.Record
	get(t, srv, "/v1/records?drive=d&kind=hold", http.StatusOK, &records)
	if len(records) != 1 || records[0].Lane != "lane-b" || records[0].Topic != "#30431" {
		t.Fatalf("records = %+v", records)
	}
	get(t, srv, "/v1/records?drive=d&since=1&limit=1", http.StatusOK, &records)
	if len(records) != 1 || records[0].Seq != first.Seq+1 {
		t.Fatalf("records after #1 = %+v", records)
	}
	get(t, srv, "/v1/records?drive=nope", http.StatusOK, &records)
	if len(records) != 0 {
		t.Fatalf("unknown drive returned %+v", records)
	}
	var digest inbox.DigestView
	get(t, srv, "/v1/digest?drive=d", http.StatusOK, &digest)
	if digest.Total != 2 || len(digest.Holds) != 1 {
		t.Fatalf("digest = %+v", digest)
	}
	var drives []store.DriveInfo
	get(t, srv, "/v1/drives", http.StatusOK, &drives)
	if len(drives) != 1 || drives[0].Drive != "d" || drives[0].Records != 2 {
		t.Fatalf("drives = %+v", drives)
	}
	var bad map[string]string
	get(t, srv, "/v1/records", http.StatusBadRequest, &bad)
	get(t, srv, "/v1/records?drive=d&kind=bogus", http.StatusBadRequest, &bad)
	if !strings.Contains(bad["error"], "unknown kind") {
		t.Fatalf("error = %v", bad)
	}
}

func TestStream(t *testing.T) {
	st, _ := testutil.Store(t)
	testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "before"})
	srv := httptest.NewServer(server.New(st, 5*time.Millisecond).Handler())
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/stream?drive=d", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	want := testutil.Post(t, st, store.Record{Kind: kinds.Go, Text: "after"})
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok {
			continue
		}
		var r store.Record
		if err := json.Unmarshal([]byte(data), &r); err != nil {
			t.Fatal(err)
		}
		if r.Seq != want.Seq || r.Text != "after" {
			t.Fatalf("first streamed record = %+v, want #%d", r, want.Seq)
		}
		return
	}
	t.Fatalf("stream ended without a record: %v", scanner.Err())
}
