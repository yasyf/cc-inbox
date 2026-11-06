package server_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/server"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/internal/testutil"
)

func TestStreamKeepsItsPlaceThroughAFailedRead(t *testing.T) {
	st, clock := testutil.Store(t)
	before := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "before"})
	owed := testutil.Post(t, st, store.Record{Kind: kinds.Note, Text: "owed"})
	raw, err := sql.Open("sqlite", "file:"+filepath.Join(st.HomeDir(), store.DBFile))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = raw.Close() })
	rename := func(from, to string) {
		if _, err := raw.ExecContext(t.Context(), fmt.Sprintf("ALTER TABLE %s RENAME TO %s", from, to)); err != nil {
			t.Errorf("rename %s: %v", from, err)
		}
	}
	var reads atomic.Int32
	restored := make(chan struct{})
	st.Now = func() time.Time {
		switch reads.Add(1) {
		case 1:
			rename("members", "members_away")
		case 2:
			close(restored)
			rename("members_away", "members")
		}
		return clock.Now()
	}
	srv := httptest.NewServer(server.New(st, 5*time.Millisecond).Handler())
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/v1/stream?drive=d&since=%d", srv.URL, before.Seq), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		data, ok := strings.CutPrefix(scanner.Text(), "data: ")
		if !ok {
			continue
		}
		select {
		case <-restored:
		default:
			t.Fatal("the stream wrote a record while its read was failing")
		}
		var r store.Record
		if err := json.Unmarshal([]byte(data), &r); err != nil {
			t.Fatal(err)
		}
		if r.Seq != owed.Seq {
			t.Fatalf("first streamed record = #%d, want #%d", r.Seq, owed.Seq)
		}
		return
	}
	t.Fatalf("the stream ended on a failed read without the record it owed: %v", scanner.Err())
}
