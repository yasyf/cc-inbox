package service_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/e2e/e2etest"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/service"
)

func serviceKey(t *testing.T) e2e.Identity {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	id, err := service.ParseKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func writer(srv *relaytest.Server, key e2e.Identity, owners ...[]byte) service.Writer {
	return service.Writer{URL: srv.URL, Mesh: relay.DefaultMesh, Key: key, Owners: owners, Lane: "trunk", HTTP: srv.Client(), Now: time.Now}
}

func TestPostReachesAnOwnerSealed(t *testing.T) {
	srv := relaytest.NewServer(t)
	svc := serviceKey(t)
	st, owner := relaytest.JoinOwner(t, srv, svc)
	at := time.Date(2026, 10, 30, 14, 33, 0, 0, time.UTC)
	err := writer(srv, svc, owner.Identity.PublicKey()).Post(context.Background(), service.Record{
		ID: "trunk:msg_1", Drive: "trunk", Kind: "queue", At: at, Text: "acme/app#7 failed in Trunk's merge queue",
		PRs: []string{"acme/app#7"}, URL: "https://app.trunk.io/acme/merge/app/7", Outcome: "failed", Attrs: map[string]string{"reason": "TEST_RUN_REQUIRED_STATUS_FAILED"},
	})
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if got, err := owner.Pull(context.Background(), 0, nil); err != nil || got.Inserted != 1 {
		t.Fatalf("owner pull = %+v, %v; want 1 inserted", got, err)
	}
	recs, err := st.Query(context.Background(), store.Filter{Drive: "trunk", Kinds: []kinds.Kind{kinds.Queue}})
	if err != nil || len(recs) != 1 {
		t.Fatalf("owner holds %d queue records, %v; want 1", len(recs), err)
	}
	r := recs[0]
	if r.Lane != "trunk" || r.Fields.Outcome != "failed" || r.Refs.URL != "https://app.trunk.io/acme/merge/app/7" ||
		len(r.Refs.PRs) != 1 || r.Refs.PRs[0] != (store.PR{Repo: "acme/app", Number: 7}) || !r.At.Equal(at) ||
		r.Fields.Attrs["reason"] != "TEST_RUN_REQUIRED_STATUS_FAILED" {
		t.Fatalf("owner read %+v", r)
	}
	for _, body := range srv.Pushes {
		if bytes.Contains(body, []byte("acme/app")) {
			t.Fatalf("push carried plaintext: %s", body)
		}
	}
}

func TestPostRefusesAGroupKeyFromAnUnpinnedOwner(t *testing.T) {
	srv := relaytest.NewServer(t)
	svc := serviceKey(t)
	relaytest.JoinOwner(t, srv, svc)
	stranger := e2etest.NewIdentity(t)
	err := writer(srv, svc, stranger.PublicKey()).Post(context.Background(), service.Record{Drive: "trunk", Kind: "queue", At: time.Now(), Text: "x", PRs: []string{"acme/app#7"}})
	if !errors.Is(err, e2e.ErrNoGroupKey) {
		t.Fatalf("post = %v, want ErrNoGroupKey", err)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("relay saw %d pushes, want none", len(srv.Pushes))
	}
}

func TestPostRefusesAnUnknownKind(t *testing.T) {
	srv := relaytest.NewServer(t)
	svc := serviceKey(t)
	_, owner := relaytest.JoinOwner(t, srv, svc)
	if err := writer(srv, svc, owner.Identity.PublicKey()).Post(context.Background(), service.Record{Drive: "trunk", Kind: "queued", At: time.Now(), Text: "x"}); err == nil {
		t.Fatal("post of kind queued succeeded")
	}
}
