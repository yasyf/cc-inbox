package github_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/e2e"
	"github.com/yasyf/cc-inbox/github"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/relay/relaytest"
	"github.com/yasyf/cc-inbox/internal/store"
	"github.com/yasyf/cc-inbox/service"
)

const secret = "github webhook secret"

const synchronized = `{"action":"synchronize","number":45700,"before":"0987654321abc",
"pull_request":{"html_url":"https://github.com/acme/app/pull/45700","draft":false,"merged":false,
"updated_at":"2026-11-03T14:33:00Z","user":{"login":"lane-bot"},"labels":[{"name":"drive"},{"name":"infra"}],
"head":{"ref":"lane/fix","sha":"abc1234567890"},"base":{"ref":"dev"}},
"repository":{"full_name":"acme/app"}}`

const merged = `{"action":"closed","number":45700,
"pull_request":{"html_url":"https://github.com/acme/app/pull/45700","draft":false,"merged":true,"merge_commit_sha":"def456",
"updated_at":"2026-11-03T15:00:00Z","user":{"login":"lane-bot"},"labels":[],
"head":{"ref":"lane/fix","sha":"abc1234567890"},"base":{"ref":"dev"}},
"repository":{"full_name":"acme/app"}}`

const greenStatus = `{"id":9001,"sha":"abc1234567890","context":"buildkite/ci","state":"success",
"target_url":"https://buildkite.com/acme/ci/builds/9","updated_at":"2026-11-03T14:40:00Z","repository":{"full_name":"acme/app"}}`

const squashed = `{"ref":"refs/heads/dev","before":"0987654321abc","after":"def4567890123",
"compare":"https://github.com/acme/app/compare/0987654321ab...def456789012",
"repository":{"full_name":"acme/app","default_branch":"dev","pushed_at":1762181400}}`

func sign(event, id, body string) http.Header {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))
	h := http.Header{}
	h.Set("X-GitHub-Event", event)
	h.Set("X-GitHub-Delivery", id)
	h.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return h
}

func TestVerify(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header http.Header
		body   string
		ok     bool
	}{
		{"signed", sign("pull_request", "d1", synchronized), synchronized, true},
		{"body changed", sign("pull_request", "d1", synchronized), strings.Replace(synchronized, "45700", "45701", 1), false},
		{"sha1 only", http.Header{"X-Hub-Signature": {"sha1=00"}}, synchronized, false},
		{"unsigned", http.Header{}, synchronized, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := github.Verify(secret, tc.header, []byte(tc.body))
			if tc.ok != (err == nil) || err != nil && !errors.Is(err, github.ErrSignature) {
				t.Fatalf("verify = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestRecordShapes(t *testing.T) {
	for _, tc := range []struct {
		name, event, body     string
		outcome, commit, text string
		prs                   []string
		attrs                 map[string]string
	}{
		{
			"a push to a pull request", "pull_request", synchronized, "synchronize", "abc1234567890",
			"acme/app#45700 synchronize at abc1234567 onto dev",
			[]string{"acme/app#45700"},
			map[string]string{"author": "lane-bot", "base": "dev", "before": "0987654321abc", "branch": "lane/fix", "draft": "false", "labels": "drive,infra"},
		},
		{
			"a merge", "pull_request", merged, "merged", "abc1234567890",
			"acme/app#45700 merged at abc1234567 onto dev",
			[]string{"acme/app#45700"},
			map[string]string{"author": "lane-bot", "base": "dev", "branch": "lane/fix", "draft": "false", "labels": "", "merge": "def456"},
		},
		{
			"a commit status, which names no pull request", "status", greenStatus, "success", "abc1234567890",
			"acme/app abc1234567 buildkite/ci success", nil,
			map[string]string{"check": "buildkite/ci", "id": "9001", "repo": "acme/app"},
		},
		{
			"a finished check run", "check_run",
			`{"action":"completed","check_run":{"id":7001,"name":"lint","head_sha":"abc1234567890","conclusion":"failure","details_url":"https://ci/1","completed_at":"2026-11-03T14:41:00Z","pull_requests":[{"number":45700}]},"repository":{"full_name":"acme/app"}}`,
			"failure", "abc1234567890", "acme/app abc1234567 lint failure",
			[]string{"acme/app#45700"},
			map[string]string{"check": "lint", "id": "7001", "repo": "acme/app"},
		},
		{
			"a squash landing on the default branch", "push", squashed, "pushed", "def4567890123",
			"acme/app dev pushed def4567890 over 0987654321", nil,
			map[string]string{"before": "0987654321abc", "branch": "dev", "repo": "acme/app"},
		},
		{
			"an approval", "pull_request_review",
			`{"action":"submitted","review":{"state":"APPROVED","commit_id":"abc1234567890","html_url":"https://github.com/acme/app/pull/45700#r1","submitted_at":"2026-11-03T14:50:00Z","user":{"login":"owner"}},"pull_request":{"number":45700},"repository":{"full_name":"acme/app"}}`,
			"approved", "abc1234567890", "acme/app#45700 review approved by owner at abc1234567",
			[]string{"acme/app#45700"},
			map[string]string{"reviewer": "owner"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ok, err := github.Record(tc.event, "d1", "github", []byte(tc.body))
			if err != nil || !ok {
				t.Fatalf("record = %v, %v", ok, err)
			}
			if r.ID != "github:d1" || r.Kind != "github" || r.Outcome != tc.outcome || r.Commit != tc.commit || r.Text != tc.text || r.At.IsZero() {
				t.Fatalf("record = %+v", r)
			}
			if strings.Join(r.PRs, ",") != strings.Join(tc.prs, ",") || len(r.Attrs) != len(tc.attrs) {
				t.Fatalf("prs %v attrs %v, want %v %v", r.PRs, r.Attrs, tc.prs, tc.attrs)
			}
			for k, v := range tc.attrs {
				if r.Attrs[k] != v {
					t.Fatalf("attr %s = %q, want %q", k, r.Attrs[k], v)
				}
			}
		})
	}
}

func TestEventsNoReaderActsOnMakeNoRecord(t *testing.T) {
	for _, tc := range []struct{ event, body string }{
		{"ping", `{"zen":"Keep it logically awesome."}`},
		{"status", strings.Replace(greenStatus, `"success"`, `"pending"`, 1)},
		{"check_run", `{"action":"created","check_run":{"name":"lint"},"repository":{"full_name":"acme/app"}}`},
		{"pull_request", strings.Replace(synchronized, "synchronize", "assigned", 1)},
		{"push", strings.Replace(squashed, "refs/heads/dev", "refs/heads/lane/fix", 1)},
	} {
		if _, ok, err := github.Record(tc.event, "d1", "github", []byte(tc.body)); ok || err != nil {
			t.Errorf("%s made a record: %v, %v", tc.event, ok, err)
		}
	}
}

func handler(t *testing.T) (github.Handler, *store.Store, *relay.Node, *relaytest.Server) {
	t.Helper()
	srv := relaytest.NewServer(t)
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	svc, err := e2e.ParseKey(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
	if err != nil {
		t.Fatal(err)
	}
	st, owner := relaytest.JoinOwner(t, srv, svc)
	w := service.Writer{URL: srv.URL, Mesh: relay.DefaultMesh, Key: svc, Owners: [][]byte{owner.Identity.PublicKey()}, Lane: "github", HTTP: srv.Client(), Now: time.Now}
	return github.Handler{Secret: secret, Writer: w, Drive: "github"}, st, owner, srv
}

func records(t *testing.T, st *store.Store, owner *relay.Node) []store.Record {
	t.Helper()
	if _, err := owner.Pull(context.Background(), 0, nil); err != nil {
		t.Fatal(err)
	}
	recs, err := st.Query(context.Background(), store.Filter{Drive: "github", Kinds: []kinds.Kind{kinds.Github}})
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestAMergeReachesTheOwnerOnceAcrossRedeliveries(t *testing.T) {
	h, st, owner, _ := handler(t)
	for range 2 {
		if code := h.Handle(context.Background(), sign("pull_request", "d1", merged), []byte(merged)); code != http.StatusOK {
			t.Fatalf("handle = %d, want 200", code)
		}
	}
	recs := records(t, st, owner)
	if len(recs) != 1 {
		t.Fatalf("owner holds %d github records, want 1", len(recs))
	}
	r := recs[0]
	if r.Fields.Outcome != "merged" || r.Fields.Commit != "abc1234567890" || r.Fields.Attrs["merge"] != "def456" ||
		r.Refs.URL != "https://github.com/acme/app/pull/45700" || r.Refs.PRs[0] != (store.PR{Repo: "acme/app", Number: 45700}) {
		t.Fatalf("owner read %+v", r)
	}
}

func TestAStatusReachesTheOwnerWithItsCommitAndNoPullRequest(t *testing.T) {
	h, st, owner, _ := handler(t)
	if code := h.Handle(context.Background(), sign("status", "d2", greenStatus), []byte(greenStatus)); code != http.StatusOK {
		t.Fatalf("handle = %d, want 200", code)
	}
	recs := records(t, st, owner)
	if len(recs) != 1 || recs[0].Fields.Commit != "abc1234567890" || len(recs[0].Refs.PRs) != 0 {
		t.Fatalf("owner read %+v", recs)
	}
}

func TestAnUnsignedDeliveryIsRefusedAndNotRelayed(t *testing.T) {
	h, _, _, srv := handler(t)
	header := sign("pull_request", "d1", merged)
	if code := h.Handle(context.Background(), header, []byte(synchronized)); code != http.StatusUnauthorized {
		t.Fatalf("handle = %d, want 401", code)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("relay saw %d pushes, want none", len(srv.Pushes))
	}
}

func TestAPingIsSettledWithoutARecord(t *testing.T) {
	h, _, _, srv := handler(t)
	ping := `{"zen":"Keep it logically awesome."}`
	if code := h.Handle(context.Background(), sign("ping", "d3", ping), []byte(ping)); code != http.StatusNoContent {
		t.Fatalf("handle = %d, want 204", code)
	}
	if len(srv.Pushes) != 0 {
		t.Fatalf("relay saw %d pushes, want none", len(srv.Pushes))
	}
}

func TestARelayFailureAsksGitHubToRedeliver(t *testing.T) {
	h, _, _, srv := handler(t)
	srv.Close()
	if code := h.Handle(context.Background(), sign("pull_request", "d1", merged), []byte(merged)); code/100 == 2 {
		t.Fatalf("handle = %d with the relay down, want a failure code", code)
	}
}

func TestAnEditedBodyReachesTheOwnerWhileAPushCarriesNone(t *testing.T) {
	h, st, owner, _ := handler(t)
	long := "## Summary\n\nnew <b>summary</b>\n\n" + strings.Repeat("é\"\n", 40_000)
	edited, err := json.Marshal(map[string]any{
		"action": "edited", "number": 45700, "repository": map[string]string{"full_name": "acme/app"},
		"pull_request": map[string]any{"body": long, "updated_at": "2026-11-03T15:10:00Z", "head": map[string]string{"sha": "abc1234567890"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for id, body := range map[string]string{"d4": string(edited), "d5": synchronized} {
		if code := h.Handle(context.Background(), sign("pull_request", id, body), []byte(body)); code != http.StatusOK {
			t.Fatalf("handle %s = %d, want 200", id, code)
		}
	}
	bodies := map[string]string{}
	for _, r := range records(t, st, owner) {
		var carried struct{ Body string }
		if len(r.Body) > 0 {
			if err := json.Unmarshal(r.Body, &carried); err != nil {
				t.Fatal(err)
			}
		}
		bodies[r.Fields.Outcome] = carried.Body
	}
	if got := bodies["edited"]; !strings.HasPrefix(long, got) || !strings.Contains(got, "new <b>summary</b>") || len(got) > 30_000 {
		t.Fatalf("edited body = %d bytes, want the clipped head of the new body", len(got))
	}
	if got, pushed := bodies["synchronize"]; !pushed || got != "" {
		t.Fatalf("synchronize body = %q, %v, want a record with no body", got, pushed)
	}
}
