package cli_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/node"
	"github.com/yasyf/cc-inbox/internal/relay"
	"github.com/yasyf/cc-inbox/internal/store"
)

func TestPostMachineReferencesKeepInternalTargetsOnNonownerNodes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("CCI_HOME", home)
	t.Setenv("CLAUDE_CODE_SESSION_ID", "")
	t.Setenv("CODEX_THREAD_ID", "")
	st, err := store.Open(t.Context(), home)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Error(err)
		}
	})
	st.SetOwner(false)
	if err := relay.SaveConfig(home, relay.Config{Mesh: "machine", URL: "https://relay.invalid", Tier: "service"}); err != nil {
		t.Fatal(err)
	}
	owner, err := store.Open(t.Context(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := st.PutMember(t.Context(), store.Member{Node: owner.Self(), PublicKey: owner.Node().PublicKey(), Tier: node.TierOwner, Via: "relay", By: "fixture", At: time.Now()}); err != nil {
		t.Fatal(err)
	}
	for _, target := range []*store.Store{st, owner} {
		for i := range 7 {
			if _, _, err := target.Post(t.Context(), store.Record{Drive: "test-drive", Lane: "root", Kind: kinds.Note, Text: fmt.Sprintf("seed %d", i)}, 0); err != nil {
				t.Fatal(err)
			}
		}
	}
	rows, err := owner.Export(t.Context(), owner.Self(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(t.Context(), rows, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Apply(t.Context(), []store.Row{{Origin: "bbbbbbbbbbbbbbbb", OriginSeq: 100, Drive: "test-drive", Lane: "root", Kind: kinds.Note, At: time.Now(), Text: "remote target", Source: "post"}}, nil); err != nil {
		t.Fatal(err)
	}
	ownerSeq, err := st.Resolve(t.Context(), "#7")
	if err != nil || ownerSeq == 7 {
		t.Fatalf("fixture failed to distinguish owner number from local sequence: %d, %v", ownerSeq, err)
	}
	remoteSeq, err := st.Resolve(t.Context(), "#100@bbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	ownerID, remoteID := strconv.FormatInt(ownerSeq, 10), strconv.FormatInt(remoteSeq, 10)
	for _, tt := range []struct {
		name string
		args []string
		want [][2]int64
		also []int64
	}{
		{"machine", []string{"--re-seq", ownerID, "--resolves-seq", remoteID}, [][2]int64{{ownerSeq, remoteSeq}}, nil},
		{"human unchanged", []string{"--re", "#7", "--resolves", "#100@bbbbbbbb"}, [][2]int64{{ownerSeq, remoteSeq}}, nil},
		{"human reply machine resolve", []string{"--re", "#7", "--resolves-seq", remoteID}, [][2]int64{{ownerSeq, remoteSeq}}, nil},
		{"machine reply human resolve", []string{"--re-seq", ownerID, "--resolves", "#100@bbbbbbbb"}, [][2]int64{{ownerSeq, remoteSeq}}, nil},
		{"multiple replies", []string{"--re-seq", ownerID + ",1", "--resolves-seq", remoteID}, [][2]int64{{ownerSeq, remoteSeq}, {1, remoteSeq}}, nil},
		{"multiple resolves", []string{"--resolves-seq", ownerID, "--resolves-seq", remoteID + "," + ownerID}, [][2]int64{{0, ownerSeq}}, []int64{remoteSeq}},
		{"pairwise", []string{"--pairwise", "--re-seq", ownerID + ",1", "--resolves-seq", remoteID + "," + ownerID}, [][2]int64{{ownerSeq, remoteSeq}, {1, ownerSeq}}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			args := append([]string{"post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "machine summary", "--id", tt.name, "--json"}, tt.args...)
			var seqs []int64
			for repeat := range 2 {
				out, err := run(t, args...)
				if err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(out), "\n")
				if len(lines) != len(tt.want) {
					t.Fatalf("post returned %d JSON records, want %d: %s", len(lines), len(tt.want), out)
				}
				for i, line := range lines {
					var got store.Record
					if err := json.Unmarshal([]byte(line), &got); err != nil {
						t.Fatal(err)
					}
					if got.Re != tt.want[i][0] || got.Resolves != tt.want[i][1] || !slices.Equal(got.AlsoResolves, tt.also) || !strings.HasSuffix(got.Num, "@"+st.Self()[:8]) {
						t.Fatalf("wrong internal targets or display number: %+v; want %v also %v", got, tt.want[i], tt.also)
					}
					if repeat == 0 {
						seqs = append(seqs, got.Seq)
					} else if got.Seq != seqs[i] {
						t.Fatalf("duplicate changed internal sequence: %d, want %d", got.Seq, seqs[i])
					}
				}
			}
		})
	}
	out, err := run(t, "post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "human output")
	if err != nil || !strings.HasSuffix(strings.TrimSpace(out), "@"+st.Self()[:8]) {
		t.Fatalf("human text output lost its origin number: %q, %v", out, err)
	}
}

func TestPostMachineReferenceErrorsPrecedeStoreCreation(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
		want string
	}{
		{"reply modes", []string{"--re", "7", "--re-seq", "14"}, "none of the others can be"},
		{"resolve modes", []string{"--resolves", "7", "--resolves-seq", "14"}, "none of the others can be"},
		{"negative reply", []string{"--re-seq", "-1"}, "must be nonnegative"},
		{"negative resolve", []string{"--resolves-seq", "1,-1"}, "must be nonnegative"},
		{"malformed", []string{"--re-seq", "#7"}, "invalid argument"},
		{"many targets", []string{"--re-seq", "1,2", "--resolves-seq", "3,4"}, "not both"},
		{"pairwise counts", []string{"--pairwise", "--re-seq", "1,2", "--resolves", "#3"}, "equal non-empty"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			home := filepath.Join(t.TempDir(), "unopened")
			t.Setenv("CCI_HOME", home)
			_, err := run(t, append([]string{"post", "--drive", "test-drive", "--lane", "root", "--kind", "note", "--text", "summary"}, tt.args...)...)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error=%v, want %q", err, tt.want)
			}
			if _, err := os.Stat(home); !os.IsNotExist(err) {
				t.Fatalf("invalid input opened the store: %v", err)
			}
		})
	}
}
