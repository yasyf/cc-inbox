package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
)

const (
	schema14MigrationDigest  = "0554a4c3e9bd8c45e2e0d2d3adb19b66f2c45d122a13f38afa3937543c245103"
	schema14Wire3Fingerprint = "85a41b1360b177e224cdf92a4c4c3af65c69475633e96cb4c13d808226a012c0"
	schema14Wire4Fingerprint = "9bdcd4d84fcdaa488462c750669e067d9d980fde327fde723cc1614ef6799294"
)

type oldStoreProbeResult struct {
	Fingerprint string `json:"fingerprint"`
	Schema      int    `json:"schema"`
	ReadSeq     int64  `json:"read_seq"`
	ReadText    string `json:"read_text"`
	PostedSeq   int64  `json:"posted_seq"`
	PostedText  string `json:"posted_text"`
}

type oldMatrixProbeResult struct {
	Ask      int64 `json:"ask"`
	Done     int64 `json:"done"`
	Imported int64 `json:"imported"`
	Remote   int64 `json:"remote"`
	Folded   int   `json:"folded"`
}

func TestPhysicalIndexesKeepLogicalSchema14Fingerprint(t *testing.T) {
	digest := sha256.Sum256([]byte(strings.Join(migrations, "\x00")))
	if got := hex.EncodeToString(digest[:]); got != schema14MigrationDigest {
		t.Fatalf("logical migration digest = %s, want %s", got, schema14MigrationDigest)
	}
	if len(migrations) != 14 || WireFormat != 4 {
		t.Fatalf("logical schema/wire = %d/%d, want 14/4", len(migrations), WireFormat)
	}
	if got := fingerprint(migrations, WireFormat); got != schema14Wire4Fingerprint {
		t.Fatalf("schema fingerprint = %s, want %s", got, schema14Wire4Fingerprint)
	}
}

func TestPhysicalIndexesAllowOldAndNewStoreOpen(t *testing.T) {
	binary := os.Getenv("CCI_OLD_STORE_PROBE")
	if binary == "" {
		t.Skip("CCI_OLD_STORE_PROBE identifies the CI-built immutable old-store probe")
	}
	for _, first := range []string{"old", "new"} {
		t.Run(first+"-first", func(t *testing.T) {
			home := t.TempDir()
			ctx := context.Background()
			var initial oldStoreProbeResult
			if first == "old" {
				initial = runOldStoreProbe(t, binary, home, 0)
			}
			st, err := Open(ctx, home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = st.Close() })
			if first == "old" {
				assertLaneIndexCount(t, st, 0)
				assertMembershipsInstalled(ctx, t, st, false)
				assertOldFixtureRecord(t, st, initial)
			} else {
				assertLaneIndexCount(t, st, len(physicalIndexes))
				assertMembershipsInstalled(ctx, t, st, true)
			}
			report, err := MaintainIndexes(ctx, home)
			if err != nil {
				t.Fatal(err)
			}
			wantCreated, wantMemberships := 0, 0
			if first == "old" {
				wantCreated, wantMemberships = len(physicalIndexes), len(recordMemberships)+len(closerMemberships)
			}
			if len(report.Created) != wantCreated || len(report.Memberships) != wantMemberships {
				t.Fatalf("maintenance report = %+v, want %d indexes and %d membership objects", report, wantCreated, wantMemberships)
			}
			assertLaneIndexCount(t, st, len(physicalIndexes))
			assertMembershipsInstalled(ctx, t, st, true)
			assertMembershipsDerived(ctx, t, st)
			candidate, _, err := st.Post(ctx, Record{
				Drive:  "compat",
				Lane:   "new",
				Kind:   "report",
				Text:   "candidate fixture record",
				Source: "post",
			}, 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			old := runOldStoreProbe(t, binary, home, candidate.Seq)
			if old.ReadSeq != candidate.Seq || old.ReadText != candidate.Text {
				t.Fatalf("old store read candidate = %+v, want #%d %q", old, candidate.Seq, candidate.Text)
			}
			matrix := runOldMatrixProbe(t, binary, home, candidate.Seq)
			reopened, err := Open(ctx, home)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = reopened.Close() })
			assertOldFixtureRecord(t, reopened, old)
			assertLaneIndexCount(t, reopened, len(physicalIndexes))
			assertMembershipsInstalled(ctx, t, reopened, true)
			assertMembership(ctx, t, reopened, "record_recipients", "'peer:compat-old'", old.PostedSeq, 1)
			assertMembership(ctx, t, reopened, "record_recipients", "'peer:old-matrix'", matrix.Ask, 1)
			assertMembership(ctx, t, reopened, "record_recipients", "'old@compat'", matrix.Ask, 1)
			assertMembership(ctx, t, reopened, "record_also_resolves", strconv.FormatInt(candidate.Seq, 10), matrix.Done, 1)
			assertMembership(ctx, t, reopened, "record_recipients", "'peer:old-import'", matrix.Imported, 0)
			assertMembership(ctx, t, reopened, "record_recipients", "'peer:old-reparsed'", matrix.Imported, 1)
			assertMembership(ctx, t, reopened, "record_also_resolves", strconv.FormatInt(candidate.Seq, 10), matrix.Remote, 1)
			assertMembership(ctx, t, reopened, "record_recipients", "'peer:old-folded'", 0, 0)
			assertMembershipsDerived(ctx, t, reopened)
			if matrix.Folded != 2 {
				t.Fatalf("old matrix folded %d records, want 2", matrix.Folded)
			}
			delivered, err := reopened.Query(ctx, Filter{Drive: "elsewhere", Peer: "old-matrix"})
			if err != nil || len(delivered) != 1 || delivered[0].Seq != matrix.Ask {
				t.Fatalf("candidate peer read of old write = %+v, %v; want #%d", delivered, err, matrix.Ask)
			}
			resolved, err := reopened.Resolvers(ctx, "compat")
			if err != nil || resolved[candidate.Seq] != max(matrix.Done, matrix.Remote) {
				t.Fatalf("candidate resolvers after old writes = %v, %v; want #%d resolved by #%d", resolved, err, candidate.Seq, max(matrix.Done, matrix.Remote))
			}
			var schema int
			if err := reopened.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schema); err != nil {
				t.Fatal(err)
			}
			if schema != 14 || reopened.SchemaFingerprint() != schema14Wire4Fingerprint {
				t.Fatalf("reopened candidate schema/fingerprint = %d/%s, old = %d/%s", schema, reopened.SchemaFingerprint(), old.Schema, old.Fingerprint)
			}
			t.Logf("first=%s old_source=0b83dbc7fc55f29e552ecafb6cd2a6206d1f57b2 old_schema=%d old_fingerprint=%s physical_indexes=%d old_read_candidate_seq=%d new_read_old_seq=%d", first, old.Schema, old.Fingerprint, len(physicalIndexes), old.ReadSeq, old.PostedSeq)
		})
	}
}

func runOldStoreProbe(t *testing.T, binary, home string, readSeq int64) oldStoreProbeResult {
	t.Helper()
	args := []string{home}
	if readSeq != 0 {
		args = append(args, strconv.FormatInt(readSeq, 10))
	}
	out, err := exec.CommandContext(t.Context(), binary, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("old-store probe: %v: %s", err, out)
	}
	var result oldStoreProbeResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("decode old-store probe: %v: %s", err, out)
	}
	if result.Schema != 14 || result.Fingerprint != schema14Wire3Fingerprint || result.PostedSeq <= 0 || result.PostedText == "" {
		t.Fatalf("old-store probe result = %+v", result)
	}
	return result
}

func runOldMatrixProbe(t *testing.T, binary, home string, target int64) oldMatrixProbeResult {
	t.Helper()
	out, err := exec.CommandContext(t.Context(), binary, "matrix", home, strconv.FormatInt(target, 10)).CombinedOutput()
	if err != nil {
		t.Fatalf("old-store matrix probe: %v: %s", err, out)
	}
	var result oldMatrixProbeResult
	if err := json.Unmarshal(out, &result); err != nil {
		t.Fatalf("decode old-store matrix probe: %v: %s", err, out)
	}
	if result.Ask <= 0 || result.Done <= 0 || result.Imported <= 0 || result.Remote <= 0 {
		t.Fatalf("old-store matrix probe result = %+v", result)
	}
	return result
}

func assertOldFixtureRecord(t *testing.T, st *Store, old oldStoreProbeResult) {
	t.Helper()
	record, err := st.Get(context.Background(), old.PostedSeq)
	if err != nil || record.Text != old.PostedText {
		t.Fatalf("candidate read old fixture = %+v, %v; want text %q", record, err, old.PostedText)
	}
}
