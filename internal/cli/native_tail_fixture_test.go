package cli_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/store"
)

type nativeTailSet struct {
	seq int64
	set string
}

var nativeTailVariants = []struct {
	name string
	sets []nativeTailSet
}{
	{"clean", nil},
	{"late malformed recipients", []nativeTailSet{{14, `recipients = '['`}}},
	{"early malformed recipients", []nativeTailSet{{3, `recipients = '['`}}},
	{"stale malformed recipients", []nativeTailSet{{3, `recipients = '[', at = 0`}}},
	{"origin early malformed recipients", []nativeTailSet{{11, `recipients = '[', origin = 'feedfacefeedface'`}}},
	{"drive malformed recipients", []nativeTailSet{{12, `recipients = '['`}}},
	{"late typed peer recipients", []nativeTailSet{{14, `recipients = '["peer:reader", 1]'`}}},
	{"late scalar peer recipients", []nativeTailSet{{14, `recipients = '"peer:reader"'`}}},
	{"late json5 peer recipients", []nativeTailSet{{14, `recipients = '[''peer:reader'']'`}}},
	{"late jsonb peer recipients", []nativeTailSet{{14, `recipients = jsonb('["peer:reader"]')`}}},
	{"late malformed jsonb recipients", []nativeTailSet{{14, `recipients = X'1B0D'`}}},
	{"late reader json5", []nativeTailSet{{14, `recipients = '[''api@d'']'`}}},
	{"drive malformed refs", []nativeTailSet{{12, `refs = '{'`}}},
	{"early malformed also_resolves", []nativeTailSet{{3, `also_resolves = '['`}}},
	{"late malformed also_resolves", []nativeTailSet{{14, `also_resolves = '['`}}},
	{"drive typed also_resolves", []nativeTailSet{{4, `also_resolves = '["oops"]'`}}},
	{"drive null also_resolves element", []nativeTailSet{{4, `also_resolves = '[null]'`}}},
	{"offdrive spaced target", []nativeTailSet{{11, `also_resolves = '[" 9"]'`}}},
	{"offdrive text target", []nativeTailSet{{11, `also_resolves = '["9"]'`}}},
	{"offdrive bool target", []nativeTailSet{{11, `also_resolves = '[true]', resolves = 0`}, {7, `resolves = 0`}}},
	{"offdrive real target", []nativeTailSet{{10, `also_resolves = '[9.0, 9.5]'`}}},
	{"corrupt drive resolves", []nativeTailSet{{4, `resolves = 'b'`}, {12, `resolves = 'a'`}}},
	{"open openers", []nativeTailSet{{7, `resolves = 0`}, {10, `also_resolves = '[]'`}}},
}

var nativeTailBaseCalls = []struct {
	name    string
	session bool
	args    []string
}{
	{"json peer newest", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "3", "--json", "--budget", "300000000"}},
	{"json peer all", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json nonpeer all", false, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json peer tiny budget", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "1"}},
	{"json peer small budget", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--json", "--budget", "700"}},
	{"text peer", true, []string{"tail", "--drive", "d", "--since", "36h", "-n", "80000", "--budget", "300000000"}},
	{"json to api", false, []string{"tail", "--drive", "d", "--to", "api", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json reader root", true, []string{"tail", "--drive", "d", "--reader", "root", "--since", "36h", "-n", "80000", "--json", "--budget", "300000000"}},
	{"json origin", true, []string{"tail", "--drive", "d", "--origin", "feedfacefeedface", "--since", "36h", "-n", "2", "--json", "--budget", "300000000"}},
	{"json peer cursor", true, []string{"tail", "--drive", "d", "--json", "--budget", "300000000"}},
}

func seedNativeTailFixture(t *testing.T, tx *sql.Tx, node string) {
	t.Helper()
	const at = 4102444800000
	for i, row := range []struct {
		drive, lane, kind, recipients string
		also                          any
		resolves                      int64
		refs                          string
	}{
		{"d", "root", "ask", `["api"]`, nil, 0, `{}`},
		{"f", "api", "note", `["peer:reader"]`, nil, 0, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900001]`, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{"stacks":["p/e"]}`},
		{"f", "api", "note", `["` + store.PeerAddressOn(node, "reader") + `"]`, nil, 0, `{}`},
		{"f", "api", "note", `["api@d"]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, nil, 1, `{}`},
		{"f", "api", "note", `["root@d"]`, nil, 0, `{}`},
		{"d", "api", "ask", `[]`, nil, 0, `{}`},
		{"f", "api", "done", `[]`, `[9]`, 4, `{}`},
		{"x", "irrelevant", "note", `["peer:other"]`, `[900002]`, 900003, `{}`},
		{"d", "root", "note", `[]`, nil, 0, `{}`},
		{"d", "root", "note", `["peer:reader"]`, nil, 0, `{"stacks":["p/e"]}`},
		{"x", "irrelevant", "note", `["someone"]`, nil, 0, `{}`},
		{"d", "api", "note", `[]`, nil, 0, `{}`},
	} {
		res, err := tx.Exec(`INSERT INTO records (drive, lane, kind, at, text, recipients, also_resolves, resolves, refs, source, hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'post', ?)`,
			row.drive, row.lane, row.kind, at, fmt.Sprintf("fixture %d", i+1), row.recipients, row.also, row.resolves, row.refs, fmt.Sprintf("fixture-%d", i+1))
		if err != nil {
			t.Fatal(err)
		}
		if seq, err := res.LastInsertId(); err != nil || seq != int64(i+1) {
			t.Fatalf("fixture seq = %d, %v; want %d", seq, err, i+1)
		}
	}
	if _, err := tx.Exec("UPDATE records SET origin = 'feedfacefeedface', origin_seq = seq WHERE seq >= 12"); err != nil {
		t.Fatal(err)
	}
}

type nativeTailEmptyImage struct {
	files, state string
	creation     time.Duration
}

type nativeTailImageFile struct {
	Path, Kind, SHA256, Data string
	Mode                     fs.FileMode
	Size                     int64
}

type nativeTailSnapshotRows struct {
	Columns []string
	Rows    []string
}

type nativeTailSnapshotTable struct {
	Name string
	Data nativeTailSnapshotRows
}

type nativeTailDatabaseSnapshot struct {
	Schema  nativeTailSnapshotRows
	Pragmas []nativeTailSnapshotTable
	Tables  []nativeTailSnapshotTable
}

type nativeTailExpectedSnapshot struct {
	node, state string
}

var (
	nativeTailProcessImage      = sync.OnceValues(nativeTailBuildEmptyImage)
	nativeTailExpectedSnapshots sync.Map
	nativeTailImageCertificate  struct {
		once   sync.Once
		passed bool
	}
)

func nativeTailSnapshotQuery(ctx context.Context, conn *sql.Conn, query string) (snapshot nativeTailSnapshotRows, err error) {
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return snapshot, err
	}
	defer func() { err = errors.Join(err, rows.Close()) }()
	snapshot.Columns, err = rows.Columns()
	if err != nil {
		return snapshot, err
	}
	for rows.Next() {
		values := make([]any, len(snapshot.Columns))
		pointers := make([]any, len(values))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return snapshot, err
		}
		typed := make([]string, len(values))
		for i, value := range values {
			typed[i] = fmt.Sprintf("%T:%v", value, value)
		}
		encoded, err := json.Marshal(typed)
		if err != nil {
			return snapshot, err
		}
		snapshot.Rows = append(snapshot.Rows, string(encoded))
	}
	sort.Strings(snapshot.Rows)
	return snapshot, rows.Err()
}

func nativeTailSnapshotDatabase(ctx context.Context, home string, empty bool) (snapshot string, err error) {
	db, err := sql.Open("sqlite", filepath.Join(home, store.DBFile))
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, db.Close()) }()
	conn, err := db.Conn(ctx)
	if err != nil {
		return "", err
	}
	defer func() { err = errors.Join(err, conn.Close()) }()
	var state nativeTailDatabaseSnapshot
	state.Schema, err = nativeTailSnapshotQuery(ctx, conn, "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
	if err != nil {
		return "", err
	}
	for _, name := range []string{"user_version", "journal_mode", "page_size", "auto_vacuum", "encoding", "busy_timeout", "synchronous", "mmap_size", "cache_size", "foreign_keys", "recursive_triggers", "locking_mode"} {
		values, err := nativeTailSnapshotQuery(ctx, conn, "PRAGMA "+name)
		if err != nil {
			return "", err
		}
		state.Pragmas = append(state.Pragmas, nativeTailSnapshotTable{Name: name, Data: values})
	}
	rows, err := conn.QueryContext(ctx, "SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
	if err != nil {
		return "", err
	}
	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return "", errors.Join(err, rows.Close())
		}
		tables = append(tables, name)
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return "", err
	}
	for _, name := range tables {
		values, err := nativeTailSnapshotQuery(ctx, conn, fmt.Sprintf("SELECT * FROM %q", name))
		if err != nil {
			return "", err
		}
		if empty && len(values.Rows) != 0 {
			return "", fmt.Errorf("empty template table %s contains %d rows", name, len(values.Rows))
		}
		state.Tables = append(state.Tables, nativeTailSnapshotTable{Name: name, Data: values})
	}
	encoded, err := json.Marshal(state)
	return string(encoded), err
}

func nativeTailImageFiles(home string) (string, error) {
	var files []nativeTailImageFile
	foundDatabase := false
	err := filepath.WalkDir(home, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name, err := filepath.Rel(home, path)
		if err != nil || name == "." {
			return err
		}
		if name == "node" && entry.IsDir() {
			return filepath.SkipDir
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("empty template unexpected file %s mode=%s", name, info.Mode())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		digest := sha256.Sum256(data)
		hash := hex.EncodeToString(digest[:])
		if name == "init.lock" {
			if len(data) != 0 {
				return fmt.Errorf("empty template init lock contains %d bytes", len(data))
			}
			return nil
		}
		if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") || strings.HasSuffix(name, "-journal") {
			return fmt.Errorf("closed template sidecar %s mode=%s size=%d sha256=%s", name, info.Mode(), info.Size(), hash)
		}
		foundDatabase = foundDatabase || name == store.DBFile
		files = append(files, nativeTailImageFile{Path: name, Kind: "regular", Mode: info.Mode(), Size: info.Size(), SHA256: hash, Data: base64.StdEncoding.EncodeToString(data)})
		return nil
	})
	if err != nil {
		return "", err
	}
	if !foundDatabase {
		return "", errors.New("closed template has no database")
	}
	encoded, err := json.Marshal(files)
	return string(encoded), err
}

func nativeTailBuildEmptyImage() (image nativeTailEmptyImage, err error) {
	started := time.Now()
	home, err := os.MkdirTemp("", "cci-native-tail-empty-")
	if err != nil {
		return image, err
	}
	defer func() {
		err = errors.Join(err, os.RemoveAll(home))
		if _, removed := os.Stat(home); !errors.Is(removed, os.ErrNotExist) {
			err = errors.Join(err, fmt.Errorf("template creator directory survives cleanup: %w", removed))
		}
		image.creation = time.Since(started)
	}()
	image.state, err = func() (state string, err error) {
		st, err := store.Open(context.Background(), home)
		if err != nil {
			return "", err
		}
		defer func() { err = errors.Join(err, st.Close()) }()
		return nativeTailSnapshotDatabase(context.Background(), home, true)
	}()
	if err != nil {
		return image, err
	}
	image.files, err = nativeTailImageFiles(home)
	return image, err
}

func nativeTailCopyEmptyImage(t *testing.T, image nativeTailEmptyImage) string {
	t.Helper()
	home := t.TempDir()
	var files []nativeTailImageFile
	if err := json.Unmarshal([]byte(image.files), &files); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		data, err := base64.StdEncoding.DecodeString(file.Data)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(home, file.Path)
		if err := os.WriteFile(path, data, file.Mode.Perm()); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, file.Mode.Perm()); err != nil {
			t.Fatal(err)
		}
	}
	state, err := nativeTailSnapshotDatabase(t.Context(), home, true)
	if err != nil || state != image.state {
		t.Fatalf("private empty copy differs before production Open: err=%v state=%s want=%s", err, state, image.state)
	}
	manifest, err := nativeTailImageFiles(home)
	if err != nil || manifest != image.files {
		t.Fatalf("private closed copy file manifest differs: err=%v manifest=%s want=%s", err, manifest, image.files)
	}
	return home
}

func nativeTailCheckFixtureClosure(t *testing.T, fixture nativeTailFixture) {
	t.Helper()
	t.Cleanup(func() {
		if err := errors.Join(fixture.db.Close(), fixture.st.Close()); err != nil {
			t.Error(err)
		}
	})
}

func nativeTailAssertRawFixture(t *testing.T, fixture nativeTailFixture, expected nativeTailExpectedSnapshot) {
	t.Helper()
	for seq, peer := range map[int]string{5: "reader", 16: store.PeerKey{Session: "reader", Agent: "worker"}.String()} {
		var recipients string
		if err := fixture.db.QueryRowContext(t.Context(), "SELECT recipients FROM records WHERE seq=?", seq).Scan(&recipients); err != nil {
			t.Fatal(err)
		}
		address, err := json.Marshal([]string{store.PeerAddressOn(fixture.node, peer)})
		if err != nil || recipients != string(address) {
			t.Fatalf("raw fixture seq=%d recipients=%q want=%s err=%v", seq, recipients, address, err)
		}
	}
	state, err := nativeTailSnapshotDatabase(t.Context(), fixture.home, false)
	want := strings.ReplaceAll(expected.state, expected.node, fixture.node)
	if err != nil || state != want {
		t.Fatalf("raw fixture differs from independent fresh creator: err=%v state=%s want=%s", err, state, want)
	}
}

func nativeTailIndependentFiles(image nativeTailEmptyImage, homes ...string) (err error) {
	files := make([]nativeTailImageFile, 0, 1)
	if err := json.Unmarshal([]byte(image.files), &files); err != nil {
		return err
	}
	files = append(files, nativeTailImageFile{Path: "init.lock"})
	for _, file := range files {
		var identities []fs.FileInfo
		for _, home := range homes {
			path := filepath.Join(home, file.Path)
			info, err := os.Lstat(path)
			if err != nil {
				return err
			}
			if !info.Mode().IsRegular() {
				return fmt.Errorf("private file %s is %s", path, info.Mode())
			}
			for _, previous := range identities {
				if os.SameFile(previous, info) {
					return fmt.Errorf("private file %s aliases another fixture", path)
				}
			}
			identities = append(identities, info)
			if file.Path == store.DBFile {
				continue
			}
			lock, err := os.OpenFile(path, os.O_RDWR, 0)
			if err != nil {
				return err
			}
			defer func() { err = errors.Join(err, syscall.Flock(int(lock.Fd()), syscall.LOCK_UN), lock.Close()) }()
			if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
				return fmt.Errorf("private lock %s is not independent: %w", path, err)
			}
		}
	}
	return nil
}

func nativeTailCertifyEmptyImage(t *testing.T, image nativeTailEmptyImage) {
	t.Helper()
	started := time.Now()
	t.Cleanup(func() {
		t.Logf("empty_template_creator_and_cleanup=%s certificate_and_cleanup=%s", image.creation, time.Since(started))
	})
	frozen := sha256.Sum256([]byte(image.files + "\x00" + image.state))
	fresh := newNativeTailFixture(t, nativeTailCall{}, nil)
	nativeTailCheckFixtureClosure(t, fresh)
	state, err := nativeTailSnapshotDatabase(t.Context(), fresh.home, false)
	if err != nil {
		t.Fatal(err)
	}
	expected := nativeTailExpectedSnapshot{node: fresh.node, state: state}
	a := nativeTailFixtureAt(t, nativeTailCall{}, nil, nativeTailCopyEmptyImage(t, image))
	b := nativeTailFixtureAt(t, nativeTailCall{}, nil, nativeTailCopyEmptyImage(t, image))
	nativeTailCheckFixtureClosure(t, a)
	nativeTailCheckFixtureClosure(t, b)
	nativeTailAssertRawFixture(t, a, expected)
	nativeTailAssertRawFixture(t, b, expected)
	if err := nativeTailIndependentFiles(image, a.home, b.home); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"UPDATE records SET text='template poison' WHERE seq=1", "INSERT INTO cursors(name,drive,seq) VALUES('template-poison','d',17)"} {
		if _, err := a.db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	config := filepath.Join(a.home, "relay", "native")
	if err := os.MkdirAll(config, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(config, "config.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	nativeTailForbidWrites(t, a)
	nativeTailAssertRawFixture(t, b, expected)
	if _, err := os.Lstat(filepath.Join(b.home, "relay")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("sibling configuration changed: %v", err)
	}
	if err := errors.Join(a.db.Close(), a.st.Close()); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(a.home); err != nil {
		t.Fatal(err)
	}
	c := nativeTailFixtureAt(t, nativeTailCall{}, nil, nativeTailCopyEmptyImage(t, image))
	nativeTailCheckFixtureClosure(t, c)
	nativeTailAssertRawFixture(t, b, expected)
	nativeTailAssertRawFixture(t, c, expected)
	if _, err := os.Lstat(filepath.Join(c.home, "relay")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("later configuration is not pristine: %v", err)
	}
	if err := nativeTailIndependentFiles(image, b.home, c.home); err != nil {
		t.Fatal(err)
	}
	nodes := []string{fresh.node, a.node, b.node, c.node}
	for i, current := range nodes {
		for _, previous := range nodes[:i] {
			if (current == previous) != (os.Getenv("CCI_NODE_KEY") != "") {
				t.Fatalf("fresh node identity contract changed: nodes=%v key_env_set=%t", nodes, os.Getenv("CCI_NODE_KEY") != "")
			}
		}
	}
	if os.Getenv("CCI_NODE_KEY") == "" {
		key, err := os.ReadFile(filepath.Join(fresh.home, "node", "key.pem"))
		if err != nil {
			t.Fatal(err)
		}
		if !t.Run("node-key-override", func(t *testing.T) {
			t.Setenv("CCI_NODE_KEY", string(key))
			fixture := nativeTailFixtureAt(t, nativeTailCall{}, nil, nativeTailCopyEmptyImage(t, image))
			nativeTailCheckFixtureClosure(t, fixture)
			if fixture.node != fresh.node {
				t.Fatal("private copy did not preserve CCI_NODE_KEY override")
			}
			nativeTailAssertRawFixture(t, fixture, expected)
		}) {
			t.Fatal("node key override certificate failed")
		}
	}
	if after := sha256.Sum256([]byte(image.files + "\x00" + image.state)); after != frozen {
		t.Fatal("frozen template image changed during fixture mutations")
	}
	var manifest []nativeTailImageFile
	if err := json.Unmarshal([]byte(image.files), &manifest); err != nil {
		t.Fatal(err)
	}
	for i := range manifest {
		manifest[i].Data = ""
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("empty_template_binary=%s frozen_sha256=%x files=%s database=%s", os.Args[0], frozen, encoded, image.state)
	nativeTailExpectedSnapshots.Store("clean", expected)
}

func newNativeTailMatrixFixture(t *testing.T, call nativeTailCall, variant string, sets []nativeTailSet) nativeTailFixture {
	t.Helper()
	setupStarted := time.Now()
	image, err := nativeTailProcessImage()
	if err != nil {
		t.Fatal(err)
	}
	nativeTailImageCertificate.once.Do(func() {
		nativeTailImageCertificate.passed = t.Run("empty-template-certificate", func(t *testing.T) {
			nativeTailCertifyEmptyImage(t, image)
		})
	})
	if !nativeTailImageCertificate.passed {
		t.Fatal("empty template certificate failed")
	}
	expected, found := nativeTailExpectedSnapshots.Load(variant)
	if !found {
		var fresh nativeTailExpectedSnapshot
		if !t.Run("independent-fresh-variant", func(t *testing.T) {
			fixture := newNativeTailFixture(t, call, sets)
			nativeTailCheckFixtureClosure(t, fixture)
			state, err := nativeTailSnapshotDatabase(t.Context(), fixture.home, false)
			if err != nil {
				t.Fatal(err)
			}
			fresh = nativeTailExpectedSnapshot{node: fixture.node, state: state}
		}) {
			t.Fatal("independent fresh variant certificate failed")
		}
		nativeTailExpectedSnapshots.Store(variant, fresh)
		expected = fresh
	}
	started := time.Now()
	home := nativeTailCopyEmptyImage(t, image)
	copied := time.Since(started)
	fixture := nativeTailFixtureAt(t, call, sets, home)
	seeded := time.Now()
	nativeTailAssertRawFixture(t, fixture, expected.(nativeTailExpectedSnapshot))
	if os.Getenv("CCI_NATIVE_TAIL_FIXTURE_TIMINGS") == "1" {
		t.Logf("empty_template_copy_and_preopen_certificate=%s raw_seeded_certificate=%s whole_fixture_factory=%s", copied, time.Since(seeded), time.Since(setupStarted))
	}
	return fixture
}

func nativeTailForbidWrites(t *testing.T, fixture nativeTailFixture) {
	t.Helper()
	rows, err := fixture.db.QueryContext(t.Context(), "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for i, name := range names {
		for _, operation := range []string{"INSERT", "UPDATE", "DELETE"} {
			query := fmt.Sprintf("CREATE TRIGGER native_write_%d_%s BEFORE %s ON %q BEGIN SELECT RAISE(FAIL,'native tail attempted a state write'); END", i, operation, operation, name)
			if _, err := fixture.db.ExecContext(t.Context(), query); err != nil {
				t.Fatal(err)
			}
		}
	}
}
