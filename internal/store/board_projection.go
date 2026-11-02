package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type boardDefinition struct{ name, sql string }

var boardDefinitions = []boardDefinition{
	{"board_query_meta", `CREATE TABLE board_query_meta(id INTEGER PRIMARY KEY CHECK(id=1), revision INTEGER NOT NULL)`},
	{"board_query_changes", `CREATE TABLE board_query_changes(seq INTEGER PRIMARY KEY, revision INTEGER NOT NULL)`},
	{"board_query_changes_revision", `CREATE INDEX board_query_changes_revision ON board_query_changes(revision,seq)`},
	{"board_query_scopes", `CREATE TABLE board_query_scopes(id TEXT PRIMARY KEY, spec TEXT NOT NULL, applied INTEGER NOT NULL, clock_ms INTEGER NOT NULL)`},
	{"board_query_rows", `CREATE TABLE board_query_rows(scope TEXT NOT NULL, category INTEGER NOT NULL, seq INTEGER NOT NULL, path TEXT NOT NULL, lane TEXT NOT NULL, expires_at INTEGER, error TEXT NOT NULL, annotation_error TEXT NOT NULL DEFAULT '', active INTEGER NOT NULL, PRIMARY KEY(scope,category,seq)) WITHOUT ROWID`},
	{"board_query_rows_expiry", `CREATE INDEX board_query_rows_expiry ON board_query_rows(scope,expires_at,category,seq)`},
	{"board_query_active_errors", `CREATE INDEX board_query_active_errors ON board_query_rows(scope,category,seq) WHERE active=1 AND (error!='' OR annotation_error!='')`},
	{"board_query_lanes", `CREATE TABLE board_query_lanes(scope TEXT NOT NULL,lane TEXT NOT NULL,n INTEGER NOT NULL,PRIMARY KEY(scope,lane)) WITHOUT ROWID`},
	{"board_query_lane_insert", `CREATE TRIGGER board_query_lane_insert AFTER INSERT ON board_query_rows WHEN NEW.category=1 AND NEW.active=1 AND NEW.error='' BEGIN INSERT INTO board_query_lanes(scope,lane,n) VALUES(NEW.scope,NEW.lane,1) ON CONFLICT(scope,lane) DO UPDATE SET n=n+1; END`},
	{"board_query_lane_delete", `CREATE TRIGGER board_query_lane_delete AFTER DELETE ON board_query_rows WHEN OLD.category=1 AND OLD.active=1 AND OLD.error='' BEGIN UPDATE board_query_lanes SET n=n-1 WHERE scope=OLD.scope AND lane=OLD.lane; DELETE FROM board_query_lanes WHERE scope=OLD.scope AND lane=OLD.lane AND n=0; END`},
	{"board_query_lane_active", `CREATE TRIGGER board_query_lane_active AFTER UPDATE OF active ON board_query_rows WHEN NEW.category=1 AND NEW.error='' AND NEW.active!=OLD.active BEGIN INSERT INTO board_query_lanes(scope,lane,n) VALUES(NEW.scope,NEW.lane,NEW.active-OLD.active) ON CONFLICT(scope,lane) DO UPDATE SET n=n+NEW.active-OLD.active; DELETE FROM board_query_lanes WHERE scope=NEW.scope AND lane=NEW.lane AND n=0; END`},
	{"board_query_rows_path", `CREATE INDEX board_query_rows_path ON board_query_rows(scope,path,category,seq)`},
	{"board_query_rows_error", `CREATE INDEX board_query_rows_error ON board_query_rows(scope,category,error,seq)`},
	{"board_query_rows_lane", `CREATE INDEX board_query_rows_lane ON board_query_rows(scope,category,lane,seq)`},
	{"board_query_keys", `CREATE TABLE board_query_keys(scope TEXT NOT NULL, category INTEGER NOT NULL, axis TEXT NOT NULL, key TEXT NOT NULL, seq INTEGER NOT NULL, PRIMARY KEY(scope,category,axis,key,seq)) WITHOUT ROWID`},
	{"board_query_keys_seq", `CREATE INDEX board_query_keys_seq ON board_query_keys(scope,seq,category)`},
	{"board_query_openers", `CREATE TABLE board_query_openers(scope TEXT NOT NULL, category INTEGER NOT NULL, seq INTEGER NOT NULL, drive TEXT NOT NULL, axis TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(scope,category,seq,drive,axis,key)) WITHOUT ROWID`},
	{"board_query_openers_lookup", `CREATE INDEX board_query_openers_lookup ON board_query_openers(scope,drive,axis,key,category,seq)`},
	{"board_query_effects", `CREATE TABLE board_query_effects(scope TEXT NOT NULL, source_seq INTEGER NOT NULL, drive TEXT NOT NULL, axis TEXT NOT NULL, key TEXT NOT NULL, PRIMARY KEY(scope,source_seq,drive,axis,key)) WITHOUT ROWID`},
	{"board_query_bodies", `CREATE TABLE board_query_bodies(scope TEXT NOT NULL, path TEXT NOT NULL, body BLOB, regular INTEGER NOT NULL, error TEXT NOT NULL, PRIMARY KEY(scope,path)) WITHOUT ROWID`},
	{"board_query_insert", `CREATE TRIGGER board_query_insert AFTER INSERT ON records BEGIN UPDATE board_query_meta SET revision=revision+1 WHERE id=1; INSERT INTO board_query_changes(seq,revision) SELECT NEW.seq,revision FROM board_query_meta WHERE id=1 ON CONFLICT(seq) DO UPDATE SET revision=excluded.revision; END`},
	{"board_query_delete", `CREATE TRIGGER board_query_delete AFTER DELETE ON records BEGIN UPDATE board_query_meta SET revision=revision+1 WHERE id=1; INSERT INTO board_query_changes(seq,revision) SELECT OLD.seq,revision FROM board_query_meta WHERE id=1 ON CONFLICT(seq) DO UPDATE SET revision=excluded.revision; END`},
	{"board_query_update", `CREATE TRIGGER board_query_update AFTER UPDATE ON records BEGIN UPDATE board_query_meta SET revision=revision+1 WHERE id=1; INSERT INTO board_query_changes(seq,revision) SELECT OLD.seq,revision FROM board_query_meta WHERE id=1 ON CONFLICT(seq) DO UPDATE SET revision=excluded.revision; INSERT INTO board_query_changes(seq,revision) SELECT NEW.seq,revision FROM board_query_meta WHERE id=1 ON CONFLICT(seq) DO UPDATE SET revision=excluded.revision; END`},
}

func boardProjectionInstalled(ctx context.Context, db *sql.DB) (bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT name,sql FROM sqlite_master WHERE name GLOB 'board_query_*'")
	if err != nil {
		return false, err
	}
	found := map[string]string{}
	for rows.Next() {
		var name, definition string
		if err := rows.Scan(&name, &definition); err != nil {
			_ = rows.Close()
			return false, err
		}
		found[name] = definition
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return false, err
	}
	if len(found) == 0 {
		return false, nil
	}
	if len(found) != len(boardDefinitions) {
		return false, errors.New("board query namespace is partial or foreign")
	}
	for _, definition := range boardDefinitions {
		if strings.TrimSpace(found[definition.name]) != definition.sql {
			return false, fmt.Errorf("board query definition mismatch: %s", definition.name)
		}
	}
	return true, nil
}

func (s *Store) InstallBoardProjection(ctx context.Context) error {
	installed, err := boardProjectionInstalled(ctx, s.db)
	if err != nil || installed {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, definition := range boardDefinitions {
		if _, err := tx.ExecContext(ctx, definition.sql); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO board_query_meta(id,revision) VALUES(1,0)"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(s.home, "board-query-owner.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	return file.Close()
}
