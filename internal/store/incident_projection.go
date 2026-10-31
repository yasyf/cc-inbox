package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const incidentRecordColumns = "seq,origin,origin_seq,id,drive,lane,kind,at,text,topic,recipients,re,resolves,refs,fields,expires_at,source,body,also_resolves"

const incidentDirtyNew = `INSERT INTO incident_versions(source_seq,revision) VALUES(NEW.seq,1)
ON CONFLICT(source_seq) DO UPDATE SET revision=revision+1;
INSERT INTO incident_changes(source_seq,revision)
SELECT NEW.seq,revision FROM incident_versions WHERE source_seq=NEW.seq
ON CONFLICT(source_seq) DO UPDATE SET revision=excluded.revision;`

const incidentDirtyOld = `INSERT INTO incident_versions(source_seq,revision) VALUES(OLD.seq,1)
ON CONFLICT(source_seq) DO UPDATE SET revision=revision+1;
INSERT INTO incident_changes(source_seq,revision)
SELECT OLD.seq,revision FROM incident_versions WHERE source_seq=OLD.seq
ON CONFLICT(source_seq) DO UPDATE SET revision=excluded.revision;`

const incidentDirtyMovedOld = `INSERT INTO incident_versions(source_seq,revision) SELECT OLD.seq,1 WHERE OLD.seq!=NEW.seq
ON CONFLICT(source_seq) DO UPDATE SET revision=revision+1;
INSERT INTO incident_changes(source_seq,revision)
SELECT OLD.seq,revision FROM incident_versions WHERE source_seq=OLD.seq AND OLD.seq!=NEW.seq
ON CONFLICT(source_seq) DO UPDATE SET revision=excluded.revision;`

var incidentObjects = [...]struct {
	name string
	sql  string
}{
	{"incident_scopes", `CREATE TABLE incident_scopes(drive TEXT PRIMARY KEY,ready INTEGER NOT NULL DEFAULT 0)`},
	{"incident_versions", `CREATE TABLE incident_versions(source_seq INTEGER PRIMARY KEY,revision INTEGER NOT NULL)`},
	{"incident_changes", `CREATE TABLE incident_changes(source_seq INTEGER PRIMARY KEY,revision INTEGER NOT NULL)`},
	{"incident_records", `CREATE TABLE incident_records(
seq INTEGER PRIMARY KEY,origin TEXT NOT NULL,origin_seq INTEGER NOT NULL,id TEXT NOT NULL,
drive TEXT NOT NULL,lane TEXT NOT NULL,kind TEXT NOT NULL,at INTEGER NOT NULL,text TEXT NOT NULL,
topic TEXT NOT NULL,recipients TEXT NOT NULL,re INTEGER NOT NULL,resolves INTEGER NOT NULL,
refs TEXT NOT NULL,fields TEXT NOT NULL,expires_at INTEGER,source TEXT NOT NULL,body TEXT NOT NULL,
also_resolves TEXT NOT NULL,revision INTEGER NOT NULL,decode_error TEXT NOT NULL DEFAULT '',
body_path TEXT NOT NULL DEFAULT '',unsupported_path INTEGER NOT NULL DEFAULT 0)`},
	{"incident_records_drive", `CREATE INDEX incident_records_drive ON incident_records(drive,seq DESC)`},
	{"incident_records_errors", `CREATE INDEX incident_records_errors ON incident_records(drive,seq) WHERE decode_error!=''`},
	{"incident_records_unsupported", `CREATE INDEX incident_records_unsupported ON incident_records(drive,seq) WHERE unsupported_path!=0`},
	{"incident_records_path", `CREATE INDEX incident_records_path ON incident_records(body_path,seq)`},
	{"incident_record_prs", `CREATE TABLE incident_record_prs(source_seq INTEGER NOT NULL,axis TEXT NOT NULL,pr TEXT NOT NULL,PRIMARY KEY(source_seq,axis,pr)) WITHOUT ROWID`},
	{"incident_record_prs_lookup", `CREATE INDEX incident_record_prs_lookup ON incident_record_prs(pr,axis,source_seq)`},
	{"incident_body_paths", `CREATE TABLE incident_body_paths(path TEXT PRIMARY KEY,body_version TEXT NOT NULL)`},
	{"incident_body_changes", `CREATE TABLE incident_body_changes(path TEXT PRIMARY KEY)`},
	{"incident_body_paths_version", `CREATE INDEX incident_body_paths_version ON incident_body_paths(body_version,path)`},
	{"incident_body_versions", `CREATE TABLE incident_body_versions(id TEXT PRIMARY KEY,identity TEXT NOT NULL,valid INTEGER,error TEXT NOT NULL DEFAULT '')`},
	{"incident_body_versions_errors", `CREATE INDEX incident_body_versions_errors ON incident_body_versions(id) WHERE error!=''`},
	{"incident_body_prs", `CREATE TABLE incident_body_prs(body_version TEXT NOT NULL,pr TEXT NOT NULL,PRIMARY KEY(body_version,pr)) WITHOUT ROWID`},
	{"incident_body_prs_lookup", `CREATE INDEX incident_body_prs_lookup ON incident_body_prs(pr,body_version)`},
	{"incident_records_insert", `CREATE TRIGGER incident_records_insert AFTER INSERT ON records
WHEN NEW.kind='incident' AND EXISTS(SELECT 1 FROM incident_scopes WHERE drive=NEW.drive)
BEGIN ` + incidentDirtyNew + ` END`},
	{"incident_records_update", `CREATE TRIGGER incident_records_update AFTER UPDATE OF seq,origin,origin_seq,id,drive,lane,kind,at,text,topic,recipients,re,resolves,refs,fields,expires_at,source,body,also_resolves ON records
WHEN (OLD.kind='incident' AND EXISTS(SELECT 1 FROM incident_scopes WHERE drive=OLD.drive))
OR (NEW.kind='incident' AND EXISTS(SELECT 1 FROM incident_scopes WHERE drive=NEW.drive))
BEGIN ` + incidentDirtyMovedOld + incidentDirtyNew + ` END`},
	{"incident_records_delete", `CREATE TRIGGER incident_records_delete AFTER DELETE ON records
WHEN OLD.kind='incident' AND EXISTS(SELECT 1 FROM incident_scopes WHERE drive=OLD.drive)
BEGIN ` + incidentDirtyOld + ` END`},
}

func incidentProjectionInstalled(ctx context.Context, q querier) (bool, error) {
	found := 0
	for _, object := range incidentObjects {
		var definition string
		err := q.QueryRowContext(ctx, "SELECT sql FROM sqlite_schema WHERE name=?", object.name).Scan(&definition)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return false, fmt.Errorf("read incident projection %s: %w", object.name, err)
		}
		if definition != object.sql {
			return false, fmt.Errorf("foreign incident projection object %s", object.name)
		}
		found++
	}
	if found != 0 && found != len(incidentObjects) {
		return false, fmt.Errorf("incomplete incident projection: %d of %d objects", found, len(incidentObjects))
	}
	return found == len(incidentObjects), nil
}

func installIncidentProjection(ctx context.Context, tx *sql.Tx) error {
	for _, object := range incidentObjects {
		if _, err := tx.ExecContext(ctx, object.sql); err != nil {
			return fmt.Errorf("install incident projection %s: %w", object.name, err)
		}
	}
	return nil
}

type incidentCapture struct {
	row    scanner
	values []any
}

func (c *incidentCapture) Scan(dest ...any) error {
	if err := c.row.Scan(dest...); err != nil {
		return err
	}
	for _, dest := range dest {
		switch value := dest.(type) {
		case *int64:
			c.values = append(c.values, *value)
		case *string:
			c.values = append(c.values, *value)
		case *sql.NullInt64:
			if value.Valid {
				c.values = append(c.values, value.Int64)
			} else {
				c.values = append(c.values, nil)
			}
		default:
			panic(fmt.Sprintf("unexpected incident snapshot column %T", dest))
		}
	}
	return nil
}

func writeIncidentRecord(ctx context.Context, tx *sql.Tx, values []any, revision int64, decodeError, path string, unsupported bool) error {
	assignments := strings.Split(incidentRecordColumns, ",")
	for i, column := range assignments {
		assignments[i] = column + "=excluded." + column
	}
	values = append(values, revision, decodeError, path, unsupported)
	_, err := tx.ExecContext(ctx, "INSERT INTO incident_records("+incidentRecordColumns+",revision,decode_error,body_path,unsupported_path) VALUES("+placeholders(len(values))+") ON CONFLICT(seq) DO UPDATE SET "+strings.Join(assignments, ",")+",revision=excluded.revision,decode_error=excluded.decode_error,body_path=excluded.body_path,unsupported_path=excluded.unsupported_path", values...)
	return err
}
