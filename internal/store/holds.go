package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

var generation = regexp.MustCompile(`^(.+)-\d+$`)

// Stem is the alias a lane answers under: the lane without a trailing -<n>,
// so signal-valkey-3 answers under signal-valkey, or the lane itself.
func Stem(lane string) string {
	if m := generation.FindStringSubmatch(lane); m != nil {
		return m[1]
	}
	return lane
}

func stemSQL(lane string) string {
	trimmed := "rtrim(" + lane + ", '0123456789')"
	return "CASE WHEN " + trimmed + " != " + lane + " AND " + trimmed + " GLOB '?*-' THEN substr(" + trimmed + ", 1, length(" + trimmed + ") - 1) ELSE " + lane + " END"
}

func holdableSQL(lane string) string {
	return lane + " IS NOT NULL AND " + lane + " NOT IN ('', 'root', 'main', 'owner', 'all') AND " + lane + " NOT GLOB 'peer:*' AND instr(" + lane + ", '@') = 0"
}

func spawnedSQL(record string) string {
	return "CASE WHEN " + record + ".kind = 'spawn' AND json_valid(" + record + ".refs) THEN json_extract(" + record + ".refs, '$.lanes[0]') WHEN " + record + ".kind = 'state' AND " + record + ".topic GLOB 'spawn:?*' THEN substr(" + record + ".topic, 7) END"
}

const liveSQL = "(down = 0 OR seen > down)"

func liveOf(table string) string {
	return "(" + table + ".down = 0 OR " + table + ".seen > " + table + ".down)"
}

func holdsFill() string {
	return `INSERT INTO lane_holds (drive, stem, lane, held, seen)
SELECT drive, ` + stemSQL("lane") + `, lane, MIN(seq), MAX(CASE WHEN kind != 'stand-down' THEN seq ELSE 0 END) FROM records
WHERE drive != '' AND ` + holdableSQL("lane") + ` GROUP BY drive, lane;
INSERT INTO lane_holds (drive, stem, lane, held, seen)
SELECT drive, ` + stemSQL("l") + `, l, MAX(seq), MAX(seq) FROM (SELECT drive, seq, ` + spawnedSQL("records") + ` AS l FROM records WHERE kind IN ('spawn', 'state') AND drive != '')
WHERE ` + holdableSQL("l") + ` GROUP BY drive, l
ON CONFLICT (drive, lane, stem) DO UPDATE SET held = max(held, excluded.held), seen = max(seen, excluded.seen);
INSERT INTO lane_holds (drive, stem, lane, held, seen)
SELECT DISTINCT drive, ` + stemSQL("lane") + `, lane, (SELECT COALESCE(MAX(seq), 0) FROM records), (SELECT COALESCE(MAX(seq), 0) FROM records) FROM sessions
WHERE drive != '' AND ` + holdableSQL("lane") + `
ON CONFLICT (drive, lane, stem) DO NOTHING;
WITH downs AS (SELECT drive, json_extract(recipients, '$[0]') AS l, seq, COALESCE(json_extract(fields, '$.attrs.successor'), '') AS successor FROM records
	WHERE kind = 'stand-down' AND drive != '' AND json_valid(recipients) AND json_type(recipients, '$[0]') = 'text'),
newest AS (SELECT * FROM downs d WHERE seq = (SELECT MAX(seq) FROM downs o WHERE o.drive = d.drive AND o.l = d.l))
INSERT INTO lane_holds (drive, stem, lane, held, down, successor)
SELECT drive, ` + stemSQL("l") + `, l, seq, seq, successor FROM newest WHERE ` + holdableSQL("l") + `
ON CONFLICT (drive, lane, stem) DO UPDATE SET down = excluded.down, successor = excluded.successor;
INSERT INTO lane_holds (drive, stem, lane, held)
SELECT drive, stem, successor, down FROM lane_holds WHERE successor != '' AND ` + holdableSQL("successor") + `
ON CONFLICT (drive, lane, stem) DO UPDATE SET held = max(held, excluded.held)`
}

var laneHoldObjects = [...]physicalObject{
	{"table", "lane_holds", "CREATE TABLE lane_holds (drive TEXT NOT NULL, stem TEXT NOT NULL, lane TEXT NOT NULL, held INTEGER NOT NULL, seen INTEGER NOT NULL DEFAULT 0, down INTEGER NOT NULL DEFAULT 0, successor TEXT NOT NULL DEFAULT '', PRIMARY KEY (drive, lane, stem)) WITHOUT ROWID", holdsFill()},
	{"index", "lane_holds_stem", "CREATE INDEX lane_holds_stem ON lane_holds(drive, stem, held)", ""},
	{"index", "lane_holds_successor", "CREATE INDEX lane_holds_successor ON lane_holds(drive, successor) WHERE successor != ''", ""},
	{"trigger", "lane_holds_post", `CREATE TRIGGER lane_holds_post AFTER INSERT ON records
WHEN NEW.drive != '' AND ` + holdableSQL("NEW.lane") + `
BEGIN
INSERT INTO lane_holds (drive, stem, lane, held, seen) VALUES (NEW.drive, ` + stemSQL("NEW.lane") + `, NEW.lane, NEW.seq, CASE WHEN NEW.kind = 'stand-down' THEN 0 ELSE NEW.seq END)
ON CONFLICT (drive, lane, stem) DO NOTHING;
UPDATE lane_holds SET seen = NEW.seq WHERE drive = NEW.drive AND lane = NEW.lane AND down > seen AND NEW.kind != 'stand-down';
END`, ""},
	{"trigger", "lane_holds_spawn", `CREATE TRIGGER lane_holds_spawn AFTER INSERT ON records
WHEN NEW.kind IN ('spawn', 'state') AND NEW.drive != '' AND ` + holdableSQL(spawnedSQL("NEW")) + `
BEGIN
INSERT INTO lane_holds (drive, stem, lane, held, seen)
VALUES (NEW.drive, ` + stemSQL(spawnedSQL("NEW")) + `, ` + spawnedSQL("NEW") + `, NEW.seq, NEW.seq)
ON CONFLICT (drive, lane, stem) DO UPDATE SET held = excluded.held, seen = excluded.seen;
END`, ""},
	{"trigger", "lane_holds_stand_down", `CREATE TRIGGER lane_holds_stand_down AFTER INSERT ON records
WHEN NEW.kind = 'stand-down' AND NEW.drive != '' AND json_valid(NEW.recipients) AND json_type(NEW.recipients, '$[0]') = 'text' AND ` + holdableSQL("json_extract(NEW.recipients, '$[0]')") + `
BEGIN
INSERT INTO lane_holds (drive, stem, lane, held)
VALUES (NEW.drive, ` + stemSQL("json_extract(NEW.recipients, '$[0]')") + `, json_extract(NEW.recipients, '$[0]'), NEW.seq)
ON CONFLICT (drive, lane, stem) DO NOTHING;
UPDATE lane_holds SET down = NEW.seq, successor = COALESCE(json_extract(NEW.fields, '$.attrs.successor'), '')
WHERE drive = NEW.drive AND lane = json_extract(NEW.recipients, '$[0]');
INSERT INTO lane_holds (drive, stem, lane, held)
SELECT drive, stem, successor, NEW.seq FROM lane_holds
WHERE drive = NEW.drive AND lane = json_extract(NEW.recipients, '$[0]') AND ` + holdableSQL("successor") + `
ON CONFLICT (drive, lane, stem) DO UPDATE SET held = max(held, excluded.held);
END`, ""},
	{"trigger", "lane_holds_bind", `CREATE TRIGGER lane_holds_bind AFTER INSERT ON sessions
WHEN NEW.drive != '' AND ` + holdableSQL("NEW.lane") + `
BEGIN
INSERT INTO lane_holds (drive, stem, lane, held, seen)
VALUES (NEW.drive, ` + stemSQL("NEW.lane") + `, NEW.lane, (SELECT COALESCE(MAX(seq), 0) FROM records), (SELECT COALESCE(MAX(seq), 0) FROM records))
ON CONFLICT (drive, lane, stem) DO UPDATE SET seen = max(seen, excluded.seen);
END`, ""},
	{"trigger", "lane_holds_rebind", `CREATE TRIGGER lane_holds_rebind AFTER UPDATE OF drive, lane ON sessions
WHEN (NEW.drive IS NOT OLD.drive OR NEW.lane IS NOT OLD.lane) AND NEW.drive != '' AND ` + holdableSQL("NEW.lane") + `
BEGIN
INSERT INTO lane_holds (drive, stem, lane, held, seen)
VALUES (NEW.drive, ` + stemSQL("NEW.lane") + `, NEW.lane, (SELECT COALESCE(MAX(seq), 0) FROM records), (SELECT COALESCE(MAX(seq), 0) FROM records))
ON CONFLICT (drive, lane, stem) DO UPDATE SET seen = max(seen, excluded.seen);
END`, ""},
}

// ErrNoLaneRegistry is what a registry read returns on a store whose lane
// registry cci maintain-indexes has not installed yet.
var ErrNoLaneRegistry = errors.New("this store has no lane registry yet; cci maintain-indexes installs it")

func (s *Store) laneRegistry(ctx context.Context) error {
	var installed bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'lane_holds')").Scan(&installed); err != nil {
		return fmt.Errorf("look for the lane registry: %w", err)
	}
	if !installed {
		return ErrNoLaneRegistry
	}
	return nil
}

// Reach says who reads mail addressed to one lane name on a drive.
type Reach struct {
	Stem string
	// Started reports whether the name is a lane that registered on the drive:
	// it posted, bound a thread, was launched by a spawn record, or stood down.
	Started bool
	// Down reports whether the name stood down and has posted nothing since.
	Down bool
	// Lane is the lane the mail reaches now, "" when no lane does.
	Lane string
	// Holders are the stem's live lanes, newest registration first; the first holds the stem.
	Holders []string
}

type hold struct {
	lane, stem, successor string
	live                  bool
}

// Reach resolves name on drive through the lane registry: a live lane reads
// its own mail; a stood-down one passes it to the successor it named, else to
// its stem's holder; a stem reaches its holder. An unregistered numbered name
// keeps its mail until it registers. Each hop is one indexed lookup.
func (s *Store) Reach(ctx context.Context, drive, name string) (Reach, error) {
	if err := s.laneRegistry(ctx); err != nil {
		return Reach{}, err
	}
	out := Reach{Stem: Stem(name)}
	for cur, visited := name, []string{}; ; {
		rows, err := s.holds(ctx, drive, cur)
		if err != nil {
			return Reach{}, err
		}
		own := slices.IndexFunc(rows, func(h hold) bool { return h.lane == cur })
		var holders []string
		for _, h := range rows {
			if h.live && h.stem == Stem(cur) && !slices.Contains(holders, h.lane) {
				holders = append(holders, h.lane)
			}
		}
		if len(visited) == 0 {
			out.Started, out.Holders = own >= 0, holders
			out.Down = out.Started && !rows[own].live
		}
		visited = append(visited, cur)
		switch {
		case own >= 0 && rows[own].live:
			out.Lane = cur
			return out, nil
		case own >= 0 && rows[own].successor != "" && !slices.Contains(visited, rows[own].successor):
			cur = rows[own].successor
		case own >= 0 || cur == Stem(cur):
			if i := slices.IndexFunc(holders, func(l string) bool { return !slices.Contains(visited, l) }); i >= 0 {
				out.Lane = holders[i]
			}
			return out, nil
		default:
			return out, nil
		}
	}
}

func (s *Store) holds(ctx context.Context, drive, lane string) ([]hold, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT lane, stem, successor, `+liveSQL+`, held FROM lane_holds WHERE drive = ? AND lane = ?
UNION SELECT lane, stem, successor, `+liveSQL+`, held FROM lane_holds INDEXED BY lane_holds_stem WHERE drive = ? AND stem = ? AND `+liveSQL+`
ORDER BY held DESC, lane`, drive, lane, drive, Stem(lane))
	if err != nil {
		return nil, fmt.Errorf("read the lane registry for %s on %s: %w", lane, drive, err)
	}
	defer func() { _ = rows.Close() }()
	var out []hold
	for rows.Next() {
		var h hold
		var held int64
		if err := rows.Scan(&h.lane, &h.stem, &h.successor, &h.live, &held); err != nil {
			return nil, fmt.Errorf("scan the lane registry for %s on %s: %w", lane, drive, err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

type rowsQuerier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Aliases lists the other names whose mail reaches lane on drive: each stem it
// holds as the stem's newest live registration, the stem's lanes that stood
// down naming no successor, and every stood-down lane whose successor chain
// ends at one of those. The query walks indexes from lane, so its cost does not
// grow with the drive's other lanes.
func (s *Store) Aliases(ctx context.Context, drive, lane string) ([]string, error) {
	switch err := s.laneRegistry(ctx); {
	case errors.Is(err, ErrNoLaneRegistry):
		return nil, nil
	case err != nil:
		return nil, err
	}
	return aliases(ctx, s.db, drive, lane)
}

func aliases(ctx context.Context, q rowsQuerier, drive, lane string) ([]string, error) {
	if RootName(lane) {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, `WITH RECURSIVE
held(stem) AS (SELECT h.stem FROM lane_holds h WHERE h.drive = ?1 AND h.lane = ?2 AND (h.down = 0 OR h.seen > h.down)
	AND NOT EXISTS (SELECT 1 FROM lane_holds o INDEXED BY lane_holds_stem WHERE o.drive = ?1 AND o.stem = h.stem AND o.lane != h.lane AND (o.down = 0 OR o.seen > o.down) AND (o.held > h.held OR o.held = h.held AND o.lane < h.lane))),
reach(name) AS (
	SELECT ?2
	UNION SELECT stem FROM held
	UNION SELECT r.lane FROM held CROSS JOIN lane_holds r INDEXED BY lane_holds_stem ON r.drive = ?1 AND r.stem = held.stem WHERE r.down > 0 AND r.seen <= r.down AND r.successor = ''
	UNION SELECT r.lane FROM reach CROSS JOIN lane_holds r INDEXED BY lane_holds_successor ON r.drive = ?1 AND r.successor = reach.name WHERE r.successor != '' AND r.down > 0 AND r.seen <= r.down
)
SELECT name FROM reach WHERE name != ?2 ORDER BY name`, drive, lane)
	if err != nil {
		return nil, fmt.Errorf("read the aliases of %s on %s: %w", lane, drive, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan the aliases of %s on %s: %w", lane, drive, err)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// Successors follows the stand-down records of drive from lane, each naming the
// lane that took the last one over; standDown is the seq of lane's own newest
// stand-down, 0 when it never stood down. A stand-down is its first --to lane's
// own: the lanes it copies, a successor it names included, did not stand down.
// Each step is one indexed registry lookup.
func (s *Store) Successors(ctx context.Context, drive, lane string) (chain []string, standDown int64, err error) {
	if err := s.laneRegistry(ctx); errors.Is(err, ErrNoLaneRegistry) {
		downs, err := s.StandDowns(ctx, drive)
		return downs.Chain(lane), downs[lane].Seq, err
	} else if err != nil {
		return nil, 0, err
	}
	for cur := lane; ; {
		var successor string
		var down int64
		err := s.db.QueryRowContext(ctx, "SELECT down, successor FROM lane_holds WHERE drive = ? AND lane = ? LIMIT 1", drive, cur).Scan(&down, &successor)
		if errors.Is(err, sql.ErrNoRows) {
			return chain, standDown, nil
		}
		if err != nil {
			return nil, 0, fmt.Errorf("read the successor of %s on %s: %w", cur, drive, err)
		}
		if cur == lane {
			standDown = down
		}
		if successor == "" || successor == lane || slices.Contains(chain, successor) {
			return chain, standDown, nil
		}
		chain, cur = append(chain, successor), successor
	}
}

// Generations lists every lane registered under stem on drive, live or stood
// down, oldest name first: the names a launch numbers past, so none is reused.
// The query reads the stem's own rows through its index.
func (s *Store) Generations(ctx context.Context, drive, stem string) ([]string, error) {
	if err := s.laneRegistry(ctx); err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, "SELECT lane FROM lane_holds INDEXED BY lane_holds_stem WHERE drive = ? AND stem = ? ORDER BY lane", drive, stem)
	if err != nil {
		return nil, fmt.Errorf("read the generations of %s on %s: %w", stem, drive, err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var lane string
		if err := rows.Scan(&lane); err != nil {
			return nil, fmt.Errorf("scan the generations of %s on %s: %w", stem, drive, err)
		}
		out = append(out, lane)
	}
	return out, rows.Err()
}

func (s *Store) waitingAliases(ctx context.Context, tx *sql.Tx, drive, lane string) (string, []any, error) {
	switch err := s.laneRegistry(ctx); {
	case errors.Is(err, ErrNoLaneRegistry):
		return "", nil, nil
	case err != nil:
		return "", nil, err
	}
	names, err := aliases(ctx, tx, drive, lane)
	if err != nil || len(names) == 0 {
		return "", nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT o.lane, o.held, o.down, `+liveOf("o")+` FROM lane_holds h CROSS JOIN lane_holds o INDEXED BY lane_holds_stem ON o.drive = h.drive AND o.stem = h.stem WHERE h.drive = ? AND h.lane = ?
UNION SELECT lane, held, down, `+liveSQL+` FROM lane_holds WHERE drive = ? AND lane IN (`+placeholders(len(names))+`)`,
		append([]any{drive, lane, drive}, toAny(names)...)...)
	if err != nil {
		return "", nil, fmt.Errorf("read the registry around %s on %s: %w", lane, drive, err)
	}
	defer func() { _ = rows.Close() }()
	downs := map[string]int64{}
	var held, lastDown int64
	otherLive := false
	for rows.Next() {
		var name string
		var at, down int64
		var live bool
		if err := rows.Scan(&name, &at, &down, &live); err != nil {
			return "", nil, fmt.Errorf("scan the registry around %s on %s: %w", lane, drive, err)
		}
		switch {
		case name == lane:
			held = at
		case live:
			otherLive = true
		default:
			downs[name] = down
			lastDown = max(lastDown, down)
		}
	}
	if err := rows.Err(); err != nil {
		return "", nil, err
	}
	floor := min(held, lastDown)
	if otherLive {
		floor = held
	}
	var arms []string
	var params []any
	for _, name := range names {
		arms = append(arms, "(a.address = ? AND a.source_seq > ?)")
		params = append(params, name, max(floor, downs[name]))
	}
	return " OR " + strings.Join(arms, " OR "), params, nil
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}
