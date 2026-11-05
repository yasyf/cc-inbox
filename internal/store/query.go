package store

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

// Follow names another drive whose broadcasts of Kinds a filter also passes.
type Follow struct {
	Drive string       `json:"drive"`
	Kinds []kinds.Kind `json:"kinds"`
}

const (
	sinceFloorStep = 5 * time.Minute
	sinceFloorsMax = 4096
)

type Filter struct {
	Drive          string
	Follow         []Follow
	Origin         string
	Peer           string
	Kinds          []kinds.Kind
	Mute           []kinds.Kind
	ToKinds        []kinds.Kind
	TopicKinds     []kinds.Kind
	FirstOnly      []kinds.Kind
	RepeatWindow   time.Duration
	Lanes          []string
	LaneAliases    bool
	To             string
	LaneFloor      string
	broadcastAfter *int64
	laneStart      int64
	laneHead       int64
	AliasFloor     bool
	aliasSince     int64
	aliasOrders    int64
	For            string
	Aliases        []string
	Addressed      string
	Topics         []string
	Stack          string
	Target         string
	PR             PR
	prKeyed        bool
	After          int64
	PeerAfter      int64
	Before         int64
	Since          time.Time
	Until          time.Time
	IncludeExpired bool
	Descending     bool
	Limit          int
	Contains       []string
	Trigrams       string
}

// where renders the filter. Records in the drive pass the selectors;
// <reader>@<Drive> records, and peer:<Peer> records past PeerAfter, pass
// whatever drive they sit in; broadcasts of a Follow's kinds pass from its drive.
func (f Filter) where(self string, now time.Time) (string, []any) {
	scoped := []string{"drive = ?"}
	params := []any{f.Drive}
	var selectors []string
	var selected []any
	if len(f.Kinds) > 0 {
		selectors = append(selectors, "kind IN ("+placeholders(len(f.Kinds))+")")
		selected = append(selected, kindArgs(f.Kinds)...)
	}
	if len(f.Lanes) > 0 {
		selectors = append(selectors, "lane IN ("+placeholders(len(f.Lanes))+")")
		for _, l := range f.Lanes {
			selected = append(selected, l)
		}
	}
	if len(f.Topics) > 0 {
		selectors = append(selectors, "topic IN ("+placeholders(len(f.Topics))+")")
		for _, t := range f.Topics {
			selected = append(selected, t)
		}
	}
	if f.For != "" {
		names := f.addressees(f.For)
		in := "(" + placeholders(len(names)) + ")"
		broadcast := append([]string{"(recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = 'all'))", "lane NOT IN " + in, "source NOT IN " + in}, selectors...)
		addressed, kindParams := f.addressedKinds(addressedTo(in))
		held, heldParams := f.heldTopics(in, names)
		scoped = append(scoped, "("+addressed+" OR ("+strings.Join(broadcast, " AND ")+")"+held+")")
		params = append(append(append(append(append(append(append(params, names...), names...), kindParams...), names...), hookSources(names)...), selected...), heldParams...)
	} else {
		scoped = append(scoped, selectors...)
		params = append(params, selected...)
	}
	if f.Addressed != "" {
		names := f.addressees(f.Addressed)
		if f.aliasSince > 0 && len(names) > 1 {
			aliases := names[1:]
			fresh, freshParams := f.aliasFresh()
			scoped = append(scoped, "("+addressedTo("(?)")+" OR ("+fresh+" AND "+addressedTo("("+placeholders(len(aliases))+")")+"))")
			params = append(append(append(append(params, names[0], names[0]), freshParams...), aliases...), aliases...)
		} else {
			scoped = append(scoped, addressedTo("("+placeholders(len(names))+")"))
			params = append(append(params, names...), names...)
		}
	}
	if f.To != "" {
		names := f.addressees(f.To)
		in := "(" + placeholders(len(names)) + ")"
		all := "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = 'all') AND lane NOT IN " + in
		allParams := names
		switch {
		case f.broadcastAfter != nil:
			all += " AND seq > ? AND at >= ?"
			allParams = append(slices.Clone(names), *f.broadcastAfter, f.laneStart)
		case f.LaneFloor != "":
			all += " AND seq > COALESCE((SELECT cursors.seq FROM cursors WHERE cursors.name = ? AND cursors.drive = ?), 0) AND at >= COALESCE((SELECT cursors.seq FROM cursors WHERE cursors.name = ? AND cursors.drive = ?), 0)"
			allParams = append(slices.Clone(names), f.LaneFloor, f.Drive, startCursor(f.LaneFloor), f.Drive)
		}
		recipient, recipientParams := f.addressedRecipient(names)
		addressed, kindParams := f.addressedKinds("(" + recipient + " OR (" + all + "))")
		scoped = append(scoped, addressed)
		params = append(append(append(params, recipientParams...), allParams...), kindParams...)
	}
	branches := []string{"(" + strings.Join(scoped, " AND ") + ")"}
	if peers := f.peerAddresses(self); len(peers) > 0 {
		branch := "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(peers)) + "))"
		params = append(params, peers...)
		if f.PeerAfter > 0 {
			branch = "(" + branch + " AND seq > ?)"
			params = append(params, f.PeerAfter)
		}
		branch, kindParams := f.addressedKinds(branch)
		branches = append(branches, branch)
		params = append(params, kindParams...)
	}
	if readers := f.readerAddresses(); len(readers) > 0 {
		clause := "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(readers)) + "))"
		params = append(params, readers...)
		if f.For == "" && f.LaneAliases {
			clause = "(" + strings.Join(append([]string{clause}, selectors...), " AND ") + ")"
			params = append(params, selected...)
		}
		branch, kindParams := f.addressedKinds(clause)
		branches = append(branches, branch)
		params = append(params, kindParams...)
	}
	for _, follow := range f.Follow {
		branches = append(branches, "(drive = ? AND kind IN ("+placeholders(len(follow.Kinds))+") AND (recipients = '[]' OR EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = 'all')))")
		params = append(append(params, follow.Drive), kindArgs(follow.Kinds)...)
	}
	clauses := []string{"(" + strings.Join(branches, " OR ") + ")"}
	if f.Origin != "" {
		clauses = append(clauses, "origin = ?")
		params = append(params, f.Origin)
	}
	if f.Trigrams != "" {
		clauses = append(clauses, "seq IN (SELECT rowid FROM record_trigrams WHERE record_trigrams MATCH ?)")
		params = append(params, f.Trigrams)
	}
	if len(f.Contains) > 0 {
		holds := make([]string, 0, len(renderedColumns)*len(f.Contains))
		for _, word := range f.Contains {
			columns := renderedColumns
			if strings.Trim(word, "0123456789") == "" {
				columns = renderedNumberColumns
			}
			for _, column := range columns {
				holds = append(holds, column+" LIKE ?")
				params = append(params, "%"+word+"%")
			}
		}
		clauses = append(clauses, "("+strings.Join(holds, " OR ")+")")
	}
	for _, ref := range []struct {
		path  string
		value any
		set   bool
	}{{"$.stacks", f.Stack, f.Stack != ""}, {"$.targets", f.Target, f.Target != ""}} {
		if ref.set {
			clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(refs, '"+ref.path+"') WHERE value = ?)")
			params = append(params, ref.value)
		}
	}
	if f.PR.Number != 0 {
		text, param := f.PR.textMatch("value")
		clauses = append(clauses, "EXISTS (SELECT 1 FROM json_each(refs, '$.prs') WHERE (type = 'integer' AND value = ?) OR (type = 'text' AND "+text+"))")
		params = append(params, f.PR.Number, param)
	}
	if len(f.Mute) > 0 {
		clauses = append(clauses, "kind NOT IN ("+placeholders(len(f.Mute))+")")
		params = append(params, kindArgs(f.Mute)...)
	}
	if len(f.FirstOnly) > 0 && f.To != "" {
		names := f.addressees(f.To)
		clauses = append(clauses, "NOT (kind IN ("+placeholders(len(f.FirstOnly))+") AND json_extract(recipients, '$[0]') NOT IN ("+placeholders(len(names))+") AND NOT (kind = ? AND "+ownPR+" IN ("+placeholders(len(names))+"))))")
		params = append(append(append(append(params, kindArgs(f.FirstOnly)...), names...), string(kinds.Review)), names...)
	}
	if f.RepeatWindow > 0 {
		clauses = append(clauses, "NOT (kind = ? AND topic != '' AND EXISTS (SELECT 1 FROM records prior WHERE prior.drive = records.drive AND prior.kind = records.kind AND prior.lane = records.lane AND prior.topic = records.topic AND prior.seq < records.seq AND prior.at > records.at - ?))")
		params = append(params, string(kinds.Urgent), f.RepeatWindow.Milliseconds())
	}
	if f.LaneFloor != "" {
		clauses = append(clauses, "(seq <= COALESCE((SELECT cursors.seq FROM cursors WHERE cursors.name = ? AND cursors.drive = ?), seq) OR at >= COALESCE((SELECT cursors.seq FROM cursors WHERE cursors.name = ? AND cursors.drive = ?), 0))")
		params = append(params, headCursor(f.LaneFloor), f.Drive, startCursor(f.LaneFloor), f.Drive)
	}
	if f.After > 0 {
		clauses = append(clauses, "seq > ?")
		params = append(params, f.After)
	}
	if f.Before > 0 {
		clauses = append(clauses, "seq < ?")
		params = append(params, f.Before)
	}
	if !f.Since.IsZero() {
		clauses = append(clauses, "at >= ?")
		params = append(params, f.Since.UnixMilli())
	}
	if !f.Until.IsZero() {
		clauses = append(clauses, "at < ?")
		params = append(params, f.Until.UnixMilli())
	}
	if !f.IncludeExpired {
		clauses = append(clauses, "(expires_at IS NULL OR expires_at > ?)")
		params = append(params, now.UnixMilli())
	}
	return strings.Join(clauses, " AND "), params
}

var (
	renderedColumns       = []string{"text", "lane", "topic", "kind", "recipients", "refs", "origin"}
	renderedNumberColumns = slices.Concat(renderedColumns, []string{"seq", "origin_seq", "re_origin_seq", "resolves_origin_seq", "COALESCE(also_resolves, '')"})
)

func (f Filter) bounded(self string, now time.Time) (string, []any) {
	return f.boundedAudience(self, now, false)
}

func (f Filter) boundedAudience(self string, now time.Time, rootAudience bool) (string, []any) {
	where, params := f.where(self, now)
	arms, candidates := f.candidateArmsAudience(self, rootAudience)
	if len(arms) == 0 {
		return where, params
	}
	candidate := "seq"
	if f.Origin != "" {
		// Unary plus retains the records_origin scan, including its native error order.
		candidate = "+seq"
	}
	return where + " AND " + candidate + " IN (" + strings.Join(arms, " UNION ALL ") + ")", append(params, candidates...)
}

func (f Filter) candidateArmsAudience(self string, rootAudience bool) ([]string, []any) {
	peers, readers := f.peerAddresses(self), f.readerAddresses()
	if len(peers) == 0 && len(readers) == 0 {
		return nil, nil
	}
	var params []any
	bounds := func(column string, after int64) string {
		var clauses string
		if after > 0 {
			clauses += " AND " + column + " > ?"
			params = append(params, after)
		}
		if f.Before > 0 {
			clauses += " AND " + column + " < ?"
			params = append(params, f.Before)
		}
		return clauses
	}
	var arms []string
	appendDrive := func(selector string, selected ...any) {
		params = append(params, f.Drive)
		params = append(params, selected...)
		arms = append(arms, "SELECT seq AS candidate_seq FROM records WHERE drive = ?"+selector+bounds("seq", f.After))
	}
	switch {
	case rootAudience:
		arms = append(arms, f.rootAudienceArms(bounds, &params)...)
	case f.To != "":
		for _, address := range append(f.addressees(f.To), "all") {
			params = append(params, address)
			arms = append(arms, "SELECT source_seq AS candidate_seq FROM record_recipients WHERE address = ?"+bounds("source_seq", f.After))
		}
	case f.prKeyed:
		params = append(params, f.Drive, strconv.Itoa(f.PR.Number))
		arms = append(arms, "SELECT source_seq AS candidate_seq FROM record_ref_keys WHERE drive = ? AND axis = 'pr' AND key = ?"+bounds("source_seq", f.After))
	case f.For == "" && f.Origin == "" && len(f.Lanes) > 0:
		for _, lane := range slices.Compact(slices.Sorted(slices.Values(f.Lanes))) {
			appendDrive(" AND lane = ?", lane)
		}
	case f.For == "" && f.Origin == "" && len(f.Kinds) > 0:
		for _, kind := range slices.Compact(slices.Sorted(slices.Values(f.Kinds))) {
			appendDrive(" AND kind = ?", string(kind))
		}
	default:
		appendDrive("")
	}
	for _, addresses := range []struct {
		values []any
		after  int64
	}{{peers, max(f.After, f.PeerAfter)}, {readers, f.After}} {
		for _, address := range addresses.values {
			params = append(params, address)
			arms = append(arms, "SELECT source_seq FROM record_recipients WHERE address = ?"+bounds("source_seq", addresses.after))
		}
	}
	for _, follow := range f.Follow {
		for _, kind := range follow.Kinds {
			params = append(params, follow.Drive, string(kind))
			arms = append(arms, "SELECT seq AS candidate_seq FROM records WHERE drive = ? AND kind = ?"+bounds("seq", f.After))
		}
	}
	arms = append(arms, "SELECT source_seq FROM record_json_errors WHERE field = 'recipients'"+bounds("source_seq", f.After))
	return arms, params
}

func (f Filter) addressedKinds(clause string) (string, []any) {
	if len(f.ToKinds) == 0 {
		return clause, nil
	}
	return "(" + clause + " AND kind IN (" + placeholders(len(f.ToKinds)) + "))", kindArgs(f.ToKinds)
}

func (f Filter) heldTopics(in string, names []any) (string, []any) {
	if len(f.TopicKinds) == 0 {
		return "", nil
	}
	clause := " OR (kind IN (" + placeholders(len(f.TopicKinds)) + ") AND topic != '' AND lane NOT IN " + in +
		" AND EXISTS (SELECT 1 FROM records held WHERE held.drive = records.drive AND held.topic = records.topic AND EXISTS (SELECT 1 FROM json_each(held.recipients) WHERE value IN " + in + ")))"
	return clause, slices.Concat(kindArgs(f.TopicKinds), names, names)
}

func kindArgs(ks []kinds.Kind) []any {
	out := make([]any, len(ks))
	for i, k := range ks {
		out[i] = string(k)
	}
	return out
}

func (f Filter) addressedRecipient(names []any) (string, []any) {
	if f.aliasSince == 0 || len(names) < 2 {
		return "EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(names)) + "))", names
	}
	aliases := names[1:]
	fresh, freshParams := f.aliasFresh()
	return "(EXISTS (SELECT 1 FROM json_each(recipients) WHERE value = ?) OR (" + fresh + " AND EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN (" + placeholders(len(aliases)) + "))))", slices.Concat([]any{names[0]}, freshParams, aliases)
}

func (f Filter) aliasFresh() (string, []any) {
	orders := kinds.Orders()
	return "(at >= ? OR (at >= ? AND kind IN (" + placeholders(len(orders)) + ")))", append([]any{f.aliasSince, f.aliasOrders}, kindArgs(orders)...)
}

func addressedTo(in string) string {
	return "(EXISTS (SELECT 1 FROM json_each(recipients) WHERE value IN " + in + ") OR EXISTS (SELECT 1 FROM records own WHERE own.seq = records.re AND own.lane IN " + in + " AND records.lane <> own.lane))"
}

func (f Filter) peerAddresses(self string) []any {
	if f.Peer == "" {
		return nil
	}
	return []any{PeerAddress(f.Peer), PeerAddressOn(self, f.Peer)}
}

func (f Filter) readerAddresses() []any {
	var out []any
	reader := cmp.Or(f.To, f.For, f.Addressed)
	if reader != "" && f.Drive != "" {
		for _, n := range f.addressees(reader) {
			out = append(out, n.(string)+"@"+f.Drive)
		}
	}
	return out
}

// PeerAddress is the recipient form that reaches one session thread in any drive.
func PeerAddress(peer string) string {
	return "peer:" + peer
}

// PeerAddressOn reaches one session on another node, in any drive.
func PeerAddressOn(node, session string) string {
	return "peer:" + node + "/" + session
}

// SplitPeer splits the target of a peer: address into its node and the rest.
// A first segment of exactly 16 lowercase hex characters is a node id; a
// session prefix that long always holds a dash, so the forms never collide.
func SplitPeer(target string) (node, rest string) {
	first, after, ok := strings.Cut(target, "/")
	if ok && len(first) == 16 && strings.Trim(first, "0123456789abcdef") == "" {
		return first, after
	}
	return "", target
}

// CrossDrive reports whether any recipient reaches outside the record's drive.
func CrossDrive(to []string) bool {
	for _, t := range to {
		if strings.HasPrefix(t, "peer:") || strings.Contains(t, "@") {
			return true
		}
	}
	return false
}

var rootNames = []any{"root", "main"}

// RootName reports whether lane addresses a drive's root, which answers to both root and main.
func RootName(lane string) bool {
	return slices.Contains(rootNames, any(lane))
}

func SameLane(a, b string) bool {
	return a == b || RootName(a) && RootName(b)
}

func (f Filter) addressees(lane string) []any {
	if RootName(lane) {
		return rootNames
	}
	if lane != cmp.Or(f.To, f.For, f.Addressed) {
		return []any{lane}
	}
	return append([]any{lane}, toAny(f.Aliases)...)
}

func (s *Store) aliased(ctx context.Context, f Filter) (Filter, error) {
	if f.PR.Number != 0 {
		switch err := s.refIndex(ctx); {
		case err == nil:
			f.prKeyed = true
		case !errors.Is(err, ErrNoRefIndex):
			return f, err
		}
	}
	var err error
	if f.LaneAliases && len(f.Lanes) > 0 {
		if f.Lanes, err = s.lanesAnswering(ctx, f.Drive, f.Lanes); err != nil {
			return f, err
		}
	}
	reader := cmp.Or(f.To, f.For, f.Addressed)
	if reader == "" || f.Drive == "" {
		return f, nil
	}
	if f.Aliases, err = s.readerAliases(ctx, f, reader); err != nil || !f.AliasFloor || len(f.Aliases) == 0 {
		return f, err
	}
	f.aliasSince, f.aliasOrders, err = s.aliasMailSince(ctx, f.Drive, reader)
	return f, err
}

func (s *Store) aliasMailSince(ctx context.Context, drive, lane string) (lastPost, started int64, err error) {
	err = s.db.QueryRowContext(ctx, `WITH predecessor(lane) AS (
	SELECT other.lane FROM lane_holds own CROSS JOIN lane_holds other INDEXED BY lane_holds_stem ON other.drive = own.drive AND other.stem = own.stem
	WHERE own.drive = ?1 AND own.lane = ?2 AND other.lane != ?2 ORDER BY `+writtenOrder("other.held")+` DESC LIMIT 1
)
SELECT COALESCE((SELECT at FROM records WHERE drive = ?1 AND lane = (SELECT lane FROM predecessor) AND kind != 'digest' ORDER BY at * 1000000 + seq DESC LIMIT 1), 0),
	COALESCE((SELECT at FROM records WHERE drive = ?1 AND lane = (SELECT lane FROM predecessor) AND kind != 'digest' ORDER BY at * 1000000 + seq ASC LIMIT 1), 0)`, drive, lane).Scan(&lastPost, &started)
	if err != nil {
		return 0, 0, fmt.Errorf("read the posts of %s's predecessor on %s: %w", lane, drive, err)
	}
	return lastPost, started, nil
}

func HookSource(reader string) string {
	return "hook:" + reader
}

func hookSources(names []any) []any {
	out := make([]any, len(names))
	for i, n := range names {
		out[i] = HookSource(n.(string))
	}
	return out
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// Query reads the records f selects. A filter with no drive must name a Peer,
// and then reads only the records addressed to it, from every drive.
func (s *Store) Query(ctx context.Context, f Filter) ([]Record, error) {
	return s.query(ctx, f, columns)
}

// QueryText reads selected records without the inline bodies omitted by text rendering.
func (s *Store) QueryText(ctx context.Context, f Filter) ([]Record, error) {
	return s.query(ctx, f, textColumns)
}

// LanesWithPrefix names the drive's lanes starting with any prefix, sorted. It
// steps from lane to lane through records_drive_lane, so its cost follows the
// lanes it names and not their records.
func (s *Store) LanesWithPrefix(ctx context.Context, drive string, prefixes []string) ([]string, error) {
	var names []string
	for _, prefix := range prefixes {
		if prefix == "" {
			return nil, errors.New("a lane prefix is not empty")
		}
		end := prefix[:len(prefix)-1] + string([]byte{prefix[len(prefix)-1] + 1})
		rows, err := s.db.QueryContext(ctx, `WITH RECURSIVE named(lane) AS (
SELECT (SELECT MIN(lane) FROM records WHERE drive = ?1 AND lane >= ?2 AND lane < ?3)
UNION ALL SELECT (SELECT MIN(lane) FROM records WHERE drive = ?1 AND lane > named.lane AND lane < ?3) FROM named WHERE named.lane IS NOT NULL
) SELECT lane FROM named WHERE lane IS NOT NULL`, drive, prefix, end)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var lane string
			if err := rows.Scan(&lane); err != nil {
				_ = rows.Close()
				return nil, err
			}
			names = append(names, lane)
		}
		if err := errors.Join(rows.Err(), rows.Close()); err != nil {
			return nil, err
		}
	}
	slices.Sort(names)
	return slices.Compact(names), nil
}

func (s *Store) query(ctx context.Context, f Filter, projection string) ([]Record, error) {
	if f.Drive == "" && f.Peer == "" {
		return nil, ErrNoDrive
	}
	f, err := s.aliased(ctx, f)
	if err != nil {
		return nil, err
	}
	now := s.Now()
	if m := s.reads.model.Load(); m != nil {
		if out, ok := m.answer(ctx, s, f, projection, now); ok {
			return out, s.numbered(ctx, out)
		}
	}
	out, err := s.selected(ctx, f, projection, now)
	if err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

func (s *Store) selected(ctx context.Context, f Filter, projection string, now time.Time) ([]Record, error) {
	conn, err := s.rootAudienceConnectionAt(ctx, f, now)
	if err != nil {
		return nil, err
	}
	reader := s.db.QueryContext
	if conn != nil {
		defer func() { _ = conn.Close() }()
		reader = conn.QueryContext
	}
	q, params := s.selectionColumnsAudienceAt(f, projection, conn != nil, now)
	rows, err := reader(ctx, q, params...)
	if err != nil {
		return nil, fmt.Errorf("query records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if conn != nil {
		_ = rows.Close()
		_ = conn.Close()
	}
	return out, nil
}

func (s *Store) selection(f Filter) (string, []any) {
	return s.selectionColumns(f, columns)
}

func (s *Store) selectionColumns(f Filter, projection string) (string, []any) {
	return s.selectionColumnsAudience(f, projection, false)
}

func (s *Store) selectionColumnsAudience(f Filter, projection string, rootAudience bool) (string, []any) {
	return s.selectionColumnsAudienceAt(f, projection, rootAudience, s.Now())
}

func (s *Store) selectionColumnsAudienceAt(f Filter, projection string, rootAudience bool, now time.Time) (string, []any) {
	order := "ASC"
	if f.Descending {
		order = "DESC"
	}
	var where string
	var params []any
	from, ordered := "records", "seq"
	if f.Origin != "" {
		where, params = f.bounded(s.Self(), now)
	} else {
		where, params = f.where(s.Self(), now)
		arms, candidates := f.candidateArmsAudience(s.Self(), rootAudience)
		if len(arms) > 0 && f.Trigrams == "" {
			// LIMIT -1 prevents flattening from moving validation ahead of the ordered candidate stream.
			from = "(" + strings.Join(arms, " UNION ") + " ORDER BY candidate_seq " + order + " LIMIT -1) c CROSS JOIN records ON records.seq = c.candidate_seq"
			ordered = "c.candidate_seq"
			params = append(candidates, params...)
		}
	}
	q := "SELECT " + projection + " FROM " + from + " WHERE " + where + " ORDER BY " + ordered + " " + order
	if f.Limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", f.Limit)
	}
	return q, params
}

// BoundSince raises f.After to just below the first record inside f.Since, so a
// walk over a drive stops at the window instead of reading all the drive's history.
func (s *Store) BoundSince(ctx context.Context, f Filter) (Filter, error) {
	if f.Since.IsZero() {
		return f, nil
	}
	first, err := s.firstSince(ctx, f.Since.Truncate(sinceFloorStep).UnixMilli())
	if err != nil {
		return f, fmt.Errorf("bound since: %w", err)
	}
	f.After = max(f.After, first-1)
	return f, nil
}

// sinceFloors remembers each answer because later records only take higher seqs,
// so a floor stays right, and the range it reads costs tens of milliseconds.
type sinceFloors struct {
	mu     sync.Mutex
	floors map[int64]int64
}

func (s *Store) firstSince(ctx context.Context, at int64) (int64, error) {
	s.floors.mu.Lock()
	first, known := s.floors.floors[at]
	s.floors.mu.Unlock()
	if known {
		return first, nil
	}
	var found sql.NullInt64
	if m := s.reads.model.Load(); m != nil {
		found = m.firstSince(at)
	}
	if !found.Valid {
		if err := s.db.QueryRowContext(ctx, "SELECT MIN(seq) FROM records WHERE at >= ?", at).Scan(&found); err != nil || !found.Valid {
			return 0, err
		}
	}
	s.floors.mu.Lock()
	defer s.floors.mu.Unlock()
	if s.floors.floors == nil || len(s.floors.floors) >= sinceFloorsMax {
		s.floors.floors = map[int64]int64{}
	}
	s.floors.floors[at] = found.Int64
	return found.Int64, nil
}

func (s *Store) Count(ctx context.Context, f Filter) (int, error) {
	f, err := s.aliased(ctx, f)
	if err != nil {
		return 0, err
	}
	now := s.Now()
	if m := s.reads.model.Load(); m != nil {
		if n, ok := m.count(ctx, s, f, now); ok {
			return n, nil
		}
	}
	conn, err := s.rootAudienceConnectionAt(ctx, f, now)
	if err != nil {
		return 0, err
	}
	reader := s.db.QueryRowContext
	if conn != nil {
		defer func() { _ = conn.Close() }()
		reader = conn.QueryRowContext
	}
	where, params := f.boundedAudience(s.Self(), now, conn != nil)
	var n int
	if err := reader(ctx, "SELECT COUNT(*) FROM records WHERE "+where, params...).Scan(&n); err != nil {
		return 0, fmt.Errorf("count records: %w", err)
	}
	return n, nil
}

func (s *Store) Cursor(ctx context.Context, name, drive string) (int64, bool, error) {
	var seq int64
	err := s.db.QueryRowContext(ctx, "SELECT seq FROM cursors WHERE name = ? AND drive = ?", name, drive).Scan(&seq)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("read cursor %s: %w", name, err)
	}
	return seq, true, nil
}

func (s *Store) SetCursor(ctx context.Context, name, drive string, seq int64) error {
	return s.MoveCursors(ctx, []CursorMove{{Name: name, Drive: drive, Seq: seq}})
}

// CursorPeak is the furthest seq cursor name reached in any drive: every
// peer:<session> record at or before it was already delivered to its reader.
func (s *Store) CursorPeak(ctx context.Context, name string) (int64, error) {
	var seq int64
	if err := s.db.QueryRowContext(ctx, "SELECT COALESCE(MAX(seq), 0) FROM cursors WHERE name = ?", name).Scan(&seq); err != nil {
		return 0, fmt.Errorf("read cursor %s peak: %w", name, err)
	}
	return seq, nil
}

// MoveCursor sets a cursor to seq exactly, backwards included, so a reader
// that restarts replays from the point it last committed.
func (s *Store) MoveCursor(ctx context.Context, name, drive string, seq int64) error {
	return s.MoveCursors(ctx, []CursorMove{{Name: name, Drive: drive, Seq: seq, Exact: true}})
}

// CursorMove is one cursor write a reader owes once its output reached the
// reader: an Exact move sets the cursor to Seq, backwards included, as
// MoveCursor does; any other only advances it, as SetCursor does.
type CursorMove struct {
	Name  string
	Drive string
	Seq   int64
	Exact bool
}

// MoveCursors applies moves in one transaction, so a reader acknowledging
// what it printed moves every cursor or none.
func (s *Store) MoveCursors(ctx context.Context, moves []CursorMove) error {
	if len(moves) == 0 {
		return nil
	}
	if s.beforeCursorMove != nil {
		if err := s.beforeCursorMove(ctx); err != nil {
			return err
		}
	}
	_, err := s.Commit(ctx, Write{Cursors: moves})
	return err
}

const inferredRoles = "('', 'root', 'lane', 'agent', 'session')"

type Binding struct {
	Drive string
	Root  bool
	Lane  string
}

func (s *Store) Bind(ctx context.Context, k PeerKey, drive string, root bool) error {
	tx, err := s.rw.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.claimSession(ctx, tx, k.Session); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, origin, drive, root, role) VALUES (?, ?, ?, ?, ?, CASE WHEN ? THEN 'root' ELSE 'session' END)
ON CONFLICT (session, agent) DO UPDATE SET origin = excluded.origin, drive = excluded.drive, root = excluded.root, lane = '',
	role = CASE WHEN excluded.root THEN 'root' WHEN sessions.role IN `+inferredRoles+` THEN 'session' ELSE sessions.role END`, k.Session, k.Agent, s.Self(), drive, root, root); err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, COALESCE(MAX(seq), 0) FROM records WHERE drive = ?
ON CONFLICT (name, drive) DO UPDATE SET seq = excluded.seq`, k.String(), drive, drive); err != nil {
		return fmt.Errorf("start session cursor: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("bind session: %w", err)
	}
	return nil
}

func LaneCursor(k PeerKey) string {
	return "lane-" + k.String()
}

func BroadcastCursor(k PeerKey) string {
	return LaneCursor(k) + "#all"
}

func startCursor(floor string) string {
	return floor + "-start"
}

func headCursor(floor string) string {
	return floor + "-head"
}

const ownPR = `EXISTS (SELECT 1 FROM json_each(records.refs, '$.prs') graded JOIN records own ON own.drive = records.drive AND own.kind = 'opened' JOIN json_each(own.refs, '$.prs') opened ON CASE opened.type WHEN 'integer' THEN CAST(opened.value AS TEXT) ELSE substr(opened.value, instr(opened.value, '#') + 1) END = CASE graded.type WHEN 'integer' THEN CAST(graded.value AS TEXT) ELSE substr(graded.value, instr(graded.value, '#') + 1) END WHERE own.lane`

// LaneMail is what the thread k bound as b reads as mail. A bound lane hears
// no broadcast from before it started, and no record a relay seed or sync
// applied after it bound that is stamped before it started.
func LaneMail(k PeerKey, b Binding) Filter {
	f := Filter{Drive: b.Drive, Peer: k.String(), To: b.Lane, RepeatWindow: UrgentRepeat}
	if b.Lane != "" {
		f.FirstOnly, f.LaneFloor, f.AliasFloor = kinds.Broadcasts(), BroadcastCursor(k), true
	}
	return f
}

// BindLane names the lane k writes as in drive and starts its lane cursor. A
// thread already bound to another drive, or already bound as a lane, keeps
// that binding: only Bind moves it, so a post as another lane never rebinds it.
// The first thread to bind a lane nobody has bound or posted as on drive
// starts its cursor before the first record addressed to that lane, so the
// lane reads what was posted to it before it started.
func (s *Store) BindLane(ctx context.Context, k PeerKey, drive, lane string, started time.Time) error {
	_, err := s.Commit(ctx, Write{Bind: &LaneBinding{Key: k, Drive: drive, Lane: lane, Started: started}})
	return err
}

// ClaimLane binds k to lane on drive over whatever lane or drive it was bound
// to, as cci announce --lane and cci subscribe --reader name it, so its hooks
// deliver and block for that lane alone. Only Bind and another ClaimLane move
// it after; a post as another lane never does. A root thread keeps its root
// flag, so its prompts are still the owner's.
func (s *Store) ClaimLane(ctx context.Context, k PeerKey, drive, lane string, started time.Time) error {
	if RootName(lane) {
		lane = "root"
	}
	_, err := s.Commit(ctx, Write{Bind: &LaneBinding{Key: k, Drive: drive, Lane: lane, Started: started, Explicit: true}})
	return err
}

// MovesLane reports whether sub names a lane its session's root thread is not
// bound to, so subscribing claims it: a thread bound there already, or its
// drive's root reading as root, stays as it is.
func (s *Store) MovesLane(ctx context.Context, sub Subscription) (bool, error) {
	b, ok, err := s.Binding(ctx, PeerKey{Session: sub.Session})
	if err != nil {
		return false, err
	}
	stays := ok && b.Drive == sub.Drive && (SameLane(b.Lane, sub.Reader) || b.Root && RootName(sub.Reader))
	return !stays, nil
}

func (s *Store) bindLane(ctx context.Context, tx *sql.Tx, lb LaneBinding) error {
	k, drive, lane, started := lb.Key, lb.Drive, lb.Lane, lb.Started
	if err := s.claimSession(ctx, tx, k.Session); err != nil {
		return err
	}
	fresh, err := freshLane(ctx, tx, k, drive, lane)
	if err != nil {
		return err
	}
	spawned := started.UnixMilli()
	if k.Agent != "" {
		spawned = s.Now().UnixMilli()
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO sessions (session, agent, origin, drive, root, lane, role) VALUES (?, ?, ?, ?, 0, ?, 'lane')
ON CONFLICT (session, agent) DO UPDATE SET origin = excluded.origin, drive = excluded.drive, lane = excluded.lane,
	role = CASE WHEN sessions.root OR sessions.role NOT IN `+inferredRoles+` THEN sessions.role ELSE 'lane' END
WHERE ? OR NOT sessions.root AND sessions.drive IN ('', excluded.drive) AND sessions.lane = ''`, k.Session, k.Agent, s.Self(), drive, lane, lb.Explicit); err != nil {
		return fmt.Errorf("bind lane: %w", err)
	}
	waiting := int64(math.MaxInt64)
	if fresh {
		if waiting, err = s.waitingFor(ctx, tx, drive, lane); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, MIN(?, COALESCE(
	(SELECT MIN(+seq) - 1 FROM records WHERE at >= COALESCE(NULLIF((SELECT announced_at FROM sessions WHERE session = ? AND agent = ? AND origin = ?), 0), ?)),
	(SELECT MAX(seq) FROM records),
	0))
WHERE NOT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)
ON CONFLICT (name, drive) DO NOTHING`, LaneCursor(k), drive, waiting, k.Session, k.Agent, s.Self(), spawned, LaneCursor(k), drive); err != nil {
		return fmt.Errorf("start lane cursor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, COALESCE(
	(SELECT MIN(+seq) - 1 FROM records WHERE at >= COALESCE(NULLIF((SELECT announced_at FROM sessions WHERE session = ? AND agent = ? AND origin = ?), 0), ?)),
	(SELECT MAX(seq) FROM records),
	0)
WHERE NOT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)
ON CONFLICT (name, drive) DO NOTHING`, BroadcastCursor(k), drive, k.Session, k.Agent, s.Self(), spawned, BroadcastCursor(k), drive); err != nil {
		return fmt.Errorf("start lane broadcast floor: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, MIN(?, COALESCE(NULLIF((SELECT announced_at FROM sessions WHERE session = ? AND agent = ? AND origin = ?), 0), ?))
WHERE NOT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)
ON CONFLICT (name, drive) DO NOTHING`, startCursor(BroadcastCursor(k)), drive, s.Now().UnixMilli(), k.Session, k.Agent, s.Self(), spawned, startCursor(BroadcastCursor(k)), drive); err != nil {
		return fmt.Errorf("start lane broadcast start: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO cursors (name, drive, seq) SELECT ?, ?, (SELECT COALESCE(MAX(seq), 0) FROM records)
WHERE NOT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)
ON CONFLICT (name, drive) DO NOTHING`, headCursor(BroadcastCursor(k)), drive, headCursor(BroadcastCursor(k)), drive); err != nil {
		return fmt.Errorf("start lane head: %w", err)
	}
	return nil
}

func freshLane(ctx context.Context, tx *sql.Tx, k PeerKey, drive, lane string) (bool, error) {
	var started bool
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM cursors WHERE name = ? AND drive = ?)", LaneCursor(k), drive).Scan(&started); err != nil || started {
		return false, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sessions WHERE drive = ? AND lane = ?) OR EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ?)", drive, lane, drive, lane).Scan(&started); err != nil {
		return false, fmt.Errorf("read whether lane %s started on %s: %w", lane, drive, err)
	}
	return !started, nil
}

func (s *Store) waitingFor(ctx context.Context, tx *sql.Tx, drive, lane string) (int64, error) {
	arms, params, err := s.waitingAliases(ctx, tx, drive, lane)
	if err != nil {
		return 0, err
	}
	waiting := int64(math.MaxInt64)
	if err := tx.QueryRowContext(ctx, "SELECT COALESCE(MIN(r.seq) - 1, ?) FROM record_recipients a JOIN records r ON r.seq = a.source_seq WHERE r.drive = ? AND (a.address = ?"+arms+")",
		append([]any{waiting, drive, lane}, params...)...).Scan(&waiting); err != nil {
		return 0, fmt.Errorf("find records waiting for lane %s: %w", lane, err)
	}
	return waiting, nil
}

// Registered reports whether lane is bound to a thread in drive or has posted there.
func (s *Store) Registered(ctx context.Context, drive, lane string) (bool, error) {
	var ok bool
	if err := s.db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM sessions WHERE drive = ? AND lane = ?) OR EXISTS (SELECT 1 FROM records WHERE drive = ? AND lane = ?)", drive, lane, drive, lane).Scan(&ok); err != nil {
		return false, fmt.Errorf("read lane %s on %s: %w", lane, drive, err)
	}
	return ok, nil
}

// OwnedByAttr is the record attribute a --owned-by decide stores its owner lane under.
const OwnedByAttr = "owned-by"

// LaneOwner reads the lane the newest --owned-by decide on drive names as the owner of lane, "" for none.
func (s *Store) LaneOwner(ctx context.Context, drive, lane string) (string, error) {
	var owner string
	err := s.db.QueryRowContext(ctx, `SELECT json_extract(fields, '$.attrs."owned-by"') FROM records
WHERE drive = ? AND kind = ? AND json_valid(recipients) AND json_extract(recipients, '$[0]') = ? AND json_extract(fields, '$.attrs."owned-by"') IS NOT NULL ORDER BY seq DESC LIMIT 1`, drive, string(kinds.Decide), lane).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read the owner of %s on %s: %w", lane, drive, err)
	}
	return owner, nil
}

// StandDown is a lane's own newest stand-down: its seq, when it was posted,
// and the successor it names, "" for none.
type StandDown struct {
	Seq       int64
	At        time.Time
	Successor string
}

// StandDowns holds each lane of a drive that stood down, keyed by lane.
type StandDowns map[string]StandDown

// Chain follows the successors from lane, each the lane that took the last one over.
func (d StandDowns) Chain(lane string) []string {
	var chain []string
	for successor := d[lane].Successor; successor != "" && successor != lane && !slices.Contains(chain, successor); successor = d[successor].Successor {
		chain = append(chain, successor)
	}
	return chain
}

// StandDowns reads every stand-down of drive in one query, each its first --to lane's own.
func (s *Store) StandDowns(ctx context.Context, drive string) (StandDowns, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT seq, at, CASE WHEN json_valid(recipients) THEN COALESCE(json_extract(recipients, '$[0]'), '') ELSE '' END, COALESCE(json_extract(fields, '$.attrs.successor'), '') FROM records
WHERE drive = ? AND kind = ? ORDER BY seq DESC`, drive, string(kinds.StandDown))
	if err != nil {
		return nil, fmt.Errorf("read the stand-downs on %s: %w", drive, err)
	}
	defer func() { _ = rows.Close() }()
	out := StandDowns{}
	for rows.Next() {
		var (
			lane string
			at   int64
			down StandDown
		)
		if err := rows.Scan(&down.Seq, &at, &lane, &down.Successor); err != nil {
			return nil, fmt.Errorf("scan a stand-down on %s: %w", drive, err)
		}
		down.At = time.UnixMilli(at).UTC()
		if _, newer := out[lane]; lane != "" && !newer {
			out[lane] = down
		}
	}
	return out, rows.Err()
}

// Spawn is a lane a spawn record started on a drive that no thread has bound yet.
type Spawn struct {
	Seq   int64     `json:"spawned"`
	At    time.Time `json:"at"`
	Drive string    `json:"drive"`
	Lane  string    `json:"lane"`
	Brief string    `json:"brief"`
	Model string    `json:"model"`
	Task  string    `json:"task,omitempty"`
}

// UnboundSpawns lists the newest spawn of each lane on drive posted since
// since that no thread on drive is bound as yet.
func (s *Store) UnboundSpawns(ctx context.Context, drive string, since time.Time) ([]Spawn, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT r.seq, r.at, json_extract(r.refs, '$.lanes[0]') AS lane, COALESCE(json_extract(r.fields, '$.attrs.brief'), ''), COALESCE(json_extract(r.fields, '$.attrs.model'), ''), COALESCE(json_extract(r.fields, '$.attrs.task'), '')
FROM records r WHERE r.drive = ? AND r.kind = ? AND r.at >= ?
AND NOT EXISTS (SELECT 1 FROM sessions WHERE drive = r.drive AND lane = json_extract(r.refs, '$.lanes[0]'))
AND NOT EXISTS (SELECT 1 FROM records later WHERE later.drive = r.drive AND later.kind = r.kind AND later.seq > r.seq AND json_extract(later.refs, '$.lanes[0]') = json_extract(r.refs, '$.lanes[0]'))
ORDER BY r.seq`, drive, string(kinds.Spawn), since.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("read spawns on %s: %w", drive, err)
	}
	defer func() { _ = rows.Close() }()
	var out []Spawn
	for rows.Next() {
		sp := Spawn{Drive: drive}
		var at int64
		if err := rows.Scan(&sp.Seq, &at, &sp.Lane, &sp.Brief, &sp.Model, &sp.Task); err != nil {
			return nil, fmt.Errorf("read spawns on %s: %w", drive, err)
		}
		sp.At = time.UnixMilli(at).UTC()
		out = append(out, sp)
	}
	return out, rows.Err()
}

// NamedSuccessor reports whether a stand-down record in drive hands a lane over to lane.
func (s *Store) NamedSuccessor(ctx context.Context, drive, lane string) (bool, error) {
	var ok bool
	if err := s.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM records WHERE drive = ? AND kind = ? AND json_extract(fields, '$.attrs.successor') = ?)`, drive, string(kinds.StandDown), lane).Scan(&ok); err != nil {
		return false, fmt.Errorf("read successors on %s: %w", drive, err)
	}
	return ok, nil
}

func (s *Store) Binding(ctx context.Context, k PeerKey) (Binding, bool, error) {
	var b Binding
	err := s.db.QueryRowContext(ctx, "SELECT drive, root, lane FROM sessions WHERE session = ? AND agent = ? AND drive != '' AND origin = ?", k.Session, k.Agent, s.Self()).Scan(&b.Drive, &b.Root, &b.Lane)
	if errors.Is(err, sql.ErrNoRows) {
		return Binding{}, false, nil
	}
	if err != nil {
		return Binding{}, false, fmt.Errorf("read session binding: %w", err)
	}
	return b, true, nil
}

// SetParent records the agent that spawned k, "" for the session's root thread.
func (s *Store) SetParent(ctx context.Context, k PeerKey, parent string) error {
	if _, err := s.rw.ExecContext(ctx, "UPDATE sessions SET parent = ? WHERE session = ? AND agent = ? AND origin = ?", parent, k.Session, k.Agent, s.Self()); err != nil {
		return fmt.Errorf("record parent of %s: %w", k, err)
	}
	return nil
}

// Owner is the thread whose lane answers for k's work: k once it is bound as
// a lane or as its drive's root, else the nearest such thread among the
// agents that spawned it, up to the session's root thread, which answers
// whenever it is bound to a drive. A k with no such thread owns itself, unbound.
func (s *Store) Owner(ctx context.Context, k PeerKey) (PeerKey, Binding, bool, error) {
	for at := k; ; {
		b, ok, err := s.Binding(ctx, at)
		switch {
		case err != nil:
			return PeerKey{}, Binding{}, false, err
		case ok && (b.Lane != "" || b.Root || at.Agent == ""):
			return at, b, true, nil
		case at.Agent == "":
			return k, Binding{}, false, nil
		}
		err = s.db.QueryRowContext(ctx, "SELECT parent FROM sessions WHERE session = ? AND agent = ? AND origin = ?", at.Session, at.Agent, s.Self()).Scan(&at.Agent)
		if errors.Is(err, sql.ErrNoRows) {
			at.Agent = ""
		} else if err != nil {
			return PeerKey{}, Binding{}, false, fmt.Errorf("read parent of %s: %w", at, err)
		}
	}
}

type Source struct {
	Path   string
	Drive  string
	Lane   string
	Offset int64
	Inode  uint64
	Parser int
}

func (s *Store) Sources(ctx context.Context) ([]Source, error) {
	rows, err := s.db.QueryContext(ctx, "SELECT path, drive, lane, offset, inode, parser FROM imports ORDER BY path")
	if err != nil {
		return nil, fmt.Errorf("list import sources: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Source
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.Path, &src.Drive, &src.Lane, &src.Offset, &src.Inode, &src.Parser); err != nil {
			return nil, fmt.Errorf("scan import source: %w", err)
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) Source(ctx context.Context, path string) (Source, bool, error) {
	src := Source{Path: path}
	err := s.db.QueryRowContext(ctx, "SELECT drive, lane, offset, inode, parser FROM imports WHERE path = ?", path).Scan(&src.Drive, &src.Lane, &src.Offset, &src.Inode, &src.Parser)
	if errors.Is(err, sql.ErrNoRows) {
		return Source{}, false, nil
	}
	if err != nil {
		return Source{}, false, fmt.Errorf("read import source: %w", err)
	}
	return src, true, nil
}

func (s *Store) SaveSource(ctx context.Context, src Source) error {
	_, err := s.rw.ExecContext(ctx, `INSERT INTO imports (path, drive, lane, offset, inode, parser) VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (path) DO UPDATE SET drive = excluded.drive, lane = excluded.lane, offset = excluded.offset, inode = excluded.inode, parser = excluded.parser`, src.Path, src.Drive, src.Lane, src.Offset, src.Inode, src.Parser)
	if err != nil {
		return fmt.Errorf("save import source: %w", err)
	}
	return nil
}

// Tally counts the records f passes by kind and by lane, in one read.
func (s *Store) Tally(ctx context.Context, f Filter) (byKind, byLane map[string]int, err error) {
	where, params := f.where(s.Self(), s.Now())
	rows, err := s.db.QueryContext(ctx, "SELECT kind, lane, COUNT(*) FROM records WHERE "+where+" GROUP BY kind, lane", params...)
	if err != nil {
		return nil, nil, fmt.Errorf("tally records: %w", err)
	}
	defer func() { _ = rows.Close() }()
	byKind, byLane = map[string]int{}, map[string]int{}
	for rows.Next() {
		var (
			kind, lane string
			n          int
		)
		if err := rows.Scan(&kind, &lane, &n); err != nil {
			return nil, nil, fmt.Errorf("scan tally: %w", err)
		}
		byKind[kind] += n
		byLane[lane] += n
	}
	return byKind, byLane, rows.Err()
}

// LaneTips returns up to n non-digest lane winners in drive, ordered by newest sequence.
func (s *Store) LaneTips(ctx context.Context, drive string, n int) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, laneTipsSQL, drive, drive, drive, n)
	if err != nil {
		return nil, fmt.Errorf("latest per lane: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

const laneTipsSQL = `WITH RECURSIVE lane_keys(lane) AS (
SELECT (SELECT lane FROM records INDEXED BY records_drive_lane_score
WHERE drive = ? AND kind != 'digest' ORDER BY lane LIMIT 1)
UNION ALL
SELECT (SELECT lane FROM records INDEXED BY records_drive_lane_score
WHERE drive = ? AND kind != 'digest' AND lane > lane_keys.lane ORDER BY lane LIMIT 1)
FROM lane_keys WHERE lane IS NOT NULL
), winners(seq) AS (
SELECT (SELECT seq FROM records INDEXED BY records_drive_lane_score
WHERE drive = ? AND lane = lane_keys.lane AND kind != 'digest'
ORDER BY at * 1000000 + seq DESC, seq ASC LIMIT 1)
FROM lane_keys WHERE lane IS NOT NULL
)
SELECT ` + columns + ` FROM records WHERE seq IN (SELECT seq FROM winners)
ORDER BY seq DESC LIMIT ?`

func (s *Store) LatestPerLane(ctx context.Context, f Filter, n int) ([]Record, error) {
	where, params := f.where(s.Self(), s.Now())
	q := "SELECT " + columns + " FROM records WHERE seq IN (SELECT seq FROM (SELECT seq, MAX(at * 1000000 + seq) FROM records WHERE " + where + " AND kind != 'digest' GROUP BY lane)) ORDER BY seq DESC LIMIT ?"
	rows, err := s.db.QueryContext(ctx, q, append(params, n)...)
	if err != nil {
		return nil, fmt.Errorf("latest per lane: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Record
	for rows.Next() {
		r, err := s.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, s.numbered(ctx, out)
}

type ReplyPage struct {
	Replies map[int64][]Record
	Err     error
}

type replyScanner struct {
	row      scanner
	re       any
	resolves any
	also     any
	seq      int64
}

func (r *replyScanner) Scan(dest ...any) error {
	err := r.row.Scan(append(dest, &r.re, &r.resolves, &r.also, &r.seq)...)
	if err == nil {
		return nil
	}
	values := make([]any, len(dest))
	raw := make([]any, len(dest), len(dest)+4)
	for i := range values {
		raw[i] = &values[i]
	}
	if rawErr := r.row.Scan(append(raw, &r.re, &r.resolves, &r.also, &r.seq)...); rawErr != nil {
		return rawErr
	}
	return err
}

func (s *Store) Replies(ctx context.Context, seqs []int64) (map[int64][]Record, error) {
	pages, err := s.ReplyPages(ctx, [][]int64{seqs})
	if err != nil {
		return nil, err
	}
	if pages[0].Err != nil {
		return nil, pages[0].Err
	}
	return pages[0].Replies, nil
}

// ReplyPages reads selected replies once and attributes decoding errors to each affected page.
func (s *Store) ReplyPages(ctx context.Context, selected [][]int64) ([]ReplyPage, error) {
	pages := make([]ReplyPage, len(selected))
	firstReply := make([]int64, len(selected))
	owners := map[int64][]int{}
	var seqs []int64
	for page, ids := range selected {
		pages[page].Replies = map[int64][]Record{}
		for _, seq := range ids {
			if len(owners[seq]) == 0 {
				seqs = append(seqs, seq)
			}
			if previous := owners[seq]; len(previous) == 0 || previous[len(previous)-1] != page {
				owners[seq] = append(owners[seq], page)
			}
		}
	}
	if len(seqs) == 0 {
		return pages, nil
	}
	ids, _ := json.Marshal(seqs)
	query := replyPagesQuery(len(owners[0]) != 0)
	rows, err := s.db.QueryContext(ctx, query, string(ids))
	if err != nil {
		return nil, fmt.Errorf("read replies: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type failure struct {
		metadata replyScanner
		err      error
	}
	var failures []failure
	var replies []Record
	for rows.Next() {
		metadata := replyScanner{row: rows}
		r, decodeErr := s.scan(&metadata)
		if decodeErr != nil {
			failures = append(failures, failure{metadata: metadata, err: decodeErr})
			continue
		}
		replies = append(replies, r)
	}
	queryErr := rows.Err()
	if err := rows.Close(); err != nil && queryErr == nil {
		queryErr = err
	}
	if err := s.numbered(ctx, replies); err != nil {
		return nil, err
	}
	for _, r := range replies {
		for _, target := range append([]int64{r.Re, r.Resolves}, r.AlsoResolves...) {
			for _, page := range owners[target] {
				if firstReply[page] == 0 {
					firstReply[page] = r.Seq
				}
			}
		}
		if r.Re != 0 {
			for _, page := range owners[r.Re] {
				pages[page].Replies[r.Re] = append(pages[page].Replies[r.Re], r)
			}
		}
		for _, target := range r.Closes() {
			if target == r.Re {
				continue
			}
			for _, page := range owners[target] {
				pages[page].Replies[target] = append(pages[page].Replies[target], r)
			}
		}
	}
	for _, failed := range failures {
		direct, err := s.replyErrorTargets(ctx, "SELECT value FROM json_each(?) WHERE value = ? OR value = ?", string(ids), failed.metadata.re, failed.metadata.resolves)
		if err != nil {
			return nil, err
		}
		also, err := s.replyErrorTargets(ctx, "SELECT value FROM json_each(?) WHERE value IN (SELECT value FROM json_each(?))", string(ids), failed.metadata.also)
		if err != nil {
			if pages[0].Err == nil {
				pages[0].Err = err
				if firstReply[0] == 0 || firstReply[0] > failed.metadata.seq {
					pages[0].Err = fmt.Errorf("read replies: %w", err)
				}
				for _, target := range direct {
					if slices.Contains(owners[target], 0) {
						pages[0].Err = failed.err
						break
					}
				}
			}
			continue
		}
		targets := append(direct, also...)
		if len(targets) == 0 {
			return nil, failed.err
		}
		for _, target := range targets {
			for _, page := range owners[target] {
				if firstReply[page] == 0 || firstReply[page] > failed.metadata.seq {
					firstReply[page] = failed.metadata.seq
				}
				if pages[page].Err == nil {
					pages[page].Err = failed.err
				}
			}
		}
	}
	if queryErr != nil {
		for page := range pages {
			if pages[page].Err == nil {
				pages[page].Err = queryErr
				if firstReply[page] == 0 {
					pages[page].Err = fmt.Errorf("read replies: %w", queryErr)
				}
			}
		}
	}
	return pages, nil
}

func (s *Store) replyErrorTargets(ctx context.Context, query string, args ...any) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var targets []int64
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			return nil, err
		}
		targets = append(targets, seq)
	}
	return targets, rows.Err()
}

const resolversSQL = `SELECT resolves, seq FROM records WHERE resolves != 0 AND drive = ?
UNION ALL SELECT resolves, seq FROM records WHERE resolves != 0 AND drive != ? AND resolves IN (SELECT seq FROM records WHERE drive = ?)
UNION ALL SELECT j.value, r.seq FROM records r, json_each(r.also_resolves) j WHERE (r.drive = ? OR j.value IN (SELECT seq FROM records WHERE drive = ?))
AND r.seq IN (SELECT seq FROM records WHERE drive = ? UNION ALL SELECT source_seq FROM record_also_resolves WHERE target IN (SELECT seq FROM records WHERE drive = ?) UNION ALL SELECT source_seq FROM record_json_errors WHERE field = 'also_resolves')`

func (s *Store) Resolvers(ctx context.Context, drive string) (map[int64]int64, error) {
	rows, err := s.db.QueryContext(ctx, resolversSQL, slices.Repeat([]any{drive}, 7)...)
	if err != nil {
		return nil, fmt.Errorf("read resolvers: %w", err)
	}
	defer func() { _ = rows.Close() }()
	out := map[int64]int64{}
	for rows.Next() {
		var target, seq int64
		if err := rows.Scan(&target, &seq); err != nil {
			return nil, fmt.Errorf("scan resolver: %w", err)
		}
		out[target] = max(out[target], seq)
	}
	return out, rows.Err()
}

func replyPagesQuery(includeZero bool) string {
	zero := ""
	if includeZero {
		zero = " UNION ALL SELECT seq FROM records WHERE resolves=0"
	}
	in := "SELECT value FROM selected"
	return `WITH selected AS MATERIALIZED(SELECT value FROM json_each(?)), candidates AS (
SELECT seq FROM records INDEXED BY records_re WHERE re IN (` + in + `)
UNION ALL SELECT seq FROM records INDEXED BY records_resolves_target WHERE resolves!=0 AND resolves IN (` + in + `)
UNION ALL SELECT source_seq FROM record_also_resolves INDEXED BY record_also_resolves_target WHERE target IN (` + in + `)
UNION ALL SELECT source_seq FROM record_json_errors INDEXED BY record_json_errors_field WHERE field='also_resolves'` + zero + `)
SELECT ` + columns + `, re, resolves, also_resolves, seq FROM records WHERE seq IN(SELECT seq FROM candidates)
AND (re IN (` + in + `) OR resolves IN (` + in + `) OR EXISTS(SELECT 1 FROM json_each(also_resolves) WHERE value IN (` + in + `))) ORDER BY seq`
}
