// Package client is the typed Go surface of the cci store for programs in other
// modules. Post, Tail, Follow and Open write and read the records cci itself
// writes and reads, in the store CCI_HOME names, under the rules cci applies.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/yasyf/cc-inbox/internal/cli"
	"github.com/yasyf/cc-inbox/internal/inbox"
	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	pageRows    = 500
	typedPrefix = "lr."
)

// Record is one stored record, the value cci tail --json prints.
type Record = store.Record

// BusDown is the error Post returns when the store did not take a post. With
// Path set, the next post that reaches the bus stores the copy kept there.
type BusDown = cli.BusDown

// Draft is one record to post. Kind is a cci kind or a dotted service kind such
// as lr.grade; Body, when set, is a JSON value of at most 64 KiB; a repost of an
// ID already in the drive returns the stored record.
type Draft struct {
	Drive string
	Lane  string
	Kind  string
	Text  string
	ID    string
	Topic string
	To    []string
	PRs   []string
	Re    int64
	Body  json.RawMessage
	// Resolves closes the opener with this seq.
	Resolves int64
	// Successor names who takes over the one To lane of a stand-down record.
	Successor string
	LaneRefs  []string
	Attrs     map[string]string
	URL       string
	// Path names the file holding the record's whole body, as cci post --path does.
	Path string
	// Force addresses each To lane as written, for a sender that takes the name
	// from a registry rather than from a person's typing.
	Force bool
	// Notices receives the lines cci post writes to stderr, such as a --to stem
	// no lane holds or a stood-down lane whose mail reaches another; nil drops them.
	Notices io.Writer
}

// Stem is the name a lane answers under, the lane without a trailing -<n>:
// cci delivers a post to the stem to the newest live lane registered under it.
func Stem(lane string) string {
	return store.Stem(lane)
}

// Reach says who reads mail addressed to one lane name: Lane is the lane it
// reaches now, "" when none does, and Holders are the stem's live lanes,
// newest registration first.
type Reach = store.Reach

// ErrNoLaneRegistry is what Resolve returns on a store whose lane registry cci
// maintain-indexes has not installed yet; cci post then addresses a name as typed.
var ErrNoLaneRegistry = store.ErrNoLaneRegistry

func init() {
	cli.ReplayThrough("cci")
}

var (
	sharedMu sync.Mutex
	shared   = map[string]*store.Store{}
)

// sharedStore is this process's one handle on the store CCI_HOME names, so a long-lived
// embedder keeps its page cache between calls. It lives as long as the process.
func sharedStore(ctx context.Context) (*store.Store, error) {
	home, err := store.Home()
	if err != nil {
		return nil, err
	}
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if st, ok := shared[home]; ok {
		return st, nil
	}
	st, err := cli.OpenStore(ctx)
	if err != nil {
		return nil, err
	}
	shared[home] = st
	return st, nil
}

// Resolve reaches name on drive as cci post --to does: a live lane reads its own
// mail, a stood-down one passes it to the successor it named, else to its stem's
// newest live lane, and a stem reaches that lane. Each hop is one indexed lookup.
func Resolve(ctx context.Context, drive, name string) (Reach, error) {
	st, err := sharedStore(ctx)
	if err != nil {
		return Reach{}, err
	}
	return st.Reach(ctx, drive, name)
}

// Generations lists every lane registered under stem on drive, live or stood
// down, so a launch can number past them and never reuse a name.
func Generations(ctx context.Context, drive, stem string) ([]string, error) {
	st, err := sharedStore(ctx)
	if err != nil {
		return nil, err
	}
	return st.Generations(ctx, drive, stem)
}

// Post stores d through cci post itself, so its recipient checks, reply routing,
// text limits and the unsent queue a down bus falls back to all apply; the error
// is the one cci post would exit on.
func Post(ctx context.Context, d Draft) (Record, error) {
	var out bytes.Buffer
	root := cli.NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below, which contextcheck cannot follow.
	root.SetArgs(d.args())
	root.SetIn(bytes.NewReader(d.Body))
	root.SetOut(&out)
	root.SetErr(io.Discard)
	if d.Notices != nil {
		root.SetErr(d.Notices)
	}
	st, err := sharedStore(ctx)
	if err != nil {
		return Record{}, err
	}
	embedded, err := cli.WithStore(ctx, st)
	if err != nil {
		return Record{}, err
	}
	if err := root.ExecuteContext(embedded); err != nil {
		return Record{}, err
	}
	var stored Record
	if err := json.Unmarshal(out.Bytes(), &stored); err != nil {
		return Record{}, fmt.Errorf("cci post printed %q: %w", out.String(), err)
	}
	return stored, nil
}

func (d Draft) args() []string {
	args := []string{"post", "--drive", d.Drive, "--lane", d.Lane, "--kind", d.Kind, "--text", d.Text, "--json"}
	if d.ID != "" {
		args = append(args, "--id", d.ID)
	}
	if d.Topic != "" {
		args = append(args, "--topic", d.Topic)
	}
	for _, pr := range d.PRs {
		args = append(args, "--pr", pr)
	}
	for _, to := range d.To {
		args = append(args, "--to", to)
	}
	if d.Re != 0 {
		args = append(args, "--re-seq", strconv.FormatInt(d.Re, 10))
	}
	if d.Body != nil {
		args = append(args, "--body-file", "-")
	}
	if d.Resolves != 0 {
		args = append(args, "--resolves-seq", strconv.FormatInt(d.Resolves, 10))
	}
	if d.Successor != "" {
		args = append(args, "--successor", d.Successor)
	}
	for _, lane := range d.LaneRefs {
		args = append(args, "--lane-ref", lane)
	}
	for _, key := range slices.Sorted(maps.Keys(d.Attrs)) {
		args = append(args, "--attr", key+"="+d.Attrs[key])
	}
	if d.URL != "" {
		args = append(args, "--url", d.URL)
	}
	if d.Path != "" {
		args = append(args, "--path", d.Path)
	}
	if d.Force {
		args = append(args, "--force")
	}
	return args
}

// Peer is one session bound to a drive, the row cci who --all --json prints.
type Peer struct {
	Session   string    `json:"session"`
	Agent     string    `json:"agent"`
	Lane      string    `json:"lane"`
	Live      bool      `json:"live"`
	SeenAt    time.Time `json:"seen_at"`
	StoodDown bool      `json:"stood_down"`
}

// Who reads every session bound to drive, ended ones included, through cci who
// itself, so its liveness and stand-down marks are the ones cci prints.
func Who(ctx context.Context, drive string) ([]Peer, error) {
	var out bytes.Buffer
	root := cli.NewRootCmd() //nolint:contextcheck // the tree takes ctx from ExecuteContext below, which contextcheck cannot follow.
	root.SetArgs([]string{"who", "--drive", drive, "--all", "--json", "--fields", "session,agent,lane,live,seen_at,stood_down"})
	root.SetOut(&out)
	root.SetErr(io.Discard)
	if err := root.ExecuteContext(ctx); err != nil {
		return nil, err
	}
	var rows []Peer
	decoder := json.NewDecoder(&out)
	for decoder.More() {
		var row Peer
		if err := decoder.Decode(&row); err != nil {
			return nil, fmt.Errorf("cci who printed a row that does not parse: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Query selects records of one drive. After is the seq a cursor reader last saw;
// Since bounds a first read that has no cursor. Empty Kinds selects every kind,
// empty Lanes every lane. PR, as owner/repo#N or a bare N, keeps records about
// that pull request, as cci tail --pr does.
type Query struct {
	Drive string
	After int64
	Since time.Time
	Kinds []string
	Lanes []string
	PR    string
}

// Tail reads every record q selects, oldest first, with Status set on each
// opening record as cci tail --json sets it. Unlike cci tail it has no byte
// budget, so a caller never pages by output size.
func Tail(ctx context.Context, q Query) ([]Record, error) {
	selected, err := kinds.ParseAll(q.Kinds)
	if err != nil {
		return nil, err
	}
	var pr store.PR
	if q.PR != "" {
		if pr, err = store.ParsePR(q.PR); err != nil {
			return nil, err
		}
	}
	st, err := sharedStore(ctx)
	if err != nil {
		return nil, err
	}
	filter := store.Filter{Drive: q.Drive, Kinds: selected, Lanes: q.Lanes, PR: pr, After: q.After, Since: q.Since, Limit: pageRows}
	var records []Record
	for {
		page, err := st.Query(ctx, filter)
		if err != nil {
			return nil, err
		}
		records = append(records, page...)
		if len(page) < pageRows {
			return records, inbox.Annotate(ctx, st, records)
		}
		filter.After = page[len(page)-1].Seq
	}
}

// Among reads the records of drive with the named seqs, oldest first, leaving out
// a seq that names no record there, as cci tail --seqs does.
func Among(ctx context.Context, drive string, seqs []int64) ([]Record, error) {
	st, err := sharedStore(ctx)
	if err != nil {
		return nil, err
	}
	records, err := st.RecordsAmong(ctx, drive, seqs)
	if err != nil {
		return nil, err
	}
	return records, inbox.Annotate(ctx, st, records)
}

// Open reads the drive's openers of the named kinds that no closer or resolver
// has closed, oldest first.
func Open(ctx context.Context, drive string, names ...string) ([]Record, error) {
	selected, err := kinds.ParseAll(names)
	if err != nil {
		return nil, err
	}
	st, err := sharedStore(ctx)
	if err != nil {
		return nil, err
	}
	openers, err := st.Query(ctx, store.Filter{Drive: drive, Kinds: selected, IncludeExpired: true})
	if err != nil {
		return nil, err
	}
	seqs := make([]int64, 0, len(openers))
	for _, r := range openers {
		seqs = append(seqs, r.Seq)
	}
	return st.OpenAmong(ctx, drive, seqs)
}

// Watch selects what Follow delivers: the records after seq After addressed to
// Reader or to this session's peer address, of every kind, and other lanes'
// broadcasts of Kinds. With no After it starts past the drive's newest record.
type Watch struct {
	Drive  string
	Reader string
	After  int64
	Kinds  []string
}

// Follow calls emit with each record w selects, oldest first, as the store takes
// it, until ctx ends or emit fails. Unlike cci watch --reader root it never
// narrows the kinds addressed to the reader, so a hold addressed to root arrives.
func Follow(ctx context.Context, w Watch, emit func(Record) error) error {
	selected, err := kinds.ParseAll(w.Kinds)
	if err != nil {
		return err
	}
	st, err := sharedStore(ctx)
	if err != nil {
		return err
	}
	filter := store.Filter{Drive: w.Drive, For: w.Reader, Peer: cli.SelfPeer(ctx).String(), Kinds: selected, After: w.After}
	return inbox.Watch(ctx, st, inbox.WatchOptions{Filter: filter, Interval: time.Second, JSON: true}, func(r store.Record, _ string) error {
		return emit(r)
	})
}

// Kinds lists the kinds a lane can post, every fixed kind but those only cci writes.
func Kinds() []string {
	return slices.DeleteFunc(kinds.Names(), func(name string) bool {
		return kinds.Kind(name).Spec().Internal
	})
}

// Schema is one typed kind's table entry: its fields, identity and audience.
type Schema = kinds.Schema

// TypedSchema returns the entry of the typed kind lr.<name> and whether it exists.
func TypedSchema(name string) (Schema, bool) {
	return kinds.Typed(kinds.Kind(typedPrefix + name))
}

// TypedKinds lists the names of the typed kinds in table order.
func TypedKinds() []string {
	return kinds.TypedNames()
}

// CheckBody returns the error Post would refuse the typed kind lr.<name> with, its
// corrected command included, and stores nothing.
func CheckBody(name string, body json.RawMessage, to []string) error {
	return kinds.CheckBody(kinds.Kind(typedPrefix+name), body, to)
}
