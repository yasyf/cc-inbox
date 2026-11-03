// Package client is the typed Go surface of the cci store for programs in other
// modules. Post, Tail and Open write and read the records cci itself writes and
// reads, in the store CCI_HOME names, under the rules cci applies.
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
	// Stamp has cci write the leading time label from the post's own clock.
	Stamp bool
	// Successor names who takes over the one To lane of a stand-down record.
	Successor string
	LaneRefs  []string
	Attrs     map[string]string
	URL       string
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
	if err := root.ExecuteContext(ctx); err != nil {
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
		args = append(args, "--re", strconv.FormatInt(d.Re, 10))
	}
	if d.Body != nil {
		args = append(args, "--body-file", "-")
	}
	if d.Resolves != 0 {
		args = append(args, "--resolves", strconv.FormatInt(d.Resolves, 10))
	}
	if d.Stamp {
		args = append(args, "--stamp")
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
// Since bounds a first read that has no cursor. Empty Kinds selects every kind.
type Query struct {
	Drive string
	After int64
	Since time.Time
	Kinds []string
}

// Tail reads every record q selects, oldest first, with Status set on each
// opening record as cci tail --json sets it. Unlike cci tail it has no byte
// budget, so a caller never pages by output size.
func Tail(ctx context.Context, q Query) ([]Record, error) {
	selected, err := kinds.ParseAll(q.Kinds)
	if err != nil {
		return nil, err
	}
	st, err := cli.OpenStore(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
	filter := store.Filter{Drive: q.Drive, Kinds: selected, After: q.After, Since: q.Since, Limit: pageRows}
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

// Open reads the drive's openers of the named kinds that no closer or resolver
// has closed, oldest first.
func Open(ctx context.Context, drive string, names ...string) ([]Record, error) {
	selected, err := kinds.ParseAll(names)
	if err != nil {
		return nil, err
	}
	st, err := cli.OpenStore(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = st.Close() }()
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
