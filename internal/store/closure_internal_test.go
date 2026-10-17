package store

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
)

func closed(opener Record, closers []Record) bool {
	for _, c := range closers {
		if c.Seq > opener.Seq && c.Kind.Closes(opener.Kind) && (c.Re == opener.Seq || (c.Topic != "" && c.Topic == opener.Topic)) {
			return true
		}
		if closesByRef(opener, c) {
			return true
		}
	}
	return false
}

func closesByRef(opener, c Record) bool {
	pairs := c.Kind.Closes(opener.Kind) || c.Kind.ClosesByRef() && strings.HasPrefix(opener.Source, "import:")
	later := c.At.After(opener.At) || c.At.Equal(opener.At) && c.Seq > opener.Seq
	if !pairs || !later {
		return false
	}
	if len(opener.Refs.Stacks) > 0 && len(c.Refs.Stacks) > 0 {
		return slices.ContainsFunc(opener.Refs.Stacks, func(s string) bool { return slices.Contains(c.Refs.Stacks, s) })
	}
	return slices.ContainsFunc(opener.Refs.Targets, func(t string) bool { return slices.Contains(c.Refs.Targets, t) })
}

var (
	closureAt   = time.Date(2026, 10, 17, 14, 0, 0, 0, time.UTC)
	closureZone = time.FixedZone("UTC+5", 5*60*60)
)

func assertClosed(t *testing.T, opener Record, closers []Record, want bool) {
	t.Helper()
	if got := closed(opener, closers); got != want {
		t.Fatalf("oracle closed(%+v, %+v) = %v, want %v", opener, closers, got, want)
	}
	if got := indexClosers(closers).closed(opener); got != want {
		t.Fatalf("indexed closed(%+v, %+v) = %v, want %v", opener, closers, got, want)
	}
}

func TestClosureIndexAgreesWithTheOracleOnEdgeCases(t *testing.T) {
	at := closureAt
	later, earlier := at.Add(time.Second), at.Add(-time.Second)
	api, web := []string{"api"}, []string{"web"}
	ask := Record{Seq: 5, Kind: kinds.Ask, At: at}
	decide := Record{Seq: 5, Kind: kinds.Decide, At: at, Topic: "#1"}
	incident := Record{Seq: 5, Kind: kinds.Incident, At: at, Refs: Refs{Targets: api}}
	importedAsk := Record{Seq: 5, Kind: kinds.Ask, At: at, Source: "import:/inbox/a.md", Refs: Refs{Targets: api}}
	refs := func(stacks, targets []string) Record {
		return Record{Seq: 5, Kind: kinds.Incident, At: at, Refs: Refs{Stacks: stacks, Targets: targets}}
	}
	fix := func(stacks, targets []string) Record {
		return Record{Seq: 6, Kind: kinds.FixLive, At: later, Refs: Refs{Stacks: stacks, Targets: targets}}
	}
	sourced := func(source string) Record {
		r := importedAsk
		r.Source = source
		return r
	}
	tests := []struct {
		name    string
		opener  Record
		closers []Record
		want    bool
	}{
		{"a later answer re the ask closes it", ask, []Record{{Seq: 6, Kind: kinds.Answer, Re: 5}}, true},
		{"an earlier answer re the ask leaves it open", ask, []Record{{Seq: 4, Kind: kinds.Answer, Re: 5}}, false},
		{"an answer at the ask's own seq leaves it open", ask, []Record{{Seq: 5, Kind: kinds.Answer, Re: 5}}, false},
		{"a later answer re another record leaves the ask open", ask, []Record{{Seq: 6, Kind: kinds.Answer, Re: 7}}, false},
		{"a later go on the decide's topic closes it", decide, []Record{{Seq: 6, Kind: kinds.Go, Topic: "#1"}}, true},
		{"a later go on another topic leaves the decide open", decide, []Record{{Seq: 6, Kind: kinds.Go, Topic: "#2"}}, false},
		{"an earlier go on the decide's topic leaves it open", decide, []Record{{Seq: 4, Kind: kinds.Go, Topic: "#1"}}, false},
		{"empty topics never match", ask, []Record{{Seq: 6, Kind: kinds.Answer}}, false},
		{"an unaddressed answer closes a seq-zero ask", Record{Kind: kinds.Ask}, []Record{{Seq: 1, Kind: kinds.Answer}}, true},
		{"re closure ignores time", ask, []Record{{Seq: 6, Kind: kinds.Answer, Re: 5, At: at.Add(-time.Hour)}}, true},
		{"re closure needs a later seq however late the closer", ask, []Record{{Seq: 4, Kind: kinds.Answer, Re: 5, At: at.Add(time.Hour)}}, false},
		{"a lift re an ask leaves it open", ask, []Record{{Seq: 6, Kind: kinds.Lift, Re: 5}}, false},
		{"an import source does not widen re closure", importedAsk, []Record{{Seq: 6, Kind: kinds.Done, Re: 5}}, false},
		{"a later fix-live on the incident's target closes it", incident, []Record{fix(nil, api)}, true},
		{"a later fix-live on another target leaves the incident open", incident, []Record{fix(nil, web)}, false},
		{"a fix-live at the same time with a higher seq closes the incident", incident, []Record{{Seq: 6, Kind: kinds.FixLive, At: at, Refs: Refs{Targets: api}}}, true},
		{"a fix-live at the same time with a lower seq leaves the incident open", incident, []Record{{Seq: 4, Kind: kinds.FixLive, At: at, Refs: Refs{Targets: api}}}, false},
		{"an earlier fix-live with a higher seq leaves the incident open", incident, []Record{{Seq: 6, Kind: kinds.FixLive, At: earlier, Refs: Refs{Targets: api}}}, false},
		{"a later fix-live with a lower seq closes the incident", incident, []Record{{Seq: 4, Kind: kinds.FixLive, At: later, Refs: Refs{Targets: api}}}, true},
		{"a fix-live one nanosecond later closes the incident", incident, []Record{{Seq: 4, Kind: kinds.FixLive, At: at.Add(time.Nanosecond), Refs: Refs{Targets: api}}}, true},
		{"a fix-live at the same instant in another zone with a higher seq closes the incident", incident, []Record{{Seq: 6, Kind: kinds.FixLive, At: at.In(closureZone), Refs: Refs{Targets: api}}}, true},
		{"a fix-live at the same instant in another zone with a lower seq leaves the incident open", incident, []Record{{Seq: 4, Kind: kinds.FixLive, At: at.In(closureZone), Refs: Refs{Targets: api}}}, false},
		{"a later answer on the incident's target leaves it open", incident, []Record{{Seq: 6, Kind: kinds.Answer, At: later, Refs: Refs{Targets: api}}}, false},
		{"a later lift on an imported ask's target closes it", importedAsk, []Record{{Seq: 6, Kind: kinds.Lift, At: later, Refs: Refs{Targets: api}}}, true},
		{"a later lift on a posted ask's target leaves it open", sourced("post"), []Record{{Seq: 6, Kind: kinds.Lift, At: later, Refs: Refs{Targets: api}}}, false},
		{"a source that only starts with import leaves the ask open", sourced("imported/a.md"), []Record{{Seq: 6, Kind: kinds.Lift, At: later, Refs: Refs{Targets: api}}}, false},
		{"a bare import: source takes the by-ref override", sourced("import:"), []Record{{Seq: 6, Kind: kinds.Done, At: later, Refs: Refs{Targets: api}}}, true},
		{"a later unblock on an imported ask's target leaves it open", importedAsk, []Record{{Seq: 6, Kind: kinds.Unblock, At: later, Refs: Refs{Targets: api}}}, false},
		{"a shared stack closes the incident", refs([]string{"a/x"}, api), []Record{fix([]string{"a/x"}, nil)}, true},
		{"disjoint stacks leave the incident open despite a shared target", refs([]string{"a/x"}, api), []Record{fix([]string{"b/y"}, api)}, false},
		{"a stackless closer matches a stacked incident on targets", refs([]string{"a/x"}, api), []Record{fix(nil, api)}, true},
		{"a stackless closer on another target leaves a stacked incident open", refs([]string{"a/x"}, api), []Record{fix(nil, web)}, false},
		{"a stacked closer matches a stackless incident on targets", refs(nil, api), []Record{fix([]string{"b/y"}, api)}, true},
		{"a stacked closer without targets leaves a stackless incident open", refs(nil, api), []Record{fix([]string{"a/x"}, nil)}, false},
		{"one shared stack among several closes the incident", refs([]string{"a/x", "b/y"}, nil), []Record{fix([]string{"c/z", "b/y"}, nil)}, true},
		{"empty-string stacks match each other", refs([]string{""}, nil), []Record{fix([]string{""}, nil)}, true},
		{"an empty-string stack still counts as carrying stacks", refs([]string{""}, api), []Record{fix([]string{"b/y"}, api)}, false},
		{"empty-string targets match each other", refs(nil, []string{""}), []Record{fix(nil, []string{""})}, true},
		{"an empty-string target does not match a missing one", refs(nil, []string{""}), []Record{fix(nil, nil)}, false},
		{"refs missing on both sides never close by ref", refs(nil, nil), []Record{fix(nil, nil)}, false},
		{"one closing record among several closes the incident", incident, []Record{
			{Seq: 6, Kind: kinds.Answer, Re: 9},
			{Seq: 4, Kind: kinds.FixLive, At: earlier, Refs: Refs{Targets: api}},
			{Seq: 7, Kind: kinds.FixLive, At: later, Refs: Refs{Targets: api}},
		}, true},
		{"a later unpaired closer does not hide a paired one on the same target", incident, []Record{
			fix(nil, api),
			{Seq: 9, Kind: kinds.Answer, At: at.Add(time.Hour), Refs: Refs{Targets: api}},
		}, true},
		{"a later unpaired closer does not lend its time to an earlier paired one", incident, []Record{
			{Seq: 9, Kind: kinds.Answer, At: at.Add(time.Hour), Refs: Refs{Targets: api}},
			{Seq: 4, Kind: kinds.FixLive, At: earlier, Refs: Refs{Targets: api}},
		}, false},
		{"a later unpaired closer does not hide a paired one re the same record", ask, []Record{
			{Seq: 6, Kind: kinds.Answer, Re: 5},
			{Seq: 9, Kind: kinds.Lift, Re: 5},
		}, true},
		{"no closers leave the ask open", ask, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertClosed(t, tt.opener, tt.closers, tt.want)
		})
	}
}

func TestClosureIndexPairsEveryOpenerWithItsClosers(t *testing.T) {
	pairs := map[kinds.Kind][]kinds.Kind{
		kinds.Ask:      {kinds.Answer, kinds.Go, kinds.Withdraw},
		kinds.Decide:   {kinds.Answer, kinds.Go, kinds.Withdraw},
		kinds.Hold:     {kinds.Lift},
		kinds.Incident: {kinds.FixLive, kinds.Done},
		kinds.Blocker:  {kinds.Withdraw, kinds.Answer, kinds.Done},
		kinds.Blocked:  {kinds.Unblock},
		kinds.Defect:   {kinds.FixLive, kinds.Done},
	}
	byRef := []kinds.Kind{kinds.FixLive, kinds.Lift, kinds.Done}
	if got, want := kinds.Openers(), slices.Sorted(maps.Keys(pairs)); !slices.Equal(got, want) {
		t.Fatalf("kinds.Openers() = %v, want %v", got, want)
	}
	api := Refs{Targets: []string{"api"}}
	for _, opener := range kinds.Openers() {
		for _, closer := range kinds.Closers() {
			paired := slices.Contains(pairs[opener], closer)
			for _, tt := range []struct {
				path   string
				opener Record
				closer Record
				want   bool
			}{
				{"re", Record{Seq: 5, Kind: opener}, Record{Seq: 6, Kind: closer, Re: 5}, paired},
				{"posted target", Record{Seq: 5, Kind: opener, At: closureAt, Source: "post", Refs: api}, Record{Seq: 6, Kind: closer, At: closureAt.Add(time.Second), Refs: api}, paired},
				{"imported target", Record{Seq: 5, Kind: opener, At: closureAt, Source: "import:/inbox/a.md", Refs: api}, Record{Seq: 6, Kind: closer, At: closureAt.Add(time.Second), Refs: api}, paired || slices.Contains(byRef, closer)},
			} {
				t.Run(fmt.Sprintf("%s by %s on %s", opener, closer, tt.path), func(t *testing.T) {
					assertClosed(t, tt.opener, []Record{tt.closer}, tt.want)
				})
			}
		}
	}
}

type recordGen struct {
	r       *rand.Rand
	sources []string
	topics  []string
	stacks  []string
	targets []string
	seqs    int64
	seconds int
}

func (g recordGen) record(kind kinds.Kind, seq int64) Record {
	at := closureAt.Add(time.Duration(g.r.IntN(g.seconds))*time.Second + time.Duration(g.r.IntN(2)))
	if g.r.IntN(4) == 0 {
		at = at.In(closureZone)
	}
	return Record{
		Seq:    seq,
		Kind:   kind,
		At:     at,
		Topic:  pick(g.r, g.topics),
		Re:     g.r.Int64N(g.seqs),
		Source: pick(g.r, g.sources),
		Refs:   Refs{Stacks: g.some(g.stacks), Targets: g.some(g.targets)},
	}
}

func (g recordGen) some(pool []string) []string {
	n := g.r.IntN(3)
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := range out {
		out[i] = pick(g.r, pool)
	}
	return out
}

func pick[T any](r *rand.Rand, from []T) T {
	return from[r.IntN(len(from))]
}

func numbered(format string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf(format, i)
	}
	return out
}

func TestClosureIndexMatchesTheOracleOnCollidingRecords(t *testing.T) {
	g := recordGen{
		r:       rand.New(rand.NewPCG(20261017, 1)),
		sources: []string{"", "post", "import:/inbox/a.md", "import:", "imported/a.md"},
		topics:  []string{"", "#1", "#2"},
		stacks:  []string{"", "a/x", "b/y"},
		targets: []string{"", "api", "web"},
		seconds: 3,
	}
	vocabulary := slices.Concat(kinds.Openers(), kinds.Closers(), []kinds.Kind{kinds.Note})
	verdicts := map[bool]int{}
	for range 4000 {
		n := 2 + g.r.IntN(16)
		g.seqs = int64(n + 2)
		var openers, closers []Record
		for range n {
			r := g.record(pick(g.r, vocabulary), g.r.Int64N(g.seqs))
			if r.Kind.Opens() {
				openers = append(openers, r)
			} else {
				closers = append(closers, r)
			}
		}
		ix := indexClosers(closers)
		for _, o := range openers {
			want := closed(o, closers)
			if got := ix.closed(o); got != want {
				t.Fatalf("indexed closed(%+v) = %v, oracle %v over closers %+v", o, got, want, closers)
			}
			verdicts[want]++
		}
	}
	if verdicts[true] == 0 || verdicts[false] == 0 {
		t.Fatalf("verdicts = %v, want both closed and open openers", verdicts)
	}
}

func TestClosureIndexMatchesTheOracleAtDriveScale(t *testing.T) {
	const openerCount, closerCount = 1500, 2000
	g := recordGen{
		r:       rand.New(rand.NewPCG(20261017, 2)),
		sources: []string{"", "post", "import:/inbox/a.md"},
		topics:  slices.Concat([]string{""}, numbered("#%d", 2000)),
		stacks:  slices.Concat([]string{""}, numbered("p%d/prod", 1000)),
		targets: slices.Concat([]string{""}, numbered("svc-%d", 1000)),
		seqs:    openerCount + closerCount,
		seconds: 4000,
	}
	seqs := g.r.Perm(openerCount + closerCount)
	openers := make([]Record, openerCount)
	for i := range openers {
		openers[i] = g.record(pick(g.r, kinds.Openers()), int64(seqs[i]+1))
	}
	closers := make([]Record, closerCount)
	for i := range closers {
		closers[i] = g.record(pick(g.r, kinds.Closers()), int64(seqs[openerCount+i]+1))
	}
	ix := indexClosers(closers)
	verdicts := map[bool]int{}
	for _, o := range openers {
		want := closed(o, closers)
		if got := ix.closed(o); got != want {
			t.Fatalf("indexed closed(%+v) = %v, oracle %v", o, got, want)
		}
		verdicts[want]++
	}
	if verdicts[true] == 0 || verdicts[false] == 0 {
		t.Fatalf("verdicts = %v, want both closed and open openers", verdicts)
	}
}

func openStore(t *testing.T) (*Store, int64) {
	t.Helper()
	ctx := context.Background()
	st, err := Open(ctx, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	base, err := st.MaxSeq(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return st, base
}

func seqsOf(records []Record) []int64 {
	out := make([]int64, len(records))
	for i, r := range records {
		out[i] = r.Seq
	}
	return out
}

func TestOpenQueriesReturnWhatTheOracleComputes(t *testing.T) {
	ctx := context.Background()
	st, base := openStore(t)
	const n = 400
	g := recordGen{
		r:       rand.New(rand.NewPCG(20261017, 3)),
		sources: []string{"", "post", "import:/inbox/a.md", "import:"},
		topics:  []string{"", "", "", "#1", "#2"},
		stacks:  slices.Concat([]string{""}, numbered("p%d/prod", 40)),
		targets: slices.Concat([]string{""}, numbered("svc-%d", 40)),
		seqs:    n + 3,
		seconds: 30,
	}
	vocabulary := slices.Concat(kinds.Openers(), kinds.Closers(), []kinds.Kind{kinds.Note})
	items := make([]Ingestion, n)
	for i := range items {
		r := g.record(pick(g.r, vocabulary), 0)
		r.Drive, r.Lane, r.Text = "d", "lane-a", fmt.Sprintf("record %d", i)
		if r.Re != 0 {
			r.Re += base
		}
		if i%5 == 0 {
			r.Kind, r.Source, r.Refs = kinds.Hold, "import:/inbox/holds.md", Refs{}
		}
		if i%4 == 1 {
			r.Resolves = base + 1 + g.r.Int64N(n)
		}
		items[i] = Ingestion{Record: r, LineHash: r.Text}
	}
	if got, err := st.IngestAll(ctx, items); err != nil || got != n {
		t.Fatalf("IngestAll() = %d, %v; want %d", got, err, n)
	}
	openers, err := st.Query(ctx, Filter{Drive: "d", Kinds: kinds.Openers(), IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	closers, err := st.Query(ctx, Filter{Drive: "d", Kinds: kinds.Closers(), IncludeExpired: true})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := st.Resolvers(ctx, "d")
	if err != nil {
		t.Fatal(err)
	}
	among := map[int64]bool{}
	seqs := []int64{0, base + n + 7}
	for seq := base + 1; seq <= base+n; seq += 3 {
		among[seq] = true
		seqs = append(seqs, seq)
	}
	tests := []struct {
		name string
		read func() ([]Record, error)
		keep func(Record) bool
	}{
		{"OpenItems", func() ([]Record, error) { return st.OpenItems(ctx, "d") }, Record.Tracked},
		{"UntrackedHolds", func() ([]Record, error) { return st.UntrackedHolds(ctx, "d") }, func(r Record) bool { return r.Kind == kinds.Hold && !r.Tracked() }},
		{"OpenAmong", func() ([]Record, error) { return st.OpenAmong(ctx, "d", seqs) }, func(r Record) bool { return among[r.Seq] && r.Tracked() }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			kept := 0
			var want []int64
			for _, o := range openers {
				if !tt.keep(o) {
					continue
				}
				kept++
				if by, ok := resolved[o.Seq]; ok && by > o.Seq {
					continue
				}
				if !closed(o, closers) {
					want = append(want, o.Seq)
				}
			}
			if len(want) == 0 || len(want) == kept {
				t.Fatalf("oracle leaves %d of %d openers open; want a data set that closes some and leaves some open", len(want), kept)
			}
			got, err := tt.read()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(seqsOf(got), want) {
				t.Fatalf("%s = %v, oracle %v", tt.name, seqsOf(got), want)
			}
		})
	}
}

func TestOpenQueriesSkipOnlyOpenersALaterSeqResolves(t *testing.T) {
	ctx := context.Background()
	st, base := openStore(t)
	held := "import:/inbox/holds.md"
	records := []Record{
		{Kind: kinds.Note, Text: "resolves the next ask before it exists", Resolves: base + 2},
		{Kind: kinds.Ask, Text: "ask resolved by an earlier seq"},
		{Kind: kinds.Ask, Text: "ask resolved by a later seq"},
		{Kind: kinds.Note, Text: "resolves the ask before it", Resolves: base + 3},
		{Kind: kinds.Note, Text: "resolves the next hold before it exists", Resolves: base + 6},
		{Kind: kinds.Hold, Text: "untracked hold resolved by an earlier seq", Source: held},
		{Kind: kinds.Hold, Text: "untracked hold resolved by a later seq", Source: held},
		{Kind: kinds.Note, Text: "resolves the hold before it", Resolves: base + 7},
	}
	items := make([]Ingestion, len(records))
	all := make([]int64, len(records))
	for i, r := range records {
		r.Drive, r.Lane, r.At = "d", "lane-a", closureAt
		items[i] = Ingestion{Record: r, LineHash: r.Text}
		all[i] = base + int64(i) + 1
	}
	if got, err := st.IngestAll(ctx, items); err != nil || got != len(items) {
		t.Fatalf("IngestAll() = %d, %v; want %d", got, err, len(items))
	}
	for i, r := range records {
		if got, err := st.Get(ctx, all[i]); err != nil || got.Text != r.Text {
			t.Fatalf("Get(%d) = %q, %v; want %q", all[i], got.Text, err, r.Text)
		}
	}
	tests := []struct {
		name string
		read func() ([]Record, error)
		want []int64
	}{
		{"OpenItems", func() ([]Record, error) { return st.OpenItems(ctx, "d") }, []int64{base + 2}},
		{"UntrackedHolds", func() ([]Record, error) { return st.UntrackedHolds(ctx, "d") }, []int64{base + 6}},
		{"OpenAmong", func() ([]Record, error) { return st.OpenAmong(ctx, "d", all) }, []int64{base + 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.read()
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(seqsOf(got), tt.want) {
				t.Fatalf("%s = %v, want %v", tt.name, seqsOf(got), tt.want)
			}
		})
	}
}
