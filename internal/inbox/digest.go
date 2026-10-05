package inbox

import (
	"cmp"
	"context"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/yasyf/cc-inbox/internal/kinds"
	"github.com/yasyf/cc-inbox/internal/render"
	"github.com/yasyf/cc-inbox/internal/store"
)

const (
	latestLanes  = 15
	DigestBudget = 32000
)

type DigestView struct {
	Drive     string         `json:"drive"`
	Since     time.Time      `json:"since"`
	Total     int            `json:"total"`
	Kinds     map[string]int `json:"kinds"`
	Lanes     map[string]int `json:"lanes"`
	Asks      []store.Record `json:"open_asks"`
	Holds     []store.Record `json:"open_holds"`
	Untracked []store.Record `json:"untracked_holds"`
	Incidents []store.Record `json:"open_incidents"`
	Blockers  []store.Record `json:"open_blockers"`
	Defects   []store.Record `json:"open_defects"`
	Latest    []store.Record `json:"latest"`
	OlderOpen int            `json:"older_open"`
	MaxSeq    int64          `json:"max_seq"`
}

func Digest(ctx context.Context, st *store.Store, drive string, since time.Time) (DigestView, error) {
	f := store.Filter{Drive: drive, Since: since}
	v := DigestView{Drive: drive, Since: since}
	var err error
	if v.Kinds, err = st.CountBy(ctx, f, "kind"); err != nil {
		return DigestView{}, err
	}
	if v.Lanes, err = st.CountBy(ctx, f, "lane"); err != nil {
		return DigestView{}, err
	}
	for _, n := range v.Kinds {
		v.Total += n
	}
	open, err := st.OpenItems(ctx, drive)
	if err != nil {
		return DigestView{}, err
	}
	for _, r := range open {
		if r.At.Before(since) {
			v.OlderOpen++
			continue
		}
		switch r.Kind {
		case kinds.Hold:
			v.Holds = append(v.Holds, r)
		case kinds.Incident:
			v.Incidents = append(v.Incidents, r)
		case kinds.Blocker, kinds.Blocked:
			v.Blockers = append(v.Blockers, r)
		case kinds.Defect:
			v.Defects = append(v.Defects, r)
		default:
			v.Asks = append(v.Asks, r)
		}
	}
	untracked, err := st.UntrackedHolds(ctx, drive)
	if err != nil {
		return DigestView{}, err
	}
	for _, r := range untracked {
		if !r.At.Before(since) {
			v.Untracked = append(v.Untracked, r)
		}
	}
	if v.Latest, err = st.LatestPerLane(ctx, f, latestLanes); err != nil {
		return DigestView{}, err
	}
	if v.MaxSeq, err = st.MaxSeq(ctx); err != nil {
		return DigestView{}, err
	}
	return v, nil
}

func (v DigestView) Write(w io.Writer, now time.Time, budget, width int) {
	b := render.NewBudget(w, budget)
	b.Line(fmt.Sprintf("cci digest %s since %s: %d records, max seq #%d", v.Drive, render.Stamp(v.Since, now), v.Total, v.MaxSeq))
	b.Line("kinds: " + ranked(v.Kinds, 12))
	b.Line("lanes: " + ranked(v.Lanes, 12))
	section(b, "open asks", v.Asks, 8, now, width)
	section(b, "open blockers", v.Blockers, 6, now, width)
	section(b, "open defects", v.Defects, 6, now, width)
	section(b, "open holds", v.Holds, 6, now, width)
	untracked(b, v.Untracked)
	section(b, "open incidents", v.Incidents, 6, now, width)
	if v.OlderOpen > 0 {
		b.Line(fmt.Sprintf("%d older open items (cci tail --kind ask --kind decide --kind blocker --kind blocked --kind defect --kind hold --kind incident --since 0)", v.OlderOpen))
	}
	section(b, "latest per lane", v.Latest, latestLanes, now, width)
	if b.Full() {
		b.Trailer("... digest truncated; read more with cci tail or cci grep")
	}
}

func section(b *render.Budget, title string, records []store.Record, limit int, now time.Time, width int) {
	if len(records) == 0 {
		return
	}
	newest := slices.SortedFunc(slices.Values(records), func(x, y store.Record) int {
		return cmp.Compare(y.Seq, x.Seq)
	})
	if !b.Line(fmt.Sprintf("%s (%d, newest first):", title, len(records))) {
		return
	}
	for i, r := range newest {
		if i == limit {
			b.Line(fmt.Sprintf("  +%d more", len(newest)-limit))
			return
		}
		if !b.Line("  " + render.Clip(render.Line(r, now), width)) {
			return
		}
	}
}

func untracked(b *render.Budget, holds []store.Record) {
	if len(holds) == 0 {
		return
	}
	names := make([]string, 0, len(holds))
	for _, r := range slices.Backward(holds) {
		names = append(names, fmt.Sprintf("#%d %s", r.Seq, r.Lane))
	}
	if len(names) > 8 {
		names = append(names[:8], fmt.Sprintf("+%d more", len(holds)-8))
	}
	b.Line(fmt.Sprintf("untracked holds (%d; name stack:<project>/<env> or target:<name> to track them): %s", len(holds), strings.Join(names, ", ")))
}

func ranked(counts map[string]int, n int) string {
	keys := slices.SortedFunc(maps.Keys(counts), func(a, b string) int {
		return cmp.Or(cmp.Compare(counts[b], counts[a]), cmp.Compare(a, b))
	})
	parts := make([]string, 0, n+1)
	for i, k := range keys {
		if i == n {
			parts = append(parts, fmt.Sprintf("+%d more", len(keys)-n))
			break
		}
		parts = append(parts, fmt.Sprintf("%s %d", k, counts[k]))
	}
	if len(parts) == 0 {
		return "none"
	}
	return strings.Join(parts, ", ")
}
