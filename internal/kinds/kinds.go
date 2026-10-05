package kinds

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

type Kind string

const (
	Go         Kind = "go"
	Owner      Kind = "owner"
	Decide     Kind = "decide"
	Decision   Kind = "decision"
	Ask        Kind = "ask"
	Answer     Kind = "answer"
	Hold       Kind = "hold"
	Lift       Kind = "lift"
	Incident   Kind = "incident"
	Mechanism  Kind = "mechanism"
	FixLive    Kind = "fix-live"
	Defect     Kind = "defect"
	Correction Kind = "correction"
	Opened     Kind = "opened"
	Landed     Kind = "landed"
	Release    Kind = "release"
	Applied    Kind = "applied"
	Handoff    Kind = "handoff"
	Done       Kind = "done"
	Head       Kind = "head"
	Contract   Kind = "contract"
	Blocker    Kind = "blocker"
	Withdraw   Kind = "withdraw"
	Digest     Kind = "digest"
	Claim      Kind = "claim"
	Note       Kind = "note"
	State      Kind = "state"
	Matrix     Kind = "matrix"
	Report     Kind = "report"
	Blocked    Kind = "blocked"
	Unblock    Kind = "unblock"
	Review     Kind = "review"
	Retro      Kind = "retro"
	Posted     Kind = "posted"
	Design     Kind = "design"
	Serving    Kind = "serving"
	Ready      Kind = "ready"
	Duplicate  Kind = "duplicate"
	StandDown  Kind = "stand-down"
	NotOurs    Kind = "not-ours"
	Stopped    Kind = "stopped"
	Refuse     Kind = "refuse"
	Fail       Kind = "fail"
	Recovered  Kind = "recovered"
	DeleteList Kind = "delete-list"
	Skew       Kind = "skew"
	NotLive    Kind = "not-live"
	Urgent     Kind = "urgent"
)

const EphemeralTTL = 24 * time.Hour

type Spec struct {
	TTL      time.Duration
	NeedsPR  bool
	NeedsRef bool
	Internal bool
}

var specs = map[Kind]Spec{
	Go:         {},
	Owner:      {},
	Decide:     {},
	Decision:   {},
	Ask:        {},
	Answer:     {NeedsRef: true},
	Hold:       {},
	Lift:       {NeedsRef: true},
	Incident:   {},
	Mechanism:  {},
	FixLive:    {},
	Defect:     {},
	Correction: {},
	Opened:     {NeedsPR: true},
	Landed:     {NeedsPR: true},
	Release:    {},
	Applied:    {},
	Handoff:    {},
	Done:       {},
	Head:       {},
	Contract:   {},
	Blocker:    {},
	Withdraw:   {NeedsRef: true},
	Digest:     {Internal: true},
	Claim:      {TTL: EphemeralTTL},
	Note:       {TTL: EphemeralTTL},
	State:      {TTL: EphemeralTTL},
	Matrix:     {TTL: EphemeralTTL},
	Report:     {TTL: EphemeralTTL},
	Blocked:    {},
	Unblock:    {NeedsRef: true},
	Review:     {},
	Retro:      {},
	Posted:     {},
	Design:     {},
	Serving:    {},
	Ready:      {},
	Duplicate:  {},
	StandDown:  {},
	NotOurs:    {},
	Stopped:    {},
	Refuse:     {},
	Fail:       {},
	Recovered:  {},
	DeleteList: {},
	Skew:       {},
	NotLive:    {},
	Urgent:     {},
}

var aliases = map[string]Kind{
	"released":      Release,
	"merged":        Landed,
	"outcome":       Report,
	"registered":    State,
	"reclaim":       Note,
	"launch-failed": Defect,
	"fixlive":       FixLive,
	"fix_live":      FixLive,
	"open":          Opened,
	"land":          Landed,
	"apply":         Applied,
	"ruling":        Owner,
	"refused":       Refuse,
	"failed":        Fail,
}

var openers = map[Kind][]Kind{
	Ask:      {Answer, Go, Withdraw},
	Decide:   {Answer, Go, Withdraw},
	Hold:     {Lift},
	Incident: {FixLive, Done},
	Blocker:  {Withdraw, Answer, Done},
	Blocked:  {Unblock},
	Defect:   {FixLive, Done},
}

func Parse(s string) (Kind, error) {
	k := Kind(s)
	if _, ok := specs[k]; ok {
		return k, nil
	}
	return "", fmt.Errorf("unknown kind %q (known: %s)", s, strings.Join(Names(), ", "))
}

func Lookup(word string) (Kind, bool) {
	w := strings.ToLower(word)
	if _, ok := specs[Kind(w)]; ok {
		return Kind(w), true
	}
	k, ok := aliases[w]
	return k, ok
}

func (k Kind) Spec() Spec {
	return specs[k]
}

func (k Kind) Opens() bool {
	_, ok := openers[k]
	return ok
}

func (k Kind) Closes(opener Kind) bool {
	return slices.Contains(openers[opener], k)
}

func Openers() []Kind {
	out := make([]Kind, 0, len(openers))
	for k := range openers {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func Closers() []Kind {
	var out []Kind
	for _, cs := range openers {
		for _, c := range cs {
			if !slices.Contains(out, c) {
				out = append(out, c)
			}
		}
	}
	slices.Sort(out)
	return out
}

func Names() []string {
	out := make([]string, 0, len(specs))
	for k := range specs {
		out = append(out, string(k))
	}
	slices.Sort(out)
	return out
}
