package store

import (
	"fmt"
	"regexp"
	"strings"
)

const HeadAttr = "head:"

var sha = regexp.MustCompile(`^[0-9a-f]{7,40}$`)

type Head struct {
	PR     PR
	Commit string
	Was    string
	Note   string
}

func ParseHead(s string) (Head, error) {
	spec, note, _ := strings.Cut(strings.TrimSpace(s), " ")
	at := strings.LastIndex(spec, "=")
	if at < 0 {
		return Head{}, fmt.Errorf("head %q: want <pr>=<sha>, <pr>=<was>..<sha>, and an optional note after a space", s)
	}
	pr, err := ParsePR(spec[:at])
	if err != nil {
		return Head{}, fmt.Errorf("head %q: %w", s, err)
	}
	was, commit, moved := strings.Cut(strings.ToLower(spec[at+1:]), "..")
	if !moved {
		was, commit = "", was
	}
	if !sha.MatchString(commit) {
		return Head{}, fmt.Errorf("head #%d: commit %q is not a 7 to 40 character hex sha", pr.Number, commit)
	}
	if moved && !sha.MatchString(was) {
		return Head{}, fmt.Errorf("head #%d: was %q is not a 7 to 40 character hex sha", pr.Number, was)
	}
	return Head{PR: pr, Commit: commit, Was: was, Note: strings.TrimSpace(note)}, nil
}

func (h Head) Attr() (string, string) {
	value := h.Commit
	if h.Was != "" {
		value = h.Was + ".." + h.Commit
	}
	if h.Note != "" {
		value += " " + h.Note
	}
	return HeadAttr + h.PR.String(), value
}

func HeadsPreview(heads []Head) string {
	named := make([]string, len(heads))
	for i, h := range heads {
		named[i] = fmt.Sprintf("#%d %s", h.PR.Number, h.Commit[:7])
	}
	if len(heads) == 1 {
		return "HEAD " + named[0]
	}
	return fmt.Sprintf("HEAD %d PRs: %s", len(heads), strings.Join(named, ", "))
}
