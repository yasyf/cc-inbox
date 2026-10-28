package store

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
)

var (
	prURL     = regexp.MustCompile(`^https://github\.com/([\w.-]+/[\w.-]+)/pull/(\d+)(?:[/?#]\S*)?$`)
	prRef     = regexp.MustCompile(`^([\w.-]+/[\w.-]+)#(\d+)$`)
	prMention = regexp.MustCompile(`#(\d+)\b`)
)

type PR struct {
	Repo   string
	Number int
}

func ParsePR(s string) (PR, error) {
	repo, num := "", s
	for _, re := range []*regexp.Regexp{prURL, prRef} {
		if m := re.FindStringSubmatch(s); m != nil {
			repo, num = m[1], m[2]
			break
		}
	}
	n, err := strconv.Atoi(num)
	if err != nil || n <= 0 {
		return PR{}, fmt.Errorf("pull request %q: want a number, owner/repo#N, or a GitHub pull request URL", s)
	}
	return PR{Repo: repo, Number: n}, nil
}

func (p PR) String() string {
	if p.Repo == "" {
		return strconv.Itoa(p.Number)
	}
	return fmt.Sprintf("%s#%d", p.Repo, p.Number)
}

func (p PR) textMatch(column string) (string, any) {
	if p.Repo != "" {
		return column + " = ?", p.String()
	}
	return column + " LIKE '%#' || ?", strconv.Itoa(p.Number)
}

func (p PR) value() any {
	if p.Repo == "" {
		return p.Number
	}
	return p.String()
}

func (p PR) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.value())
}

func (p *PR) UnmarshalJSON(b []byte) error {
	var n int
	if json.Unmarshal(b, &n) == nil {
		*p = PR{Number: n}
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	parsed, err := ParsePR(s)
	*p = parsed
	return err
}

func UnboundPRs(r Record) []int {
	var missing []int
	for _, m := range prMention.FindAllStringSubmatch(r.Topic+" "+r.Text, -1) {
		n, _ := strconv.Atoi(m[1])
		bound := slices.ContainsFunc(r.Refs.PRs, func(p PR) bool { return p.Number == n })
		if !bound && !slices.Contains(missing, n) {
			missing = append(missing, n)
		}
	}
	return missing
}
