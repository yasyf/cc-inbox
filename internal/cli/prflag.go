package cli

import (
	"strings"

	"github.com/yasyf/cc-inbox/internal/store"
)

type prFlag store.PR

func (f *prFlag) Set(s string) error {
	pr, err := store.ParsePR(s)
	*f = prFlag(pr)
	return err
}

func (f *prFlag) String() string {
	if f.Number == 0 {
		return ""
	}
	return store.PR(*f).String()
}

func (*prFlag) Type() string { return "pr" }

type prsFlag struct {
	prs   *[]store.PR
	named *[]string
	given []string
}

func (f *prsFlag) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		if f.named != nil && store.IsNamedRef(part) && !strings.HasPrefix(part, "pr#") {
			*f.named = append(*f.named, part)
			continue
		}
		pr, err := store.ParsePR(strings.TrimPrefix(part, "pr#"))
		if err != nil {
			return err
		}
		*f.prs = append(*f.prs, pr)
	}
	f.given = append(f.given, s)
	return nil
}

func (f *prsFlag) Given() []string { return f.given }

func (f *prsFlag) String() string {
	if len(*f.prs) == 0 {
		return ""
	}
	keys := make([]string, len(*f.prs))
	for i, pr := range *f.prs {
		keys[i] = pr.String()
	}
	return "[" + strings.Join(keys, ",") + "]"
}

func (*prsFlag) Type() string { return "prs" }

type headsFlag struct {
	heads *[]store.Head
	given []string
}

func (f *headsFlag) Set(s string) error {
	head, err := store.ParseHead(s)
	if err != nil {
		return err
	}
	*f.heads = append(*f.heads, head)
	f.given = append(f.given, s)
	return nil
}

func (f *headsFlag) Given() []string { return f.given }

func (f *headsFlag) String() string {
	if len(*f.heads) == 0 {
		return ""
	}
	keys := make([]string, len(*f.heads))
	for i, head := range *f.heads {
		keys[i] = head.PR.String() + "=" + head.Commit
	}
	return "[" + strings.Join(keys, ",") + "]"
}

func (*headsFlag) Type() string { return "head" }
