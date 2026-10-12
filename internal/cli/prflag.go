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

type prsFlag struct{ prs *[]store.PR }

func (f *prsFlag) Set(s string) error {
	for _, part := range strings.Split(s, ",") {
		pr, err := store.ParsePR(part)
		if err != nil {
			return err
		}
		*f.prs = append(*f.prs, pr)
	}
	return nil
}

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
