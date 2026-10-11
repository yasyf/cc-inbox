package pairing

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
)

func rosterStatement(mesh string, roster []keyRef) []byte {
	sorted := slices.SortedFunc(slices.Values(roster), func(a, b keyRef) int { return strings.Compare(a.ID, b.ID) })
	var sb strings.Builder
	fmt.Fprintf(&sb, "cci-mesh/roster/v1\n%s\n", mesh)
	for _, k := range sorted {
		fmt.Fprintf(&sb, "%s %s\n", k.ID, b64(k.Pub))
	}
	return []byte(sb.String())
}

func CanonicalAllow(a Allow) string {
	return a.Label + "|" + strings.Join(a.Drives, ",") + "|" + strings.Join(a.Kinds, ",")
}

func ParseAllow(spec, label string) (Allow, error) {
	drives, kinds, ok := strings.Cut(spec, "|")
	if !ok || drives == "" || kinds == "" {
		return Allow{}, fmt.Errorf("--allow %q is not <drives>|<kinds>, e.g. 'page:*|oncall.say,oncall.report'", spec)
	}
	a := Allow{Drives: strings.Split(drives, ","), Kinds: strings.Split(kinds, ","), Label: label}
	for _, g := range slices.Concat(a.Drives, a.Kinds) {
		if g == "" || strings.Contains(strings.TrimSuffix(g, "*"), "*") {
			return Allow{}, fmt.Errorf("--allow glob %q: globs are exact names or end in a single *", g)
		}
	}
	return a, nil
}

func b64(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}
