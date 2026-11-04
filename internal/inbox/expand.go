package inbox

import (
	"context"
	"maps"
	"regexp"
	"slices"
	"strings"

	"github.com/yasyf/cc-inbox/internal/store"
)

var wordsToken = regexp.MustCompile(`\b(?:owner:m\d+|slack:\d+\.\d+)\b`)

type idTextReader interface {
	TextsByID(context.Context, string, []string) (map[string]string, error)
}

// Expand follows the first owner:m<N> or slack:<ts> token of each id in a
// record's text with the quoted text of the record that carries that id, so
// the bus carries the id and the reader still gets the owner's words.
func Expand(ctx context.Context, st idTextReader, records []store.Record) error {
	wanted := map[string][]string{}
	for _, r := range records {
		if !quotesWords(r.Text) {
			continue
		}
		if tokens := wordsToken.FindAllString(r.Text, -1); len(tokens) > 0 {
			wanted[r.Drive] = append(wanted[r.Drive], tokens...)
		}
	}
	if len(wanted) == 0 {
		return nil
	}
	texts := map[string]map[string]string{}
	for _, drive := range slices.Sorted(maps.Keys(wanted)) {
		found, err := st.TextsByID(ctx, drive, slices.Compact(slices.Sorted(slices.Values(wanted[drive]))))
		if err != nil {
			return err
		}
		texts[drive] = found
	}
	for i, r := range records {
		if !quotesWords(r.Text) {
			continue
		}
		expanded := map[string]bool{}
		records[i].Text = wordsToken.ReplaceAllStringFunc(r.Text, func(token string) string {
			words, ok := texts[r.Drive][token]
			if !ok || expanded[token] || token == r.ID {
				return token
			}
			expanded[token] = true
			return token + ` "` + words + `"`
		})
	}
	return nil
}

func quotesWords(text string) bool {
	return strings.Contains(text, "owner:m") || strings.Contains(text, "slack:")
}
