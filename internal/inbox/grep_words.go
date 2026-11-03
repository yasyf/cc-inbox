package inbox

import (
	"regexp/syntax"
	"slices"
	"strings"
)

const minRequiredWord = 3

// renderedWords are what a record's line says that no column holds, so a word
// inside one of them can match a record whose columns never hold it.
var renderedWords = []string{"origin", "stack", "target", "resolves", "ccn", "build", "board", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"}

// requiredWords returns lower-case words of which a line that pattern matches
// must hold at least one, nil when no such words are known. A word runs over
// letters, digits, '-' and '.', none of which a line puts between two columns,
// so it sits whole in one column.
func requiredWords(pattern string) []string {
	re, err := syntax.Parse(pattern, syntax.Perl)
	if err != nil {
		return nil
	}
	return required(re.Simplify())
}

func required(re *syntax.Regexp) []string {
	switch re.Op {
	case syntax.OpLiteral:
		if word := longestWord(re.Rune); word != "" {
			return []string{word}
		}
	case syntax.OpCapture, syntax.OpPlus:
		return required(re.Sub[0])
	case syntax.OpConcat:
		var best []string
		for _, sub := range re.Sub {
			if words := required(sub); stronger(words, best) {
				best = words
			}
		}
		return best
	case syntax.OpAlternate:
		var all []string
		for _, sub := range re.Sub {
			words := required(sub)
			if words == nil {
				return nil
			}
			all = append(all, words...)
		}
		return all
	}
	return nil
}

func stronger(words, than []string) bool {
	if len(words) == 0 {
		return false
	}
	if len(than) == 0 {
		return true
	}
	shortest := func(ws []string) int {
		return len(slices.MinFunc(ws, func(a, b string) int { return len(a) - len(b) }))
	}
	return shortest(words) > shortest(than)
}

func longestWord(literal []rune) string {
	lowered := strings.ToLower(string(literal))
	words := strings.FieldsFunc(lowered, func(c rune) bool {
		return !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.')
	})
	best := ""
	for _, word := range words {
		if len(word) > len(best) && len(word) >= minRequiredWord && !slices.ContainsFunc(renderedWords, func(rendered string) bool { return strings.Contains(rendered, word) }) {
			best = word
		}
	}
	return best
}
