package store

import (
	"bufio"
	"context"
	"errors"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
)

func incidentWords(ctx context.Context, input io.Reader) (map[string]bool, bool, error) {
	reader := bufio.NewReaderSize(input, 32*1024)
	words := map[string]bool{}
	var digits strings.Builder
	mention := false
	for n := 0; ; n++ {
		if n&4095 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		r, size, err := reader.ReadRune()
		if errors.Is(err, io.EOF) {
			if mention && digits.Len() != 0 {
				words[digits.String()] = true
			}
			return words, true, nil
		}
		if err != nil {
			return nil, false, err
		}
		if r == utf8.RuneError && size == 1 {
			return nil, false, nil
		}
		if mention {
			if r >= '0' && r <= '9' {
				digits.WriteRune(r)
				continue
			}
			if digits.Len() != 0 && !incidentWordRune(r) {
				words[digits.String()] = true
			}
			digits.Reset()
			mention = false
		}
		if r == '#' {
			mention = true
		}
	}
}

func incidentWordRune(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsNumber(r)
}
