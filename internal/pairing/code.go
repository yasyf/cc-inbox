package pairing

import (
	"crypto/rand"
	"fmt"
	"strings"
)

const (
	codeAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	codeLen      = 8
)

func NewCode() (string, error) {
	b := make([]byte, codeLen)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("mint pairing code: %w", err)
	}
	for i, v := range b {
		b[i] = codeAlphabet[v&31]
	}
	return string(b), nil
}

func ParseCode(s string) (string, error) {
	norm := strings.NewReplacer("-", "", " ", "", "O", "0", "I", "1", "L", "1").Replace(strings.ToUpper(s))
	if len(norm) != codeLen || strings.Trim(norm, codeAlphabet) != "" {
		return "", fmt.Errorf("pairing code %q is not %d characters of Crockford base32 (ABCD-EFGH)", s, codeLen)
	}
	return norm, nil
}

func FormatCode(code string) string {
	return code[:codeLen/2] + "-" + code[codeLen/2:]
}

func FormatFingerprint(fp string) string {
	var groups []string
	for len(fp) > 4 {
		groups = append(groups, fp[:4])
		fp = fp[4:]
	}
	return strings.Join(append(groups, fp), " ")
}
