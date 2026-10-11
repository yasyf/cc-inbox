package pairing

import (
	"strings"
	"testing"
)

func TestCodeRoundTripsThroughItsDisplayForm(t *testing.T) {
	seen := map[string]bool{}
	for range 64 {
		code, err := NewCode()
		if err != nil {
			t.Fatal(err)
		}
		seen[code] = true
		got, err := ParseCode(strings.ToLower(FormatCode(code)))
		if err != nil || got != code {
			t.Fatalf("ParseCode(%q) = %q, %v; want %q", FormatCode(code), got, err, code)
		}
	}
	if len(seen) < 60 {
		t.Fatalf("64 codes produced only %d distinct values", len(seen))
	}
}

func TestParseCodeFoldsLookalikesAndRejectsJunk(t *testing.T) {
	if got, err := ParseCode("abcd-efgo"); err != nil || got != "ABCDEFG0" {
		t.Fatalf("ParseCode folded O to %q, %v", got, err)
	}
	if got, err := ParseCode("il00 0000"); err != nil || got != "11000000" {
		t.Fatalf("ParseCode folded I and L to %q, %v", got, err)
	}
	for _, bad := range []string{"ABCD-EFG", "ABCD-EFGHJ", "ABCD-EFGU"} {
		if _, err := ParseCode(bad); err == nil {
			t.Fatalf("ParseCode(%q) accepted a malformed code", bad)
		}
	}
}

func TestFormatFingerprintGroupsInFours(t *testing.T) {
	if got := FormatFingerprint("0123456789abcdef01"); got != "0123 4567 89ab cdef 01" {
		t.Fatalf("FormatFingerprint = %q", got)
	}
}

func TestParseAllowBuildsTheCanonicalScope(t *testing.T) {
	a, err := ParseAllow("page:*|oncall.say,oncall.report", "ops@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if got := CanonicalAllow(a); got != "ops@example.com|page:*|oncall.say,oncall.report" {
		t.Fatalf("CanonicalAllow = %q", got)
	}
	for _, bad := range []string{"page:*", "|oncall.say", "pa*ge|x", "page:*|"} {
		if _, err := ParseAllow(bad, "x"); err == nil {
			t.Fatalf("ParseAllow(%q) accepted a malformed scope", bad)
		}
	}
}
