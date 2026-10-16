package store

import "regexp"

var secrets = []struct {
	pattern *regexp.Regexp
	marker  string
}{
	{regexp.MustCompile(`github_pat_[A-Za-z0-9_]{22,}`), "[redacted:github-token]"},
	{regexp.MustCompile(`gh[opsur]_[A-Za-z0-9_]{36,}`), "[redacted:github-token]"},
	{regexp.MustCompile(`bkua_[A-Za-z0-9]{40,}`), "[redacted:buildkite-token]"},
}

func redact(s string) string {
	for _, secret := range secrets {
		s = secret.pattern.ReplaceAllLiteralString(s, secret.marker)
	}
	return s
}
