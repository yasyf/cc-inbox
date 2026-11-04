package store

import "regexp"

var credentialShapes = []struct {
	name    string
	pattern *regexp.Regexp
}{
	{"a GitHub token", regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{20,}\b`)},
	{"a GitHub fine-grained token", regexp.MustCompile(`\bgithub_pat_\w{20,}\b`)},
	{"an sk- API key", regexp.MustCompile(`\bsk-[\w-]{20,}\b`)},
	{"a Slack token", regexp.MustCompile(`\bxox[abprs]-[\w-]{10,}\b`)},
	{"an AWS access key id", regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)},
}

func CredentialShape(text []byte) (string, bool) {
	for _, shape := range credentialShapes {
		if shape.pattern.Match(text) {
			return shape.name, true
		}
	}
	return "", false
}

func WithoutCredentials(text string) string {
	for _, shape := range credentialShapes {
		text = shape.pattern.ReplaceAllLiteralString(text, "<"+shape.name+" withheld>")
	}
	return text
}
