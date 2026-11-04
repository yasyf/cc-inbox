package store_test

import (
	"strings"
	"testing"

	"github.com/yasyf/cc-inbox/internal/store"
)

func TestCredentialShapesMatchTokensAndNotTheirNeighbors(t *testing.T) {
	for _, tt := range []struct{ text, shape string }{
		{"gho_" + strings.Repeat("a", 36), "a GitHub token"},
		{"github_" + "pat_" + strings.Repeat("a", 22), "a GitHub fine-grained token"},
		{"sk-" + strings.Repeat("a", 20), "an sk- API key"},
		{"xox" + "b-" + strings.Repeat("1", 10), "a Slack token"},
		{"AS" + "IA" + strings.Repeat("A1B2", 4), "an AWS access key id"},
		{"AK" + "IA" + strings.Repeat("A1B2", 4), "an AWS access key id"},
		{"AWS_ACCESS_KEY_ID", ""},
		{"TEMP_EXECUTOR_AUTH_TOKEN", ""},
		{"gho_", ""},
		{"a risk-based-rollout-with-a-long-name", ""},
		{"AK" + "IA" + "short", ""},
	} {
		got, found := store.CredentialShape([]byte("said " + tt.text + " here"))
		if got != tt.shape || found != (tt.shape != "") {
			t.Errorf("CredentialShape(%q) = %q, %v; want %q", tt.text, got, found, tt.shape)
		}
	}
}

func TestWithoutCredentialsKeepsTheWordsAroundTheValue(t *testing.T) {
	value := "AS" + "IA" + strings.Repeat("A1B2", 4)
	if got, want := store.WithoutCredentials("the key "+value+" leaked"), "the key <an AWS access key id withheld> leaked"; got != want {
		t.Errorf("WithoutCredentials = %q, want %q", got, want)
	}
}
