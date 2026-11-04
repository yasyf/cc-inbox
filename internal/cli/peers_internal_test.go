package cli

import (
	"context"
	"maps"
	"testing"

	"github.com/yasyf/cc-inbox/internal/peers"
)

func TestCallerIdentityIsTheCallersEnvironment(t *testing.T) {
	name := peers.IdentityEnv[0]
	t.Setenv(name, "this process")
	if got, want := callerIdentity(context.Background()), (map[string]string{name: "this process"}); !maps.Equal(got, want) {
		t.Errorf("in its own process = %v, want %v", got, want)
	}
	ctx := context.WithValue(context.Background(), invocationKey{}, &invocation{env: map[string]string{name: "the caller"}})
	if got, want := callerIdentity(ctx), (map[string]string{name: "the caller"}); !maps.Equal(got, want) {
		t.Errorf("for a daemon's caller = %v, want %v", got, want)
	}
	ctx = context.WithValue(context.Background(), invocationKey{}, &invocation{env: map[string]string{}})
	if got := callerIdentity(ctx); len(got) != 0 {
		t.Errorf("for a caller with none = %v, want none", got)
	}
}
