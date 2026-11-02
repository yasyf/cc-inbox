package cli_test

import "testing"

func TestNativeTailFixtureSeedTransactionCalls(t *testing.T) {
	fixture := newNativeTailFixture(t, nativeTailCall{}, nil)
	calls := fixture.seed
	t.Logf("NATIVE_TAIL_SEED_SUCCESSFUL_CALLS begin_tx=%d commit=%d unit=successful_Go_seed_API_returns SQLite_internal_commits=UNKNOWN CPU_gain=UNKNOWN", calls.begins, calls.commits)
	if calls.begins != 1 || calls.commits != 1 {
		t.Fatalf("NATIVE_TAIL_SEED_CALL_BOUND_EXCEEDED begin_tx=%d commit=%d want=1", calls.begins, calls.commits)
	}
}
