import json
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
summaries = {}
old_work = {}
for population in (32, 10000, 100000):
    for phase in ("warm", "changed"):
        raw = (root / f"board-real-cli-{population}-{phase}.log").read_text()
        assert not re.search(r"^--- (?:FAIL|SKIP):", raw, re.M)
        assert len(re.findall(r"^--- PASS: TestBoardRealFreshCLIWarmChanged ", raw, re.M)) == 1
        points = [json.loads(value) for value in re.findall(r"BOARD_REAL_CLI_POINT (\{[^\n]+\})", raw)]
        assert len(points) == 32 and {point["iteration"] for point in points} == set(range(32))
        exits = re.findall(r"BOARD_REAL_CLI_EXIT population=(\d+) phase=(warm|changed) iteration=(\d+) variant=(retained845|candidate) pid=(\d+) code=0 cci_receipts=(\d+)", raw)
        assert len(exits) == 64
        for variant in ("retained845", "candidate"):
            selected = [row for row in exits if row[3] == variant]
            assert len(selected) == 32 and len({row[4] for row in selected}) == 32
            assert {int(row[2]) for row in selected} == set(range(32))
            assert all(int(row[0]) == population and row[1] == phase for row in selected)
        for point in points:
            assert point["population"] == population and point["phase"] == phase
            assert point["old_target_query_successes"] > 0 and point["current_target_query_successes"] == 0
            assert point["old_residual_query_successes"] == point["current_residual_query_successes"]
            assert point["server"]["tracked_query_context_successes"] > 0
            assert point["server"]["body_read_failures"] == 0
        found = [json.loads(value) for value in re.findall(r"BOARD_REAL_CLI_SUMMARY (\{[^\n]+\})", raw)]
        assert len(found) == 1
        summary = found[0]
        assert summary["population"] == population and summary["phase"] == phase
        assert summary["actual_fresh_current_CLI"] == 32 and summary["actual_fresh_reference_CLI"] == 32
        assert summary["physical_install"]["tracked_exec_context_successes"] > 0
        assert summary["physical_install"]["tracked_commit_successes"] == 1
        assert summary["cold"]["body_read_successes"] == 2 and summary["cold"]["tracked_commit_successes"] == 1
        summaries[population, phase] = summary
        old_work[population, phase] = points[0]["old_target_query_successes"]
for phase in ("warm", "changed"):
    selected = [summaries[population, phase] for population in (32, 10000, 100000)]
    for key in ("tracked_query_context_successes", "tracked_exec_context_successes", "tracked_commit_successes", "body_read_successes"):
        assert len({row["server_first"][key] for row in selected}) == 1, (phase, key)
    for key in ("tracked_query_context_successes", "tracked_exec_context_successes"):
        values = [row["cold"][key] for row in selected]
        difference = values[1] - values[0]
        assert difference > 0 and difference % 9968 == 0, (phase, key, values)
        assert values[2] == values[0] + (difference // 9968) * 99968, (phase, key, values)
for phase in ("warm", "changed"):
    assert old_work[32, phase] < old_work[10000, phase] < old_work[100000, phase]
    assert summaries[100000, phase]["server_first"]["tracked_query_context_successes"] < old_work[100000, phase]
for population in (32, 10000, 100000):
    assert summaries[population, "warm"]["cold"] == summaries[population, "changed"]["cold"]
    assert summaries[population, "warm"]["physical_install"] == summaries[population, "changed"]["physical_install"]
print("BOARD_REAL_FRESH_CLI_MATRIX_PASS actual_current=192 actual_reference=192 populations=32,10000,100000 phases=warm,changed residual_tail_preserved=true")
