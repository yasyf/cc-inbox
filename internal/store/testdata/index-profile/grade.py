import json
import re
import sys
from pathlib import Path

label, raw_path, exit_text = sys.argv[1:]
status = int(exit_text)
raw = Path(raw_path).read_text()
assert label in {"f16b", "current"}
assert status in {0, 1}, (label, status)
records = []
for line in raw.splitlines():
    if line.startswith("CCI_INDEX_PROFILE "):
        record = json.loads(line.removeprefix("CCI_INDEX_PROFILE "))
        assert record["label"] == label, record
        assert record["elapsed_ns"] >= 0 and record["at_ns"] > 0, record
        records.append(record)

def one(phase, name):
    matches = [record for record in records if (record["phase"], record["object"]) == (phase, name)]
    assert len(matches) == 1, (label, phase, name, matches)
    return matches[0]

for phase, name in [
    ("LockOpen", "init.lock"), ("LockAcquire", "init.lock"),
    ("LockRelease", "init.lock"), ("LockHeld", "init.lock"),
    ("HandleOpen", "sqlite"), ("HandleClose", "sqlite"),
    ("Begin", "maintenance"), ("Commit", "maintenance"),
    ("Transaction", "maintenance"), ("Total", "maintenance"),
    ("Create", "record_closer_keys"), ("Index", "record_closer_keys_lookup"),
    ("Trigger", "record_closer_keys_insert"),
    ("Trigger", "record_closer_keys_update"),
    ("Trigger", "record_closer_keys_delete"),
    ("StatsAggregate", "before"), ("StatsAggregate", "after"),
]:
    one(phase, name)
assert one("Validate", "user_version")["value"] > 0
assert one("Validate", "physical_indexes")["value"] == 0
assert one("Validate", "record_membership")["installed"] is True
assert one("Validate", "closer_membership")["installed"] is False
assert one("Validate", "incident_projection")["installed"] is True
if label == "current":
    assert one("Validate", "root_audience")["installed"] is True
else:
    assert not any(record["object"] == "root_audience" for record in records)
for name in ["page_count", "page_size", "freelist_count"]:
    values = [record["value"] for record in records if (record["phase"], record["object"]) == ("Stats", name)]
    assert len(values) == 2 and all(value >= 0 for value in values), (name, values)
for name in ["database", "wal"]:
    values = [record for record in records if record["phase"] in {"FileStat", "FileAbsent"} and record["object"] == name]
    assert len(values) == 2, (name, values)
    assert all(record["phase"] == "FileAbsent" or record["value"] >= 0 for record in values)
fill = one("Populate", "record_closer_keys")
if "rows_affected" in fill:
    assert fill["rows_affected"] == 1000000, fill
else:
    assert fill.get("rows_affected_error"), fill
observed_keys = one("Observe", "record_closer_keys")["value"]
observed_records = one("Observe", "records")["value"]
assert observed_keys == 1000000
assert observed_records == 250000
budget = one("Budget", "maintenance")["elapsed_ns"]
assert one("Total", "maintenance")["elapsed_ns"] == budget
assert not any(record["phase"] == "Rollback" for record in records)
objects = [(record["phase"], record["object"]) for record in records if record["phase"] in {"Create", "Populate", "Index", "Trigger"}]
assert objects == [
    ("Create", "record_closer_keys"), ("Populate", "record_closer_keys"),
    ("Index", "record_closer_keys_lookup"), ("Trigger", "record_closer_keys_insert"),
    ("Trigger", "record_closer_keys_update"), ("Trigger", "record_closer_keys_delete"),
], objects
for population in [32, 10000]:
    assert re.search(r"--- PASS: TestSelectedClosureProvisioningPhaseProfile/old-writer-" + str(population) + r" \(", raw)
assert not re.search(r"--- SKIP:", raw)
failures = re.findall(r"^\s*--- FAIL: (\S+) \(", raw, re.MULTILINE)
if status:
    assert failures == ["TestSelectedClosureProvisioningPhaseProfile"], failures
    assert budget >= 5000000000, budget
    assert re.search(r"new closer projection writer budget: records=250000 report=.* error=<nil>", raw)
    grade = "OBSERVED_PROFILE_BUDGET_FAILURE"
else:
    assert not failures
    assert budget < 5000000000, budget
    assert re.search(r"^--- PASS: TestSelectedClosureProvisioningPhaseProfile \(", raw, re.MULTILINE)
    grade = "OBSERVED_PROFILE_WITHIN_BUDGET"
print("CCI_INDEX_PROFILE_GRADE " + json.dumps({
    "label": label, "diagnostic_status": grade, "exit_code": status,
    "profile_elapsed_ns": budget, "records": observed_records, "observed_keys": observed_keys,
    "populate_rows_affected": fill.get("rows_affected"),
    "populate_rows_affected_error": fill.get("rows_affected_error"),
    "phase_records": records, "required_uninstrumented_positive": "SEPARATE_AND_MANDATORY",
    "vm_steps": None, "visited_rows": None, "physical_io": None,
}, sort_keys=True))
