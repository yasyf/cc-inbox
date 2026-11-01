import json
import os
from pathlib import Path
import sys


phase = sys.argv[1]
assert phase in {"old", "current"}
paths = sorted(Path(".ci-timestamp-range-native-oracle").glob(f"*/*/results-{phase}.json"))
assert len(paths) == 70, (phase, len(paths))
profiles = {}
compared = 0
for path in paths:
    assert path.stat().st_uid == os.getuid(), path
    payload = json.loads(path.read_text())
    assert set(payload) == {"Variant", "Filters", "Results"}, path
    assert isinstance(payload["Variant"], str), path
    assert len(payload["Filters"]) == 10, path
    assert all(isinstance(f, dict) for f in payload["Filters"]), path
    assert len(payload["Results"]) == 2, path
    for stage in payload["Results"]:
        assert len(stage) == 10, path
        for result in stage:
            assert set(result) == {"Rows", "QueryErr", "Count", "CountErr", "QuerySQL", "QueryArgs", "CountSQL", "CountArgs"}, path
            assert all(isinstance(result[key], str) for key in ("Rows", "QueryErr", "CountErr", "QuerySQL", "CountSQL")), path
            assert isinstance(result["Count"], int), path
            assert isinstance(result["QueryArgs"], list), path
            assert isinstance(result["CountArgs"], list), path
    profile = path.parent.parent.name
    profiles[profile] = profiles.get(profile, 0) + 1
    if phase == "current":
        old_path = path.with_name("results-old.json")
        assert old_path.stat().st_uid == os.getuid(), old_path
        old = json.loads(old_path.read_text())
        assert old["Variant"] == payload["Variant"], path
        assert old["Filters"] == payload["Filters"], path
        for old_stage, stage in zip(old["Results"], payload["Results"], strict=True):
            for previous, current in zip(old_stage, stage, strict=True):
                for key in ("Rows", "QueryErr", "Count", "CountErr", "QuerySQL", "QueryArgs"):
                    assert previous[key] == current[key], (path, key, previous[key], current[key])
                compared += 1
assert profiles == {"at": 35, "drive-at": 35}, profiles
if phase == "current":
    assert compared == 1400, compared
print(f"COUNT_TIMESTAMP_ORACLE_READBACK phase={phase} files={len(paths)} compared={compared} owned=true")
