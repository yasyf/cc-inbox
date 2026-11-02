from __future__ import annotations

import json
import re
import sys
from types import SimpleNamespace

ROW_ID = re.compile(r"^merge-row-(?P<pr>\d+)(?:-(?P<sha>[0-9a-f]{7,40}))?$")
REKEY = re.compile(r"\bre-?keys?\b|\bre-?keyed\b", re.IGNORECASE)
SHORT_SHA = re.compile(r"\b[0-9a-f]{7,40}\b")

def names_head(named: str, sha: str) -> bool:
    return bool(sha) and (sha.startswith(named) or named.startswith(sha))

grades = SimpleNamespace(names_head=names_head, SHORT_SHA=SHORT_SHA)

def rekeyed(rulings: list[dict], row: Row) -> dict | None:
    """The newest root record that re-keys a row and names `row`'s id, or its PR with its head or id's short sha."""
    named = ROW_ID.match(row.id)
    keyed = (named["sha"] or "") if named else ""
    shas = [sha for sha in (row.head, keyed) if sha]

    def names(text: str) -> bool:
        if named and row.id in text:
            return True
        if not re.search(rf"#{row.pr}\b|\bmerge-row-{row.pr}\b", text):
            return False
        return any(grades.names_head(named, sha) for named in grades.SHORT_SHA.findall(text) for sha in shas)

    return max((record for record in rulings if REKEY.search(record["full"]) and names(record["full"])), key=lambda record: record["seq"], default=None)

answers = []
for case in json.load(sys.stdin):
    named = ROW_ID.match(case["row"])
    assert named is not None and named["pr"] == case["pr"]
    row = SimpleNamespace(id=case["row"], pr=named["pr"], head="abcdef0123456789")
    record = {"seq": 1, "full": case["text"]}
    found = rekeyed([record], row)
    answers.append(found is not None and found["seq"] == 1)
json.dump(answers, sys.stdout)
