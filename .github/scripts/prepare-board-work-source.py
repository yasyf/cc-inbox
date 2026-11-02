import base64
import json
import hashlib
from pathlib import Path
import re
import subprocess
import sys

root = Path(sys.argv[1])
bundle = json.loads(Path(sys.argv[2]).read_text())

def formatted(source):
    return subprocess.run(["gofmt"], input=source, text=True, capture_output=True, check=True).stdout

def functions(source):
    found = {}
    for match in re.finditer(r"^func (?:\(([^)]+)\) )?([A-Za-z_]\w*)\(", source, re.M):
        end = source.index("\n}\n", match.start()) + 3
        receiver = match.group(1)
        key = ((receiver.split()[-1].lstrip("*") + ".") if receiver else "") + match.group(2)
        found[key] = source[match.start():end]
    return found

for item in bundle["files"]:
    path = root / item["path"]
    current = formatted(path.read_text())
    for change in item["functions"]:
        preimage = base64.b64decode(change["preimage_b64"], validate=True).decode()
        postimage = base64.b64decode(change["postimage_b64"], validate=True).decode()
        assert hashlib.sha256(preimage.encode()).hexdigest() == change["preimage_sha256"]
        assert hashlib.sha256(postimage.encode()).hexdigest() == change["postimage_sha256"]
        expected = functions(formatted("package p\n" + preimage))[change["symbol"]]
        actual = functions(current)[change["symbol"]]
        assert actual == expected, (item["path"], change["symbol"])
        replacement = functions(formatted("package p\n" + postimage))[change["symbol"]]
        assert current.count(actual) == 1
        current = current.replace(actual, replacement, 1)
    for imported in item["extra_imports"]:
        line = '"' + imported + '"'
        if line not in current:
            at = current.index("import (") + len("import (")
            current = current[:at] + "\n\t" + line + current[at:]
    path.write_text(formatted(current))
for item in bundle["new_files"]:
    path = root / item["path"]
    assert not path.exists(), item["path"]
    raw = base64.b64decode(item["source_b64"], validate=True)
    assert hashlib.sha256(raw).hexdigest() == item["sha256"]
    path.write_bytes(raw)
