import base64
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
assert root.is_dir() and not any(root.iterdir())
cli = json.loads(Path(sys.argv[2]).read_text())
controls = json.loads(Path(sys.argv[3]).read_text())
for group, prefix in ((cli["common"], "scripts"), (cli["candidate"], "scripts"), (controls["files"], "")):
    for name, image in group.items():
        raw = base64.b64decode(image["source_b64"], validate=True)
        assert hashlib.sha256(raw).hexdigest() == image["sha256"], name
        path = root / prefix / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(raw)
