import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(sys.argv[1])
assert subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip() == "23dcba01c1512bf876f6caf6e431cdc0f3731ec7"
items = [('internal/store/incident_watch_linux.go', 'c9db0261a925bd73bd2588bd985f3f510f16be1cb60cb87f40f87080d7077cd5', 'func (q *incidentInotify) Add(key incidentWatchKey) error {', 'func (q *incidentInotify) Add(key incidentWatchKey) error {\n\treturn q.addWithRegistration(key, unix.InotifyAddWatch)\n}\n\nfunc (q *incidentInotify) addWithRegistration(key incidentWatchKey, register func(int, string, uint32) (int, error)) error {', 'wd, err := unix.InotifyAddWatch(', 'wd, err := register('), ('internal/store/incident_watch_darwin.go', '85c2df68782a6b3fcf1d7f4d1558accf1b053932029bb69b3ffd4766a7bde929', 'func (q *incidentKqueue) Add(key incidentWatchKey) error {', 'func (q *incidentKqueue) Add(key incidentWatchKey) error {\n\treturn q.addWithRegistration(key, func(change unix.Kevent_t) error {\n\t\t_, err := unix.Kevent(q.fd, []unix.Kevent_t{change}, nil, nil)\n\t\treturn err\n\t})\n}\n\nfunc (q *incidentKqueue) addWithRegistration(key incidentWatchKey, register func(unix.Kevent_t) error) error {', 'if _, err := unix.Kevent(q.fd, []unix.Kevent_t{change}, nil, nil); err != nil {', 'if err := register(change); err != nil {')]
for relative, expected, header, replacement, before, after in items:
    path = root / relative
    original = path.read_text()
    assert hashlib.sha256(original.encode()).hexdigest() == expected
    assert original.count(header) == 1 and original.count(before) == 1
    patched = original.replace(header, replacement, 1).replace(before, after, 1)
    assert patched.replace(replacement, header, 1).replace(after, before, 1) == original
    path.write_text(patched)
    print(json.dumps({"file": relative, "original_sha256": expected, "seam_only_sha256": hashlib.sha256(patched.encode()).hexdigest(), "inverse_exact": True}))
