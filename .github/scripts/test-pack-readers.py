import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


CCI = str(Path(os.environ["CCI_BIN"]).resolve())
sys.path.insert(0, os.environ["PACK_SCRIPTS"])
cci = importlib.import_module("long_running.cci")

DRIVE = "pack-readers"
POSTED = 240
KINDS = ("review", "head", "note")


class PackReadersTest(unittest.TestCase):
    """The long-running pack's cci readers, `cci.paged` and the board gate's history read, run against this commit's cci and read every record in order (cc-inbox#250 broke the pager for the pack and #262 fixed it)."""

    def setUp(self):
        self.home = tempfile.TemporaryDirectory()
        self.addCleanup(self.home.cleanup)
        self.env = dict(os.environ, CCI_HOME=self.home.name, CLAUDE_CODE_SESSION_ID="pack-readers")
        self.posted = {kind: [] for kind in KINDS}
        self.post_many(POSTED)
        cci.PAGE_BYTES = 16_000

    def cci(self, argv):
        done = subprocess.run([CCI, *argv[1:]], capture_output=True, text=True, env=self.env, timeout=120)
        if done.returncode:
            raise AssertionError(f"{' '.join(argv)} exited {done.returncode}: {done.stderr}")
        return done.stdout

    def post_many(self, count):
        for _ in range(count):
            step = sum(map(len, self.posted.values()))
            kind = KINDS[step % len(KINDS)]
            out = self.cci(["cci", "post", "--drive", DRIVE, "--lane", f"lane-{step % 7}", "--kind", kind, "--pr", str(100 + step % 5), "--json", "--text", f"step {step:04d} {'x' * 60}"])
            self.posted[kind].append(json.loads(out)["seq"])

    def seqs(self, records):
        return [record["seq"] for record in records]

    def test_the_pager_reads_every_record_in_order_across_budget_cut_pages(self):
        found = cci.paged(self.cci, DRIVE, (), 0)

        self.assertEqual(self.seqs(found), sorted(seq for seqs in self.posted.values() for seq in seqs))

    def test_the_board_gate_history_read_follows_new_records_after_its_first_read(self):
        tail = cci.Tail(self.cci)
        before = sorted(self.posted["review"] + self.posted["head"])
        first = cci.records(tail, DRIVE, "--kind=review", "--kind=head")
        self.post_many(30)
        then = cci.records(tail, DRIVE, "--kind=review", "--kind=head")
        one_pr = cci.records(tail, DRIVE, "--kind=review", "--kind=head", "--pr", "102")

        self.assertEqual(self.seqs(first), before)
        self.assertEqual(self.seqs(then), sorted(self.posted["review"] + self.posted["head"]))
        self.assertEqual(self.seqs(one_pr), self.seqs(cci.paged(self.cci, DRIVE, ("--kind=review", "--kind=head", "--pr", "102"), 0)))
        self.assertTrue(one_pr)


if __name__ == "__main__":
    unittest.main()
