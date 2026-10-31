import json
import os
from pathlib import Path
import runpy
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[2]
load_workflow = runpy.run_path(str(ROOT / ".github/scripts/test-native-release-gates.py"))["load_workflow"]


class ReleaseMainSyncTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        workflow = load_workflow("release.yml")
        steps = workflow["jobs"]["descriptor"]["steps"]
        cls.move = next(step for step in steps if step.get("id") == "commit")
        cls.upload = next(step["run"] for step in steps if step.get("name") == "Upload descriptor to release")
        cls.comparator = str(Path(os.environ["RELEASE_VERSION_COMPARE"]).resolve())

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.case_number = 0
        self.git_env = dict(os.environ, GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)

    def git(self, repo, *args):
        return subprocess.check_output(["git", *map(str, args)], cwd=repo, env=self.git_env, text=True, stderr=subprocess.PIPE, timeout=30).strip()

    def manifest(self, repo):
        return repo / "plugin/.claude-plugin/plugin.json"

    def descriptor(self, repo):
        return repo / "plugin/bin/cci.binrun"

    def commit(self, repo, message):
        self.git(repo, "add", "-A")
        self.git(repo, "commit", "-m", message)
        return self.git(repo, "rev-parse", "HEAD")

    def prepare(self, target, current, current_payload=None, source_matches=False):
        case = self.base / str(self.case_number)
        self.case_number += 1
        case.mkdir()
        origin, seed, actor = (case / name for name in ["origin.git", "seed", "actor"])
        self.git(case, "init", "--bare", "--initial-branch=main", origin)
        self.git(case, "init", "--initial-branch=main", seed)
        self.git(seed, "config", "user.name", "Fixture")
        self.git(seed, "config", "user.email", "fixture@example.com")
        self.manifest(seed).parent.mkdir(parents=True)
        self.descriptor(seed).parent.mkdir(parents=True)
        official = f"official descriptor {target}\n"
        self.manifest(seed).write_text(json.dumps({"name": "cci", "version": target if source_matches else "0.1.0", "description": "release source", "nested": {"source": True}}, indent=2) + "\n")
        self.descriptor(seed).write_text(official if source_matches else "source descriptor\n")
        self.descriptor(seed).chmod(0o755)
        (seed / "code.txt").write_text("release source code\n")
        source = self.commit(seed, "release source")
        self.git(seed, "remote", "add", "origin", origin)
        self.git(seed, "push", "origin", "HEAD:main")
        tag = "v" + target
        self.git(seed, "tag", tag)
        self.git(seed, "push", "origin", tag)
        fields = {"name": "cci", "version": current, "description": "fresh main", "nested": {"preserved": "main"}}
        self.manifest(seed).write_text(json.dumps(fields) + "\n")
        self.descriptor(seed).write_text(current_payload if current_payload is not None else f"main descriptor {current}\n")
        (seed / "code.txt").write_text("fresh main code\n")
        (seed / "main-only.txt").write_text("preserve unrelated main\n")
        main = self.commit(seed, "advance main")
        self.git(seed, "push", "origin", "HEAD:main")
        self.git(case, "clone", origin, actor)
        self.git(actor, "checkout", "--detach", source)
        manifest = json.loads(self.manifest(actor).read_text())
        manifest["version"] = target
        self.manifest(actor).write_text(json.dumps(manifest, indent=2) + "\n")
        self.descriptor(actor).write_text(official)
        self.descriptor(actor).chmod(0o755)
        runner = case / "runner"
        runner.mkdir()
        fakebin = case / "bin"
        fakebin.mkdir()
        gh = fakebin / "gh"
        gh.write_text("#!/usr/bin/env python3\nimport json,os,sys\nfrom pathlib import Path\nPath(os.environ['UPLOAD_LOG']).write_text(json.dumps({'argv':sys.argv[1:],'payload_hex':Path(sys.argv[4]).read_bytes().hex()}))\n")
        gh.chmod(0o755)
        env = dict(self.git_env, GITHUB_REF_NAME=tag, GITHUB_REPOSITORY="fixture/repository", GITHUB_OUTPUT=str(runner / "output"), RUNNER_TEMP=str(runner), RELEASE_VERSION_COMPARE=self.comparator, UPLOAD_LOG=str(runner / "uploaded.json"), PATH=str(fakebin) + os.pathsep + os.environ["PATH"])
        script = runner / "move.sh"
        script.write_text(self.move["run"])
        return {"case": case, "origin": origin, "seed": seed, "actor": actor, "source": source, "main": main, "tag": tag, "target": target, "official": official, "fields": fields, "env": env, "script": script, "runner": runner}

    def execute(self, fixture, success=True):
        result = subprocess.run(["bash", "-e", "-o", "pipefail", str(fixture["script"])], cwd=fixture["actor"], env=fixture["env"], capture_output=True, text=True, timeout=30)
        if not success:
            self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse(Path(fixture["env"]["GITHUB_OUTPUT"]).exists())
            return result
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        actor, origin = fixture["actor"], fixture["origin"]
        accepted = self.git(origin, "rev-parse", "refs/heads/main")
        outputs = Path(fixture["env"]["GITHUB_OUTPUT"]).read_text().splitlines()
        self.assertEqual(outputs, ["sha=" + accepted])
        self.assertEqual(self.git(actor, "rev-parse", "HEAD"), accepted)
        tagged = self.git(origin, "rev-parse", fixture["tag"] + "^{}")
        self.git(origin, "merge-base", "--is-ancestor", fixture["source"], tagged)
        self.git(origin, "merge-base", "--is-ancestor", tagged, accepted)
        changed = set(self.git(origin, "diff", "--name-only", fixture["source"], tagged).splitlines())
        self.assertLessEqual(changed, {"plugin/bin/cci.binrun", "plugin/.claude-plugin/plugin.json"})
        self.assertEqual(self.git(origin, "show", tagged + ":code.txt"), "release source code")
        self.assertEqual(self.git(origin, "show", accepted + ":code.txt"), "fresh main code")
        self.assertEqual(self.git(origin, "show", accepted + ":main-only.txt"), "preserve unrelated main")
        subprocess.run(["bash", "-e", "-o", "pipefail", "-c", self.upload], cwd=actor, env=fixture["env"], check=True, capture_output=True, text=True, timeout=30)
        uploaded = json.loads(Path(fixture["env"]["UPLOAD_LOG"]).read_text())
        self.assertEqual(uploaded["argv"], ["release", "upload", fixture["tag"], str(fixture["runner"] / "cci.binrun"), "--repo", "fixture/repository", "--clobber"])
        tagged_payload = subprocess.check_output(["git", "show", tagged + ":plugin/bin/cci.binrun"], cwd=origin, env=self.git_env, timeout=30)
        self.assertEqual(tagged_payload, fixture["official"].encode())
        self.assertEqual(uploaded["payload_hex"], tagged_payload.hex())
        self.assertEqual((fixture["runner"] / "cci.binrun").read_bytes(), tagged_payload)
        return accepted, tagged

    def install_push_counter(self, fixture, competitors=()):
        count = fixture["case"] / "main-pushes"
        hook = fixture["actor"] / ".git/hooks/pre-push"
        hook.write_text("#!/usr/bin/env python3\nfrom pathlib import Path\nimport subprocess,sys\n" + f"count=Path({str(count)!r})\ncompetitors={list(competitors)!r}\norigin={str(fixture['origin'])!r}\n" + "for line in sys.stdin:\n fields=line.split()\n if fields[2]!='refs/heads/main':continue\n with count.open('a') as output:output.write('main\\n')\n index=len(count.read_text().splitlines())-1\n if index<len(competitors):\n  subprocess.run(['git','--git-dir',origin,'update-ref','refs/heads/main',competitors[index],fields[3]],check=True)\n")
        hook.chmod(0o755)
        self.git(fixture["actor"], "config", "core.hooksPath", hook.parent)
        return count

    def test_actual_workflow_comparator_binding(self):
        self.assertEqual(self.move["env"], {"RELEASE_VERSION_COMPARE": "$" + "{{ runner.temp }}/release-version"})
        self.assertIn('echo "sha=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"', self.move["run"])
        self.assertNotIn("git rebase", self.move["run"])

    def test_newer_preserves_main_fields_code_and_tag_source(self):
        f = self.prepare("1.2.0", "1.1.0")
        accepted, tagged = self.execute(f)
        expected = dict(f["fields"], version=f["target"])
        self.assertEqual(json.loads(self.manifest(f["actor"]).read_text()), expected)
        self.assertEqual(self.descriptor(f["actor"]).read_text(), f["official"])
        self.assertEqual(self.git(f["origin"], "rev-list", "--parents", "-n", "1", accepted).split()[1:], [f["main"], tagged])

    def test_older_keeps_main_tree_but_uploads_own_descriptor(self):
        f = self.prepare("1.1.0", "1.2.0")
        accepted, tagged = self.execute(f)
        self.assertEqual(self.git(f["origin"], "rev-parse", accepted + "^{tree}"), self.git(f["origin"], "rev-parse", f["main"] + "^{tree}"))
        self.assertEqual(json.loads(self.manifest(f["actor"]).read_text()), f["fields"])
        self.assertNotEqual(self.descriptor(f["actor"]).read_text(), f["official"])
        self.assertEqual(self.git(f["origin"], "rev-list", "--parents", "-n", "1", accepted).split()[1:], [f["main"], tagged])

    def test_equal_changed_official_descriptor_wins_without_json_churn(self):
        f = self.prepare("1.2.0", "1.2.0")
        before = self.git(f["seed"], "show", f["main"] + ":plugin/.claude-plugin/plugin.json")
        accepted, _ = self.execute(f)
        self.assertEqual(self.git(f["origin"], "show", accepted + ":plugin/.claude-plugin/plugin.json"), before)
        self.assertEqual(self.descriptor(f["actor"]).read_text(), f["official"])

    def test_equal_matching_metadata_still_integrates_tag_ancestry(self):
        f = self.prepare("1.2.0", "1.2.0", "official descriptor 1.2.0\n")
        accepted, tagged = self.execute(f)
        self.assertNotEqual(accepted, f["main"])
        self.assertEqual(self.git(f["origin"], "rev-parse", accepted + "^{tree}"), self.git(f["origin"], "rev-parse", f["main"] + "^{tree}"))
        self.assertEqual(self.git(f["origin"], "rev-list", "--parents", "-n", "1", accepted).split()[1:], [f["main"], tagged])

    def test_already_integrated_matching_metadata_does_not_commit(self):
        f = self.prepare("1.2.0", "1.2.0", "official descriptor 1.2.0\n", source_matches=True)
        accepted, tagged = self.execute(f)
        self.assertEqual(accepted, f["main"])
        self.assertEqual(tagged, f["source"])

    def test_semver_rc_and_build_policy(self):
        for target, current, expected in [("1.2.0-rc.1", "1.2.0", "1.2.0"), ("1.2.0+first", "1.2.0+second", "1.2.0+first"), ("1.0.10", "1.0.9", "1.0.10")]:
            with self.subTest(target=target, current=current):
                f = self.prepare(target, current)
                self.execute(f)
                self.assertEqual(json.loads(self.manifest(f["actor"]).read_text())["version"], expected)

    def test_rejected_push_rereads_newer_main_and_preserves_both_ancestors(self):
        f = self.prepare("1.2.0", "1.1.0")
        data = json.loads(self.manifest(f["seed"]).read_text())
        data.update(version="1.3.0", concurrent="preserved")
        self.manifest(f["seed"]).write_text(json.dumps(data) + "\n")
        self.descriptor(f["seed"]).write_text("newer concurrent descriptor\n")
        competitor = self.commit(f["seed"], "concurrent newer release")
        self.git(f["seed"], "push", "origin", competitor + ":refs/heads/race-input")
        counter = self.install_push_counter(f, [competitor])
        accepted, _ = self.execute(f)
        self.assertEqual(counter.read_text().splitlines(), ["main", "main"])
        self.git(f["origin"], "merge-base", "--is-ancestor", competitor, accepted)
        self.assertEqual(json.loads(self.manifest(f["actor"]).read_text()), data)
        self.assertEqual(self.descriptor(f["actor"]).read_text(), "newer concurrent descriptor\n")

    def test_three_advancing_rejections_are_bounded(self):
        f = self.prepare("1.2.0", "1.1.0")
        competitors = []
        for number in range(3):
            data = json.loads(self.manifest(f["seed"]).read_text())
            data["version"] = f"1.3.{number}"
            self.manifest(f["seed"]).write_text(json.dumps(data) + "\n")
            competitors.append(self.commit(f["seed"], f"concurrent advance {number}"))
        self.git(f["seed"], "push", "origin", competitors[-1] + ":refs/heads/race-input")
        counter = self.install_push_counter(f, competitors)
        self.execute(f, success=False)
        self.assertEqual(counter.read_text().splitlines(), ["main", "main", "main"])
        self.assertEqual(self.git(f["origin"], "rev-parse", "refs/heads/main"), competitors[-1])

    def test_rejection_without_main_advance_fails_without_blind_retry(self):
        f = self.prepare("1.2.0", "1.1.0")
        hook = f["origin"] / "hooks/pre-receive"
        hook.write_text("#!/usr/bin/env python3\nimport sys\nfor line in sys.stdin:\n if line.split()[2]=='refs/heads/main':sys.exit(1)\n")
        hook.chmod(0o755)
        counter = self.install_push_counter(f)
        self.execute(f, success=False)
        self.assertEqual(counter.read_text().splitlines(), ["main"])
        self.assertEqual(self.git(f["origin"], "rev-parse", "refs/heads/main"), f["main"])

    def test_unlanded_source_is_not_marked_merged(self):
        f = self.prepare("1.2.0", "1.1.0")
        tree = self.git(f["seed"], "rev-parse", f["main"] + "^{tree}")
        unrelated = self.git(f["seed"], "commit-tree", tree, "-m", "unrelated replacement")
        self.git(f["seed"], "push", "origin", unrelated + ":refs/heads/unrelated-input")
        self.git(f["origin"], "update-ref", "refs/heads/main", unrelated, f["main"])
        result = self.execute(f, success=False)
        self.assertIn("release source is not an ancestor", result.stderr)
        self.assertEqual(self.git(f["origin"], "rev-parse", "refs/heads/main"), unrelated)

    def test_malformed_main_version_fails_without_main_publication(self):
        f = self.prepare("1.2.0", "dev")
        result = self.execute(f, success=False)
        self.assertIn("invalid release version", result.stderr)
        self.assertEqual(self.git(f["origin"], "rev-parse", "refs/heads/main"), f["main"])


if __name__ == "__main__":
    unittest.main()
