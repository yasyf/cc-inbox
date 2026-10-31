from graphlib import TopologicalSorter
import json
from pathlib import Path
import os
import subprocess
import tempfile
import unittest

EXPECTED_CANCEL = "${{ inputs.commit == '' && github.event_name == 'pull_request' }}"
EXPECTED_DYNAMIC_PROFILES = 0


def load_workflow(name):
    return json.loads(subprocess.check_output(["ruby", "-rjson", "-ryaml", "-e", "puts JSON.generate(YAML.load_file(ARGV[0]))", str(Path(".github/workflows") / name)], text=True))


class NativeReleaseGateTest(unittest.TestCase):
    def setUp(self):
        self.release = load_workflow("release.yml")
        self.ci = load_workflow("ci.yml")

    def test_exact_commit_and_native_failure_dependencies(self):
        jobs = self.release["jobs"]
        descriptor = jobs["descriptor"]
        self.assertEqual(descriptor["needs"], "release")
        self.assertEqual(descriptor["outputs"], {"commit": "${{ steps.commit.outputs.sha }}"})
        commit_steps = [step for step in descriptor["steps"] if step.get("id") == "commit"]
        self.assertEqual(len(commit_steps), 1)
        self.assertIn('echo "sha=$(git rev-parse HEAD)" >> "$GITHUB_OUTPUT"', commit_steps[0]["run"])
        for name in ["post-release-ci", "post-release-guides"]:
            job = jobs[name]
            self.assertEqual(job["needs"], "descriptor")
            expected = {"commit": "${{ needs.descriptor.outputs.commit }}"}
            if name == "post-release-guides":
                expected["parallel_commits"] = True
            self.assertEqual(job["with"], expected)
            for key in ["if", "continue-on-error", "concurrency", "steps"]:
                self.assertNotIn(key, job)
        self.assertNotIn("concurrency", self.release)
        graph = {name: ([job["needs"]] if isinstance(job.get("needs"), str) else job.get("needs", [])) for name, job in jobs.items()}
        for parents in graph.values():
            self.assertTrue(set(parents) <= jobs.keys())
        self.assertEqual(set(TopologicalSorter(graph).static_order()), jobs.keys())

    def test_descriptor_sync_lands_on_main_after_a_concurrent_release_synced_first(self):
        [step] = [step for step in self.release["jobs"]["descriptor"]["steps"] if step.get("id") == "commit"]
        for main_version, want_main, want_sha in [("0.44.1", "0.45.0", "main"), ("0.46.0", "0.46.0", "tag")]:
            with self.subTest(main=main_version), tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp)
                git = lambda *args, cwd=root / "work": subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()
                subprocess.run(["git", "init", "--quiet", "--bare", "--initial-branch=main", str(root / "origin.git")], check=True)
                subprocess.run(["git", "clone", "--quiet", str(root / "origin.git"), str(root / "work")], check=True, capture_output=True)
                git("config", "user.name", "test")
                git("config", "user.email", "test@example.com")
                manifest = root / "work" / "plugin" / ".claude-plugin" / "plugin.json"
                descriptor = root / "work" / "plugin" / "bin" / "cci.binrun"
                manifest.parent.mkdir(parents=True)
                descriptor.parent.mkdir(parents=True)

                def release(version, message):
                    manifest.write_text(json.dumps({"name": "cc-inbox", "version": version}) + "\n")
                    descriptor.write_text(f'{{"version": {{"static": "{version}"}}}}\n')
                    git("add", "-A")
                    git("commit", "--quiet", "-m", message)

                release("0.44.0", "base")
                git("push", "--quiet", "origin", "HEAD:main")
                release("0.45.0", "chore: sync cci.binrun + manifest to v0.45.0")
                tag = git("rev-parse", "HEAD")
                git("checkout", "--quiet", "-B", "concurrent", "origin/main")
                release(main_version, f"chore: sync cci.binrun + manifest to v{main_version}")
                git("push", "--quiet", "origin", "HEAD:main")
                git("checkout", "--quiet", "--detach", tag)
                output = root / "output"
                env = {**os.environ, "GITHUB_REF_NAME": "v0.45.0", "GITHUB_OUTPUT": str(output)}
                subprocess.run(["bash", "-e", "-c", step["run"]], cwd=root / "work", env=env, check=True, capture_output=True, text=True)
                git("fetch", "--quiet", "origin", "main")
                main = json.loads(git("show", "origin/main:plugin/.claude-plugin/plugin.json"))["version"]
                self.assertEqual(main, want_main)
                sha = output.read_text().strip().removeprefix("sha=")
                self.assertEqual(sha, git("rev-parse", "origin/main") if want_sha == "main" else tag)

    def test_permissions_and_single_validation_calls(self):
        jobs = self.release["jobs"]
        ci = jobs["post-release-ci"]
        guides = jobs["post-release-guides"]
        self.assertEqual(ci["uses"], "./.github/workflows/ci.yml")
        self.assertEqual(ci["permissions"], {"contents": "read"})
        self.assertNotIn("secrets", ci)
        self.assertRegex(guides["uses"], r"^yasyf/cc-guides/\.github/workflows/guides\.yml@[0-9a-f]{40}$")
        self.assertEqual(guides["permissions"], {"contents": "write"})
        self.assertEqual(guides["secrets"], "inherit")
        self.assertEqual(jobs["descriptor"]["permissions"], {"contents": "write"})
        self.assertEqual(sum(job.get("uses") == ci["uses"] for job in jobs.values()), 1)
        self.assertEqual(sum(job.get("uses") == guides["uses"] for job in jobs.values()), 1)

    def test_read_only_pr_native_call_fixture_matches_release(self):
        fixture = load_workflow("native-release-contract.yml")
        jobs = fixture["jobs"]
        self.assertEqual(set(jobs), {"source", "native-ci", "native-guides"})
        self.assertEqual(fixture["permissions"], {"contents": "read"})
        self.assertNotIn("concurrency", fixture)
        self.assertEqual(jobs["source"]["outputs"], {"commit": "${{ steps.commit.outputs.sha }}"})
        self.assertEqual(jobs["source"]["steps"][0]["with"], {"ref": "${{ github.event.pull_request.head.sha }}"})
        for role in ["ci", "guides"]:
            expected = dict(self.release["jobs"]["post-release-" + role])
            expected["needs"] = "source"
            expected["with"] = dict(expected["with"], commit="${{ needs.source.outputs.commit }}")
            self.assertEqual(jobs["native-" + role], expected)
        source = Path(".github/workflows/native-release-contract.yml").read_text()
        self.assertIn("  pull_request:", source)
        self.assertNotIn("workflow_dispatch", source)
        self.assertNotIn("  push:", source)

    def test_polling_and_redispatch_removed(self):
        source = Path(".github/workflows/release.yml").read_text()
        for command in ["gh run watch", "gh run view", "workflow_run_id", "/dispatches", "wait_for_run"]:
            self.assertNotIn(command, source)
        self.assertFalse(Path(".github/scripts/test-release-observer.py").exists())

    def test_exact_commit_profile_and_cancellation(self):
        self.assertEqual(self.ci["permissions"], {"contents": "read"})
        self.assertEqual(self.ci["concurrency"]["group"], "ci-${{ inputs.commit || github.ref }}")
        self.assertEqual(self.ci["concurrency"]["cancel-in-progress"], EXPECTED_CANCEL)
        profiles = []
        for job in self.ci["jobs"].values():
            if "strategy" in job and "os" in job["strategy"]["matrix"]:
                os = job["strategy"]["matrix"]["os"]
                if isinstance(os, str):
                    profiles.append(os)
        self.assertEqual(len(profiles), EXPECTED_DYNAMIC_PROFILES)
        for profile in profiles:
            self.assertIn("github.event_name == 'pull_request' || inputs.commit != ''", profile)
            self.assertNotIn("github.event_name == 'workflow_dispatch'", profile)


if __name__ == "__main__":
    unittest.main(verbosity=2)
