import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from cron_publish_utils import normalize_publish_content, staged_files_have_substantive_changes


class CronPublishUtilsTests(unittest.TestCase):
    def test_normalize_publish_content_masks_report_timestamps(self) -> None:
        content = "\n".join(
            [
                '<p class="updated-at">Last updated: 2026-07-12 01:19:16 EDT</p>',
                "<p>07/12/2026 slate. Updated 2026-07-12 01:13:02 EDT.</p>",
                "<p>07/12/2026 slate. Updated 2026-07-12 01:14:03 EDT. Matchup context.</p>",
                '{"team": "NYM", "updated_at": "2026-07-12T05:13:02Z"}',
            ]
        )

        normalized = normalize_publish_content(content)

        self.assertIn('<p class="updated-at">Last updated: __TIMESTAMP__</p>', normalized)
        self.assertIn("Updated __TIMESTAMP__.", normalized)
        self.assertIn('"updated_at": "__TIMESTAMP__"', normalized)
        self.assertNotIn("2026-07-12 01:19:16 EDT", normalized)
        self.assertNotIn("2026-07-12T05:13:02Z", normalized)

    def test_staged_substantive_detection_ignores_timestamp_only_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._git(repo, "init")
            self._git(repo, "config", "user.email", "test@example.com")
            self._git(repo, "config", "user.name", "Test User")
            report = repo / "index.html"
            report.write_text(
                '<p class="updated-at">Last updated: 2026-07-12 01:00:00 EDT</p>\n'
                '<table><tr><td>Same</td></tr></table>\n',
                encoding="utf-8",
            )
            self._git(repo, "add", "index.html")
            self._git(repo, "commit", "-m", "initial")

            report.write_text(
                '<p class="updated-at">Last updated: 2026-07-12 01:20:00 EDT</p>\n'
                '<table><tr><td>Same</td></tr></table>\n',
                encoding="utf-8",
            )
            self._git(repo, "add", "index.html")

            self.assertFalse(staged_files_have_substantive_changes(repo, ["index.html"]))

            report.write_text(
                '<p class="updated-at">Last updated: 2026-07-12 01:20:00 EDT</p>\n'
                '<table><tr><td>Changed</td></tr></table>\n',
                encoding="utf-8",
            )
            self._git(repo, "add", "index.html")

            self.assertTrue(staged_files_have_substantive_changes(repo, ["index.html"]))

    def test_staged_substantive_detection_ignores_json_updated_at_only_changes(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            repo = Path(tmpdir)
            self._git(repo, "init")
            self._git(repo, "config", "user.email", "test@example.com")
            self._git(repo, "config", "user.name", "Test User")
            lock_file = repo / "locks.json"
            lock_file.write_text('{"team": "BOS", "updated_at": "2026-07-12T05:00:00Z"}\n', encoding="utf-8")
            self._git(repo, "add", "locks.json")
            self._git(repo, "commit", "-m", "initial")

            lock_file.write_text('{"team": "BOS", "updated_at": "2026-07-12T05:20:00Z"}\n', encoding="utf-8")
            self._git(repo, "add", "locks.json")

            self.assertFalse(staged_files_have_substantive_changes(repo, ["locks.json"]))

    def test_frequent_mode_dry_run_generates_root_once(self) -> None:
        result = self._run_cron_dry_run("frequent")

        self.assertIn("DRY RUN: python3 Pitchers.py today y", result.stdout)
        self.assertIn("DRY RUN: python3 Batters.py today", result.stdout)
        self.assertIn("DRY RUN: python3 Matchups.py today", result.stdout)
        self.assertNotIn("--exact --no-root", result.stdout)

    def test_archive_mode_dry_run_generates_yesterday_and_tomorrow_only(self) -> None:
        result = self._run_cron_dry_run("archive", odds="n")

        self.assertIn("DRY RUN: python3 Pitchers.py 07/11/2026 n --exact --no-root", result.stdout)
        self.assertIn("DRY RUN: python3 Batters.py 07/11/2026 --exact --no-root", result.stdout)
        self.assertIn("DRY RUN: python3 Matchups.py 07/13/2026 --exact --no-root", result.stdout)
        self.assertNotIn("Pitchers.py 07/12/2026", result.stdout)

    def test_script_managed_log_rotation_rotates_before_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "cron.log"
            log_path.write_text("x" * 32, encoding="utf-8")
            env = self._cron_env("frequent")
            env.update(
                {
                    "CRON_LOG_TO_FILE": "1",
                    "CRON_LOG_FILE": str(log_path),
                    "CRON_LOG_MAX_BYTES": "10",
                    "CRON_LOG_BACKUPS": "2",
                }
            )

            subprocess.run(
                ["bash", str(REPO_ROOT / "scripts" / "run_pitcher_cron.sh"), "today", "y", "frequent"],
                cwd=REPO_ROOT,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True,
            )

            self.assertTrue(log_path.exists())
            self.assertTrue(log_path.with_name("cron.log.1").exists())
            self.assertEqual(log_path.with_name("cron.log.1").read_text(encoding="utf-8"), "x" * 32)
            self.assertIn("Starting MLB report cron", log_path.read_text(encoding="utf-8"))

    def _run_cron_dry_run(self, mode: str, odds: str = "y") -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["bash", str(REPO_ROOT / "scripts" / "run_pitcher_cron.sh"), "today", odds, mode],
            cwd=REPO_ROOT,
            env=self._cron_env(mode),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

    def _cron_env(self, mode: str) -> dict[str, str]:
        del mode
        env = os.environ.copy()
        env.update(
            {
                "CRON_DRY_RUN": "1",
                "CRON_LOG_TO_FILE": "0",
                "CRON_SKIP_GIT_SYNC": "1",
                "CRON_TODAY": "07/12/2026",
                "CRON_REPO_DIR": str(REPO_ROOT),
            }
        )
        return env

    def _git(self, repo: Path, *args: str) -> None:
        subprocess.run(["git", *args], cwd=repo, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


if __name__ == "__main__":
    unittest.main()
