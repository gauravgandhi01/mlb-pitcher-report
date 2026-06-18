import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import Pitchers as pitchers_module
from Pitchers import (
    MATCHUP_SOURCE_COLUMN,
    OPP_HAND_K_COLUMN,
    START_TIME_COLUMN,
    resolve_effective_report_date_and_schedule,
    write_to_html,
)


class PitchersRenderTests(unittest.TestCase):
    def _sample_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "Name": "Alpha Ace",
                    "Hand": "R",
                    "GP": 12,
                    "AB": 48,
                    "K": 18,
                    "BB": 4,
                    "AVG": 0.211,
                    "AB/GP": 4.0,
                    "K/9": 10.1,
                    "Whiff%": 31.4,
                    "K/AB": 34.6,
                    "K%": 26.5,
                    "PA": 37,
                    MATCHUP_SOURCE_COLUMN: "ESPN (AB)",
                    "SO/PA": 24.3,
                    OPP_HAND_K_COLUMN: 23.1,
                    "r": 4,
                    "Opponent": "Boston Red Sox",
                    START_TIME_COLUMN: "7:10p",
                    "Status": "Scheduled",
                    "Ks": 0,
                },
                {
                    "Name": "Bravo Ball",
                    "Hand": "L",
                    "GP": 10,
                    "AB": 41,
                    "K": 12,
                    "BB": 5,
                    "AVG": 0.254,
                    "AB/GP": 4.1,
                    "K/9": 8.7,
                    "Whiff%": 27.2,
                    "K/AB": 26.1,
                    "K%": 21.8,
                    "PA": 34,
                    MATCHUP_SOURCE_COLUMN: "Savant (PA)",
                    "SO/PA": 22.1,
                    OPP_HAND_K_COLUMN: 21.0,
                    "r": 9,
                    "Opponent": "New York Yankees",
                    START_TIME_COLUMN: "4:10p",
                    "Status": "In Progress",
                    "Ks": 5,
                },
                {
                    "Name": "Charlie Check",
                    "Hand": "R",
                    "GP": 11,
                    "AB": 45,
                    "K": 15,
                    "BB": 6,
                    "AVG": 0.239,
                    "AB/GP": 4.1,
                    "K/9": 9.4,
                    "Whiff%": 28.7,
                    "K/AB": 29.4,
                    "K%": 23.6,
                    "PA": 36,
                    MATCHUP_SOURCE_COLUMN: "ESPN (AB)",
                    "SO/PA": 20.4,
                    OPP_HAND_K_COLUMN: 19.7,
                    "r": 15,
                    "Opponent": "Seattle Mariners",
                    START_TIME_COLUMN: "1:10p",
                    "Status": "Final",
                    "Ks": 7,
                },
            ]
        )

    def test_write_to_html_renders_pitcher_controls_sort_hooks_and_archive_tabs(self) -> None:
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        tomorrow = today + dt.timedelta(days=1)
        display_date = today.strftime("%m/%d/%Y")
        report_key = display_date.replace("/", "")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            reports_dir = tmp_path / "reports"
            root_index = tmp_path / "index.html"
            with patch.object(pitchers_module, "REPORTS_DIR", reports_dir), patch.object(
                pitchers_module,
                "ROOT_INDEX_FILE",
                root_index,
            ):
                archive_path = write_to_html(
                    self._sample_dataframe(),
                    report_key,
                    display_date,
                    pitcher_arsenal_lookup={},
                )

            archive_html = archive_path.read_text(encoding="utf-8")
            root_html = root_index.read_text(encoding="utf-8")

            self.assertIn("Hide In Progress", archive_html)
            self.assertIn("Hide Final", archive_html)
            self.assertIn('data-sort-key="GP"', archive_html)
            self.assertIn('data-sort-key="Ks"', archive_html)
            self.assertNotIn('data-sort-key="Name"', archive_html)
            self.assertNotIn('data-sort-key="Opponent"', archive_html)
            self.assertNotIn('data-sort-key="Status"', archive_html)
            self.assertIn('data-initial-index="0"', archive_html)
            self.assertIn('data-sort-value="12"', archive_html)
            self.assertIn('href="./batters-report-' + report_key + '.html"', archive_html)
            self.assertIn('href="./matchups-report-' + report_key + '.html"', archive_html)
            self.assertIn("hide-live-toggle", archive_html)
            self.assertIn("applyTableState()", archive_html)
            self.assertIn('href="./batters.html"', root_html)
            self.assertIn('href="./matchups.html"', root_html)
            self.assertIn(
                f'href="./reports/report-{yesterday.strftime("%m%d%Y")}.html"',
                root_html,
            )
            self.assertIn(
                f'href="./reports/report-{tomorrow.strftime("%m%d%Y")}.html"',
                root_html,
            )

    def test_write_to_html_skips_root_output_when_requested(self) -> None:
        display_date = dt.date.today().strftime("%m/%d/%Y")
        report_key = display_date.replace("/", "")

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            reports_dir = tmp_path / "reports"
            root_index = tmp_path / "index.html"
            with patch.object(pitchers_module, "REPORTS_DIR", reports_dir), patch.object(
                pitchers_module,
                "ROOT_INDEX_FILE",
                root_index,
            ):
                write_to_html(
                    self._sample_dataframe(),
                    report_key,
                    display_date,
                    pitcher_arsenal_lookup={},
                    write_root=False,
                )

            self.assertFalse(root_index.exists())

    def test_pitcher_date_resolution_respects_exact_mode(self) -> None:
        with patch("Pitchers.fetch_schedule", return_value=[{"status": "Final"}]):
            report_date, schedule = resolve_effective_report_date_and_schedule(
                "06/17/2026",
                allow_roll_forward=False,
            )

        self.assertEqual(report_date, "06/17/2026")
        self.assertEqual(schedule, [{"status": "Final"}])


if __name__ == "__main__":
    unittest.main()
