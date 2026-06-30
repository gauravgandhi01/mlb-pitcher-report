import datetime as dt
import unittest
from unittest.mock import patch

from report_data import (
    extract_batter_vs_pitcher_stat_lines_from_plays,
    parse_vs_pitcher_stats,
    resolve_effective_report_date_and_schedule,
)


class ReportDataDateResolutionTests(unittest.TestCase):
    def test_resolve_effective_report_date_rolls_forward_by_default(self) -> None:
        completed_schedule = [{"status": "Final"}]
        next_schedule = [{"status": "Scheduled"}]

        with patch("report_data.fetch_schedule", side_effect=[completed_schedule, next_schedule]):
            report_date, schedule = resolve_effective_report_date_and_schedule("06/17/2026")

        self.assertEqual(report_date, "06/18/2026")
        self.assertEqual(schedule, next_schedule)

    def test_resolve_effective_report_date_respects_exact_mode(self) -> None:
        completed_schedule = [{"status": "Final"}]

        with patch("report_data.fetch_schedule", return_value=completed_schedule):
            report_date, schedule = resolve_effective_report_date_and_schedule(
                "06/17/2026",
                allow_roll_forward=False,
            )

        self.assertEqual(report_date, "06/17/2026")
        self.assertEqual(schedule, completed_schedule)

    def test_parse_vs_pitcher_stats_freezes_current_season_with_same_day_adjustment(self) -> None:
        indexed_blocks = {
            "vsPlayer": [
                {
                    "splits": [
                        {
                            "season": "2023",
                            "stat": {
                                "plateAppearances": 3,
                                "atBats": 3,
                                "hits": 1,
                                "baseOnBalls": 0,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 4,
                                "strikeOuts": 2,
                                "homeRuns": 1,
                                "rbi": 1,
                            },
                        },
                        {
                            "season": "2026",
                            "stat": {
                                "plateAppearances": 2,
                                "atBats": 1,
                                "hits": 1,
                                "baseOnBalls": 1,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 4,
                                "strikeOuts": 0,
                                "homeRuns": 1,
                                "rbi": 1,
                            },
                        },
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(
            indexed_blocks,
            report_date=dt.date(2026, 6, 17),
            same_day_line={"PA": 2, "AB": 1, "H": 1, "BB": 1, "HBP": 0, "SF": 0, "TB": 4, "K": 0, "HR": 1, "RBI": 1},
        )

        self.assertEqual(stats["PA"], 3)
        self.assertEqual(stats["AB"], 3)
        self.assertEqual(stats["H"], 1)
        self.assertEqual(stats["HR"], 1)
        self.assertEqual(stats["RBI"], 1)
        self.assertAlmostEqual(stats["AVG"], 1 / 3, places=6)

    def test_extract_batter_vs_pitcher_stat_lines_from_plays_tracks_only_target_pitcher(self) -> None:
        plays = [
            {
                "result": {"type": "atBat", "eventType": "home_run", "rbi": 1},
                "matchup": {"batter": {"id": 10}, "pitcher": {"id": 20}},
            },
            {
                "result": {"type": "atBat", "eventType": "walk", "rbi": 0},
                "matchup": {"batter": {"id": 10}, "pitcher": {"id": 20}},
            },
            {
                "result": {"type": "atBat", "eventType": "single", "rbi": 0},
                "matchup": {"batter": {"id": 10}, "pitcher": {"id": 21}},
            },
        ]

        lines = extract_batter_vs_pitcher_stat_lines_from_plays(plays, 20)

        self.assertEqual(lines[10]["PA"], 2)
        self.assertEqual(lines[10]["AB"], 1)
        self.assertEqual(lines[10]["H"], 1)
        self.assertEqual(lines[10]["BB"], 1)
        self.assertEqual(lines[10]["HR"], 1)
        self.assertEqual(lines[10]["TB"], 4)


if __name__ == "__main__":
    unittest.main()
