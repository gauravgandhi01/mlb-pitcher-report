import datetime as dt
import unittest
from unittest.mock import patch

from mlb_pitcher_report.shared import report_data as report_data_module
from mlb_pitcher_report.shared.report_data import (
    extract_batter_vs_pitcher_stat_lines_from_plays,
    fetch_pitcher_historical_batter_vs_pitcher_stat_lines,
    fetch_people_stats_map,
    parse_vs_pitcher_stats,
    resolve_effective_report_date_and_schedule,
)


class ReportDataDateResolutionTests(unittest.TestCase):
    def test_resolve_effective_report_date_rolls_forward_by_default(self) -> None:
        completed_schedule = [{"status": "Final"}]
        next_schedule = [{"status": "Scheduled"}]

        with patch("mlb_pitcher_report.shared.report_data.fetch_schedule", side_effect=[completed_schedule, next_schedule]):
            report_date, schedule = resolve_effective_report_date_and_schedule("06/17/2026")

        self.assertEqual(report_date, "06/18/2026")
        self.assertEqual(schedule, next_schedule)

    def test_resolve_effective_report_date_keeps_current_slate_when_next_day_is_empty(self) -> None:
        completed_schedule = [{"status": "Final"}]

        with patch("mlb_pitcher_report.shared.report_data.fetch_schedule", side_effect=[completed_schedule, []]):
            report_date, schedule = resolve_effective_report_date_and_schedule("07/12/2026")

        self.assertEqual(report_date, "07/12/2026")
        self.assertEqual(schedule, completed_schedule)

    def test_resolve_effective_report_date_respects_exact_mode(self) -> None:
        completed_schedule = [{"status": "Final"}]

        with patch("mlb_pitcher_report.shared.report_data.fetch_schedule", return_value=completed_schedule):
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

    def test_fetch_people_stats_map_includes_end_date_in_hydrate(self) -> None:
        with patch("mlb_pitcher_report.shared.report_data.statsapi.get", return_value={"people": [{"id": 123}]}) as get_mock:
            fetch_people_stats_map(
                [123],
                season=2026,
                pitch_hand="R",
                pitcher_id=456,
                stats_end_date=dt.date(2026, 7, 10),
            )

        params = get_mock.call_args.args[1]
        self.assertIn("endDate=2026-07-10", params["hydrate"])
        self.assertIn("sitCodes=[vr]", params["hydrate"])
        self.assertIn("opposingPlayerId=456", params["hydrate"])

    def test_parse_vs_pitcher_stats_can_skip_same_day_subtraction_for_dated_splits(self) -> None:
        indexed_blocks = {
            "vsPlayer": [
                {
                    "splits": [
                        {
                            "season": "2026",
                            "stat": {
                                "plateAppearances": 4,
                                "atBats": 4,
                                "hits": 2,
                                "baseOnBalls": 0,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 2,
                                "strikeOuts": 1,
                                "homeRuns": 0,
                                "rbi": 1,
                            },
                        }
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(
            indexed_blocks,
            report_date=dt.date(2026, 7, 11),
            same_day_line={"PA": 1, "AB": 1, "H": 1, "BB": 0, "HBP": 0, "SF": 0, "TB": 1, "K": 0, "HR": 0, "RBI": 0},
            subtract_same_day_from_season_splits=False,
        )

        self.assertEqual(stats["PA"], 4)
        self.assertEqual(stats["AB"], 4)
        self.assertEqual(stats["H"], 2)
        self.assertAlmostEqual(stats["AVG"], 0.5, places=6)

    def test_parse_vs_pitcher_stats_uses_matching_batter_total_split(self) -> None:
        indexed_blocks = {
            "vsPlayerTotal": [
                {
                    "splits": [
                        {
                            "pitcher": {"id": 30},
                            "batter": {"id": 10},
                            "stat": {
                                "plateAppearances": 6,
                                "atBats": 5,
                                "hits": 1,
                                "baseOnBalls": 1,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 3,
                                "strikeOuts": 0,
                                "homeRuns": 0,
                                "rbi": 1,
                            },
                        },
                        {
                            "pitcher": {"id": 30},
                            "batter": {"id": 11},
                            "stat": {
                                "plateAppearances": 115,
                                "atBats": 103,
                                "hits": 32,
                                "baseOnBalls": 10,
                                "hitByPitch": 0,
                                "sacFlies": 2,
                                "totalBases": 64,
                                "strikeOuts": 25,
                                "homeRuns": 6,
                                "rbi": 24,
                            },
                        },
                    ]
                }
            ],
            "vsPlayer": [
                {
                    "splits": [
                        {
                            "season": "2026",
                            "stat": {
                                "plateAppearances": 115,
                                "atBats": 103,
                                "hits": 32,
                                "baseOnBalls": 10,
                                "hitByPitch": 0,
                                "sacFlies": 2,
                                "totalBases": 64,
                                "strikeOuts": 25,
                                "homeRuns": 6,
                                "rbi": 24,
                            },
                        }
                    ]
                }
            ],
        }

        stats = parse_vs_pitcher_stats(indexed_blocks, batter_id=10, pitcher_id=30)

        self.assertEqual(stats["PA"], 6)
        self.assertEqual(stats["AB"], 5)
        self.assertEqual(stats["H"], 1)
        self.assertEqual(stats["HR"], 0)
        self.assertAlmostEqual(stats["AVG"], 0.2, places=6)

    def test_parse_vs_pitcher_stats_can_skip_same_day_subtraction_for_dated_total_split(self) -> None:
        indexed_blocks = {
            "vsPlayerTotal": [
                {
                    "splits": [
                        {
                            "pitcher": {"id": 30},
                            "batter": {"id": 10},
                            "stat": {
                                "plateAppearances": 5,
                                "atBats": 5,
                                "hits": 3,
                                "baseOnBalls": 0,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 4,
                                "strikeOuts": 2,
                                "homeRuns": 0,
                                "rbi": 1,
                            },
                        }
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(
            indexed_blocks,
            batter_id=10,
            pitcher_id=30,
            same_day_line={"PA": 2, "AB": 1, "H": 1, "BB": 1, "HBP": 0, "SF": 0, "TB": 3, "K": 0, "HR": 0, "RBI": 0},
            subtract_same_day_from_season_splits=False,
        )

        self.assertEqual(stats["PA"], 5)
        self.assertEqual(stats["AB"], 5)
        self.assertEqual(stats["H"], 3)
        self.assertAlmostEqual(stats["AVG"], 0.6, places=6)

    def test_parse_vs_pitcher_stats_ignores_unattributed_splits_when_batter_known(self) -> None:
        indexed_blocks = {
            "vsPlayer": [
                {
                    "splits": [
                        {
                            "season": "2026",
                            "stat": {
                                "plateAppearances": 115,
                                "atBats": 103,
                                "hits": 32,
                                "baseOnBalls": 10,
                                "hitByPitch": 0,
                                "sacFlies": 2,
                                "totalBases": 64,
                                "strikeOuts": 25,
                                "homeRuns": 6,
                                "rbi": 24,
                            },
                        }
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(indexed_blocks, batter_id=10, pitcher_id=30)

        self.assertEqual(stats["PA"], 0)
        self.assertEqual(stats["AB"], 0)
        self.assertEqual(stats["H"], 0)
        self.assertIsNone(stats["AVG"])

    def test_fetch_pitcher_historical_bvp_uses_only_games_before_report_date(self) -> None:
        def line(pa: int, ab: int, hits: int, total_bases: int) -> dict:
            return {
                "PA": pa,
                "AB": ab,
                "H": hits,
                "BB": pa - ab,
                "HBP": 0,
                "SF": 0,
                "TB": total_bases,
                "K": 0,
                "HR": 0,
                "RBI": 0,
            }

        game_logs_by_season = {
            2025: [
                {"date": "2025-09-01", "game": {"gamePk": 100}},
            ],
            2026: [
                {"date": "2026-07-11", "game": {"gamePk": 101}},
                {"date": "2026-07-12", "game": {"gamePk": 102}},
            ],
        }
        game_lines = {
            100: {10: line(1, 1, 1, 1)},
            101: {10: line(2, 2, 1, 2), 11: line(1, 1, 0, 0)},
            102: {10: line(4, 4, 4, 8)},
        }

        with patch.object(report_data_module, "PITCHER_HISTORICAL_BVP_CACHE", {}), patch(
            "mlb_pitcher_report.shared.report_data.fetch_pitcher_debut_year",
            return_value=2025,
        ), patch(
            "mlb_pitcher_report.shared.report_data.fetch_pitcher_game_log_splits",
            side_effect=lambda pitcher_id, season: game_logs_by_season.get(season, []),
        ), patch(
            "mlb_pitcher_report.shared.report_data.fetch_game_batter_vs_pitcher_stat_lines",
            side_effect=lambda game_id, pitcher_id: game_lines.get(game_id, {}),
        ) as game_bvp_mock:
            stats = fetch_pitcher_historical_batter_vs_pitcher_stat_lines(
                30,
                dt.date(2026, 7, 12),
            )

        self.assertEqual(stats[10]["PA"], 3)
        self.assertEqual(stats[10]["AB"], 3)
        self.assertEqual(stats[10]["H"], 2)
        self.assertAlmostEqual(stats[10]["AVG"], 2 / 3, places=6)
        self.assertEqual(stats[11]["PA"], 1)
        self.assertEqual([call.args[0] for call in game_bvp_mock.call_args_list], [100, 101])

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
