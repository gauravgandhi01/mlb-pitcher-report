import datetime as dt
import unittest

import pandas as pd

from Batters import (
    ACTIVE_STREAK_SECTION_MIN,
    STREAK_SECTION_MIN,
    apply_hot_scores,
    build_active_hit_streak_section,
    build_good_matchups_section,
    build_hot_streak_matchup_section,
    compute_hit_streak,
    compute_recent_metrics,
    parse_vs_pitcher_stats,
    rank_active_roster_candidates,
    sort_batters_for_report,
)


class BattersLogicTests(unittest.TestCase):
    def test_compute_recent_metrics_aggregates_last_games(self) -> None:
        report_date = dt.date(2026, 6, 7)
        logs = [
            {
                "date": dt.date(2026, 6, 6),
                "gamePk": 3,
                "atBats": 4,
                "hits": 2,
                "walks": 1,
                "hitByPitch": 0,
                "sacFlies": 0,
                "plateAppearances": 5,
                "totalBases": 4,
                "strikeOuts": 1,
                "homeRuns": 1,
                "rbi": 2,
            },
            {
                "date": dt.date(2026, 6, 5),
                "gamePk": 2,
                "atBats": 3,
                "hits": 0,
                "walks": 1,
                "hitByPitch": 0,
                "sacFlies": 1,
                "plateAppearances": 5,
                "totalBases": 0,
                "strikeOuts": 2,
                "homeRuns": 0,
                "rbi": 0,
            },
            {
                "date": dt.date(2026, 6, 4),
                "gamePk": 1,
                "atBats": 5,
                "hits": 3,
                "walks": 0,
                "hitByPitch": 0,
                "sacFlies": 0,
                "plateAppearances": 5,
                "totalBases": 5,
                "strikeOuts": 0,
                "homeRuns": 0,
                "rbi": 1,
            },
        ]

        metrics = compute_recent_metrics(logs, report_date, max_games=2)
        self.assertEqual(metrics["PA"], 10)
        self.assertEqual(metrics["HR"], 1)
        self.assertEqual(metrics["RBI"], 2)
        self.assertAlmostEqual(metrics["AVG"], 2 / 7, places=6)
        self.assertAlmostEqual(metrics["OBP"], 4 / 10, places=6)
        self.assertAlmostEqual(metrics["SLG"], 4 / 7, places=6)
        self.assertAlmostEqual(metrics["OPS"], (4 / 10) + (4 / 7), places=6)
        self.assertAlmostEqual(metrics["K%"], 30.0, places=6)

    def test_compute_hit_streak_ignores_zero_ab_games(self) -> None:
        report_date = dt.date(2026, 6, 7)
        logs = [
            {"date": dt.date(2026, 6, 6), "gamePk": 4, "atBats": 4, "hits": 1},
            {"date": dt.date(2026, 6, 5), "gamePk": 3, "atBats": 0, "hits": 0},
            {"date": dt.date(2026, 6, 4), "gamePk": 2, "atBats": 3, "hits": 2},
            {"date": dt.date(2026, 6, 3), "gamePk": 1, "atBats": 4, "hits": 0},
        ]

        self.assertEqual(compute_hit_streak(logs, report_date), 2)

    def test_parse_vs_pitcher_stats_prefers_total_split(self) -> None:
        indexed_blocks = {
            "vsPlayerTotal": [
                {
                    "splits": [
                        {
                            "stat": {
                                "plateAppearances": 10,
                                "hits": 3,
                                "homeRuns": 1,
                                "rbi": 4,
                                "avg": ".300",
                                "ops": ".900",
                                "strikeOuts": 2,
                            }
                        }
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(indexed_blocks)
        self.assertEqual(stats["PA"], 10)
        self.assertEqual(stats["H"], 3)
        self.assertEqual(stats["HR"], 1)
        self.assertEqual(stats["RBI"], 4)
        self.assertAlmostEqual(stats["AVG"], 0.300, places=6)
        self.assertAlmostEqual(stats["OPS"], 0.900, places=6)
        self.assertAlmostEqual(stats["K%"], 20.0, places=6)

    def test_parse_vs_pitcher_stats_aggregates_season_splits_without_total(self) -> None:
        indexed_blocks = {
            "vsPlayer": [
                {
                    "splits": [
                        {
                            "stat": {
                                "atBats": 4,
                                "hits": 1,
                                "baseOnBalls": 1,
                                "hitByPitch": 0,
                                "sacFlies": 0,
                                "totalBases": 1,
                                "strikeOuts": 1,
                                "plateAppearances": 5,
                                "homeRuns": 0,
                                "rbi": 1,
                            }
                        },
                        {
                            "stat": {
                                "atBats": 6,
                                "hits": 3,
                                "baseOnBalls": 0,
                                "hitByPitch": 0,
                                "sacFlies": 1,
                                "totalBases": 5,
                                "strikeOuts": 2,
                                "plateAppearances": 7,
                                "homeRuns": 1,
                                "rbi": 2,
                            }
                        },
                    ]
                }
            ]
        }

        stats = parse_vs_pitcher_stats(indexed_blocks)
        self.assertEqual(stats["PA"], 12)
        self.assertEqual(stats["H"], 4)
        self.assertEqual(stats["HR"], 1)
        self.assertEqual(stats["RBI"], 3)
        self.assertAlmostEqual(stats["AVG"], 0.4, places=6)
        self.assertAlmostEqual(stats["OPS"], (5 / 12) + (6 / 10), places=6)
        self.assertAlmostEqual(stats["K%"], 25.0, places=6)

    def test_rank_active_roster_candidates_uses_recent_and_season_volume_first(self) -> None:
        rows = []
        for index in range(13):
            rows.append(
                {
                    "Batter": f"Player {index}",
                    "__recent14d_pa": 30 - index,
                    "__season_pa": 100 - index,
                    "__recent14d_ops": 0.700 + (index * 0.001),
                    "__season_ops": 0.750 + (index * 0.001),
                }
            )

        ranked = rank_active_roster_candidates(rows)
        self.assertEqual(ranked[0]["Batter"], "Player 0")
        self.assertEqual(ranked[-1]["Batter"], "Player 12")

    def test_sort_batters_for_report_pushes_low_sample_behind_sampled_rows(self) -> None:
        rows = [
            {
                "Batter": "Low Sample Star",
                "Status": "Scheduled",
                "Recent PA": 4,
                "Recent OPS": 1.200,
                "Recent OBP": 0.500,
                "Recent SLG": 0.700,
                "Recent K%": 10.0,
                "VsP OPS": 1.000,
                "Season OPS": 0.900,
                "Hit Stk": 2,
                "Pool Rank": 1,
            },
            {
                "Batter": "Full Sample",
                "Status": "Scheduled",
                "Recent PA": 18,
                "Recent OPS": 0.900,
                "Recent OBP": 0.400,
                "Recent SLG": 0.500,
                "Recent K%": 15.0,
                "VsP OPS": 0.800,
                "Season OPS": 0.850,
                "Hit Stk": 1,
                "Pool Rank": 2,
            },
        ]

        scored = apply_hot_scores(rows)
        sorted_df = sort_batters_for_report(scored)
        self.assertEqual(list(sorted_df["Batter"]), ["Full Sample", "Low Sample Star"])

    def test_sort_batters_for_report_moves_in_progress_to_bottom(self) -> None:
        rows = [
            {
                "Batter": "Live Bat",
                "Status": "In Progress",
                "Recent PA": 18,
                "Recent OPS": 1.000,
                "Recent OBP": 0.430,
                "Recent SLG": 0.570,
                "Recent K%": 10.0,
                "VsP OPS": 0.950,
                "Season OPS": 0.860,
                "Hit Stk": 7,
                "Pool Rank": 1,
            },
            {
                "Batter": "Pregame Bat",
                "Status": "Scheduled",
                "Recent PA": 18,
                "Recent OPS": 0.820,
                "Recent OBP": 0.360,
                "Recent SLG": 0.460,
                "Recent K%": 14.0,
                "VsP OPS": 0.720,
                "Season OPS": 0.790,
                "Hit Stk": 4,
                "Pool Rank": 2,
            },
            {
                "Batter": "Final Bat",
                "Status": "Final",
                "Recent PA": 18,
                "Recent OPS": 0.840,
                "Recent OBP": 0.365,
                "Recent SLG": 0.475,
                "Recent K%": 13.0,
                "VsP OPS": 0.730,
                "Season OPS": 0.800,
                "Hit Stk": 5,
                "Pool Rank": 3,
            },
        ]

        scored = apply_hot_scores(rows)
        sorted_df = sort_batters_for_report(scored)
        self.assertEqual(list(sorted_df["Batter"]), ["Pregame Bat", "Final Bat", "Live Bat"])

    def test_build_active_hit_streak_section_filters_and_sorts_live_games_last(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Scheduled Streak",
                    "Team": "T1",
                    "Opponent": "O1",
                    "Pitcher": "P1",
                    "Hit Stk": ACTIVE_STREAK_SECTION_MIN,
                    "Recent AVG": 0.310,
                    "VsP AVG": 0.250,
                    "Season AVG": 0.280,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "Live Streak",
                    "Team": "T2",
                    "Opponent": "O2",
                    "Pitcher": "P2",
                    "Hit Stk": ACTIVE_STREAK_SECTION_MIN + 2,
                    "Recent AVG": 0.340,
                    "VsP AVG": 0.320,
                    "Season AVG": 0.290,
                    "Status": "In Progress",
                },
                {
                    "Batter": "Too Short",
                    "Team": "T3",
                    "Opponent": "O3",
                    "Pitcher": "P3",
                    "Hit Stk": ACTIVE_STREAK_SECTION_MIN - 1,
                    "Recent AVG": 0.350,
                    "VsP AVG": 0.400,
                    "Season AVG": 0.300,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "Final Streak",
                    "Team": "T4",
                    "Opponent": "O4",
                    "Pitcher": "P4",
                    "Hit Stk": ACTIVE_STREAK_SECTION_MIN + 1,
                    "Recent AVG": 0.300,
                    "VsP AVG": 0.260,
                    "Season AVG": 0.275,
                    "Status": "Final",
                },
            ]
        )

        streaks = build_active_hit_streak_section(df)
        self.assertEqual(list(streaks["Batter"]), ["Scheduled Streak", "Final Streak", "Live Streak"])

    def test_build_focus_sections_filter_and_de_duplicate(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "A",
                    "Team": "T1",
                    "Opponent": "O1",
                    "Pitcher": "P1",
                    "Hit Stk": STREAK_SECTION_MIN,
                    "Recent AVG": 0.320,
                    "VsP AVG": 0.333,
                    "VsP PA": 9,
                    "Season AVG": 0.290,
                    "Start": "1:05p",
                    "Status": "Scheduled",
                    "Source": "ESPN Confirmed",
                },
                {
                    "Batter": "B",
                    "Team": "T2",
                    "Opponent": "O2",
                    "Pitcher": "P2",
                    "Hit Stk": STREAK_SECTION_MIN - 1,
                    "Recent AVG": 0.290,
                    "VsP AVG": 0.360,
                    "VsP PA": 8,
                    "Season AVG": 0.250,
                    "Start": "4:10p",
                    "Status": "In Progress",
                    "Source": "ESPN Confirmed",
                },
                {
                    "Batter": "C",
                    "Team": "T3",
                    "Opponent": "O3",
                    "Pitcher": "P3",
                    "Hit Stk": 1,
                    "Recent AVG": 0.260,
                    "VsP AVG": 0.310,
                    "VsP PA": 7,
                    "Season AVG": 0.255,
                    "Start": "7:10p",
                    "Status": "Scheduled",
                    "Source": "Active Roster",
                },
            ]
        )

        hot = build_hot_streak_matchup_section(df)
        self.assertEqual(list(hot["Batter"]), ["A"])

        matchups = build_good_matchups_section(df, hot)
        self.assertEqual(list(matchups["Batter"]), ["C", "B"])


if __name__ == "__main__":
    unittest.main()
