import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from mlb_pitcher_report.reports import batters as batters_module
from mlb_pitcher_report.reports.batters import (
    HOME_RUN_REPORT_COLUMNS,
    ACTIVE_STREAK_SECTION_MIN,
    RECENT_GAMES,
    RECENT_WINDOW_DAYS,
    STREAK_SECTION_MIN,
    _final_team_result,
    _final_total_result,
    apply_hot_scores,
    build_active_hit_streak_section,
    build_good_matchups_section,
    build_home_run_matchup_section,
    build_hot_streak_matchup_section,
    compute_hit_streak,
    compute_recent_metrics,
    extract_espn_game_total,
    format_home_run_focus_dataframe,
    format_report_dataframe,
    rank_active_roster_candidates,
    sort_batters_for_report,
    verify_historical_bvp_for_feature_candidates,
    write_html,
)
from mlb_pitcher_report.shared.report_data import parse_vs_pitcher_stats


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

    def test_final_indicators_resolve_win_and_over_under(self) -> None:
        self.assertEqual(_final_team_result("Final", 6, 4), "win")
        self.assertEqual(_final_team_result("Final", 2, 5), "loss")
        self.assertEqual(_final_team_result("Scheduled", 6, 4), "")
        self.assertEqual(_final_total_result("Final", 8.5, 6, 4), "over")
        self.assertEqual(_final_total_result("Final", 9.5, 3, 4), "under")
        self.assertEqual(_final_total_result("Final", 8.0, 5, 3), "push")

    def test_extract_espn_game_total_prefers_pickcenter_over_under(self) -> None:
        summary = {
            "pickcenter": [
                {"overUnder": 8.5},
            ]
        }

        self.assertEqual(extract_espn_game_total(summary), 8.5)

    def test_extract_espn_game_total_returns_none_when_missing(self) -> None:
        self.assertIsNone(extract_espn_game_total({"pickcenter": []}))

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

    def test_current_day_game_log_does_not_qualify_hot_streaks(self) -> None:
        report_date = dt.date(2026, 7, 11)
        people_by_id = {
            1: {
                "id": 1,
                "fullName": "Today Hot",
                "stats": [
                    {
                        "type": {"displayName": "gameLog"},
                        "splits": [
                            {
                                "date": "2026-07-11",
                                "game": {"gamePk": 99},
                                "stat": {
                                    "atBats": 4,
                                    "hits": 1,
                                    "baseOnBalls": 0,
                                    "hitByPitch": 0,
                                    "sacFlies": 0,
                                    "plateAppearances": 4,
                                    "totalBases": 1,
                                    "strikeOuts": 1,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            },
                            {
                                "date": "2026-07-10",
                                "game": {"gamePk": 98},
                                "stat": {
                                    "atBats": 4,
                                    "hits": 1,
                                    "baseOnBalls": 0,
                                    "hitByPitch": 0,
                                    "sacFlies": 0,
                                    "plateAppearances": 4,
                                    "totalBases": 1,
                                    "strikeOuts": 1,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            },
                            {
                                "date": "2026-07-09",
                                "game": {"gamePk": 97},
                                "stat": {
                                    "atBats": 4,
                                    "hits": 1,
                                    "baseOnBalls": 0,
                                    "hitByPitch": 0,
                                    "sacFlies": 0,
                                    "plateAppearances": 4,
                                    "totalBases": 1,
                                    "strikeOuts": 1,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            },
                            {
                                "date": "2026-07-08",
                                "game": {"gamePk": 96},
                                "stat": {
                                    "atBats": 4,
                                    "hits": 0,
                                    "baseOnBalls": 0,
                                    "hitByPitch": 0,
                                    "sacFlies": 0,
                                    "plateAppearances": 4,
                                    "totalBases": 0,
                                    "strikeOuts": 1,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            },
                        ],
                    },
                    {
                        "type": {"displayName": "vsPlayer"},
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
                                    "strikeOuts": 0,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            }
                        ],
                    },
                ],
            }
        }

        rows = batters_module.build_candidate_rows(
            team_id=1,
            team_name="Team A",
            team_abbrev="TMA",
            opponent_id=2,
            opponent_name="Team B",
            opponent_abbrev="TMB",
            pitcher_name="Pitcher",
            game_total=8.5,
            pitch_hand="R",
            pitcher_id=None,
            start_time="7:05p",
            status="Scheduled",
            game_id=99,
            team_score=None,
            opponent_score=None,
            team_result="",
            total_result="",
            final_total_runs=None,
            roster_entries=[{"person": {"id": 1, "fullName": "Today Hot"}}],
            people_by_id=people_by_id,
            report_date=report_date,
        )

        self.assertEqual(rows[0]["Hit Stk"], 2)
        hot = build_hot_streak_matchup_section(apply_hot_scores(rows))
        self.assertTrue(hot.empty)

    def test_final_game_markers_use_current_game_result_lines(self) -> None:
        report_date = dt.date(2026, 7, 11)
        people_by_id = {
            1: {
                "id": 1,
                "fullName": "Final Marker",
                "stats": [
                    {
                        "type": {"displayName": "gameLog"},
                        "splits": [
                            {
                                "date": "2026-07-10",
                                "game": {"gamePk": 98},
                                "stat": {
                                    "atBats": 4,
                                    "hits": 1,
                                    "baseOnBalls": 0,
                                    "hitByPitch": 0,
                                    "sacFlies": 0,
                                    "plateAppearances": 4,
                                    "totalBases": 1,
                                    "strikeOuts": 1,
                                    "homeRuns": 0,
                                    "rbi": 0,
                                },
                            }
                        ],
                    }
                ],
            }
        }

        rows = batters_module.build_candidate_rows(
            team_id=1,
            team_name="Team A",
            team_abbrev="TMA",
            opponent_id=2,
            opponent_name="Team B",
            opponent_abbrev="TMB",
            pitcher_name="Pitcher",
            game_total=8.5,
            pitch_hand="R",
            pitcher_id=None,
            start_time="7:05p",
            status="Final",
            game_id=99,
            team_score=5,
            opponent_score=3,
            team_result="win",
            total_result="under",
            final_total_runs=8,
            roster_entries=[{"person": {"id": 1, "fullName": "Final Marker"}}],
            people_by_id=people_by_id,
            report_date=report_date,
            current_game_batter_lines={1: {"H": 1, "HR": 1}},
        )

        self.assertEqual(rows[0]["Game Hit Result"], "hit")
        self.assertEqual(rows[0]["Game Home Run Result"], "home-run")

    def test_lineup_lock_reuses_pregame_ids_after_final(self) -> None:
        locks = {}
        pregame_ids = list(range(10, 19))

        selected_ids, changed = batters_module._resolve_lineup_ids_for_game_state(
            report_date="07/11/2026",
            game_id=100,
            team_id=200,
            pitcher_id=300,
            status="Pre-Game",
            confirmed_lineup_player_ids=pregame_ids,
            lineup_locks=locks,
        )
        self.assertTrue(changed)
        self.assertEqual(selected_ids, pregame_ids)

        selected_ids, changed = batters_module._resolve_lineup_ids_for_game_state(
            report_date="07/11/2026",
            game_id=100,
            team_id=200,
            pitcher_id=300,
            status="Final",
            confirmed_lineup_player_ids=list(range(90, 99)),
            lineup_locks=locks,
        )
        self.assertFalse(changed)
        self.assertEqual(selected_ids, pregame_ids)

    def test_late_final_lineup_is_ignored_without_pregame_lock(self) -> None:
        locks = {}

        selected_ids, changed = batters_module._resolve_lineup_ids_for_game_state(
            report_date="07/11/2026",
            game_id=100,
            team_id=200,
            pitcher_id=300,
            status="Final",
            confirmed_lineup_player_ids=list(range(90, 99)),
            lineup_locks=locks,
        )

        self.assertFalse(changed)
        self.assertEqual(selected_ids, [])
        self.assertEqual(locks, {})

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

    def test_build_home_run_matchup_section_filters_and_sorts_by_hr_rate(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Two Homer Bat",
                    "Team": "T1",
                    "Opponent": "O1",
                    "Pitcher": "P1",
                    "VsP HR": 2,
                    "VsP PA": 10,
                    "VsP H": 4,
                    "VsP AB": 7,
                    "Recent HR": 1,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "Rate Bat",
                    "Team": "T2",
                    "Opponent": "O2",
                    "Pitcher": "P2",
                    "VsP HR": 1,
                    "VsP PA": 4,
                    "VsP H": 2,
                    "VsP AB": 4,
                    "Recent HR": 0,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "Volume Bat",
                    "Team": "T3",
                    "Opponent": "O3",
                    "Pitcher": "P3",
                    "VsP HR": 1,
                    "VsP PA": 9,
                    "VsP H": 3,
                    "VsP AB": 8,
                    "Recent HR": 2,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "Live Homer",
                    "Team": "T4",
                    "Opponent": "O4",
                    "Pitcher": "P4",
                    "VsP HR": 3,
                    "VsP PA": 10,
                    "VsP H": 5,
                    "VsP AB": 9,
                    "Recent HR": 1,
                    "Status": "In Progress",
                },
                {
                    "Batter": "Too Small",
                    "Team": "T5",
                    "Opponent": "O5",
                    "Pitcher": "P5",
                    "VsP HR": 1,
                    "VsP PA": 3,
                    "VsP H": 1,
                    "VsP AB": 3,
                    "Recent HR": 1,
                    "Status": "Scheduled",
                },
                {
                    "Batter": "No Homers",
                    "Team": "T6",
                    "Opponent": "O6",
                    "Pitcher": "P6",
                    "VsP HR": 0,
                    "VsP PA": 7,
                    "VsP H": 2,
                    "VsP AB": 6,
                    "Recent HR": 0,
                    "Status": "Scheduled",
                },
            ]
        )

        home_run_df = build_home_run_matchup_section(df)
        self.assertEqual(list(home_run_df["Batter"]), ["Rate Bat", "Two Homer Bat", "Volume Bat", "Live Homer"])

    def test_format_report_dataframe_supports_home_run_focus_columns(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Slugger",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Scheduled",
                    "VsP HR": 2,
                    "VsP PA": 8,
                    "VsP H": 4,
                    "VsP AB": 7,
                    "Start": "6:40p",
                }
            ]
        )

        formatted = format_report_dataframe(df, columns=HOME_RUN_REPORT_COLUMNS)

        self.assertEqual(list(formatted.columns), HOME_RUN_REPORT_COLUMNS)
        self.assertEqual(formatted.iloc[0]["VsP HR"], "2")
        self.assertEqual(formatted.iloc[0]["VsP PA"], "8")
        self.assertEqual(formatted.iloc[0]["VsP HR/PA"], "25.0%")
        self.assertEqual(formatted.iloc[0]["VsP H-AB"], "4-7")

    def test_verifies_historical_bvp_before_good_matchup_section(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Split Mirage",
                    "Team": "Pittsburgh Pirates",
                    "Pitcher": "Gasser",
                    "Hit Stk": 1,
                    "Season AVG": 0.250,
                    "Recent AVG": 0.300,
                    "VsP PA": 10,
                    "VsP AB": 10,
                    "VsP H": 5,
                    "VsP HR": 0,
                    "VsP RBI": 0,
                    "VsP AVG": 0.500,
                    "VsP OPS": 1.000,
                    "VsP K%": 0.0,
                    "__player_id": 10,
                    "__pitcher_id": 30,
                    "Status": "Scheduled",
                }
            ]
        )

        with patch(
            "mlb_pitcher_report.reports.batters.fetch_pitcher_historical_batter_vs_pitcher_stat_lines",
            return_value={
                10: {
                    "PA": 2,
                    "AB": 2,
                    "H": 1,
                    "HR": 0,
                    "RBI": 0,
                    "AVG": 0.500,
                    "OPS": 1.000,
                    "K%": 0.0,
                }
            },
        ):
            verified = verify_historical_bvp_for_feature_candidates(df, dt.date(2026, 7, 12))

        self.assertEqual(verified.iloc[0]["VsP PA"], 2)
        self.assertTrue(build_good_matchups_section(verified, pd.DataFrame()).empty)

    def test_verifies_preliminary_avg_miss_before_good_matchup_section(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "False Negative",
                    "Team": "Philadelphia Phillies",
                    "Pitcher": "Skubal",
                    "Hit Stk": 1,
                    "Season AVG": 0.250,
                    "Recent AVG": 0.300,
                    "VsP PA": 11,
                    "VsP AB": 11,
                    "VsP H": 3,
                    "VsP HR": 0,
                    "VsP RBI": 0,
                    "VsP AVG": 3 / 11,
                    "VsP OPS": 0.600,
                    "VsP K%": 0.0,
                    "__player_id": 10,
                    "__pitcher_id": 30,
                    "Status": "Scheduled",
                }
            ]
        )

        with patch(
            "mlb_pitcher_report.reports.batters.fetch_pitcher_historical_batter_vs_pitcher_stat_lines",
            return_value={
                10: {
                    "PA": 6,
                    "AB": 6,
                    "H": 3,
                    "HR": 0,
                    "RBI": 0,
                    "AVG": 0.500,
                    "OPS": 1.100,
                    "K%": 10.0,
                }
            },
        ):
            verified = verify_historical_bvp_for_feature_candidates(df, dt.date(2026, 7, 12))

        good = build_good_matchups_section(verified, pd.DataFrame())
        self.assertEqual(verified.iloc[0]["VsP PA"], 6)
        self.assertEqual(verified.iloc[0]["VsP H"], 3)
        self.assertFalse(good.empty)
        self.assertEqual(good.iloc[0]["Batter"], "False Negative")

    def test_verifies_when_preliminary_pa_is_below_threshold_but_ab_qualifies(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Undercounted PA",
                    "Team": "Texas Rangers",
                    "Pitcher": "Javier",
                    "Hit Stk": 0,
                    "Season AVG": 0.290,
                    "Recent AVG": 0.290,
                    "VsP PA": 3,
                    "VsP AB": 4,
                    "VsP H": 2,
                    "VsP HR": 0,
                    "VsP RBI": 0,
                    "VsP AVG": 0.500,
                    "VsP OPS": 1.000,
                    "VsP K%": 0.0,
                    "__player_id": 10,
                    "__pitcher_id": 30,
                    "Status": "In Progress",
                }
            ]
        )

        with patch(
            "mlb_pitcher_report.reports.batters.fetch_pitcher_historical_batter_vs_pitcher_stat_lines",
            return_value={
                10: {
                    "PA": 5,
                    "AB": 5,
                    "H": 3,
                    "HR": 0,
                    "RBI": 1,
                    "AVG": 0.600,
                    "OPS": 1.400,
                    "K%": 40.0,
                }
            },
        ):
            verified = verify_historical_bvp_for_feature_candidates(df, dt.date(2026, 7, 12))

        good = build_good_matchups_section(verified, pd.DataFrame())
        self.assertEqual(verified.iloc[0]["VsP PA"], 5)
        self.assertEqual(verified.iloc[0]["VsP H"], 3)
        self.assertFalse(good.empty)
        self.assertEqual(good.iloc[0]["Batter"], "Undercounted PA")

    def test_format_report_dataframe_adds_final_game_hit_markers_to_batter_name(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Hit Guy",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Final",
                    "Game Hit Result": "hit",
                    "Hit Stk": 4,
                    "Team Result": "win",
                    "Final Total Runs": 11,
                    "Total Result": "over",
                    "Recent AVG": 0.286,
                    f"Last {RECENT_WINDOW_DAYS} AVG": 0.301,
                    "Season AVG": 0.274,
                    "VsP AVG": 0.333,
                    "VsP H": 3,
                    "VsP AB": 9,
                    "Start": "6:40p",
                },
                {
                    "Batter": "No Hit Guy",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Final",
                    "Game Hit Result": "no-hit",
                    "Hit Stk": 1,
                    "Team Result": "loss",
                    "Final Total Runs": 7,
                    "Total Result": "under",
                    "Recent AVG": 0.143,
                    f"Last {RECENT_WINDOW_DAYS} AVG": 0.211,
                    "Season AVG": 0.240,
                    "VsP AVG": 0.125,
                    "VsP H": 1,
                    "VsP AB": 8,
                    "Start": "6:40p",
                },
            ]
        )

        formatted = format_report_dataframe(df)

        self.assertIn("batter-game-mark-hit", formatted.iloc[0]["Batter"])
        self.assertIn("&#10003;", formatted.iloc[0]["Batter"])
        self.assertIn("batter-game-mark-no-hit", formatted.iloc[1]["Batter"])
        self.assertIn(">X</span>", formatted.iloc[1]["Batter"])

    def test_format_home_run_focus_dataframe_uses_home_run_markers_for_final_rows(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Single Only",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Final",
                    "Game Hit Result": "hit",
                    "Game Home Run Result": "no-home-run",
                    "VsP HR": 2,
                    "VsP PA": 8,
                    "VsP H": 4,
                    "VsP AB": 7,
                    "Start": "6:40p",
                },
                {
                    "Batter": "Went Deep",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Final",
                    "Game Hit Result": "hit",
                    "Game Home Run Result": "home-run",
                    "VsP HR": 1,
                    "VsP PA": 4,
                    "VsP H": 1,
                    "VsP AB": 4,
                    "Start": "6:40p",
                },
            ]
        )

        formatted = format_home_run_focus_dataframe(df)

        self.assertIn("batter-game-mark-no-hit", formatted.iloc[0]["Batter"])
        self.assertIn("No home run in this final game", formatted.iloc[0]["Batter"])
        self.assertNotIn("Had a hit in this final game", formatted.iloc[0]["Batter"])
        self.assertIn("batter-game-mark-hit", formatted.iloc[1]["Batter"])
        self.assertIn("Had a home run in this final game", formatted.iloc[1]["Batter"])

    def test_format_report_dataframe_replaces_season_hit_ab_with_last_14_avg(self) -> None:
        df = pd.DataFrame(
            [
                {
                    "Batter": "Sample Batter",
                    "Team": "Miami Marlins",
                    "Team Abbrev": "MIA",
                    "Team Id": 146,
                    "Opponent": "Pittsburgh Pirates",
                    "Opponent Abbrev": "PIT",
                    "Opponent Id": 134,
                    "Pitcher": "Braxton Ashcraft",
                    "Total": 8.5,
                    "Status": "Final",
                    "Hit Stk": 4,
                    "Team Result": "win",
                    "Final Total Runs": 11,
                    "Total Result": "over",
                    "Recent AVG": 0.286,
                    f"Last {RECENT_WINDOW_DAYS} AVG": 0.301,
                    "Recent H": 2,
                    "Recent AB": 7,
                    "Season AVG": 0.274,
                    "Season H": 30,
                    "Season AB": 110,
                    "VsP AVG": 0.333,
                    "VsP H": 3,
                    "VsP AB": 9,
                    "Start": "6:40p",
                }
            ]
        )

        formatted = format_report_dataframe(df)

        self.assertEqual(
            list(formatted.columns),
            [
                "Batter",
                "Opponent",
                "Pitcher",
                "Hit Stk",
                f"Last {RECENT_GAMES} AVG",
                f"Last {RECENT_WINDOW_DAYS} AVG",
                "Season AVG",
                "VsP AVG",
                "VsP H-AB",
            ],
        )
        self.assertEqual(formatted.iloc[0][f"Last {RECENT_WINDOW_DAYS} AVG"], "0.301")
        self.assertNotIn(f"Last {RECENT_GAMES} H-AB", formatted.columns)
        self.assertNotIn("Season H-AB", formatted.columns)
        self.assertIn("batter-team-badge", formatted.iloc[0]["Batter"])
        self.assertIn("total-badge total-badge-strong", formatted.iloc[0]["Opponent"])
        self.assertIn("status-pill status-final", formatted.iloc[0]["Opponent"])
        self.assertNotIn("team-result", formatted.iloc[0]["Batter"])
        self.assertNotIn("total-result", formatted.iloc[0]["Opponent"])


class BattersRenderTests(unittest.TestCase):
    def _sample_row(self) -> dict:
        return {
            "Batter": "Sample Batter",
            "Team": "Miami Marlins",
            "Team Abbrev": "MIA",
            "Team Id": 146,
            "Opponent": "Pittsburgh Pirates",
            "Opponent Abbrev": "PIT",
            "Opponent Id": 134,
            "Pitcher": "Braxton Ashcraft",
            "Total": 8.5,
            "Status": "Scheduled",
            "Hit Stk": 6,
            "Team Result": "",
            "Total Result": "",
            "Final Total Runs": pd.NA,
            "Recent AVG": 0.321,
            f"Last {RECENT_WINDOW_DAYS} AVG": 0.305,
            "Season AVG": 0.284,
            "VsP AVG": 0.333,
            "VsP H": 3,
            "VsP AB": 9,
            "VsP HR": 2,
            "VsP PA": 8,
            "Recent HR": 1,
            "Start": "6:40p",
            "Source": "ESPN Confirmed",
        }

    def test_write_html_renders_same_date_archive_tabs_and_date_nav(self) -> None:
        display_date = dt.date.today().strftime("%m/%d/%Y")
        report_key = display_date.replace("/", "")
        df = pd.DataFrame([self._sample_row()])

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            reports_dir = tmp_path / "reports"
            root_file = tmp_path / "batters.html"
            with patch.object(batters_module, "REPORTS_DIR", reports_dir), patch.object(
                batters_module,
                "ROOT_BATTERS_FILE",
                root_file,
            ):
                archive_path = write_html(df, df, df, df, report_key, display_date)

            archive_html = archive_path.read_text(encoding="utf-8")
            root_html = root_file.read_text(encoding="utf-8")

            self.assertIn('<link rel="icon" href="../favicon.svg" type="image/svg+xml">', archive_html)
            self.assertIn('<link rel="icon" href="./favicon.svg" type="image/svg+xml">', root_html)
            self.assertIn('class="date-nav"', archive_html)
            self.assertIn("hero-nav-row", archive_html)
            self.assertIn('class="featured-tables"', archive_html)
            self.assertEqual(archive_html.count('class="featured-tables"'), 1)
            self.assertEqual(archive_html.count('class="featured-column"'), 2)
            self.assertIn("grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);", archive_html)
            self.assertIn(".featured-tables table.pitchers-table {", archive_html)
            self.assertIn("min-width: 0;", archive_html)
            self.assertIn("<h2>Active Hit Streaks 6+ Games</h2>", archive_html)
            self.assertIn("<h2>Home Run History vs Scheduled Pitcher</h2>", archive_html)
            self.assertLess(
                archive_html.index("<h2>Hot Streaks With Pitcher History</h2>"),
                archive_html.index("<h2>Active Hit Streaks 6+ Games</h2>"),
            )
            self.assertLess(
                archive_html.index("<h2>Active Hit Streaks 6+ Games</h2>"),
                archive_html.index("<h2>Good Historical Matchups</h2>"),
            )
            self.assertIn(".date-pill-label {", archive_html)
            self.assertIn("display: none;", archive_html)
            self.assertIn("justify-content: flex-end;", archive_html)
            self.assertIn(f'href="./report-{report_key}.html"', archive_html)
            self.assertIn(f'href="./matchups-report-{report_key}.html"', archive_html)
            self.assertIn('href="./index.html"', root_html)
            self.assertIn('href="./matchups.html"', root_html)


if __name__ == "__main__":
    unittest.main()
