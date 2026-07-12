import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import Matchups as matchups_module

from Matchups import (
    GameMatchup,
    OffenseMatchup,
    ParkContext,
    _build_summary_chips,
    _build_summary_lean,
    _collect_best_spots,
    _compute_summary_score,
    _game_anchor_id,
    _metric_tone,
    _pitcher_last_name,
    _rank_tone,
    _render_game_detail_card,
    _render_game_header,
    _render_hand_split_section,
    _render_offense_panel,
    _render_summary_card,
    _render_summary_chip_spans,
    _select_summary_badges,
    _sort_matchups,
    write_html,
    build_pitcher_recent_form,
)
from report_data import (
    aggregate_stat_lines,
    build_metric_rank_index,
    extract_open_meteo_hourly_park_context,
    extract_espn_odds,
    extract_espn_scoreboard_snapshot,
    extract_last_game_lineup_player_ids_from_boxscore,
    fetch_park_context,
    get_park_metadata,
    parse_team_split_stats,
)


class MatchupsLogicTests(unittest.TestCase):
    def _sample_offense(self, *, team_name: str = "Miami Marlins", team_abbrev: str = "MIA", pitcher_name: str = "Braxton Ashcraft", lineup_source: str = "ESPN Confirmed") -> OffenseMatchup:
        badges = ["Lineup Hot", "Pitcher Cold", "Low Sample"]
        recent7_stats = {"OPS": 0.801, "AVG": 0.281, "K%": 20.1, "HR": 8}
        recent14_stats = {"OPS": 0.776, "AVG": 0.268, "K%": 21.8, "HR": 11}
        hand_split_ranks = {"OPS": 8, "AVG": 11, "K%": 15, "HR": 6}
        return OffenseMatchup(
            team_id=146,
            team_name=team_name,
            team_abbrev=team_abbrev,
            opponent_id=134,
            opponent_name="Pittsburgh Pirates",
            opponent_abbrev="PIT",
            pitcher_name=pitcher_name,
            pitcher_hand="R",
            lineup_source=lineup_source,
            selected_player_ids=[],
            lineup_names=[],
            matchup_stats={"PA": 12, "OPS": 0.812, "AVG": 0.278, "K%": 21.0, "HR": 2},
            recent7_stats=recent7_stats,
            recent14_stats=recent14_stats,
            hand_split_stats={"OPS": 0.744, "AVG": 0.251, "K%": 22.4, "HR": 39},
            hand_split_ranks=hand_split_ranks,
            pitcher_id=1,
            pitcher_season={"ERA": 3.41, "WHIP": 1.13, "K/9": 9.4, "AVG": 0.231},
            pitcher_season_ranks={"ERA": 42, "WHIP": 29, "K/9": 35, "AVG": 54},
            pitcher_recent={"Starts": 5, "ERA": 4.02, "WHIP": 1.21, "K/9": 8.8, "AVG": 0.254},
            badges=badges,
            summary_chips=_build_summary_chips(badges),
            summary_lean=_build_summary_lean(badges, recent7_stats, recent14_stats, hand_split_ranks),
            summary_score=_compute_summary_score(badges),
        )

    def test_extract_espn_odds_reads_full_pickcenter_payload(self) -> None:
        summary = {
            "pickcenter": [
                {
                    "provider": {"name": "DraftKings"},
                    "details": "PIT -149",
                    "overUnder": 8.5,
                    "overOdds": -112,
                    "underOdds": -107,
                    "awayTeamOdds": {"moneyLine": 123},
                    "homeTeamOdds": {"moneyLine": -149},
                }
            ]
        }

        odds = extract_espn_odds(summary)
        self.assertEqual(odds["provider"], "DraftKings")
        self.assertEqual(odds["total"], 8.5)
        self.assertEqual(odds["away_moneyline"], 123)
        self.assertEqual(odds["home_moneyline"], -149)
        self.assertEqual(odds["over_odds"], -112)
        self.assertEqual(odds["under_odds"], -107)

    def test_extract_espn_odds_falls_back_to_odds_block(self) -> None:
        summary = {
            "pickcenter": [],
            "odds": [
                {
                    "provider": {"name": "ESPN Odds"},
                    "overUnder": 7.5,
                    "awayTeamOdds": {"moneyLine": -105},
                    "homeTeamOdds": {"moneyLine": -115},
                }
            ],
        }

        odds = extract_espn_odds(summary)
        self.assertEqual(odds["provider"], "ESPN Odds")
        self.assertEqual(odds["total"], 7.5)
        self.assertEqual(odds["away_moneyline"], -105)
        self.assertEqual(odds["home_moneyline"], -115)

    def test_extract_espn_odds_returns_blanks_when_missing(self) -> None:
        odds = extract_espn_odds({"pickcenter": [], "odds": []})
        self.assertIsNone(odds["provider"])
        self.assertIsNone(odds["total"])
        self.assertIsNone(odds["away_moneyline"])
        self.assertIsNone(odds["home_moneyline"])

    def test_extract_espn_scoreboard_snapshot_reads_live_scores_and_state(self) -> None:
        snapshot = extract_espn_scoreboard_snapshot(
            {
                "id": "401815722",
                "status": {
                    "type": {
                        "state": "in",
                        "detail": "Top 4th",
                        "shortDetail": "Top 4th",
                    }
                },
                "competitions": [
                    {
                        "competitors": [
                            {
                                "homeAway": "away",
                                "score": "3",
                                "team": {"displayName": "Miami Marlins"},
                            },
                            {
                                "homeAway": "home",
                                "score": "1",
                                "team": {"displayName": "Pittsburgh Pirates"},
                            },
                        ]
                    }
                ],
            }
        )

        self.assertEqual(snapshot["event_id"], "401815722")
        self.assertEqual(snapshot["status_state"], "in")
        self.assertEqual(snapshot["status_short_detail"], "Top 4th")
        self.assertEqual(snapshot["away_score"], 3)
        self.assertEqual(snapshot["home_score"], 1)

    def test_extract_last_game_lineup_player_ids_from_home_boxscore(self) -> None:
        boxscore = {
            "home": {"team": {"id": 121}, "battingOrder": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
            "away": {"team": {"id": 138}, "battingOrder": [11, 12, 13, 14, 15, 16, 17, 18, 19]},
        }

        self.assertEqual(
            extract_last_game_lineup_player_ids_from_boxscore(boxscore, 121),
            [1, 2, 3, 4, 5, 6, 7, 8, 9],
        )

    def test_extract_last_game_lineup_player_ids_from_away_boxscore(self) -> None:
        boxscore = {
            "home": {"team": {"id": 121}, "battingOrder": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
            "away": {"team": {"id": 138}, "battingOrder": [11, 12, 13, 14, 15, 16, 17, 18, 19]},
        }

        self.assertEqual(
            extract_last_game_lineup_player_ids_from_boxscore(boxscore, 138),
            [11, 12, 13, 14, 15, 16, 17, 18, 19],
        )

    def test_aggregate_stat_lines_rolls_up_lineup_matchup_math(self) -> None:
        lines = [
            {"PA": 8, "AB": 7, "H": 3, "BB": 1, "HBP": 0, "SF": 0, "TB": 5, "K": 2, "HR": 1, "RBI": 3},
            {"PA": 6, "AB": 5, "H": 1, "BB": 1, "HBP": 0, "SF": 0, "TB": 1, "K": 3, "HR": 0, "RBI": 0},
        ]

        stats = aggregate_stat_lines(lines)
        self.assertEqual(stats["PA"], 14)
        self.assertEqual(stats["AB"], 12)
        self.assertEqual(stats["H"], 4)
        self.assertEqual(stats["BB"], 2)
        self.assertEqual(stats["TB"], 6)
        self.assertEqual(stats["HR"], 1)
        self.assertAlmostEqual(stats["AVG"], 4 / 12, places=6)
        self.assertAlmostEqual(stats["OBP"], 6 / 14, places=6)
        self.assertAlmostEqual(stats["SLG"], 6 / 12, places=6)
        self.assertAlmostEqual(stats["OPS"], (6 / 14) + (6 / 12), places=6)
        self.assertAlmostEqual(stats["K%"], (5 / 14) * 100.0, places=6)

    def test_aggregate_stat_lines_handles_zero_denominator(self) -> None:
        stats = aggregate_stat_lines([{"PA": 0, "AB": 0, "H": 0, "BB": 0, "HBP": 0, "SF": 0, "TB": 0, "K": 0, "HR": 0, "RBI": 0}])
        self.assertIsNone(stats["AVG"])
        self.assertIsNone(stats["OBP"])
        self.assertIsNone(stats["SLG"])
        self.assertIsNone(stats["OPS"])
        self.assertIsNone(stats["K%"])

    def test_parse_team_split_stats_maps_handedness_snapshot(self) -> None:
        parsed = parse_team_split_stats(
            {
                "plateAppearances": 120,
                "atBats": 100,
                "hits": 28,
                "baseOnBalls": 15,
                "hitByPitch": 2,
                "sacFlies": 1,
                "totalBases": 47,
                "strikeOuts": 31,
                "homeRuns": 6,
                "rbi": 22,
            }
        )

        self.assertEqual(parsed["PA"], 120)
        self.assertEqual(parsed["HR"], 6)
        self.assertAlmostEqual(parsed["AVG"], 0.28, places=6)
        self.assertAlmostEqual(parsed["OBP"], 45 / 118, places=6)
        self.assertAlmostEqual(parsed["SLG"], 0.47, places=6)
        self.assertAlmostEqual(parsed["OPS"], (45 / 118) + 0.47, places=6)
        self.assertAlmostEqual(parsed["K%"], (31 / 120) * 100.0, places=6)

    def test_build_metric_rank_index_supports_high_and_low_metrics(self) -> None:
        ranks = build_metric_rank_index(
            [
                {"id": 1, "OPS": 0.781, "K%": 24.2},
                {"id": 2, "OPS": 0.744, "K%": 18.0},
                {"id": 3, "OPS": 0.812, "K%": 26.7},
            ],
            identifier_key="id",
            metric_directions={"OPS": True, "K%": False},
        )

        self.assertEqual(ranks[3]["OPS"], 1)
        self.assertEqual(ranks[1]["OPS"], 2)
        self.assertEqual(ranks[2]["OPS"], 3)
        self.assertEqual(ranks[2]["K%"], 1)
        self.assertEqual(ranks[1]["K%"], 2)
        self.assertEqual(ranks[3]["K%"], 3)

    def test_pitcher_last_name_drops_hand_and_suffix(self) -> None:
        self.assertEqual(_pitcher_last_name("Ronald Bolanos Jr. (R)"), "Bolanos")
        self.assertEqual(_pitcher_last_name("Chris Sale"), "Sale")
        self.assertEqual(_pitcher_last_name("TBD"), "TBD")

    def test_metric_tone_supports_deeper_negative_tier(self) -> None:
        self.assertEqual(_metric_tone(0.185, elite=0.320, strong=0.285, weak=0.225, poor=0.190), "metric-poor")
        self.assertEqual(_metric_tone(33.5, elite=18.0, strong=22.0, weak=28.0, poor=33.0, inverse=True), "metric-poor")

    def test_rank_tone_adds_good_and_bad_color_classes(self) -> None:
        self.assertEqual(_rank_tone(3), "rank-elite")
        self.assertEqual(_rank_tone(9), "rank-strong")
        self.assertEqual(_rank_tone(29), "rank-weak")
        self.assertEqual(_rank_tone(48), "rank-poor")

    def test_select_summary_badges_prioritizes_and_truncates(self) -> None:
        selected = _select_summary_badges(
            ["Pitcher Hot", "Low Sample", "Lineup Hot", "Weak vs Hand", "Strong BvP"],
            limit=3,
        )

        self.assertEqual(selected, ["Strong BvP", "Weak vs Hand", "Lineup Hot"])

    def test_build_summary_chips_maps_codes_and_tooltips(self) -> None:
        chips = _build_summary_chips(["Strong BvP", "Pitcher Hot", "Low Sample"])
        html = _render_summary_chip_spans(chips)

        self.assertEqual([chip["code"] for chip in chips], ["B+", "P+", "LS"])
        self.assertEqual(chips[1]["tooltip"], "Opposing pitcher is in strong recent form.")
        self.assertIn('data-tooltip="Strong lineup batter-vs-pitcher history."', html)
        self.assertIn('title="Opposing pitcher is in strong recent form."', html)
        self.assertIn(">Strong BvP<", html)
        self.assertIn(">Pitcher Hot<", html)
        self.assertIn(">Low Sample<", html)

    def test_build_summary_lean_covers_supported_paths(self) -> None:
        self.assertEqual(
            _build_summary_lean(["Pitcher TBD"], {"HR": 1}, {"HR": 2}, {"AVG": 20, "K%": 20, "HR": 20}),
            "",
        )
        self.assertEqual(
            _build_summary_lean(["Low Sample"], {"HR": 1}, {"HR": 2}, {"AVG": 20, "K%": 20, "HR": 20}),
            "",
        )
        self.assertEqual(
            _build_summary_lean(["Weak vs Hand", "Lineup Cold"], {"HR": 1}, {"HR": 2}, {"AVG": 20, "K%": 20, "HR": 20}),
            "Fade",
        )
        self.assertEqual(
            _build_summary_lean(["Strong vs Hand", "Lineup Hot"], {"HR": 7}, {"HR": 12}, {"AVG": 12, "K%": 15, "HR": 9}),
            "Attack: power",
        )
        self.assertEqual(
            _build_summary_lean(["Strong vs Hand", "Lineup Hot"], {"HR": 2}, {"HR": 4}, {"AVG": 9, "K%": 10, "HR": 18}),
            "Attack: contact",
        )
        self.assertEqual(
            _build_summary_lean(["Strong vs Hand", "Pitcher Cold"], {"HR": 2}, {"HR": 3}, {"AVG": 17, "K%": 15, "HR": 18}),
            "Attack",
        )

    def test_game_anchor_id_falls_back_without_event_id(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        game = GameMatchup(
            event_id="",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={},
            away_offense=away_offense,
            home_offense=home_offense,
            sort_datetime="2026-06-12T22:40:00Z",
        )

        self.assertEqual(_game_anchor_id(game), "game-mia-pit-2026-06-12t22-40-00z")

    def test_render_hand_split_section_omits_pa_and_uses_four_column_grid(self) -> None:
        html = _render_hand_split_section(
            "R",
            {"PA": 1887, "OPS": 0.702, "AVG": 0.247, "K%": 20.9, "HR": 41},
            {"OPS": 21, "AVG": 9, "K%": 9, "HR": 29},
        )

        self.assertIn('class="stat-grid stat-grid-4"', html)
        self.assertNotIn(">PA<", html)
        self.assertIn(">OPS<", html)
        self.assertIn(">AVG<", html)
        self.assertIn(">K%<", html)
        self.assertIn(">HR<", html)
        self.assertIn('class="stat-rank rank-strong">MLB #9</span>', html)
        self.assertIn('class="stat-rank rank-weak">MLB #29</span>', html)

    def test_render_game_header_inlines_moneylines_and_chip_starters(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        game = GameMatchup(
            event_id="401694912",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={"provider": "DraftKings", "away_moneyline": 123, "home_moneyline": -149, "total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            park_context=ParkContext(roof_type="open", temp_f=72.0, wind_mph=8.0, wind_dir="NW", precip_pct=15.0, source="Open-Meteo"),
        )

        html = _render_game_header(game)
        self.assertIn('class="team-chip-ml">+123</span>', html)
        self.assertIn('class="team-chip-ml">-149</span>', html)
        self.assertIn('class="total-pill">8.5</span>', html)
        self.assertIn('class="park-pill"', html)
        self.assertIn(">72° W8<", html)
        self.assertIn("Ashcraft (R)", html)
        self.assertIn("Alcantara (R)", html)
        self.assertNotIn("starter-ribbon", html)
        self.assertNotIn("DraftKings", html)
        self.assertNotIn("Moneyline", html)
        self.assertNotIn("Total 8.5", html)

    def test_render_game_header_swaps_to_live_scores_once_started(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        game = GameMatchup(
            event_id="401694912",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="In Progress",
            odds={"provider": "DraftKings", "away_moneyline": 123, "home_moneyline": -149, "total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="in",
            status_detail="Top 4th",
            away_score=3,
            home_score=1,
            park_context=ParkContext(roof_type="retractable", source="Static"),
        )

        html = _render_game_header(game)
        self.assertIn('class="team-chip-score">3</span>', html)
        self.assertIn('class="team-chip-score">1</span>', html)
        self.assertIn('class="total-pill">8.5</span>', html)
        self.assertIn(">Retractable<", html)
        self.assertIn("Ashcraft (R)", html)
        self.assertIn("Alcantara (R)", html)
        self.assertIn(">Top 4th<", html)
        self.assertNotIn('class="team-chip-ml">+123</span>', html)
        self.assertNotIn('class="team-chip-ml">-149</span>', html)

    def test_render_cards_apply_game_state_classes(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        live_game = GameMatchup(
            event_id="401694912",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="In Progress",
            odds={"total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="in",
            status_detail="Top 4th",
            away_score=3,
            home_score=1,
        )
        final_game = GameMatchup(
            event_id="401694913",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Final",
            odds={"total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="post",
            away_score=5,
            home_score=2,
        )

        live_summary_html = _render_summary_card(live_game, "./matchups-detail.html")
        final_detail_html = _render_game_detail_card(final_game)

        self.assertIn('class="summary-card game-state-in"', live_summary_html)
        self.assertIn('class="game-card game-state-post"', final_detail_html)

    def test_render_offense_panel_hides_source_text_and_uses_pitcher_last_name(self) -> None:
        html = _render_offense_panel(self._sample_offense(pitcher_name="Ronald Bolanos Jr."))

        self.assertIn("Vs Bolanos", html)
        self.assertNotIn("Vs Starter", html)
        self.assertNotIn("Opp SP", html)
        self.assertNotIn("ESPN Confirmed", html)
        self.assertIn("signal-positive", html)
        self.assertIn("signal-warning", html)

    def test_build_pitcher_recent_form_uses_last_available_starts(self) -> None:
        report_date = dt.date(2026, 6, 12)
        splits = [
            {
                "date": "2026-06-11",
                "game": {"gamePk": 4},
                "stat": {"gamesStarted": 1, "outs": 18, "earnedRuns": 1, "hits": 4, "baseOnBalls": 1, "strikeOuts": 7, "atBats": 22},
            },
            {
                "date": "2026-06-05",
                "game": {"gamePk": 3},
                "stat": {"gamesStarted": 1, "outs": 15, "earnedRuns": 3, "hits": 6, "baseOnBalls": 2, "strikeOuts": 5, "atBats": 20},
            },
            {
                "date": "2026-05-29",
                "game": {"gamePk": 2},
                "stat": {"gamesStarted": 1, "outs": 21, "earnedRuns": 0, "hits": 2, "baseOnBalls": 1, "strikeOuts": 8, "atBats": 23},
            },
            {
                "date": "2026-05-24",
                "game": {"gamePk": 1},
                "stat": {"gamesStarted": 0, "outs": 3, "earnedRuns": 0, "hits": 0, "baseOnBalls": 0, "strikeOuts": 1, "atBats": 1},
            },
        ]

        form = build_pitcher_recent_form(splits, report_date, limit=5)
        self.assertEqual(form["Starts"], 3)
        self.assertAlmostEqual(form["IP"], 18.0, places=6)
        self.assertAlmostEqual(form["IP/start"], 6.0, places=6)
        self.assertAlmostEqual(form["ERA"], 2.0, places=6)
        self.assertAlmostEqual(form["WHIP"], (12 + 4) / 18.0, places=6)
        self.assertAlmostEqual(form["K/9"], 10.0, places=6)
        self.assertAlmostEqual(form["BB/9"], 2.0, places=6)
        self.assertAlmostEqual(form["AVG"], 12 / 65, places=6)

    def test_sort_matchups_keeps_pregame_above_started_games(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        pregame = GameMatchup(
            event_id="1",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="pre",
            sort_datetime="2026-06-12T22:40:00Z",
        )
        live = GameMatchup(
            event_id="2",
            away_team_id=136,
            away_team_name="Seattle Mariners",
            away_team_abbrev="SEA",
            home_team_id=120,
            home_team_name="Washington Nationals",
            home_team_abbrev="WSH",
            start_time="6:45p",
            status="In Progress",
            odds={},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="in",
            sort_datetime="2026-06-12T22:45:00Z",
        )
        final = GameMatchup(
            event_id="3",
            away_team_id=135,
            away_team_name="San Diego Padres",
            away_team_abbrev="SD",
            home_team_id=110,
            home_team_name="Baltimore Orioles",
            home_team_abbrev="BAL",
            start_time="7:05p",
            status="Final",
            odds={},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="post",
            sort_datetime="2026-06-12T23:05:00Z",
        )

        ordered = _sort_matchups([final, live, pregame])
        self.assertEqual([game.event_id for game in ordered], ["1", "2", "3"])

    def test_collect_best_spots_filters_positive_scores_and_breaks_ties_by_order(self) -> None:
        away_offense = self._sample_offense(team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        away_offense.summary_score = 4
        away_offense.summary_chips = _build_summary_chips(["Strong BvP", "Lineup Hot"])

        home_offense = self._sample_offense(team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        home_offense.summary_score = 4
        home_offense.summary_chips = _build_summary_chips(["Strong vs Hand", "Pitcher Cold"])

        neutral_offense = self._sample_offense(team_abbrev="SEA", pitcher_name="MacKenzie Gore")
        neutral_offense.summary_score = 0
        neutral_offense.summary_chips = _build_summary_chips(["Low Sample"])

        game_one = GameMatchup(
            event_id="1",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={},
            away_offense=away_offense,
            home_offense=neutral_offense,
            status_state="pre",
            sort_datetime="2026-06-12T22:40:00Z",
        )
        game_two = GameMatchup(
            event_id="2",
            away_team_id=134,
            away_team_name="Pittsburgh Pirates",
            away_team_abbrev="PIT",
            home_team_id=120,
            home_team_name="Washington Nationals",
            home_team_abbrev="WSH",
            start_time="6:45p",
            status="Pre-Game",
            odds={},
            away_offense=home_offense,
            home_offense=neutral_offense,
            status_state="pre",
            sort_datetime="2026-06-12T22:45:00Z",
        )

        spots = _collect_best_spots([game_one, game_two])
        self.assertEqual([spot.display_label for spot in spots], ["MIA vs Alcantara", "PIT vs Ashcraft"])

    def test_get_park_metadata_returns_known_venue(self) -> None:
        metadata = get_park_metadata(31)
        self.assertIsNotNone(metadata)
        self.assertEqual(metadata["name"], "PNC Park")
        self.assertEqual(metadata["roof_type"], "open")

    def test_extract_open_meteo_hourly_park_context_uses_nearest_hour(self) -> None:
        context = extract_open_meteo_hourly_park_context(
            {
                "timezone": "America/New_York",
                "hourly": {
                    "time": ["2026-06-12T18:00", "2026-06-12T19:00", "2026-06-12T20:00"],
                    "temperature_2m": [69.0, 72.0, 75.0],
                    "wind_speed_10m": [5.0, 8.0, 11.0],
                    "wind_direction_10m": [0, 225, 270],
                    "precipitation_probability": [20.0, 15.0, 10.0],
                },
            },
            "2026-06-12T23:05:00Z",
        )

        self.assertEqual(context["temp_f"], 72.0)
        self.assertEqual(context["wind_mph"], 8.0)
        self.assertEqual(context["wind_dir"], "SW")
        self.assertEqual(context["precip_pct"], 15.0)
        self.assertEqual(context["source"], "Open-Meteo")

    def test_fetch_park_context_skips_weather_call_for_indoor_roofs(self) -> None:
        with patch("report_data.requests.get") as mock_get:
            context = fetch_park_context(12, "2026-06-12T23:05:00Z", "06/12/2026")

        self.assertEqual(context["roof_type"], "indoor")
        self.assertIsNone(context["temp_f"])
        mock_get.assert_not_called()

    def test_fetch_park_context_returns_none_for_unknown_venue(self) -> None:
        self.assertIsNone(fetch_park_context(999999, "2026-06-12T23:05:00Z", "06/12/2026"))

    def test_write_html_generates_summary_and_detail_pages(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        game = GameMatchup(
            event_id="401694912",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={"away_moneyline": 123, "home_moneyline": -149, "total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="pre",
            sort_datetime="2026-06-12T22:40:00Z",
            park_context=ParkContext(roof_type="open", temp_f=72.0, wind_mph=8.0, wind_dir="NW", precip_pct=15.0, source="Open-Meteo"),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            reports_dir = tmp_path / "reports"
            root_summary = tmp_path / "matchups.html"
            root_detail = tmp_path / "matchups-detail.html"
            reports_dir.mkdir(parents=True, exist_ok=True)
            (reports_dir / "report-06122026.html").write_text("pitchers", encoding="utf-8")
            (reports_dir / "batters-report-06122026.html").write_text("batters", encoding="utf-8")
            with patch.object(matchups_module, "REPORTS_DIR", reports_dir), patch.object(matchups_module, "ROOT_MATCHUPS_FILE", root_summary), patch.object(matchups_module, "ROOT_MATCHUPS_DETAIL_FILE", root_detail):
                archive_summary_path = write_html([game], "06122026", "06/12/2026")

            archive_detail_path = reports_dir / "matchups-detail-report-06122026.html"
            self.assertEqual(archive_summary_path, reports_dir / "matchups-report-06122026.html")
            self.assertTrue(root_summary.exists())
            self.assertTrue(root_detail.exists())
            self.assertTrue(archive_summary_path.exists())
            self.assertTrue(archive_detail_path.exists())

            root_summary_html = root_summary.read_text(encoding="utf-8")
            root_detail_html = root_detail.read_text(encoding="utf-8")
            archive_summary_html = archive_summary_path.read_text(encoding="utf-8")
            archive_detail_html = archive_detail_path.read_text(encoding="utf-8")

            self.assertIn('<link rel="icon" href="./favicon.svg" type="image/svg+xml">', root_summary_html)
            self.assertIn('<link rel="icon" href="./favicon.svg" type="image/svg+xml">', root_detail_html)
            self.assertIn('<link rel="icon" href="../favicon.svg" type="image/svg+xml">', archive_summary_html)
            self.assertIn('<link rel="icon" href="../favicon.svg" type="image/svg+xml">', archive_detail_html)
            self.assertIn('href="./matchups-detail.html#game-401694912"', root_summary_html)
            self.assertIn('href="./matchups-detail-report-06122026.html#game-401694912"', archive_summary_html)
            self.assertIn('id="game-401694912"', root_detail_html)
            self.assertIn('class="date-nav"', root_summary_html)
            self.assertIn('class="date-nav"', root_detail_html)
            self.assertIn("hero-nav-row", root_summary_html)
            self.assertIn("hero-nav-row", root_detail_html)
            self.assertIn(".date-pill-label {", root_summary_html)
            self.assertIn("display: none;", root_summary_html)
            self.assertIn("justify-content: flex-end;", root_summary_html)
            self.assertIn('class="summary-cards"', root_summary_html)
            self.assertNotIn('class="stat-grid"', root_summary_html)
            self.assertIn("Best Spots", root_summary_html)
            self.assertIn('class="summary-chip', root_summary_html)
            self.assertIn("Attack: power", root_summary_html)
            self.assertIn('class="park-pill"', root_summary_html)
            self.assertNotIn(">Watch<", root_summary_html)
            self.assertNotIn(">Wait<", root_summary_html)
            self.assertIn('class="cards"', root_detail_html)
            self.assertIn('class="stat-grid"', root_detail_html)
            self.assertIn('class="park-pill"', root_detail_html)
            self.assertIn('href="./matchups.html"', root_detail_html)
            self.assertIn('href="./report-06122026.html"', archive_summary_html)
            self.assertIn('href="./batters-report-06122026.html"', archive_summary_html)
            self.assertIn('href="./matchups-report-06122026.html"', archive_detail_html)
            self.assertIn('href="./matchups-detail-report-06122026.html#game-401694912"', archive_summary_html)

    def test_write_html_skips_root_pages_when_requested(self) -> None:
        away_offense = self._sample_offense(team_name="Miami Marlins", team_abbrev="MIA", pitcher_name="Sandy Alcantara")
        home_offense = self._sample_offense(team_name="Pittsburgh Pirates", team_abbrev="PIT", pitcher_name="Braxton Ashcraft")
        game = GameMatchup(
            event_id="401694912",
            away_team_id=146,
            away_team_name="Miami Marlins",
            away_team_abbrev="MIA",
            home_team_id=134,
            home_team_name="Pittsburgh Pirates",
            home_team_abbrev="PIT",
            start_time="6:40p",
            status="Pre-Game",
            odds={"away_moneyline": 123, "home_moneyline": -149, "total": 8.5},
            away_offense=away_offense,
            home_offense=home_offense,
            status_state="pre",
            sort_datetime="2026-06-12T22:40:00Z",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            reports_dir = tmp_path / "reports"
            root_summary = tmp_path / "matchups.html"
            root_detail = tmp_path / "matchups-detail.html"
            with patch.object(matchups_module, "REPORTS_DIR", reports_dir), patch.object(matchups_module, "ROOT_MATCHUPS_FILE", root_summary), patch.object(matchups_module, "ROOT_MATCHUPS_DETAIL_FILE", root_detail):
                write_html([game], "06122026", "06/12/2026", write_root=False)

            self.assertFalse(root_summary.exists())
            self.assertFalse(root_detail.exists())


if __name__ == "__main__":
    unittest.main()
