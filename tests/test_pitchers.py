import datetime as dt
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from bs4 import BeautifulSoup

from mlb_pitcher_report.reports import pitchers as pitchers_module
from mlb_pitcher_report.reports.pitchers import (
    BEST_K_ODDS_COLUMN,
    K_PA_COLUMN,
    MATCHUP_LINES_COLUMN,
    MATCHUP_SOURCE_COLUMN,
    MATCHUP_SOURCE_ESPN,
    MATCHUP_SOURCE_PREVIOUS_LINEUP,
    MATCHUP_SOURCE_SAVANT,
    OPP_HAND_K_COLUMN,
    OPP_HAND_K_RANK_COLUMN,
    OPP_LAST_10_K_COLUMN,
    OPP_LAST_5_K_COLUMN,
    PA_GP_COLUMN,
    RECENT_PITCHER_GAMES_COLUMN,
    START_TIME_COLUMN,
    _classify_best_odds_point,
    _classify_matchup_k_percent,
    _classify_matchup_sample_size,
    _extract_espn_lineup_matchup_stats,
    _format_recent_pitcher_game_line,
    _get_team_recent_k_lookup,
    _previous_lineup_k_percent,
    _render_best_k_odds_cell,
    _render_matchup_source_marker,
    build_opponent_hand_k_lookup,
    calculate_additional_metrics,
    fetch_pitcher_recent_game_lines,
    get_opp_data,
    prepare_team_batting_df,
    resolve_effective_report_date_and_schedule,
    summarize_pitcher_best_k_odds,
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
                    "AVG": 0.211,
                    PA_GP_COLUMN: 6.8,
                    "K/9": 10.1,
                    "Whiff%": 31.4,
                    K_PA_COLUMN: 28.6,
                    "K%": 29.4,
                    "PA": 25,
                    MATCHUP_SOURCE_COLUMN: "ESPN (AB)",
                    MATCHUP_LINES_COLUMN: ["Batter 1-3 2K", "Batter 0-2 1K"],
                    "SO/PA": 24.3,
                    OPP_HAND_K_COLUMN: 23.1,
                    OPP_HAND_K_RANK_COLUMN: 1,
                    OPP_LAST_5_K_COLUMN: 24.5,
                    OPP_LAST_10_K_COLUMN: 22.8,
                    "r": 4,
                    "Opponent": "Boston Red Sox",
                    START_TIME_COLUMN: "7:10p",
                    "Status": "Scheduled",
                    "Ks": "N/A",
                    RECENT_PITCHER_GAMES_COLUMN: ["v PIT 8K 98P", "@ MIL 6K 91P"],
                    "FanDuel": "6.5: +102|-118 || ALT: 5.5: -150|+120; 7.5: +160|-210",
                    "BetRivers": "6.5: +100|-108 || ALT: 7.5: +170|-220",
                    "Novig": "7.5: +125|-145 || ALT: 6.5: +104|-110",
                    "DraftKings": "6.5: +101|-115",
                },
                {
                    "Name": "Bravo Ball",
                    "Hand": "L",
                    "GP": 10,
                    "AB": 41,
                    "K": 12,
                    "AVG": 0.254,
                    PA_GP_COLUMN: 5.0,
                    "K/9": 8.7,
                    "Whiff%": 27.2,
                    K_PA_COLUMN: 24.4,
                    "K%": 26.2,
                    "PA": 18,
                    MATCHUP_SOURCE_COLUMN: "Savant (PA)",
                    MATCHUP_LINES_COLUMN: [],
                    "SO/PA": 22.1,
                    OPP_HAND_K_COLUMN: 21.0,
                    OPP_HAND_K_RANK_COLUMN: 12,
                    OPP_LAST_5_K_COLUMN: 19.4,
                    OPP_LAST_10_K_COLUMN: 20.7,
                    "r": 9,
                    "Opponent": "New York Yankees",
                    START_TIME_COLUMN: "4:10p",
                    "Status": "In Progress",
                    "Ks": 5,
                    RECENT_PITCHER_GAMES_COLUMN: [],
                    "FanDuel": "5.5: -104|-122 || ALT: 4.5: -220|+168",
                    "BetRivers": "5.5: -101|-119",
                },
                {
                    "Name": "Charlie Check",
                    "Hand": "R",
                    "GP": 11,
                    "AB": 45,
                    "K": 15,
                    "AVG": 0.239,
                    PA_GP_COLUMN: 3.8,
                    "K/9": 9.4,
                    "Whiff%": 28.7,
                    K_PA_COLUMN: 26.8,
                    "K%": 31.0,
                    "PA": 12,
                    MATCHUP_SOURCE_COLUMN: "ESPN (AB)",
                    MATCHUP_LINES_COLUMN: [],
                    "SO/PA": 25.6,
                    OPP_HAND_K_COLUMN: 19.7,
                    OPP_HAND_K_RANK_COLUMN: None,
                    OPP_LAST_5_K_COLUMN: 18.6,
                    OPP_LAST_10_K_COLUMN: 21.3,
                    "r": 15,
                    "Opponent": "Seattle Mariners",
                    START_TIME_COLUMN: "1:10p",
                    "Status": "Scheduled",
                    "Ks": "",
                    RECENT_PITCHER_GAMES_COLUMN: [],
                },
                {
                    "Name": "Delta Dart",
                    "Hand": "L",
                    "GP": 13,
                    "AB": 50,
                    "K": 13,
                    "AVG": 0.261,
                    PA_GP_COLUMN: 4.4,
                    "K/9": 8.1,
                    "Whiff%": 25.9,
                    K_PA_COLUMN: 22.7,
                    "K%": 16.4,
                    "PA": 22,
                    MATCHUP_SOURCE_COLUMN: "Savant (PA)",
                    MATCHUP_LINES_COLUMN: [],
                    "SO/PA": 19.2,
                    OPP_HAND_K_COLUMN: 18.8,
                    OPP_HAND_K_RANK_COLUMN: 30,
                    OPP_LAST_5_K_COLUMN: 17.9,
                    OPP_LAST_10_K_COLUMN: 18.7,
                    "r": 19,
                    "Opponent": "Houston Astros",
                    START_TIME_COLUMN: "8:10p",
                    "Status": "Final",
                    "Ks": 7,
                    RECENT_PITCHER_GAMES_COLUMN: [],
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
            archive_soup = BeautifulSoup(archive_html, "html.parser")
            header_texts = [
                th.get_text(strip=True)
                for th in archive_soup.select("table.pitchers-table thead th")
            ]
            pa_gp_index = header_texts.index(PA_GP_COLUMN)
            k_pct_index = header_texts.index("K%")
            pa_index = header_texts.index("PA")
            opponent_index = header_texts.index("Opponent")
            body_rows = archive_soup.select("table.pitchers-table tbody tr")
            alpha_pa_gp_cell = body_rows[0].find_all("td")[pa_gp_index]
            charlie_pa_gp_cell = body_rows[2].find_all("td")[pa_gp_index]
            alpha_k_pct_cell = body_rows[0].find_all("td")[k_pct_index]
            alpha_pa_cell = body_rows[0].find_all("td")[pa_index]
            bravo_k_pct_cell = body_rows[1].find_all("td")[k_pct_index]
            charlie_k_pct_cell = body_rows[2].find_all("td")[k_pct_index]
            charlie_pa_cell = body_rows[2].find_all("td")[pa_index]
            delta_k_pct_cell = body_rows[3].find_all("td")[k_pct_index]
            delta_pa_cell = body_rows[3].find_all("td")[pa_index]
            alpha_opponent_cell = body_rows[0].find_all("td")[opponent_index]
            bravo_opponent_cell = body_rows[1].find_all("td")[opponent_index]
            delta_opponent_cell = body_rows[3].find_all("td")[opponent_index]
            control_labels = [
                label.get_text(" ", strip=True)
                for label in archive_soup.select(".table-controls label.toggle-chip")
            ]

            self.assertIn("show-live-toggle", archive_html)
            self.assertIn("show-final-toggle", archive_html)
            self.assertIn('<link rel="icon" href="../favicon.svg" type="image/svg+xml">', archive_html)
            self.assertIn('<link rel="icon" href="./favicon.svg" type="image/svg+xml">', root_html)
            self.assertIn("hero-nav-row", archive_html)
            self.assertIn(">Show</span>", archive_html)
            self.assertIn("In Progress", control_labels)
            self.assertIn("Final", control_labels)
            self.assertNotIn("Hide In Progress", archive_html)
            self.assertNotIn("Hide Final", archive_html)
            self.assertIn('data-sort-key="GP"', archive_html)
            self.assertIn('data-sort-key="Ks"', archive_html)
            self.assertIn(f'data-sort-key="{PA_GP_COLUMN}"', archive_html)
            self.assertIn(f'data-sort-key="{OPP_LAST_5_K_COLUMN}"', archive_html)
            self.assertIn(f'data-sort-key="{OPP_LAST_10_K_COLUMN}"', archive_html)
            self.assertIn(f">{BEST_K_ODDS_COLUMN}</th>", archive_html)
            self.assertIn(f'data-sort-key="{K_PA_COLUMN}"', archive_html)
            self.assertIn(f">{PA_GP_COLUMN}</th>", archive_html)
            self.assertNotIn('data-sort-key="Name"', archive_html)
            self.assertNotIn('data-sort-key="Opponent"', archive_html)
            self.assertNotIn('data-sort-key="Status"', archive_html)
            self.assertNotIn(f'data-sort-key="{BEST_K_ODDS_COLUMN}"', archive_html)
            self.assertNotIn('data-sort-key="BB"', archive_html)
            self.assertNotIn('data-sort-key="Hand"', archive_html)
            self.assertNotIn(">BB</th>", archive_html)
            self.assertNotIn(">Hand</th>", archive_html)
            self.assertNotIn(">FanDuel</th>", archive_html)
            self.assertNotIn(">BetRivers</th>", archive_html)
            self.assertNotIn(">DraftKings</th>", archive_html)
            self.assertNotIn(">Novig</th>", archive_html)
            self.assertNotIn(">Status</th>", archive_html)
            self.assertNotIn("status-pill", archive_html)
            self.assertNotIn("K/AB", archive_html)
            self.assertNotIn("AB/GP", archive_html)
            self.assertNotIn(">N/A</td>", archive_html)
            self.assertIn("pitcher-name-cell", archive_html)
            self.assertIn("pitcher-has-recent", archive_html)
            self.assertIn("pitcher-recent-popup", archive_html)
            self.assertIn("v PIT 8K 98P", archive_html)
            self.assertIn("@ MIL 6K 91P", archive_html)
            self.assertNotIn(f">{RECENT_PITCHER_GAMES_COLUMN}</th>", archive_html)
            self.assertIn("matchup-k-cell matchup-k-has-popup matchup-k-has-lines", archive_html)
            self.assertIn("matchup-k-popup", archive_html)
            self.assertIn("matchup-k-source-line", archive_html)
            self.assertIn("ESPN confirmed lineup", archive_html)
            self.assertIn("Savant fallback", archive_html)
            self.assertIn("Batter 1-3 2K", archive_html)
            self.assertNotIn(f">{MATCHUP_LINES_COLUMN}</th>", archive_html)
            self.assertIn("opp-hand-rank-badge", archive_html)
            self.assertIn("--rank-hue: 140", archive_html)
            self.assertIn("--rank-hue: 0", archive_html)
            self.assertIn(">1</span>", archive_html)
            self.assertNotIn(">(1)</span>", archive_html)
            self.assertNotIn(f">{OPP_HAND_K_RANK_COLUMN}</th>", archive_html)
            self.assertIn("hand-marker hand-right", archive_html)
            self.assertIn("hand-marker hand-left", archive_html)
            self.assertIn("6.5 | O +104 NV | U -108 BR", archive_html)
            self.assertIn("best-odds-cell", archive_html)
            self.assertIn("best-odds-cell-missing-side", archive_html)
            self.assertIn("best-odds-point best-odds-point-strong", archive_html)
            self.assertIn("best-odds-point best-odds-point-neutral", archive_html)
            self.assertIn("sportsbook-badge-summary", archive_html)
            self.assertIn("sportsbook-badge-detail", archive_html)
            self.assertIn("--sportsbook-color:", archive_html)
            self.assertIn("odds-details-list", archive_html)
            self.assertIn('odds-line-label">5.5</span>', archive_html)
            self.assertIn('odds-line-label">7.5</span>', archive_html)
            self.assertLess(
                header_texts.index(OPP_LAST_10_K_COLUMN),
                header_texts.index("SO/PA"),
            )
            self.assertLess(
                header_texts.index("SO/PA"),
                header_texts.index("r"),
            )
            self.assertLess(
                header_texts.index("r"),
                header_texts.index("Ks"),
            )
            self.assertLess(
                header_texts.index("Ks"),
                header_texts.index("Opponent"),
            )
            self.assertLess(header_texts.index("Opponent"), header_texts.index(BEST_K_ODDS_COLUMN))
            self.assertIn("group-pitcher", alpha_pa_gp_cell.get("class", []))
            self.assertIn("cell-elite", alpha_pa_gp_cell.get("class", []))
            self.assertIn("cell-weak", charlie_pa_gp_cell.get("class", []))
            self.assertIn("cell-elite", alpha_k_pct_cell.get("class", []))
            self.assertEqual(alpha_k_pct_cell.select_one(".matchup-k-value").get_text(strip=True), "29.4")
            self.assertIsNone(alpha_k_pct_cell.select_one(".matchup-k-value .k-src-marker"))
            self.assertIsNotNone(alpha_k_pct_cell.select_one(".matchup-k-popup .k-src-marker.src-espn"))
            self.assertIsNotNone(bravo_k_pct_cell.select_one(".matchup-k-popup .k-src-marker.src-savant"))
            self.assertIn("cell-confidence-high", alpha_pa_cell.get("class", []))
            self.assertIn("cell-strong", bravo_k_pct_cell.get("class", []))
            self.assertIn("cell-low-confidence", charlie_k_pct_cell.get("class", []))
            self.assertNotIn("cell-strong", charlie_k_pct_cell.get("class", []))
            self.assertNotIn("cell-elite", charlie_k_pct_cell.get("class", []))
            self.assertIn("cell-confidence-low", charlie_pa_cell.get("class", []))
            self.assertNotIn("row-target", body_rows[2].get("class", []))
            self.assertIn("cell-weak", delta_k_pct_cell.get("class", []))
            self.assertIn("cell-confidence-high", delta_pa_cell.get("class", []))
            self.assertIn("opp-time-upcoming", alpha_opponent_cell.decode())
            self.assertIn("opp-time-live", bravo_opponent_cell.decode())
            self.assertIn("opp-time-final", delta_opponent_cell.decode())
            self.assertIn('data-initial-index="0"', archive_html)
            self.assertIn('data-sort-value="12"', archive_html)
            self.assertIn('href="./batters-report-' + report_key + '.html"', archive_html)
            self.assertIn('href="./matchups-report-' + report_key + '.html"', archive_html)
            self.assertNotIn("hide-live-toggle", archive_html)
            self.assertIn("justify-content: flex-end;", archive_html)
            self.assertIn(".date-pill-label {", archive_html)
            self.assertIn("display: none;", archive_html)
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
        with patch("mlb_pitcher_report.reports.pitchers.fetch_schedule", return_value=[{"status": "Final"}]):
            report_date, schedule = resolve_effective_report_date_and_schedule(
                "06/17/2026",
                allow_roll_forward=False,
            )

        self.assertEqual(report_date, "06/17/2026")
        self.assertEqual(schedule, [{"status": "Final"}])

    def test_team_recent_k_lookup_aggregates_last_5_and_last_10_before_report_date(self) -> None:
        cutoff_date = dt.date(2026, 6, 27)
        game_log_splits = [
            {
                "date": f"2026-06-{day:02d}",
                "game": {"gamePk": 1000 + day},
                "stat": {"strikeOuts": strikeouts, "plateAppearances": 40},
            }
            for day, strikeouts in [
                (20, 5),
                (17, 6),
                (19, 7),
                (18, 8),
                (21, 9),
                (22, 10),
                (23, 11),
                (24, 12),
                (25, 13),
                (26, 14),
            ]
        ]
        game_log_splits.extend(
            [
                {
                    "date": "2026-06-27",
                    "game": {"gamePk": 2026062701},
                    "stat": {"strikeOuts": 99, "plateAppearances": 99},
                },
                {
                    "date": "2026-06-28",
                    "game": {"gamePk": 2026062801},
                    "stat": {"strikeOuts": 88, "plateAppearances": 88},
                },
            ]
        )
        payload = {"stats": [{"splits": game_log_splits}]}

        with patch.object(pitchers_module, "TEAM_RECENT_K_CACHE", {}), patch(
            "mlb_pitcher_report.reports.pitchers.statsapi.get",
            return_value=payload,
        ):
            result = _get_team_recent_k_lookup(140, 2026, cutoff_date)

        self.assertAlmostEqual(result["last_5"], 30.0)
        self.assertAlmostEqual(result["last_10"], 23.75)

    def test_team_recent_k_lookup_handles_short_and_empty_history(self) -> None:
        cutoff_date = dt.date(2026, 4, 1)
        short_payload = {
            "stats": [
                {
                    "splits": [
                        {
                            "date": "2026-03-28",
                            "game": {"gamePk": 1},
                            "stat": {"strikeOuts": 7, "plateAppearances": 35},
                        },
                        {
                            "date": "2026-03-29",
                            "game": {"gamePk": 2},
                            "stat": {"strikeOuts": 9, "plateAppearances": 45},
                        },
                        {
                            "date": "2026-03-30",
                            "game": {"gamePk": 3},
                            "stat": {"strikeOuts": 8, "plateAppearances": 40},
                        },
                    ]
                }
            ]
        }
        empty_payload = {"stats": [{"splits": []}]}

        with patch.object(pitchers_module, "TEAM_RECENT_K_CACHE", {}), patch(
            "mlb_pitcher_report.reports.pitchers.statsapi.get",
            return_value=short_payload,
        ):
            short_result = _get_team_recent_k_lookup(111, 2026, cutoff_date)

        expected_short_rate = 100 * (7 + 9 + 8) / (35 + 45 + 40)
        self.assertAlmostEqual(short_result["last_5"], expected_short_rate)
        self.assertAlmostEqual(short_result["last_10"], expected_short_rate)

        with patch.object(pitchers_module, "TEAM_RECENT_K_CACHE", {}), patch(
            "mlb_pitcher_report.reports.pitchers.statsapi.get",
            return_value=empty_payload,
        ):
            empty_result = _get_team_recent_k_lookup(112, 2026, cutoff_date)

        self.assertIsNone(empty_result["last_5"])
        self.assertIsNone(empty_result["last_10"])

    def test_opponent_hand_lookup_includes_k_rank_by_pitcher_hand(self) -> None:
        schedule = [
            {
                "away_name": "Boston Red Sox",
                "away_id": 111,
                "home_name": "New York Yankees",
                "home_id": 222,
            }
        ]

        split_by_team = {
            111: {"vs_lhp": 28.0, "vs_rhp": 21.0},
            222: {"vs_lhp": 19.0, "vs_rhp": 24.0},
            333: {"vs_lhp": 25.0, "vs_rhp": 30.0},
        }

        with patch.object(pitchers_module, "TEAM_HAND_SPLIT_RANK_CACHE", {}), patch(
            "mlb_pitcher_report.reports.pitchers.fetch_mlb_team_ids",
            return_value=[111, 222, 333],
        ), patch(
            "mlb_pitcher_report.reports.pitchers._get_team_hand_split_k_lookup",
            side_effect=lambda team_id, season: split_by_team[team_id],
        ):
            lookup = build_opponent_hand_k_lookup(schedule, 2026)

        self.assertEqual(lookup["Boston Red Sox"]["vs_lhp"], 28.0)
        self.assertEqual(lookup["Boston Red Sox"]["vs_lhp_rank"], 1)
        self.assertEqual(lookup["Boston Red Sox"]["vs_rhp_rank"], 3)
        self.assertEqual(lookup["New York Yankees"]["vs_rhp_rank"], 2)

    def test_espn_lineup_matchup_stats_include_batter_hover_lines(self) -> None:
        athletes = []
        for index in range(9):
            athletes.append(
                {
                    "starter": True,
                    "batOrder": index + 1,
                    "athlete": {"shortName": f"Sample Batter {index + 1}", "lastName": f"Batter{index + 1}"},
                    "vsStats": [1, 3, 2 if index == 0 else 1],
                }
            )
        summary_data = {
            "boxscore": {
                "players": [
                    {
                        "team": {"abbreviation": "NYY"},
                        "statistics": [
                            {
                                "type": "batting",
                                "keys": ["hits", "atBats", "strikeouts"],
                                "athletes": athletes,
                            }
                        ],
                    }
                ]
            }
        }

        result = _extract_espn_lineup_matchup_stats(summary_data)

        self.assertIn("NYY", result)
        self.assertEqual(result["NYY"]["PA"], 27.0)
        self.assertAlmostEqual(result["NYY"]["K%"], 100 * 10 / 27)
        self.assertIn("Batter1 1-3 2K", result["NYY"][MATCHUP_LINES_COLUMN])

    def test_previous_lineup_k_percent_aggregates_batter_vs_pitcher_stats(self) -> None:
        lineup_ids = list(range(101, 110))

        def person(player_id: int, strikeouts: int, plate_appearances: int, hits: int, at_bats: int) -> dict:
            return {
                "id": player_id,
                "fullName": f"Hitter {player_id}",
                "lastName": f"Last{player_id}",
                "stats": [
                    {
                        "type": {"displayName": "vsPlayer"},
                        "group": {"displayName": "hitting"},
                        "splits": [
                            {
                                "season": "2026",
                                "stat": {
                                    "strikeOuts": strikeouts,
                                    "plateAppearances": plate_appearances,
                                    "hits": hits,
                                    "atBats": at_bats,
                                },
                            }
                        ],
                    }
                ],
            }

        people = {
            player_id: person(player_id, strikeouts=index + 1, plate_appearances=10, hits=1, at_bats=3)
            for index, player_id in enumerate(lineup_ids)
        }

        with patch.object(pitchers_module, "PREVIOUS_LINEUP_K_CACHE", {}), patch(
            "mlb_pitcher_report.reports.pitchers._fetch_previous_lineup_player_ids",
            return_value=lineup_ids,
        ), patch(
            "mlb_pitcher_report.reports.pitchers.fetch_hitter_people_stats_map",
            return_value=people,
        ) as fetch_people:
            result = _previous_lineup_k_percent(158, 2026, dt.date(2026, 7, 12), 999)

        self.assertIsNotNone(result)
        self.assertEqual(result["PA"], 90.0)
        self.assertAlmostEqual(result["K%"], 50.0)
        self.assertEqual(len(result[MATCHUP_LINES_COLUMN]), 9)
        self.assertIn("Last101 1-3 1K", result[MATCHUP_LINES_COLUMN])
        fetch_people.assert_called_once_with(
            lineup_ids,
            2026,
            None,
            999,
            stats_end_date=dt.date(2026, 7, 11),
        )

    def test_get_opp_data_prefers_previous_lineup_over_savant_when_espn_missing(self) -> None:
        savant_df = pd.DataFrame(
            [
                {
                    "Pitcher": "Alpha Ace",
                    "Hand": "R",
                    "PA": 4,
                    "K%": 10.0,
                    MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_SAVANT,
                    MATCHUP_LINES_COLUMN: [],
                }
            ]
        )
        previous_df = pd.DataFrame(
            [
                {
                    "Pitcher": "Alpha Ace",
                    "PA": 90,
                    "K%": 24.4,
                    MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_PREVIOUS_LINEUP,
                    MATCHUP_LINES_COLUMN: ["One 1-3 2K"],
                }
            ]
        )
        espn_df = pd.DataFrame(columns=["Pitcher", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])

        with patch("mlb_pitcher_report.reports.pitchers.get_savant_opp_data", return_value=savant_df), patch(
            "mlb_pitcher_report.reports.pitchers.get_espn_opp_data",
            return_value=espn_df,
        ), patch("mlb_pitcher_report.reports.pitchers.get_previous_lineup_opp_data", return_value=previous_df):
            result = get_opp_data("07/12/2026", [])

        alpha = result.loc[result["Pitcher"] == "Alpha Ace"].iloc[0]
        self.assertEqual(alpha["Hand"], "R")
        self.assertEqual(alpha["PA"], 90)
        self.assertEqual(alpha["K%"], 24.4)
        self.assertEqual(alpha[MATCHUP_SOURCE_COLUMN], MATCHUP_SOURCE_PREVIOUS_LINEUP)
        self.assertEqual(alpha[MATCHUP_LINES_COLUMN], ["One 1-3 2K"])

    def test_matchup_source_marker_includes_previous_lineup_marker(self) -> None:
        html = _render_matchup_source_marker(MATCHUP_SOURCE_PREVIOUS_LINEUP)

        self.assertIn(">P</span>", html)
        self.assertIn("Previous completed lineup BvP sample", html)

    def test_format_recent_pitcher_game_line_uses_opponent_strikeouts_pitch_count_and_location(self) -> None:
        home_split = {
            "opponent": {"name": "Pittsburgh Pirates"},
            "stat": {"strikeOuts": 8, "numberOfPitches": 98},
            "isHome": True,
        }
        away_split = {
            "opponent": {"name": "Milwaukee Brewers"},
            "stat": {"strikeOuts": 6, "numberOfPitches": 91},
            "isHome": False,
        }

        self.assertEqual(_format_recent_pitcher_game_line(home_split), "v PIT 8K 98P")
        self.assertEqual(_format_recent_pitcher_game_line(away_split), "@ MIL 6K 91P")

    def test_fetch_pitcher_recent_game_lines_uses_last_five_before_report_date(self) -> None:
        def split(
            date: str,
            game_pk: int,
            opponent: str,
            strikeouts: int,
            pitches: int,
            is_home: bool,
        ) -> dict:
            return {
                "date": date,
                "game": {"gamePk": game_pk},
                "opponent": {"name": opponent},
                "stat": {"strikeOuts": strikeouts, "numberOfPitches": pitches},
                "isHome": is_home,
            }

        current_season_splits = [
            split("2026-06-01", 1, "Pittsburgh Pirates", 4, 80, True),
            split("2026-06-08", 2, "Milwaukee Brewers", 6, 91, False),
            split("2026-06-15", 3, "Chicago Cubs", 7, 94, True),
            split("2026-06-22", 4, "Cincinnati Reds", 8, 98, False),
            split("2026-06-29", 5, "St. Louis Cardinals", 9, 101, True),
            split("2026-07-12", 6, "Philadelphia Phillies", 10, 105, False),
        ]
        previous_season_splits = [
            split("2025-09-20", 7, "New York Mets", 5, 88, False),
        ]

        def fake_splits(_player_id: int, season: int) -> list:
            return current_season_splits if season == 2026 else previous_season_splits

        with patch("mlb_pitcher_report.reports.pitchers._pitcher_game_log_splits", side_effect=fake_splits):
            lines = fetch_pitcher_recent_game_lines(123, 2026, dt.date(2026, 7, 12))

        self.assertEqual(
            lines,
            [
                "v STL 9K 101P",
                "@ CIN 8K 98P",
                "v CHC 7K 94P",
                "@ MIL 6K 91P",
                "v PIT 4K 80P",
            ],
        )

    def test_summarize_pitcher_best_k_odds_uses_consensus_line_and_best_prices(self) -> None:
        row = {
            "FanDuel": "7.5: +108|-132 || ALT: 6.5: -118|-104",
            "BetRivers": "6.5: +100|-108 || ALT: 7.5: +172|-218",
            "Novig": "7.5: +105|-128 || ALT: 6.5: +109|-111",
            "DraftKings": "6.5: +103|-114",
        }

        summary = summarize_pitcher_best_k_odds(
            row,
            ["FanDuel", "BetRivers", "Novig", "DraftKings"],
        )

        self.assertEqual(summary["consensus_point"], 7.5)
        self.assertEqual(summary["summary"], "7.5 | O +172 BR | U -128 NV")
        self.assertEqual(
            [group["point_text"] for group in summary["line_groups"]],
            ["7.5", "6.5"],
        )

    def test_summarize_pitcher_best_k_odds_includes_betonlineag(self) -> None:
        summary = summarize_pitcher_best_k_odds(
            {"BetOnline.ag": "5.5: +115|-135"},
            ["BetOnline.ag"],
        )

        self.assertEqual(summary["summary"], "5.5 | O +115 BOL | U -135 BOL")
        self.assertEqual(summary["best_over"]["book"], "BetOnline.ag")
        self.assertEqual(summary["best_over"]["tag"], "BOL")

    def test_classify_best_odds_point_uses_fixed_buckets(self) -> None:
        self.assertEqual(_classify_best_odds_point(7.5), "best-odds-point-elite")
        self.assertEqual(_classify_best_odds_point(6.5), "best-odds-point-strong")
        self.assertEqual(_classify_best_odds_point(5.5), "best-odds-point-neutral")
        self.assertEqual(_classify_best_odds_point(4.5), "best-odds-point-weak")
        self.assertEqual(_classify_best_odds_point(None), "best-odds-point-neutral")

    def test_classify_matchup_k_percent_uses_sample_thresholds(self) -> None:
        self.assertEqual(_classify_matchup_k_percent(29.0, 25), "cell-elite")
        self.assertEqual(_classify_matchup_k_percent(26.0, 18), "cell-strong")
        self.assertEqual(_classify_matchup_k_percent(31.0, 12), "cell-low-confidence")
        self.assertEqual(_classify_matchup_k_percent(16.4, 22), "cell-weak")
        self.assertIsNone(_classify_matchup_k_percent(24.5, 22))
        self.assertIsNone(_classify_matchup_k_percent(27.0, None))

    def test_classify_matchup_sample_size_uses_confidence_buckets(self) -> None:
        self.assertEqual(_classify_matchup_sample_size(22), "cell-confidence-high")
        self.assertEqual(_classify_matchup_sample_size(12), "cell-confidence-low")
        self.assertIsNone(_classify_matchup_sample_size(18))
        self.assertIsNone(_classify_matchup_sample_size(None))

    def test_render_best_k_odds_cell_marks_missing_side_for_left_alignment(self) -> None:
        html = _render_best_k_odds_cell(
            {"ProphetX": "3.5: -136|N/A"},
            ["ProphetX"],
        )

        self.assertIn("best-odds-cell-missing-side", html)
        self.assertIn('class="odds-under best-under">-</span>', html)

    def test_prepare_team_batting_df_assigns_leaguewide_rank(self) -> None:
        payload = {
            "stats": [
                {
                    "splits": [
                        {
                            "team": {"name": "Los Angeles Angels"},
                            "stat": {"strikeOuts": 250, "plateAppearances": 997},
                        },
                        {
                            "team": {"name": "Seattle Mariners"},
                            "stat": {"strikeOuts": 200, "plateAppearances": 900},
                        },
                        {
                            "team": {"name": "Houston Astros"},
                            "stat": {"strikeOuts": 180, "plateAppearances": 910},
                        },
                    ]
                }
            ]
        }

        with patch("mlb_pitcher_report.reports.pitchers.statsapi.get", return_value=payload):
            result = prepare_team_batting_df(2026)

        self.assertEqual(
            list(result["Team"]),
            ["Los Angeles Angels", "Seattle Mariners", "Houston Astros"],
        )
        self.assertEqual(list(result["r"]), [1, 2, 3])
        self.assertAlmostEqual(float(result.iloc[0]["SO/PA"]), 100 * 250 / 997)

    def test_calculate_additional_metrics_uses_batters_faced_for_k_pa(self) -> None:
        pitchers = pd.DataFrame(
            [
                {
                    "Name": "Alpha Ace",
                    "Status": "Scheduled",
                    "AB": 48,
                    "GP": 12,
                    "K": 18,
                    "BF": 63,
                    "SO/PA": 24.3,
                    "K%": 26.5,
                    "PA": 37,
                    "r": 1,
                    "Hand": "R",
                    "Opponent": "Boston Red Sox",
                },
                {
                    "Name": "Bravo Ball",
                    "Status": "Final",
                    "AB": 41,
                    "GP": 10,
                    "K": 12,
                    "BF": pd.NA,
                    "SO/PA": 22.1,
                    "K%": 21.8,
                    "PA": 34,
                    "r": 6,
                    "Hand": "L",
                    "Opponent": "New York Yankees",
                },
            ]
        )

        with patch("mlb_pitcher_report.reports.pitchers.get_strikeouts_by_player_name", return_value=7):
            result = calculate_additional_metrics("06/27/2026", pitchers)

        alpha_row = result.loc[result["Name"] == "Alpha Ace"].iloc[0]
        bravo_row = result.loc[result["Name"] == "Bravo Ball"].iloc[0]
        self.assertAlmostEqual(float(alpha_row[PA_GP_COLUMN]), 63 / 12)
        self.assertTrue(pd.isna(bravo_row[PA_GP_COLUMN]))
        self.assertAlmostEqual(float(alpha_row[K_PA_COLUMN]), 100 * 18 / 63)
        self.assertTrue(pd.isna(bravo_row[K_PA_COLUMN]))
        self.assertEqual(alpha_row["Ks"], "")
        self.assertEqual(bravo_row["Ks"], 7)
        self.assertEqual(alpha_row["r"], 1)
        self.assertEqual(bravo_row["r"], 6)


if __name__ == "__main__":
    unittest.main()
