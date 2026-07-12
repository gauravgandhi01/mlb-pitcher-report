from __future__ import annotations

import datetime
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from html import escape
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import pandas as pd
import requests
import statsapi
from bs4 import BeautifulSoup
from pybaseball import team_batting
from unidecode import unidecode

from mlb_pitcher_report.odds.oddapi import ALT_LINES_TOKEN, get_pitcher_odds_by_team
from mlb_pitcher_report.shared.report_data import (
    fetch_people_stats_map as fetch_hitter_people_stats_map,
    fetch_mlb_team_ids,
    index_stat_blocks as index_hitter_stat_blocks,
    parse_vs_pitcher_stats as parse_hitter_vs_pitcher_stats,
)
from mlb_pitcher_report.shared.site_nav import build_date_nav_html, build_report_tabs
from mlb_pitcher_report.shared.team_logos import get_team_logo_src

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = Path("reports")
ROOT_INDEX_FILE = PROJECT_ROOT / "index.html"
SCHEDULE_STATUSES = {"Pre-Game", "Scheduled", "Warmup", "Final", "In Progress"}
NOT_STARTED_STATUSES = {"Pre-Game", "Scheduled", "Warmup"}
COMPLETED_STATUSES = {"Final", "In Progress"}
PREFERRED_ODDS_COLUMNS = ["FanDuel", "BetRivers", "Novig", "ProphetX", "DraftKings", "BetOnline.ag"]
OPP_HAND_K_COLUMN = "Opp K% vH"
OPP_HAND_K_RANK_COLUMN = "Opp K% vH Rank"
OPP_LAST_5_K_COLUMN = "Opp l5 K%"
OPP_LAST_10_K_COLUMN = "Opp l10 K%"
K_PA_COLUMN = "K/PA"
PA_GP_COLUMN = "PA/GP"
BEST_K_ODDS_COLUMN = "Best K Odds"
MATCHUP_SOURCE_COLUMN = "Matchup Src"
START_TIME_COLUMN = "Start"
PLAYER_ID_COLUMN = "Player ID"
RECENT_PITCHER_GAMES_COLUMN = "Recent Games"
MATCHUP_LINES_COLUMN = "Matchup Lines"
MATCHUP_SOURCE_ESPN = "ESPN (AB)"
MATCHUP_SOURCE_PREVIOUS_LINEUP = "Prev Lineup (BvP)"
MATCHUP_SOURCE_SAVANT = "Savant (PA)"
MATCHUP_K_MIN_SAMPLE = 15
MATCHUP_K_HIGH_CONFIDENCE_SAMPLE = 20
MATCHUP_K_STRONG_PCT = 25.0
MATCHUP_K_ELITE_PCT = 28.0
MATCHUP_K_WEAK_PCT = 18.0
WHIFF_CSV_URL_TEMPLATE = (
    "https://baseballsavant.mlb.com/leaderboard/custom"
    "?year={year}&type=pitcher&filter=&min=0"
    "&selections=z_swing_miss_percent%2Coz_swing_miss_percent%2Cwhiff_percent"
    "%2Cn_ff_formatted%2Cn_sl_formatted%2Cn_ch_formatted%2Cn_cu_formatted"
    "%2Cn_si_formatted%2Cn_fc_formatted%2Cn_fs_formatted%2Cn_st_formatted"
    "%2Cn_sv_formatted%2Cn_fastball_formatted"
    "&chart=false&x=whiff_percent&y=whiff_percent&r=no&chartType=beeswarm"
    "&sort=z_swing_miss_percent&sortDir=desc"
    "&csv=true"
)
ARSENAL_PITCH_COLUMNS = [
    ("n_ff_formatted", "FF", "Four-Seam"),
    ("n_sl_formatted", "SL", "Slider"),
    ("n_ch_formatted", "CH", "Changeup"),
    ("n_cu_formatted", "CU", "Curveball"),
    ("n_si_formatted", "SI", "Sinker"),
    ("n_fc_formatted", "FC", "Cutter"),
    ("n_fs_formatted", "FS", "Splitter"),
    ("n_st_formatted", "ST", "Sweeper"),
    ("n_sv_formatted", "SV", "Slurve"),
]
ARSENAL_META_KEY = "__league_averages__"
REPORT_COLUMN_ORDER = [
    "Name",
    "GP",
    "AB",
    "K",
    "AVG",
    PA_GP_COLUMN,
    "K/9",
    "Whiff%",
    K_PA_COLUMN,
    "K%",
    "PA",
    MATCHUP_SOURCE_COLUMN,
    OPP_HAND_K_COLUMN,
    OPP_LAST_5_K_COLUMN,
    OPP_LAST_10_K_COLUMN,
    "SO/PA",
    "r",
    "Ks",
    "Opponent",
    START_TIME_COLUMN,
    "Status",
    BEST_K_ODDS_COLUMN,
]
PITCHER_STAT_COLUMNS = {
    "GP",
    "AB",
    "K",
    "AVG",
    PA_GP_COLUMN,
    "K/9",
    K_PA_COLUMN,
    "Ks",
}
OPPONENT_STAT_COLUMNS = {
    "K%",
    "PA",
    MATCHUP_SOURCE_COLUMN,
    "SO/PA",
    OPP_HAND_K_COLUMN,
    OPP_LAST_5_K_COLUMN,
    OPP_LAST_10_K_COLUMN,
    "r",
}
SAVANT_STAT_COLUMNS = {"Whiff%"}
OPPONENT_K_CONTEXT_COLUMNS = {
    OPP_HAND_K_COLUMN,
    OPP_LAST_5_K_COLUMN,
    OPP_LAST_10_K_COLUMN,
}
PITCHERS_SORTABLE_COLUMNS = {
    "GP",
    "AB",
    "K",
    "AVG",
    PA_GP_COLUMN,
    "K/9",
    "Whiff%",
    K_PA_COLUMN,
    "K%",
    "PA",
    "SO/PA",
    OPP_HAND_K_COLUMN,
    OPP_LAST_5_K_COLUMN,
    OPP_LAST_10_K_COLUMN,
    "r",
    "Ks",
}
STAT_HEADER_TOOLTIPS = {
    "Name": "Probable starting pitcher.",
    "Hand": "Pitching handedness (R/L).",
    "GP": "Games pitched this season.",
    "AB": "At-bats against this pitcher this season.",
    "K": "Pitcher strikeouts this season.",
    "BB": "Walks issued by this pitcher this season.",
    "AVG": "Opponent batting average allowed by this pitcher.",
    PA_GP_COLUMN: "Average plate appearances faced per game pitched.",
    "K/9": "Strikeouts per 9 innings pitched.",
    "Whiff%": "Statcast whiff rate (swing-and-miss rate) from Baseball Savant.",
    K_PA_COLUMN: "Strikeout rate: strikeouts divided by batters faced, shown as a percent.",
    "K%": "Opponent-lineup strikeout context. ESPN confirmed lineups use batter-vs-pitcher history; previous-lineup fallback uses yesterday's lineup batter-vs-pitcher history; Savant uses its probable-lineup sample. Hover K% to see the source.",
    "PA": "Matchup sample size shown as a whole number. ESPN source = AB from batter-vs-pitcher splits; previous lineup source = PA from batter-vs-pitcher splits; Savant source = PA. Coloring reflects confidence for the K% matchup sample.",
    MATCHUP_SOURCE_COLUMN: "Source for K% and PA sample: ESPN confirmed lineup, previous completed lineup, or Savant probable lineup.",
    "SO/PA": "Opponent team strikeouts per plate appearance (season percent), used for the overall MLB rank in r.",
    OPP_HAND_K_COLUMN: "Opponent team strikeout percent versus the pitcher's handedness.",
    OPP_LAST_5_K_COLUMN: "Opponent team strikeout percent over its last 5 completed games before the report date.",
    OPP_LAST_10_K_COLUMN: "Opponent team strikeout percent over its last 10 completed games before the report date.",
    "r": "Overall MLB rank of the opponent's season strikeout rate (1 = most strikeout-prone opponent).",
    "Opponent": "Team this pitcher is facing, with the game time color-coded by status (pregame, in progress, or final).",
    START_TIME_COLUMN: "Scheduled game start time (local time).",
    "Status": "Game status.",
    "Ks": "Strikeouts recorded by this pitcher in the game once started/final.",
    BEST_K_ODDS_COLUMN: "Consensus strikeout prop line plus best over and under prices. Expand for all books and alternate lines.",
}
INTERNAL_ONLY_COLUMNS = {
    "Hand",
    "BB",
    "BF",
    "Team",
    "Pitcher",
    "Error",
    PLAYER_ID_COLUMN,
    RECENT_PITCHER_GAMES_COLUMN,
    MATCHUP_LINES_COLUMN,
    OPP_HAND_K_RANK_COLUMN,
}
SPORTSBOOK_TAGS = {
    "fanduel": "FD",
    "betrivers": "BR",
    "novig": "NV",
    "prophetx": "PX",
    "draftkings": "DK",
    "betonlineag": "BOL",
}
TEAM_MAPPING = {
    "SEA": "Seattle Mariners",
    "OAK": "Oakland Athletics",
    "ATH": "Athletics",
    "CIN": "Cincinnati Reds",
    "BOS": "Boston Red Sox",
    "COL": "Colorado Rockies",
    "PIT": "Pittsburgh Pirates",
    "TBR": "Tampa Bay Rays",
    "DET": "Detroit Tigers",
    "MIN": "Minnesota Twins",
    "CHC": "Chicago Cubs",
    "ATL": "Atlanta Braves",
    "MIL": "Milwaukee Brewers",
    "CHW": "Chicago White Sox",
    "LAA": "Los Angeles Angels",
    "STL": "St. Louis Cardinals",
    "WSN": "Washington Nationals",
    "LAD": "Los Angeles Dodgers",
    "PHI": "Philadelphia Phillies",
    "BAL": "Baltimore Orioles",
    "SFG": "San Francisco Giants",
    "MIA": "Miami Marlins",
    "TEX": "Texas Rangers",
    "NYM": "New York Mets",
    "ARI": "Arizona Diamondbacks",
    "CLE": "Cleveland Guardians",
    "TOR": "Toronto Blue Jays",
    "NYY": "New York Yankees",
    "SDP": "San Diego Padres",
    "KCR": "Kansas City Royals",
    "HOU": "Houston Astros",
}
TEAM_DISPLAY_ABBREVIATIONS = {
    "Arizona Diamondbacks": "ARI",
    "Athletics": "ATH",
    "Oakland Athletics": "OAK",
    "Atlanta Braves": "ATL",
    "Baltimore Orioles": "BAL",
    "Boston Red Sox": "BOS",
    "Chicago Cubs": "CHC",
    "Chicago White Sox": "CWS",
    "Cincinnati Reds": "CIN",
    "Cleveland Guardians": "CLE",
    "Colorado Rockies": "COL",
    "Detroit Tigers": "DET",
    "Houston Astros": "HOU",
    "Kansas City Royals": "KC",
    "Los Angeles Angels": "LAA",
    "Los Angeles Dodgers": "LAD",
    "Miami Marlins": "MIA",
    "Milwaukee Brewers": "MIL",
    "Minnesota Twins": "MIN",
    "New York Mets": "NYM",
    "New York Yankees": "NYY",
    "Philadelphia Phillies": "PHI",
    "Pittsburgh Pirates": "PIT",
    "San Diego Padres": "SD",
    "San Francisco Giants": "SF",
    "Seattle Mariners": "SEA",
    "St. Louis Cardinals": "STL",
    "Tampa Bay Rays": "TB",
    "Texas Rangers": "TEX",
    "Toronto Blue Jays": "TOR",
    "Washington Nationals": "WSH",
}
PRIMARY_ODDS_PATTERN = re.compile(r"([0-9]+(?:\.[0-9]+)?):\s*(N/A|[+-]?\d+)\s*\|\s*(N/A|[+-]?\d+)")
SPORTSBOOK_COLORS = {
    "fanduel": "#1E5EFF",
    "draftkings": "#53C200",
    "betrivers": "#002A5E",
    "betmgm": "#C9A66B",
    "caesars": "#D4AF37",
    "betonlineag": "#FF7A00",
    "bovada": "#B91C1C",
    "mybookieag": "#1D4ED8",
    "novig": "#0EA5A4",
    "prophetx": "#7C3AED",
    "betopenly": "#0F766E",
}
SPORTSBOOK_FALLBACK_COLORS = [
    "#2563EB",
    "#16A34A",
    "#9333EA",
    "#EA580C",
    "#0891B2",
    "#BE123C",
]
TEAM_HAND_SPLIT_CACHE: Dict[Tuple[int, int], Dict[str, Optional[float]]] = {}
TEAM_RECENT_K_CACHE: Dict[Tuple[int, int, str], Dict[str, Optional[float]]] = {}
TEAM_HAND_SPLIT_RANK_CACHE: Dict[Tuple[int, str], Dict[int, int]] = {}
PREVIOUS_LINEUP_PLAYER_IDS_CACHE: Dict[Tuple[int, str], List[int]] = {}
PITCHER_ID_CACHE: Dict[str, Optional[int]] = {}
PREVIOUS_LINEUP_K_CACHE: Dict[Tuple[int, int, str, int], Optional[Dict[str, Any]]] = {}


def _normalize_person_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


def _normalize_team_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(text.split())


def _format_local_start_time(game_datetime: Any) -> str:
    text = str(game_datetime or "").strip()
    if not text:
        return ""
    try:
        dt = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    local_time = dt.astimezone().strftime("%I:%M %p").lstrip("0")
    return local_time.replace(" AM", "a").replace(" PM", "p")


def _choose_best_player_match(players: List[Dict[str, Any]], player_name: str) -> Optional[Dict[str, Any]]:
    if not players:
        return None

    target = _normalize_person_name(player_name)
    if not target:
        return players[0]

    exact_full = [p for p in players if _normalize_person_name(p.get("fullName")) == target]
    if exact_full:
        return exact_full[0]

    exact_first_last = [
        p
        for p in players
        if _normalize_person_name(p.get("firstLastName")) == target
        or _normalize_person_name(p.get("nameFirstLast")) == target
    ]
    if exact_first_last:
        return exact_first_last[0]

    return players[0]


def parse_pitcher_stats(raw_data: str, name: str) -> Dict[str, Any]:
    lines = [line.strip() for line in raw_data.split("\n") if line.strip()]
    data: Dict[str, str] = {}
    for line in lines[2:]:
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[key.strip()] = value.strip()
    return {
        "Name": name,
        "GP": data.get("gamesPlayed"),
        "AB": data.get("atBats"),
        "BB": data.get("baseOnBalls"),
        "BF": data.get("battersFaced"),
        "AVG": data.get("avg"),
        "K": data.get("strikeOuts"),
        "K/9": data.get("strikeoutsPer9Inn"),
    }


def get_game_id_by_probable_pitcher(date: str, pitcher_name: str) -> Optional[int]:
    games = statsapi.schedule(start_date=date, end_date=date)
    for game in games:
        if game.get("away_probable_pitcher") == pitcher_name or game.get("home_probable_pitcher") == pitcher_name:
            if game.get("status") in NOT_STARTED_STATUSES:
                return None
            return game.get("game_id")
    return None


def get_strikeouts_by_player_name(date: str, full_name: str) -> Any:
    game_id = get_game_id_by_probable_pitcher(date, full_name)
    if not game_id:
        return "N/A"

    data = statsapi.boxscore_data(game_id, timecode=None)
    player_info = data.get("playerInfo", {})
    player_id = None
    for pid, player_data in player_info.items():
        if player_data.get("fullName") == full_name:
            player_id = pid
            break

    if not player_id:
        return None

    players = data.get("home", {}).get("players", {})
    player_data = players.get(player_id, {})
    if not player_data:
        players = data.get("away", {}).get("players", {})
        player_data = players.get(player_id, {})
    return player_data.get("stats", {}).get("pitching", {}).get("strikeOuts", 0)


def fetch_pitcher_stats(name: str, team: str, opponent: str, status: str, start_time: str) -> Dict[str, Any]:
    try:
        players = statsapi.lookup_player(name)
        player = _choose_best_player_match(players or [], name)
        if not player:
            raise ValueError(f"Player {name} not found")
        player_id = player["id"]
        stats = statsapi.player_stats(player_id, group="[pitching]", type="season")
        pitcher_stats = parse_pitcher_stats(stats, name)
        pitcher_stats[PLAYER_ID_COLUMN] = player_id
        pitcher_stats["Opponent"] = opponent
        pitcher_stats["Status"] = status
        pitcher_stats[START_TIME_COLUMN] = start_time
        return pitcher_stats
    except Exception as exc:
        return {
            "Name": name,
            "Team": team,
            "Opponent": opponent,
            "Status": status,
            START_TIME_COLUMN: start_time,
            "Error": str(exc),
        }


def fetch_pitcher_odds(name: str, report_date: str) -> Optional[pd.DataFrame]:
    try:
        return get_pitcher_odds_by_team(name, report_date)
    except Exception as exc:
        print(f"Odds lookup failed for {name}: {exc}")
        return None


def get_team_full_name(abbreviation: str) -> str:
    return TEAM_MAPPING.get(abbreviation, "Unknown")


def make_pitcher_hyperlink(name: str) -> str:
    safe_name = quote(str(name))
    return f'<a href="https://statmuse.com/mlb/ask/{safe_name}-k-log">{name}</a>'


def make_opponent_hyperlink(team: str, label_html: Optional[str] = None) -> str:
    safe_team = quote(str(team))
    title = escape(str(team), quote=True)
    label = label_html if label_html is not None else escape(str(team))
    return f'<a href="https://statmuse.com/mlb/ask/{safe_team}-k-per-pa-log" title="{title}">{label}</a>'


def get_pitcher_data(pitcher_div: Any) -> Tuple[str, Any, Any, str]:
    player_info = pitcher_div.find("div", class_="player-info")
    if not player_info:
        return "Unknown", 0, 0, "TBD"

    throws_tag = player_info.find("span", class_="throws")
    throws_text = throws_tag.get_text(strip=True) if throws_tag else ""
    handedness = "R" if "Right" in throws_text else "L" if "Left" in throws_text else "TBD"

    name_tag = player_info.find("h3")
    name_link = name_tag.find("a") if name_tag else None
    name = name_link.get_text(strip=True) if name_link else "Unknown"

    probable_stats = pitcher_div.find("p", class_="probable-stats")
    if probable_stats:
        table = probable_stats.find("table", class_="pitcher-stats")
        if table:
            rows = table.find_all("tr")
            if len(rows) > 1:
                data_row = rows[1].find_all("td")
                if len(data_row) >= 2:
                    pa = data_row[0].get_text(strip=True)
                    k_percentage = data_row[1].get_text(strip=True)
                    return name, pa, k_percentage, handedness

    return name, 0, 0, handedness


def get_savant_opp_data(date: str) -> pd.DataFrame:
    date_obj = datetime.datetime.strptime(date, "%m/%d/%Y")
    converted_date = date_obj.strftime("%Y-%m-%d")
    url = f"https://baseballsavant.mlb.com/probable-pitchers?date={converted_date}"
    data: List[Dict[str, Any]] = []

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"\033[91mFailed to retrieve probable pitcher data: {exc}\033[0m")
        return pd.DataFrame(columns=["Pitcher", "Hand", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])

    soup = BeautifulSoup(response.content, "html.parser")
    blocks = soup.find_all("div", class_="mod")
    for block in blocks:
        cols = block.find_all("div", class_="col")
        for col in cols:
            try:
                name, pa, k_percentage, handedness = get_pitcher_data(col)
                data.append(
                    {
                        "Pitcher": name,
                        "Hand": handedness,
                        "PA": pa,
                        "K%": k_percentage,
                        MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_SAVANT,
                        MATCHUP_LINES_COLUMN: [],
                    }
                )
            except Exception:
                data.append(
                    {
                        "Pitcher": "TBD",
                        "Hand": "TBD",
                        "PA": 0,
                        "K%": 0,
                        MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_SAVANT,
                        MATCHUP_LINES_COLUMN: [],
                    }
                )

    return pd.DataFrame(data, columns=["Pitcher", "Hand", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])


def _fetch_espn_scoreboard_events(date: str) -> List[Dict[str, Any]]:
    date_obj = datetime.datetime.strptime(date, "%m/%d/%Y")
    url = (
        "https://site.web.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        f"?dates={date_obj.strftime('%Y%m%d')}"
    )
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        payload = response.json()
    except Exception as exc:
        print(f"\033[91mFailed to load ESPN scoreboard data: {exc}\033[0m")
        return []
    return payload.get("events") or []


def _build_espn_event_id_lookup(date: str) -> Dict[Tuple[str, str], str]:
    lookup: Dict[Tuple[str, str], str] = {}
    for event in _fetch_espn_scoreboard_events(date):
        event_id = str(event.get("id", "")).strip()
        comp = (event.get("competitions") or [{}])[0]
        competitors = comp.get("competitors") or []
        away_name = ""
        home_name = ""
        for competitor in competitors:
            team = competitor.get("team") or {}
            display_name = str(team.get("displayName", "")).strip()
            home_away = str(competitor.get("homeAway", "")).strip().lower()
            if home_away == "away":
                away_name = display_name
            elif home_away == "home":
                home_name = display_name
        if not event_id or not away_name or not home_name:
            continue
        lookup[(_normalize_team_name(away_name), _normalize_team_name(home_name))] = event_id
    return lookup


def _fetch_espn_summary(event_id: str) -> Optional[Dict[str, Any]]:
    if not event_id:
        return None
    url = f"https://site.web.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={event_id}"
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        return response.json()
    except Exception:
        return None


def _is_espn_lineup_confirmed(
    summary_data: Dict[str, Any],
    team_abbrev: str,
    batting_athletes: Sequence[Dict[str, Any]],
) -> bool:
    target = str(team_abbrev or "").strip().upper()
    roster_checked = False
    for roster_block in summary_data.get("rosters") or []:
        roster_team = (roster_block.get("team") or {}).get("abbreviation")
        if str(roster_team or "").strip().upper() != target:
            continue
        roster_checked = True
        roster = roster_block.get("roster") or []
        starters = [player for player in roster if player.get("starter")]
        roster_with_order = [player for player in roster if _to_int(player.get("batOrder")) is not None]
        starters_with_order = [player for player in starters if _to_int(player.get("batOrder")) is not None]
        if len(starters_with_order) >= 9 or len(roster_with_order) >= 9:
            return True

    starters = [athlete for athlete in batting_athletes if athlete.get("starter")]
    athletes_with_order = [athlete for athlete in batting_athletes if _to_int(athlete.get("batOrder")) is not None]
    starters_with_order = [athlete for athlete in starters if _to_int(athlete.get("batOrder")) is not None]
    if len(starters_with_order) >= 9 or len(athletes_with_order) >= 9:
        return True
    if roster_checked:
        return False
    return False


def _matchup_stat_int(value: Any) -> Optional[int]:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return int(numeric)


def _last_name_from_display_name(name: Any) -> str:
    text = str(name or "").strip()
    if not text:
        return ""
    if "," in text:
        return text.split(",", 1)[0].strip()

    parts = text.split()
    suffixes = {"jr", "jr.", "sr", "sr.", "ii", "iii", "iv", "v"}
    while len(parts) > 1 and parts[-1].strip(".").lower() in suffixes:
        parts.pop()
    return parts[-1] if parts else ""


def _format_matchup_k_line(
    hits: Any,
    at_bats: Any,
    strikeouts: Any,
    player_name: Any = None,
) -> Optional[str]:
    hit_count = _matchup_stat_int(hits)
    at_bat_count = _matchup_stat_int(at_bats)
    strikeout_count = _matchup_stat_int(strikeouts)
    if hit_count is None and at_bat_count is None and strikeout_count is None:
        return None

    line = f"{hit_count or 0}-{at_bat_count or 0} {strikeout_count or 0}K"
    name_text = str(player_name or "").strip()
    if name_text:
        return f"{name_text} {line}"
    return line


def _espn_athlete_display_name(athlete: Dict[str, Any]) -> str:
    athlete_info = athlete.get("athlete") or {}
    last_name = str(athlete_info.get("lastName") or athlete.get("lastName") or "").strip()
    if last_name:
        return last_name
    for key in ("shortName", "displayName", "fullName"):
        value = str(athlete_info.get(key) or "").strip()
        if value:
            return _last_name_from_display_name(value)
    for key in ("shortName", "displayName", "name"):
        value = str(athlete.get(key) or "").strip()
        if value:
            return _last_name_from_display_name(value)
    return ""


def _extract_espn_lineup_matchup_stats(summary_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    matchup_by_team: Dict[str, Dict[str, Any]] = {}
    boxscore_players = (summary_data.get("boxscore") or {}).get("players") or []
    for team_block in boxscore_players:
        team = team_block.get("team") or {}
        team_abbrev = str(team.get("abbreviation", "")).strip().upper()
        if not team_abbrev:
            continue
        statistics = team_block.get("statistics") or []
        batting_block = next((item for item in statistics if item.get("type") == "batting"), None)
        if not batting_block:
            continue
        athletes = batting_block.get("athletes") or []
        confirmed = _is_espn_lineup_confirmed(summary_data, team_abbrev, athletes)
        if not confirmed:
            continue

        lineup_athletes = [athlete for athlete in athletes if athlete.get("starter")]
        if not lineup_athletes:
            lineup_athletes = athletes
        lineup_athletes = sorted(
            lineup_athletes,
            key=lambda athlete: (_to_int(athlete.get("batOrder")) is None, _to_int(athlete.get("batOrder")) or 99),
        )[:9]

        keys = batting_block.get("keys") or []
        try:
            at_bats_idx = keys.index("atBats")
            strikeouts_idx = keys.index("strikeouts")
        except ValueError:
            continue
        hits_idx = keys.index("hits") if "hits" in keys else None

        total_ab = 0.0
        total_ks = 0.0
        has_any_vs_stats = False
        has_any_numeric_value = False
        matchup_lines: List[str] = []
        for athlete in lineup_athletes:
            vs_stats = athlete.get("vsStats") or []
            if vs_stats:
                has_any_vs_stats = True
            raw_ab = vs_stats[at_bats_idx] if at_bats_idx < len(vs_stats) else None
            raw_ks = vs_stats[strikeouts_idx] if strikeouts_idx < len(vs_stats) else None
            raw_hits = vs_stats[hits_idx] if hits_idx is not None and hits_idx < len(vs_stats) else None
            ab_value = pd.to_numeric(raw_ab, errors="coerce")
            ks_value = pd.to_numeric(raw_ks, errors="coerce")
            if pd.notna(ab_value):
                total_ab += float(ab_value)
                has_any_numeric_value = True
            if pd.notna(ks_value):
                total_ks += float(ks_value)
                has_any_numeric_value = True
            line = _format_matchup_k_line(raw_hits, raw_ab, raw_ks, _espn_athlete_display_name(athlete))
            if line:
                matchup_lines.append(line)

        # ESPN removes lineup vsStats after first pitch for many games.
        # When that happens, keep Savant fallback instead of forcing zeroes.
        if not has_any_vs_stats or not has_any_numeric_value:
            continue

        k_percent = float(100 * total_ks / total_ab) if total_ab > 0 else 0.0
        matchup_by_team[team_abbrev] = {"PA": total_ab, "K%": k_percent, MATCHUP_LINES_COLUMN: matchup_lines}

    return matchup_by_team


def _extract_lineup_player_ids_from_boxscore(boxscore_data: Dict[str, Any], team_id: int) -> List[int]:
    for side in ("home", "away"):
        team_block = boxscore_data.get(side) or {}
        team = team_block.get("team") or {}
        if _to_int(team.get("id")) != int(team_id):
            continue

        batting_order = team_block.get("battingOrder") or []
        player_ids = [int(player_id) for player_id in batting_order if _to_int(player_id) is not None]
        return player_ids[:9] if len(player_ids) >= 9 else []
    return []


def _fetch_previous_lineup_player_ids(team_id: int, report_date: datetime.date) -> List[int]:
    cache_key = (int(team_id), report_date.isoformat())
    cached = PREVIOUS_LINEUP_PLAYER_IDS_CACHE.get(cache_key)
    if cached is not None:
        return cached

    start_date = report_date - datetime.timedelta(days=45)
    end_date = report_date - datetime.timedelta(days=1)
    try:
        games = statsapi.schedule(
            start_date=start_date.strftime("%m/%d/%Y"),
            end_date=end_date.strftime("%m/%d/%Y"),
            team=team_id,
        )
    except Exception:
        PREVIOUS_LINEUP_PLAYER_IDS_CACHE[cache_key] = []
        return []

    completed_games = []
    for game in games:
        if str(game.get("status") or "").strip() != "Final":
            continue
        game_date = _parse_stat_date(game.get("game_date"))
        game_id = _to_int(game.get("game_id"))
        if game_date is None or game_id is None or game_date >= report_date:
            continue
        completed_games.append((game_date, game_id))

    completed_games.sort(key=lambda item: (item[0], item[1]), reverse=True)
    for _, game_id in completed_games:
        try:
            boxscore_data = statsapi.boxscore_data(game_id)
        except Exception:
            continue
        lineup_ids = _extract_lineup_player_ids_from_boxscore(boxscore_data, team_id)
        if lineup_ids:
            PREVIOUS_LINEUP_PLAYER_IDS_CACHE[cache_key] = lineup_ids
            return lineup_ids

    PREVIOUS_LINEUP_PLAYER_IDS_CACHE[cache_key] = []
    return []


def _lookup_pitcher_id(pitcher_name: Any) -> Optional[int]:
    name_text = str(pitcher_name or "").strip()
    pitcher_key = _normalize_person_name(name_text)
    if not pitcher_key:
        return None
    if pitcher_key in PITCHER_ID_CACHE:
        return PITCHER_ID_CACHE[pitcher_key]

    try:
        players = statsapi.lookup_player(name_text)
        player = _choose_best_player_match(players or [], name_text)
        pitcher_id = _to_int(player.get("id") if player else None)
    except Exception:
        pitcher_id = None

    PITCHER_ID_CACHE[pitcher_key] = pitcher_id
    return pitcher_id


def _person_display_name(person: Dict[str, Any]) -> str:
    last_name = str(person.get("lastName") or "").strip()
    if last_name:
        return last_name
    for key in ("fullName", "firstLastName", "nameFirstLast", "lastFirstName"):
        value = str(person.get(key) or "").strip()
        if value:
            return _last_name_from_display_name(value)
    return ""


def _hitter_vs_pitcher_totals(person: Dict[str, Any], report_date: datetime.date) -> Dict[str, Any]:
    return parse_hitter_vs_pitcher_stats(
        index_hitter_stat_blocks(person),
        report_date=report_date,
        subtract_same_day_from_season_splits=False,
    )


def _previous_lineup_k_percent(
    team_id: int,
    season: int,
    report_date: datetime.date,
    pitcher_id: int,
) -> Optional[Dict[str, Any]]:
    pitcher_id = int(pitcher_id)
    cache_key = (int(team_id), int(season), report_date.isoformat(), pitcher_id)
    cached = PREVIOUS_LINEUP_K_CACHE.get(cache_key)
    if cached is not None or cache_key in PREVIOUS_LINEUP_K_CACHE:
        return cached

    lineup_ids = _fetch_previous_lineup_player_ids(team_id, report_date)
    if len(lineup_ids) < 9:
        PREVIOUS_LINEUP_K_CACHE[cache_key] = None
        return None

    try:
        people_by_id = fetch_hitter_people_stats_map(
            lineup_ids[:9],
            season,
            None,
            pitcher_id,
            stats_end_date=report_date - datetime.timedelta(days=1),
        )
    except Exception:
        PREVIOUS_LINEUP_K_CACHE[cache_key] = None
        return None

    total_strikeouts = 0
    total_plate_appearances = 0
    matchup_lines: List[str] = []
    for player_id in lineup_ids[:9]:
        person = people_by_id.get(int(player_id))
        if not person:
            continue
        totals = _hitter_vs_pitcher_totals(person, report_date)
        strikeouts = _to_int(totals.get("K")) or 0
        plate_appearances = _to_int(totals.get("PA")) or 0
        total_strikeouts += strikeouts
        total_plate_appearances += plate_appearances
        line = _format_matchup_k_line(
            totals.get("H"),
            totals.get("AB"),
            strikeouts,
            _person_display_name(person),
        )
        if line:
            matchup_lines.append(line)

    if total_plate_appearances <= 0:
        PREVIOUS_LINEUP_K_CACHE[cache_key] = None
        return None

    result = {
        "PA": float(total_plate_appearances),
        "K%": float(100 * total_strikeouts / total_plate_appearances),
        MATCHUP_LINES_COLUMN: matchup_lines,
    }
    PREVIOUS_LINEUP_K_CACHE[cache_key] = result
    return result


def get_espn_opp_data(date: str, schedule: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    event_lookup = _build_espn_event_id_lookup(date)
    if not event_lookup:
        return pd.DataFrame(columns=["Pitcher", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])

    for game in schedule:
        away_team = str(game.get("away_name", "")).strip()
        home_team = str(game.get("home_name", "")).strip()
        away_pitcher = str(game.get("away_probable_pitcher", "")).strip()
        home_pitcher = str(game.get("home_probable_pitcher", "")).strip()
        event_id = event_lookup.get((_normalize_team_name(away_team), _normalize_team_name(home_team)))
        if not event_id:
            continue

        summary_data = _fetch_espn_summary(event_id)
        if not summary_data:
            continue

        comp = ((summary_data.get("header") or {}).get("competitions") or [{}])[0]
        competitors = comp.get("competitors") or []
        home_abbrev = ""
        away_abbrev = ""
        for competitor in competitors:
            team_abbrev = str((competitor.get("team") or {}).get("abbreviation", "")).strip().upper()
            home_away = str(competitor.get("homeAway", "")).strip().lower()
            if home_away == "home":
                home_abbrev = team_abbrev
            elif home_away == "away":
                away_abbrev = team_abbrev
        if not home_abbrev or not away_abbrev:
            continue

        lineup_stats = _extract_espn_lineup_matchup_stats(summary_data)
        if away_pitcher and home_abbrev in lineup_stats:
            rows.append(
                {
                    "Pitcher": away_pitcher,
                    "PA": lineup_stats[home_abbrev]["PA"],
                    "K%": lineup_stats[home_abbrev]["K%"],
                    MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_ESPN,
                    MATCHUP_LINES_COLUMN: lineup_stats[home_abbrev].get(MATCHUP_LINES_COLUMN, []),
                }
            )
        if home_pitcher and away_abbrev in lineup_stats:
            rows.append(
                {
                    "Pitcher": home_pitcher,
                    "PA": lineup_stats[away_abbrev]["PA"],
                    "K%": lineup_stats[away_abbrev]["K%"],
                    MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_ESPN,
                    MATCHUP_LINES_COLUMN: lineup_stats[away_abbrev].get(MATCHUP_LINES_COLUMN, []),
                }
            )

    return pd.DataFrame(rows, columns=["Pitcher", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])


def get_previous_lineup_opp_data(
    date: str,
    schedule: Sequence[Dict[str, Any]],
    savant_df: pd.DataFrame,
    excluded_pitchers: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    del savant_df
    report_date = datetime.datetime.strptime(date, "%m/%d/%Y").date()
    season = report_date.year
    excluded_keys = {_normalize_person_name(name) for name in (excluded_pitchers or [])}
    rows: List[Dict[str, Any]] = []

    for game in schedule:
        matchup_tasks = [
            (game.get("away_probable_pitcher"), game.get("home_id")),
            (game.get("home_probable_pitcher"), game.get("away_id")),
        ]
        for pitcher_name_raw, opponent_team_id_raw in matchup_tasks:
            pitcher_name = str(pitcher_name_raw or "").strip()
            pitcher_key = _normalize_person_name(pitcher_name)
            opponent_team_id = _to_int(opponent_team_id_raw)
            if not pitcher_name or not pitcher_key or pitcher_key in excluded_keys or opponent_team_id is None:
                continue
            pitcher_id = _lookup_pitcher_id(pitcher_name)
            if pitcher_id is None:
                continue

            lineup_stats = _previous_lineup_k_percent(
                opponent_team_id,
                season,
                report_date,
                pitcher_id,
            )
            if not lineup_stats:
                continue

            rows.append(
                {
                    "Pitcher": pitcher_name,
                    "PA": lineup_stats["PA"],
                    "K%": lineup_stats["K%"],
                    MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_PREVIOUS_LINEUP,
                    MATCHUP_LINES_COLUMN: lineup_stats.get(MATCHUP_LINES_COLUMN, []),
                }
            )

    return pd.DataFrame(rows, columns=["Pitcher", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN])


def get_opp_data(date: str, schedule: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    savant_df = get_savant_opp_data(date)
    espn_df = get_espn_opp_data(date, schedule)
    espn_pitchers = [
        str(row.get("Pitcher") or "").strip()
        for _, row in espn_df.iterrows()
        if str(row.get("Pitcher") or "").strip()
    ]
    previous_lineup_df = get_previous_lineup_opp_data(
        date,
        schedule,
        savant_df,
        excluded_pitchers=espn_pitchers,
    )

    columns = ["Pitcher", "Hand", "PA", "K%", MATCHUP_SOURCE_COLUMN, MATCHUP_LINES_COLUMN]
    merged_lookup: Dict[str, Dict[str, Any]] = {}

    for _, row in savant_df.iterrows():
        name = str(row.get("Pitcher", "")).strip()
        if not name:
            continue
        key = _normalize_person_name(name)
        merged_lookup[key] = {
            "Pitcher": name,
            "Hand": row.get("Hand"),
            "PA": row.get("PA"),
            "K%": row.get("K%"),
            MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_SAVANT,
            MATCHUP_LINES_COLUMN: row.get(MATCHUP_LINES_COLUMN) if MATCHUP_LINES_COLUMN in row else [],
        }

    for _, row in previous_lineup_df.iterrows():
        name = str(row.get("Pitcher", "")).strip()
        if not name:
            continue
        key = _normalize_person_name(name)
        existing = merged_lookup.get(
            key,
            {
                "Pitcher": name,
                "Hand": pd.NA,
                "PA": pd.NA,
                "K%": pd.NA,
                MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_PREVIOUS_LINEUP,
                MATCHUP_LINES_COLUMN: [],
            },
        )
        existing["PA"] = row.get("PA")
        existing["K%"] = row.get("K%")
        existing[MATCHUP_SOURCE_COLUMN] = MATCHUP_SOURCE_PREVIOUS_LINEUP
        existing[MATCHUP_LINES_COLUMN] = row.get(MATCHUP_LINES_COLUMN) if MATCHUP_LINES_COLUMN in row else []
        merged_lookup[key] = existing

    for _, row in espn_df.iterrows():
        name = str(row.get("Pitcher", "")).strip()
        if not name:
            continue
        key = _normalize_person_name(name)
        existing = merged_lookup.get(
            key,
            {
                "Pitcher": name,
                "Hand": pd.NA,
                "PA": pd.NA,
                "K%": pd.NA,
                MATCHUP_SOURCE_COLUMN: MATCHUP_SOURCE_ESPN,
                MATCHUP_LINES_COLUMN: [],
            },
        )
        existing["PA"] = row.get("PA")
        existing["K%"] = row.get("K%")
        existing[MATCHUP_SOURCE_COLUMN] = MATCHUP_SOURCE_ESPN
        existing[MATCHUP_LINES_COLUMN] = row.get(MATCHUP_LINES_COLUMN) if MATCHUP_LINES_COLUMN in row else []
        merged_lookup[key] = existing

    if not merged_lookup:
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(list(merged_lookup.values()), columns=columns)


def fetch_schedule(date: str) -> List[Dict[str, Any]]:
    sched = statsapi.schedule(start_date=date, end_date=date)
    return [game for game in sched if game.get("status") in SCHEDULE_STATUSES]


def get_pitcher_tasks(schedule: Sequence[Dict[str, Any]]) -> List[Tuple[str, str, str, str, str]]:
    pitcher_tasks: List[Tuple[str, str, str, str, str]] = []
    for game in schedule:
        status = game.get("status", "")
        start_time = _format_local_start_time(game.get("game_datetime"))
        away_team, home_team = game.get("away_name", ""), game.get("home_name", "")
        away_pitcher = game.get("away_probable_pitcher")
        home_pitcher = game.get("home_probable_pitcher")
        if away_pitcher:
            pitcher_tasks.append((away_pitcher, away_team, home_team, status, start_time))
        if home_pitcher:
            pitcher_tasks.append((home_pitcher, home_team, away_team, status, start_time))
    return pitcher_tasks


def fetch_pitcher_stats_concurrently(pitcher_tasks: Sequence[Tuple[str, str, str, str, str]]) -> List[Dict[str, Any]]:
    if not pitcher_tasks:
        return []
    max_workers = min(16, len(pitcher_tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_pitcher_stats, pitcher, team, opponent, status, start_time)
            for pitcher, team, opponent, status, start_time in pitcher_tasks
        ]
        return [future.result() for future in futures]


def prepare_team_batting_df(year: int) -> pd.DataFrame:
    last_error: Optional[Exception] = None

    for candidate_year in [year, year - 1]:
        try:
            data = statsapi.get(
                "teams_stats",
                {
                    "season": candidate_year,
                    "group": "hitting",
                    "stats": "season",
                    "sportIds": 1,
                },
            )
            stats_blocks = data.get("stats") or []
            if not stats_blocks:
                continue
            splits = stats_blocks[0].get("splits") or []
            rows: List[Dict[str, Any]] = []
            for split in splits:
                team_name = str((split.get("team") or {}).get("name", "")).strip()
                stat = split.get("stat") or {}
                strikeouts = pd.to_numeric(stat.get("strikeOuts"), errors="coerce")
                plate_appearances = pd.to_numeric(stat.get("plateAppearances"), errors="coerce")
                if not team_name or pd.isna(strikeouts) or pd.isna(plate_appearances) or plate_appearances <= 0:
                    continue
                rows.append({"Team": team_name, "SO/PA": float(100 * strikeouts / plate_appearances)})
            if rows:
                df = pd.DataFrame(rows).sort_values(by=["SO/PA", "Team"], ascending=[False, True]).reset_index(drop=True)
                df["r"] = df.index + 1
                return df
        except Exception as exc:
            last_error = exc

    for candidate_year in [year, year - 1]:
        try:
            df = team_batting(candidate_year)
            df["SO/PA"] = 100 * df["SO"] / df["PA"]
            df = df[["Team", "SO/PA"]].sort_values(by=["SO/PA", "Team"], ascending=[False, True]).reset_index(drop=True)
            df["Team"] = df["Team"].apply(get_team_full_name)
            df["r"] = df.index + 1
            return df
        except Exception as exc:
            last_error = exc

    print(f"\033[91mFailed to load team batting data: {last_error}\033[0m")
    return pd.DataFrame(columns=["Team", "SO/PA", "r"])


def _to_optional_float(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _last_first_to_full_name(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "," not in text:
        return text
    last, first = text.split(",", 1)
    return f"{first.strip()} {last.strip()}".strip()


def prepare_pitcher_arsenal_lookup(year: int) -> Dict[str, Dict[str, Any]]:
    try:
        response = requests.get(WHIFF_CSV_URL_TEMPLATE.format(year=year), timeout=20)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text), encoding="utf-8-sig")
    except Exception as exc:
        print(f"\033[91mFailed to load pitcher arsenal data: {exc}\033[0m")
        return {}

    required_columns = {"last_name, first_name", "whiff_percent", "z_swing_miss_percent", "oz_swing_miss_percent"}
    if df.empty or not required_columns.issubset(df.columns):
        return {}

    metric_columns = ["whiff_percent", "z_swing_miss_percent", "oz_swing_miss_percent"]
    league_averages = {
        metric: _to_optional_float(pd.to_numeric(df[metric], errors="coerce").mean())
        for metric in metric_columns
    }

    arsenal_lookup: Dict[str, Dict[str, Any]] = {}
    arsenal_lookup[ARSENAL_META_KEY] = league_averages
    for _, row in df.iterrows():
        display_name = _last_first_to_full_name(row.get("last_name, first_name"))
        name_key = _normalize_person_name(display_name)
        if not name_key:
            continue

        arsenal_entries: List[Dict[str, Any]] = []
        for column_name, pitch_code, pitch_label in ARSENAL_PITCH_COLUMNS:
            usage_percent = _to_optional_float(row.get(column_name))
            if usage_percent is None or usage_percent <= 0:
                continue
            arsenal_entries.append(
                {
                    "code": pitch_code,
                    "label": pitch_label,
                    "usage_percent": usage_percent,
                }
            )
        arsenal_entries.sort(key=lambda pitch: pitch["usage_percent"], reverse=True)

        arsenal_lookup[name_key] = {
            "name": display_name,
            "whiff_percent": _to_optional_float(row.get("whiff_percent")),
            "z_swing_miss_percent": _to_optional_float(row.get("z_swing_miss_percent")),
            "oz_swing_miss_percent": _to_optional_float(row.get("oz_swing_miss_percent")),
            "fastball_percent": _to_optional_float(row.get("n_fastball_formatted")),
            "league_averages": league_averages,
            "arsenal": arsenal_entries,
        }

    return arsenal_lookup


def prepare_pitcher_whiff_lookup(year: int) -> Dict[str, float]:
    arsenal_lookup = prepare_pitcher_arsenal_lookup(year)
    return {
        name_key: details["whiff_percent"]
        for name_key, details in arsenal_lookup.items()
        if name_key != ARSENAL_META_KEY
        if details.get("whiff_percent") is not None
    }


def merge_pitcher_with_batting_data(
    results: Sequence[Dict[str, Any]],
    team_batting_df: pd.DataFrame,
) -> pd.DataFrame:
    main_df = pd.DataFrame(results)
    return pd.merge(main_df, team_batting_df, left_on="Opponent", right_on="Team", how="left")


def merge_with_opponent_data(merged_df: pd.DataFrame, opp_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(merged_df, opp_df, left_on="Name", right_on="Pitcher", how="left")


def _to_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_stat_date(value: Any) -> Optional[datetime.date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _team_display_abbreviation(team_name: Any) -> str:
    text = str(team_name or "").strip()
    if not text:
        return ""
    mapped = TEAM_DISPLAY_ABBREVIATIONS.get(text)
    if mapped:
        return mapped
    reverse_mapping = {full_name: abbreviation for abbreviation, full_name in TEAM_MAPPING.items()}
    mapped = reverse_mapping.get(text)
    if mapped:
        return mapped
    words = re.findall(r"[A-Za-z0-9]+", text)
    if not words:
        return text[:3].upper()
    if len(words) == 1:
        return words[0][:3].upper()
    return "".join(word[0] for word in words[-3:]).upper()


def _pitcher_game_log_splits(player_id: int, season: int) -> List[Dict[str, Any]]:
    try:
        data = statsapi.get(
            "person",
            {
                "personId": player_id,
                "hydrate": (
                    "stats(group=pitching,type=gameLog,"
                    f"sportId=1,season={season}),currentTeam"
                ),
            },
        )
    except Exception:
        return []

    people = data.get("people") or []
    if not people:
        return []

    for stats_block in people[0].get("stats") or []:
        type_name = str((stats_block.get("type") or {}).get("displayName", "")).lower()
        group_name = str((stats_block.get("group") or {}).get("displayName", "")).lower()
        if type_name == "gamelog" and group_name == "pitching":
            return list(stats_block.get("splits") or [])
    return []


def _format_recent_pitcher_game_line(split: Dict[str, Any]) -> Optional[str]:
    stat = split.get("stat") or {}
    strikeouts = _to_int(stat.get("strikeOuts"))
    pitch_count = _to_int(stat.get("numberOfPitches"))
    opponent = split.get("opponent") or {}
    opponent_abbrev = _team_display_abbreviation(opponent.get("name"))
    location_marker = "v" if split.get("isHome") is True else "@"

    if not opponent_abbrev or strikeouts is None or pitch_count is None:
        return None
    return f"{location_marker} {opponent_abbrev} {strikeouts}K {pitch_count}P"


def fetch_pitcher_recent_game_lines(
    player_id: int,
    season: int,
    report_date: datetime.date,
    *,
    limit: int = 5,
) -> List[str]:
    all_splits: Dict[Tuple[datetime.date, int], Dict[str, Any]] = {}
    for candidate_season in [season, season - 1]:
        if candidate_season < 1900:
            continue
        for split in _pitcher_game_log_splits(player_id, candidate_season):
            game_date = _parse_stat_date(split.get("date"))
            if game_date is None or game_date >= report_date:
                continue
            game_pk = _to_int((split.get("game") or {}).get("gamePk")) or 0
            all_splits[(game_date, game_pk)] = split

    recent_splits = [
        split
        for _, split in sorted(all_splits.items(), key=lambda item: item[0], reverse=True)[:limit]
    ]
    lines = [_format_recent_pitcher_game_line(split) for split in recent_splits]
    return [line for line in lines if line]


def add_pitcher_recent_game_logs(
    pitchers: pd.DataFrame,
    season: int,
    report_date: datetime.date,
) -> pd.DataFrame:
    enriched = pitchers.copy()
    if enriched.empty:
        enriched[RECENT_PITCHER_GAMES_COLUMN] = pd.Series(dtype="object")
        return enriched

    values: List[List[str]] = [[] for _ in range(len(enriched))]
    tasks: List[Tuple[int, int]] = []
    for row_position, (_, row) in enumerate(enriched.iterrows()):
        player_id = _to_int(row.get(PLAYER_ID_COLUMN))
        if player_id is None:
            continue
        tasks.append((row_position, player_id))

    if not tasks:
        enriched[RECENT_PITCHER_GAMES_COLUMN] = values
        return enriched

    max_workers = min(16, len(tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_position = {
            executor.submit(fetch_pitcher_recent_game_lines, player_id, season, report_date): row_position
            for row_position, player_id in tasks
        }
        for future, row_position in future_to_position.items():
            try:
                values[row_position] = future.result()
            except Exception:
                values[row_position] = []

    enriched[RECENT_PITCHER_GAMES_COLUMN] = values
    return enriched


def _recent_team_k_percent(
    splits: Sequence[Dict[str, Any]],
    cutoff_date: datetime.date,
    limit: int,
) -> Optional[float]:
    prior_games: List[Tuple[datetime.date, int, float, float]] = []
    for split in splits:
        game_date = _parse_stat_date(split.get("date"))
        if game_date is None or game_date >= cutoff_date:
            continue

        stat = split.get("stat") or {}
        strikeouts = pd.to_numeric(stat.get("strikeOuts"), errors="coerce")
        plate_appearances = pd.to_numeric(stat.get("plateAppearances"), errors="coerce")
        if pd.isna(strikeouts) or pd.isna(plate_appearances) or plate_appearances <= 0:
            continue

        game_info = split.get("game") or {}
        game_pk = _to_int(game_info.get("gamePk")) or 0
        prior_games.append((game_date, game_pk, float(strikeouts), float(plate_appearances)))

    if not prior_games:
        return None

    prior_games.sort(key=lambda row: (row[0], row[1]))
    recent_games = prior_games[-limit:]
    total_strikeouts = sum(row[2] for row in recent_games)
    total_plate_appearances = sum(row[3] for row in recent_games)
    if total_plate_appearances <= 0:
        return None
    return float(100 * total_strikeouts / total_plate_appearances)


def _get_team_recent_k_lookup(
    team_id: int,
    season: int,
    cutoff_date: datetime.date,
) -> Dict[str, Optional[float]]:
    cache_key = (team_id, season, cutoff_date.isoformat())
    cached = TEAM_RECENT_K_CACHE.get(cache_key)
    if cached is not None:
        return cached

    result = {
        "last_5": None,
        "last_10": None,
    }
    try:
        data = statsapi.get(
            "team_stats",
            {
                "teamId": team_id,
                "season": season,
                "group": "hitting",
                "stats": "gameLog",
            },
        )
    except Exception:
        TEAM_RECENT_K_CACHE[cache_key] = result
        return result

    stats_blocks = data.get("stats") or []
    splits = (stats_blocks[0].get("splits") or []) if stats_blocks else []
    result = {
        "last_5": _recent_team_k_percent(splits, cutoff_date, 5),
        "last_10": _recent_team_k_percent(splits, cutoff_date, 10),
    }
    TEAM_RECENT_K_CACHE[cache_key] = result
    return result


def _fetch_team_split_k_percent(team_id: int, season: int, sit_code: str) -> Optional[float]:
    try:
        data = statsapi.get(
            "team_stats",
            {
                "teamId": team_id,
                "season": season,
                "group": "hitting",
                "stats": "statSplits",
                "sitCodes": sit_code,
            },
        )
    except Exception:
        return None

    stats_blocks = data.get("stats") or []
    if not stats_blocks:
        return None
    splits = stats_blocks[0].get("splits") or []
    if not splits:
        return None
    stat = splits[0].get("stat") or {}

    strikeouts = pd.to_numeric(stat.get("strikeOuts"), errors="coerce")
    plate_appearances = pd.to_numeric(stat.get("plateAppearances"), errors="coerce")
    if pd.isna(strikeouts) or pd.isna(plate_appearances) or plate_appearances <= 0:
        return None
    return float(100 * strikeouts / plate_appearances)


def _get_team_hand_split_k_lookup(team_id: int, season: int) -> Dict[str, Optional[float]]:
    cache_key = (team_id, season)
    cached = TEAM_HAND_SPLIT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    candidate_seasons = [season]
    if season > 1900:
        candidate_seasons.append(season - 1)

    result = {"vs_lhp": None, "vs_rhp": None}
    for candidate_season in candidate_seasons:
        vs_lhp = _fetch_team_split_k_percent(team_id, candidate_season, "vl")
        vs_rhp = _fetch_team_split_k_percent(team_id, candidate_season, "vr")
        if vs_lhp is not None or vs_rhp is not None:
            result = {"vs_lhp": vs_lhp, "vs_rhp": vs_rhp}
            break

    TEAM_HAND_SPLIT_CACHE[cache_key] = result
    return result


def _get_team_hand_split_k_rank_map(season: int, pitcher_hand: str) -> Dict[int, int]:
    hand = str(pitcher_hand or "").strip().upper()
    if hand == "L":
        split_key = "vs_lhp"
    elif hand == "R":
        split_key = "vs_rhp"
    else:
        return {}

    cache_key = (season, hand)
    cached = TEAM_HAND_SPLIT_RANK_CACHE.get(cache_key)
    if cached is not None:
        return cached

    rows: List[Tuple[int, float]] = []
    for team_id in fetch_mlb_team_ids(season):
        split_lookup = _get_team_hand_split_k_lookup(team_id, season)
        k_percent = _to_float(split_lookup.get(split_key))
        if k_percent is None:
            continue
        rows.append((team_id, k_percent))

    rows.sort(key=lambda row: row[1], reverse=True)
    rank_map = {team_id: rank for rank, (team_id, _) in enumerate(rows, start=1)}
    TEAM_HAND_SPLIT_RANK_CACHE[cache_key] = rank_map
    return rank_map


def build_opponent_hand_k_lookup(
    schedule: Sequence[Dict[str, Any]],
    season: int,
) -> Dict[str, Dict[str, Any]]:
    team_name_to_id: Dict[str, int] = {}
    for game in schedule:
        away_name = str(game.get("away_name", "")).strip()
        home_name = str(game.get("home_name", "")).strip()
        away_id = _to_int(game.get("away_id"))
        home_id = _to_int(game.get("home_id"))
        if away_name and away_id is not None:
            team_name_to_id[away_name] = away_id
        if home_name and home_id is not None:
            team_name_to_id[home_name] = home_id

    rank_maps = {
        "L": _get_team_hand_split_k_rank_map(season, "L"),
        "R": _get_team_hand_split_k_rank_map(season, "R"),
    }
    lookup: Dict[str, Dict[str, Any]] = {}
    for team_name, team_id in team_name_to_id.items():
        split_lookup = dict(_get_team_hand_split_k_lookup(team_id, season))
        split_lookup["vs_lhp_rank"] = rank_maps["L"].get(team_id)
        split_lookup["vs_rhp_rank"] = rank_maps["R"].get(team_id)
        lookup[team_name] = split_lookup
    return lookup


def build_opponent_recent_k_lookup(
    schedule: Sequence[Dict[str, Any]],
    season: int,
    report_date: datetime.date,
) -> Dict[str, Dict[str, Optional[float]]]:
    team_name_to_id: Dict[str, int] = {}
    for game in schedule:
        away_name = str(game.get("away_name", "")).strip()
        home_name = str(game.get("home_name", "")).strip()
        away_id = _to_int(game.get("away_id"))
        home_id = _to_int(game.get("home_id"))
        if away_name and away_id is not None:
            team_name_to_id[away_name] = away_id
        if home_name and home_id is not None:
            team_name_to_id[home_name] = home_id

    return {
        team_name: _get_team_recent_k_lookup(team_id, season, report_date)
        for team_name, team_id in team_name_to_id.items()
    }


def add_opponent_hand_matchup_k_percent(
    pitchers: pd.DataFrame,
    opponent_hand_lookup: Dict[str, Dict[str, Any]],
) -> pd.DataFrame:
    enriched = pitchers.copy()
    if enriched.empty:
        enriched[OPP_HAND_K_COLUMN] = pd.NA
        enriched[OPP_HAND_K_RANK_COLUMN] = pd.NA
        return enriched

    values: List[Optional[float]] = []
    ranks: List[Optional[int]] = []
    for _, row in enriched.iterrows():
        hand = str(row.get("Hand", "")).strip().upper()
        opponent = str(row.get("Opponent", "")).strip()
        split_lookup = opponent_hand_lookup.get(opponent, {})
        if hand == "L":
            values.append(split_lookup.get("vs_lhp"))
            ranks.append(_to_int(split_lookup.get("vs_lhp_rank")))
        elif hand == "R":
            values.append(split_lookup.get("vs_rhp"))
            ranks.append(_to_int(split_lookup.get("vs_rhp_rank")))
        else:
            values.append(None)
            ranks.append(None)

    enriched[OPP_HAND_K_COLUMN] = values
    enriched[OPP_HAND_K_RANK_COLUMN] = ranks
    return enriched


def add_opponent_recent_k_percent(
    pitchers: pd.DataFrame,
    opponent_recent_lookup: Dict[str, Dict[str, Optional[float]]],
) -> pd.DataFrame:
    enriched = pitchers.copy()
    if enriched.empty:
        enriched[OPP_LAST_5_K_COLUMN] = pd.NA
        enriched[OPP_LAST_10_K_COLUMN] = pd.NA
        return enriched

    last_5_values: List[Optional[float]] = []
    last_10_values: List[Optional[float]] = []
    for _, row in enriched.iterrows():
        opponent = str(row.get("Opponent", "")).strip()
        recent_lookup = opponent_recent_lookup.get(opponent, {})
        last_5_values.append(recent_lookup.get("last_5"))
        last_10_values.append(recent_lookup.get("last_10"))

    enriched[OPP_LAST_5_K_COLUMN] = last_5_values
    enriched[OPP_LAST_10_K_COLUMN] = last_10_values
    return enriched


def add_pitcher_whiff_percent(
    pitchers: pd.DataFrame,
    whiff_lookup: Dict[str, float],
) -> pd.DataFrame:
    enriched = pitchers.copy()
    if enriched.empty:
        enriched["Whiff%"] = pd.NA
        return enriched

    values: List[Optional[float]] = []
    for name in enriched.get("Name", pd.Series(index=enriched.index, dtype=object)):
        values.append(whiff_lookup.get(_normalize_person_name(name)))
    enriched["Whiff%"] = values
    return enriched


def sort_pitchers_for_report(pitchers: pd.DataFrame) -> pd.DataFrame:
    if pitchers.empty:
        return pitchers

    sorted_df = pitchers.copy()
    status_series = (
        sorted_df["Status"].astype(str).str.strip()
        if "Status" in sorted_df.columns
        else pd.Series([""] * len(sorted_df), index=sorted_df.index)
    )
    k_pa_series = (
        pd.to_numeric(sorted_df[K_PA_COLUMN], errors="coerce")
        if K_PA_COLUMN in sorted_df.columns
        else pd.Series([float("nan")] * len(sorted_df), index=sorted_df.index)
    )
    so_pa_series = (
        pd.to_numeric(sorted_df["SO/PA"], errors="coerce")
        if "SO/PA" in sorted_df.columns
        else pd.Series([float("nan")] * len(sorted_df), index=sorted_df.index)
    )
    sort_metric_series = k_pa_series if k_pa_series.notna().any() else so_pa_series

    sorted_df["__status_sort"] = status_series.map(lambda status: 0 if status in NOT_STARTED_STATUSES else 1)
    sorted_df["__metric_missing"] = sort_metric_series.isna().astype(int)
    sorted_df["__metric_sort"] = sort_metric_series.fillna(float("-inf"))
    if "Name" in sorted_df.columns:
        sorted_df["__name_sort"] = sorted_df["Name"].astype(str)
        sort_by = ["__status_sort", "__metric_missing", "__metric_sort", "__name_sort"]
        ascending = [True, True, False, True]
    else:
        sort_by = ["__status_sort", "__metric_missing", "__metric_sort"]
        ascending = [True, True, False]

    sorted_df = sorted_df.sort_values(by=sort_by, ascending=ascending, kind="mergesort")
    return sorted_df.drop(
        columns=["__status_sort", "__metric_missing", "__metric_sort", "__name_sort"],
        errors="ignore",
    )


def calculate_additional_metrics(date: str, pitchers: pd.DataFrame) -> pd.DataFrame:
    pitchers = pitchers.copy()

    for col in ["AB", "GP", "K", "BB", "BF", "SO/PA", "K%", "PA", "K/9", "r"]:
        if col not in pitchers.columns:
            pitchers[col] = pd.NA

    gp = pd.to_numeric(pitchers["GP"], errors="coerce")
    k = pd.to_numeric(pitchers["K"], errors="coerce")
    bf = pd.to_numeric(pitchers["BF"], errors="coerce")
    status_series = (
        pitchers["Status"].astype(str).str.strip()
        if "Status" in pitchers.columns
        else pd.Series([""] * len(pitchers), index=pitchers.index)
    )

    valid_pa_gp = bf.notna() & (bf > 0) & gp.notna() & (gp > 0)
    pitchers[PA_GP_COLUMN] = pd.Series(pd.NA, index=pitchers.index, dtype="object")
    pitchers.loc[valid_pa_gp, PA_GP_COLUMN] = bf[valid_pa_gp] / gp[valid_pa_gp]
    pitchers[K_PA_COLUMN] = pd.Series(pd.NA, index=pitchers.index, dtype="object")
    valid_bf = bf.notna() & (bf > 0)
    pitchers.loc[valid_bf, K_PA_COLUMN] = 100 * (k[valid_bf] / bf[valid_bf])
    pitchers["Ks"] = [
        get_strikeouts_by_player_name(date, name) if status in COMPLETED_STATUSES else ""
        for name, status in zip(pitchers["Name"], status_series)
    ]

    for col in REPORT_COLUMN_ORDER:
        if col not in pitchers.columns:
            pitchers[col] = pd.NA

    extra_columns = [col for col in pitchers.columns if col not in REPORT_COLUMN_ORDER]
    pitchers = pitchers[REPORT_COLUMN_ORDER + extra_columns].copy()
    return sort_pitchers_for_report(pitchers)


def merge_with_odds_data(pitchers: pd.DataFrame, report_date: str) -> pd.DataFrame:
    odds_frames: List[pd.DataFrame] = []
    pending_names = [
        name
        for name, status in zip(pitchers["Name"], pitchers["Status"])
        if status not in COMPLETED_STATUSES
    ]
    unique_pending_names = list(dict.fromkeys(pending_names))

    odds_lookup_cache: Dict[str, Optional[pd.DataFrame]] = {}
    for name in unique_pending_names:
        odds_lookup_cache[name] = fetch_pitcher_odds(name, report_date)

    for odds_df in odds_lookup_cache.values():
        if isinstance(odds_df, pd.DataFrame) and not odds_df.empty:
            odds_frames.append(odds_df)

    if not odds_frames:
        return pitchers

    odds_df_all = pd.concat(odds_frames, ignore_index=True)
    preferred_existing = [col for col in PREFERRED_ODDS_COLUMNS if col in odds_df_all.columns]
    if preferred_existing:
        odds_df_all = odds_df_all[["pitcher"] + preferred_existing]

    final_df = pd.merge(pitchers, odds_df_all, left_on="Name", right_on="pitcher", how="left")
    if "pitcher" in final_df.columns:
        final_df.drop(columns=["pitcher"], inplace=True)
    return final_df


def _odds_columns_from_df(df: pd.DataFrame) -> List[str]:
    return [col for col in df.columns if col not in REPORT_COLUMN_ORDER and col not in INTERNAL_ONLY_COLUMNS]


def _split_primary_and_alts(value: Any) -> Tuple[str, List[str]]:
    text = str(value).strip() if value is not None else ""
    if not text:
        return "", []
    if ALT_LINES_TOKEN not in text:
        return text, []
    primary_line, raw_alts = text.split(ALT_LINES_TOKEN, 1)
    alternate_lines = [line.strip() for line in raw_alts.split(";") if line.strip()]
    return primary_line.strip(), alternate_lines


def _format_odds_point(point: Any) -> str:
    numeric = pd.to_numeric(point, errors="coerce")
    if pd.isna(numeric):
        return str(point)
    return f"{float(numeric):g}"


def _sportsbook_tag_for_column(column_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(column_name or "").lower())
    if normalized in SPORTSBOOK_TAGS:
        return SPORTSBOOK_TAGS[normalized]
    fallback = re.sub(r"[^A-Z0-9]+", "", str(column_name or "").upper())
    return (fallback or "BOOK")[:4]


def _book_sort_key(book_name: str) -> Tuple[int, str]:
    try:
        return (PREFERRED_ODDS_COLUMNS.index(book_name), book_name.lower())
    except ValueError:
        return (len(PREFERRED_ODDS_COLUMNS), book_name.lower())


def _parse_odds_line(line_text: str) -> Optional[Dict[str, Any]]:
    match = PRIMARY_ODDS_PATTERN.match(str(line_text or "").strip())
    if not match:
        return None

    point_value = pd.to_numeric(match.group(1), errors="coerce")
    if pd.isna(point_value):
        return None

    over_price = _safe_int(match.group(2))
    under_price = _safe_int(match.group(3))
    if over_price is None and under_price is None:
        return None

    return {
        "point": float(point_value),
        "point_text": _format_odds_point(point_value),
        "over_price": over_price,
        "under_price": under_price,
    }


def _parse_book_odds_entries(book_name: str, value: Any) -> List[Dict[str, Any]]:
    if value is None:
        return []
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return []

    primary_line, alternate_lines = _split_primary_and_alts(text)
    parsed_entries: List[Dict[str, Any]] = []
    for line_text, is_primary in [(primary_line, True), *[(line, False) for line in alternate_lines]]:
        parsed = _parse_odds_line(line_text)
        if not parsed:
            continue
        parsed_entries.append(
            {
                "book": book_name,
                "tag": _sportsbook_tag_for_column(book_name),
                "is_primary": is_primary,
                **parsed,
            }
        )

    deduped: Dict[Tuple[str, float], Dict[str, Any]] = {}
    for entry in parsed_entries:
        key = (entry["book"], entry["point"])
        existing = deduped.get(key)
        if existing is None or (entry["is_primary"] and not existing["is_primary"]):
            deduped[key] = entry
    return list(deduped.values())


def _collect_pitcher_odds_entries(row_data: Any, odds_columns: Sequence[str]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for odds_column in odds_columns:
        entries.extend(_parse_book_odds_entries(odds_column, row_data.get(odds_column)))
    return entries


def _choose_consensus_primary_point(entries: Sequence[Dict[str, Any]]) -> Optional[float]:
    primary_entries = [entry for entry in entries if entry.get("is_primary")]
    if not primary_entries:
        return None

    counts: Dict[float, int] = {}
    earliest_book_rank: Dict[float, Tuple[int, str]] = {}
    for entry in primary_entries:
        point = float(entry["point"])
        counts[point] = counts.get(point, 0) + 1
        book_rank = _book_sort_key(str(entry.get("book", "")))
        existing_rank = earliest_book_rank.get(point)
        if existing_rank is None or book_rank < existing_rank:
            earliest_book_rank[point] = book_rank

    return min(
        counts,
        key=lambda point: (-counts[point], earliest_book_rank.get(point, (len(PREFERRED_ODDS_COLUMNS), "")), point),
    )


def _best_price_entry(
    entries: Sequence[Dict[str, Any]],
    point: float,
    side: str,
) -> Optional[Dict[str, Any]]:
    side_key = f"{side}_price"
    candidates = [
        entry
        for entry in entries
        if float(entry.get("point", float("nan"))) == float(point) and entry.get(side_key) is not None
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda entry: (int(entry[side_key]), -_book_sort_key(str(entry.get("book", "")))[0], str(entry.get("book", ""))),
    )


def _price_text(price: Optional[int]) -> str:
    if price is None:
        return "-"
    return f"{price:+d}"


def _render_sportsbook_badge(
    book_name: Any,
    tag_text: Any,
    *,
    extra_class: str = "",
) -> str:
    book_text = str(book_name or "").strip()
    tag = str(tag_text or "").strip()
    if not book_text or not tag or tag == "-":
        return ""

    classes = ["k-src-marker", "sportsbook-badge"]
    if extra_class:
        classes.append(extra_class)
    color = _sportsbook_color_for_column(book_text)
    title = escape(book_text, quote=True)
    return (
        f'<span class="{" ".join(classes)}" style="--sportsbook-color: {escape(color)};" '
        f'title="{title}">{escape(tag)}</span>'
    )


def _render_odds_price_span(side: str, price: Optional[int], *, is_best: bool = False) -> str:
    classes = [f"odds-{side}"]
    if is_best:
        classes.append(f"best-{side}")
    return f'<span class="{" ".join(classes)}">{escape(_price_text(price))}</span>'


def _classify_best_odds_point(point: Any) -> str:
    numeric = pd.to_numeric(point, errors="coerce")
    if pd.isna(numeric):
        return "best-odds-point-neutral"

    value = float(numeric)
    if value >= 7.5:
        return "best-odds-point-elite"
    if value >= 6.5:
        return "best-odds-point-strong"
    if value <= 4.5:
        return "best-odds-point-weak"
    return "best-odds-point-neutral"


def summarize_pitcher_best_k_odds(
    row_data: Any,
    odds_columns: Sequence[str],
) -> Dict[str, Any]:
    entries = _collect_pitcher_odds_entries(row_data, odds_columns)
    if not entries:
        return {"summary": "-", "consensus_point": None, "entries": [], "line_groups": []}

    consensus_point = _choose_consensus_primary_point(entries)
    if consensus_point is None:
        return {"summary": "-", "consensus_point": None, "entries": entries, "line_groups": []}

    best_over = _best_price_entry(entries, consensus_point, "over")
    best_under = _best_price_entry(entries, consensus_point, "under")
    summary = " | ".join(
        [
            _format_odds_point(consensus_point),
            f'O {_price_text(best_over.get("over_price") if best_over else None)} {best_over.get("tag") if best_over else "-"}',
            f'U {_price_text(best_under.get("under_price") if best_under else None)} {best_under.get("tag") if best_under else "-"}',
        ]
    )

    grouped_entries: Dict[float, List[Dict[str, Any]]] = {}
    for entry in entries:
        grouped_entries.setdefault(float(entry["point"]), []).append(entry)

    sorted_points = [consensus_point] + sorted(point for point in grouped_entries if point != consensus_point)
    line_groups: List[Dict[str, Any]] = []
    for point in sorted_points:
        point_entries = sorted(grouped_entries[point], key=lambda entry: _book_sort_key(str(entry.get("book", ""))))
        best_over_entry = _best_price_entry(point_entries, point, "over")
        best_under_entry = _best_price_entry(point_entries, point, "under")
        line_groups.append(
            {
                "point": point,
                "point_text": _format_odds_point(point),
                "entries": point_entries,
                "best_over_book": best_over_entry.get("book") if best_over_entry else None,
                "best_under_book": best_under_entry.get("book") if best_under_entry else None,
            }
        )

    return {
        "summary": summary,
        "consensus_point": consensus_point,
        "best_over": best_over,
        "best_under": best_under,
        "entries": entries,
        "line_groups": line_groups,
    }


def _render_best_k_odds_cell(row_data: Any, odds_columns: Sequence[str]) -> str:
    odds_summary = summarize_pitcher_best_k_odds(row_data, odds_columns)
    summary_text = str(odds_summary.get("summary", "-")).strip() or "-"
    line_groups = odds_summary.get("line_groups") or []
    if summary_text == "-" or not line_groups:
        return (
            '<div class="best-odds-cell best-odds-cell-empty" title="No strikeout odds available">'
            '<span class="best-odds-summary">-</span></div>'
        )

    best_over = odds_summary.get("best_over")
    best_under = odds_summary.get("best_under")
    consensus_point = line_groups[0].get("point")
    consensus_point_text = str(line_groups[0].get("point_text", ""))
    consensus_point_class = _classify_best_odds_point(consensus_point)
    summary_parts = [
        f'<span class="best-odds-point {consensus_point_class}">{escape(consensus_point_text)}</span>',
        '<span class="best-odds-divider" aria-hidden="true">|</span>',
        '<span class="best-odds-side">'
        '<span class="best-odds-side-label">O</span>'
        f'<span class="odds-over best-over">{escape(_price_text(best_over.get("over_price") if best_over else None))}</span>'
        f'{_render_sportsbook_badge(best_over.get("book") if best_over else "", best_over.get("tag") if best_over else "", extra_class="sportsbook-badge-summary")}'
        "</span>",
        '<span class="best-odds-divider" aria-hidden="true">|</span>',
        '<span class="best-odds-side">'
        '<span class="best-odds-side-label">U</span>'
        f'<span class="odds-under best-under">{escape(_price_text(best_under.get("under_price") if best_under else None))}</span>'
        f'{_render_sportsbook_badge(best_under.get("book") if best_under else "", best_under.get("tag") if best_under else "", extra_class="sportsbook-badge-summary")}'
        "</span>",
    ]

    detail_groups: List[str] = []
    for group in line_groups:
        group_entries_html = []
        for entry in group.get("entries", []):
            is_best_over = entry.get("book") == group.get("best_over_book")
            is_best_under = entry.get("book") == group.get("best_under_book")
            group_entries_html.append(
                '<li class="odds-book-row">'
                f'{_render_sportsbook_badge(entry.get("book"), entry.get("tag"), extra_class="sportsbook-badge-detail")}'
                f'{_render_odds_price_span("over", entry.get("over_price"), is_best=is_best_over)}'
                f'{_render_odds_price_span("under", entry.get("under_price"), is_best=is_best_under)}'
                "</li>"
            )
        detail_groups.append(
            '<li class="odds-line-group">'
            f'<span class="odds-line-label">{escape(str(group.get("point_text", "")))}</span>'
            '<ul class="odds-book-list">'
            + "".join(group_entries_html)
            + "</ul></li>"
        )

    cell_classes = ["best-odds-cell"]
    if best_over is None or best_under is None:
        cell_classes.append("best-odds-cell-missing-side")

    return (
        f'<div class="{" ".join(cell_classes)}" title="{escape(summary_text, quote=True)}" aria-label="{escape(summary_text, quote=True)}">'
        '<span class="best-odds-summary">'
        + "".join(summary_parts)
        + "</span>"
        '<details class="odds-details">'
        '<summary aria-label="Show all strikeout odds" title="Show all strikeout odds">'
        '<span class="odds-arrow">&#9662;</span>'
        "</summary>"
        '<ul class="odds-details-list">'
        + "".join(detail_groups)
        + "</ul></details></div>"
    )


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return None
    text = text.replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_sportsbook_name(name: str) -> str:
    return "".join(char.lower() for char in str(name) if char.isalnum())


def _sportsbook_color_for_column(column_name: str) -> str:
    normalized = _normalize_sportsbook_name(column_name)
    if normalized in SPORTSBOOK_COLORS:
        return SPORTSBOOK_COLORS[normalized]
    fallback_index = abs(hash(normalized)) % len(SPORTSBOOK_FALLBACK_COLORS)
    return SPORTSBOOK_FALLBACK_COLORS[fallback_index]


def _add_tag_class(tag: Any, class_name: str) -> None:
    classes = tag.get("class", [])
    if class_name not in classes:
        classes.append(class_name)
        tag["class"] = classes


def _set_tag_style_var(tag: Any, var_name: str, var_value: str) -> None:
    style_parts = [part.strip() for part in tag.get("style", "").split(";") if part.strip()]
    style_map: Dict[str, str] = {}
    for part in style_parts:
        if ":" not in part:
            continue
        key, value = part.split(":", 1)
        style_map[key.strip()] = value.strip()
    style_map[var_name] = var_value
    tag["style"] = "; ".join(f"{key}: {value}" for key, value in style_map.items()) + ";"


def _render_matchup_source_marker(value: Any) -> str:
    text = str(value or "").strip()
    if text == MATCHUP_SOURCE_ESPN:
        return '<span class="k-src-marker src-espn" title="ESPN confirmed lineup sample">E</span>'
    if text == MATCHUP_SOURCE_PREVIOUS_LINEUP:
        return '<span class="k-src-marker src-previous-lineup" title="Previous completed lineup BvP sample">P</span>'
    if text == MATCHUP_SOURCE_SAVANT:
        return '<span class="k-src-marker src-savant" title="Savant probable-lineup sample">S</span>'
    return ""


def _matchup_source_label(value: Any) -> str:
    text = str(value or "").strip()
    if text == MATCHUP_SOURCE_ESPN:
        return "ESPN confirmed lineup"
    if text == MATCHUP_SOURCE_PREVIOUS_LINEUP:
        return "Previous lineup BvP"
    if text == MATCHUP_SOURCE_SAVANT:
        return "Savant fallback"
    return ""


def _matchup_lines_from_value(value: Any) -> List[str]:
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _annotate_k_percent_with_source(k_percent_value: Any, source_value: Any, matchup_lines: Any = None) -> str:
    text = str(k_percent_value or "").strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return "-"
    marker_html = _render_matchup_source_marker(source_value)
    if not marker_html:
        return escape(text)
    source_label = _matchup_source_label(source_value)
    lines = _matchup_lines_from_value(matchup_lines)[:9]
    popup_lines = "".join(f'<span class="matchup-k-line">{escape(line)}</span>' for line in lines)
    classes = ["matchup-k-cell", "matchup-k-has-popup"]
    if lines:
        classes.append("matchup-k-has-lines")
    return (
        f'<span class="{" ".join(classes)}">'
        f'<span class="matchup-k-value">{escape(text)}</span>'
        '<span class="matchup-k-popup" role="tooltip" aria-label="K% source and batter lines">'
        '<span class="matchup-k-source-line">'
        f"{marker_html}"
        f'<span class="matchup-k-source-text">{escape(source_label)}</span>'
        "</span>"
        f"{popup_lines}"
        "</span>"
        "</span>"
    )


def _render_opponent_hand_k_with_rank(k_percent_value: Any, rank_value: Any) -> str:
    text = str(k_percent_value or "").strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return "-"
    rank = _to_int(rank_value)
    if rank is None or rank <= 0:
        return escape(text)
    max_rank = 30
    rank_pct = min(max((rank - 1) / (max_rank - 1), 0.0), 1.0)
    rank_hue = int(round(140 - (140 * rank_pct)))
    return (
        '<span class="opp-hand-k-cell">'
        f'<span class="opp-hand-k-value">{escape(text)}</span>'
        '<span class="k-src-gap" aria-hidden="true"></span>'
        f'<span class="opp-hand-rank-badge" style="--rank-hue: {rank_hue};" '
        'title="Opponent MLB rank in K% versus pitcher hand; 1 = highest K%, 30 = lowest K%">'
        f"{rank}</span>"
        "</span>"
    )


def _opponent_time_chip_metadata(status: Any) -> Tuple[str, str]:
    status_text = str(status or "").strip()
    if status_text in NOT_STARTED_STATUSES:
        return ("opp-time opp-time-upcoming", f"Game status: {status_text}")
    if status_text == "In Progress":
        return ("opp-time opp-time-live", "Game status: In Progress")
    if status_text == "Final":
        return ("opp-time opp-time-final", "Game status: Final")
    if status_text:
        return ("opp-time", f"Game status: {status_text}")
    return ("opp-time", "Game time")


def _render_opponent_with_start(opponent: Any, start_time: Any, status: Any = None) -> str:
    opponent_text = str(opponent or "").strip()
    start_text = str(start_time or "").strip()
    if opponent_text in {"", "-", "N/A", "nan", "None"} and start_text in {"", "-", "N/A", "nan", "None"}:
        return "-"

    if opponent_text in {"", "-", "N/A", "nan", "None"}:
        opponent_html = "-"
    else:
        logo_src = get_team_logo_src(team_name=opponent_text)
        if logo_src:
            logo_url = escape(logo_src, quote=True)
            logo_alt = escape(f"{opponent_text} logo", quote=True)
            logo_html = (
                f'<img class="opp-logo" src="{logo_url}" alt="{logo_alt}" '
                'loading="lazy" decoding="async" referrerpolicy="no-referrer">'
            )
            opponent_html = make_opponent_hyperlink(opponent_text, logo_html)
        else:
            opponent_html = make_opponent_hyperlink(opponent_text)

    if start_text in {"", "-", "N/A", "nan", "None"}:
        return opponent_html

    time_classes, time_title = _opponent_time_chip_metadata(status)
    return (
        '<span class="opp-cell">'
        f'<span class="opp-team">{opponent_html}</span>'
        f'<span class="{escape(time_classes, quote=True)}" title="{escape(time_title, quote=True)}">{escape(start_text)}</span>'
        "</span>"
    )


def _render_hand_badge(hand: Any) -> str:
    try:
        if pd.isna(hand):
            hand_text = ""
        else:
            hand_text = str(hand).strip().upper()
    except (TypeError, ValueError):
        hand_text = str(hand or "").strip().upper()
    if hand_text not in {"R", "L"}:
        hand_text = "TBD"
    hand_class = {"R": "hand-right", "L": "hand-left"}.get(hand_text, "hand-tbd")
    return (
        f'<span class="k-src-marker hand-marker {hand_class}" '
        f'title="Pitcher handedness: {escape(hand_text)}">{escape(hand_text)}</span>'
    )


def _recent_game_lines_from_value(value: Any) -> List[str]:
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    if value is None:
        return []
    try:
        if pd.isna(value):
            return []
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return []
    return [line.strip() for line in text.splitlines() if line.strip()]


def _render_pitcher_name_cell(name: Any, hand: Any, recent_games: Any = None) -> str:
    name_text = str(name or "").strip()
    if not name_text:
        return "-"
    recent_lines = _recent_game_lines_from_value(recent_games)[:5]
    classes = ["pitcher-name-cell"]
    popup_html = ""
    if recent_lines:
        classes.append("pitcher-has-recent")
        popup_lines = "".join(
            f'<span class="pitcher-recent-line">{escape(line)}</span>'
            for line in recent_lines
        )
        popup_html = (
            '<span class="pitcher-recent-popup" role="tooltip" aria-label="Last 5 pitcher games">'
            f"{popup_lines}"
            "</span>"
        )
    return (
        f'<span class="{" ".join(classes)}">'
        f"{make_pitcher_hyperlink(name_text)}"
        f"{_render_hand_badge(hand)}"
        f"{popup_html}"
        "</span>"
    )


def _classify_odds_cell(cell_text: str) -> str:
    text = cell_text.strip()
    if text in {"", "-", "N/A"}:
        return "odds-missing"

    match = PRIMARY_ODDS_PATTERN.search(text)
    if not match:
        return "odds-missing"

    over_price = _safe_int(match.group(2))
    under_price = _safe_int(match.group(3))
    if over_price is None and under_price is None:
        return "odds-missing"
    if (over_price is not None and over_price > 0) or (under_price is not None and under_price > 0):
        return "odds-plus"
    if (over_price is not None and over_price <= -140) or (under_price is not None and under_price <= -140):
        return "odds-juice"
    return "odds-even"


def _add_cell_class(cells: List[Any], column_map: Dict[str, int], column_name: str, class_name: str) -> None:
    column_index = column_map.get(column_name)
    if column_index is None or column_index >= len(cells):
        return
    existing_classes = cells[column_index].get("class", [])
    if class_name not in existing_classes:
        existing_classes.append(class_name)
        cells[column_index]["class"] = existing_classes


def _column_group_class(column_name: str) -> Optional[str]:
    if column_name in PITCHER_STAT_COLUMNS:
        return "group-pitcher"
    if column_name in OPPONENT_STAT_COLUMNS:
        return "group-opponent"
    if column_name in SAVANT_STAT_COLUMNS:
        return "group-savant"
    return None


def _column_tooltip_text(column_name: str, is_odds_column: bool) -> Optional[str]:
    if column_name in STAT_HEADER_TOOLTIPS:
        return STAT_HEADER_TOOLTIPS[column_name]
    if is_odds_column:
        return "Strikeout prop line and prices (line: over | under). Expand for alternate lines."
    return None


def _build_numeric_metric_context(series: pd.Series, mean_override: Optional[float] = None) -> Dict[str, Optional[float]]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    mean = mean_override if mean_override is not None else (float(numeric.mean()) if not numeric.empty else None)
    std = float(numeric.std(ddof=0)) if len(numeric) >= 2 else None
    if std is not None and std <= 0:
        std = None
    return {"mean": mean, "std": std}


def _metric_highlight_bands(metric_context: Dict[str, Optional[float]]) -> Tuple[Optional[float], Optional[float]]:
    mean = _to_optional_float(metric_context.get("mean"))
    std = _to_optional_float(metric_context.get("std"))
    if mean is None:
        return None, None

    if std is None or std < 0.25:
        strong = max(abs(mean) * 0.06, 0.6)
        elite = max(abs(mean) * 0.12, 1.2, strong + 0.4)
        return float(strong), float(elite)

    strong = max(std * 0.5, 0.45)
    elite = max(std, strong + 0.35)
    return float(strong), float(elite)


def _build_metric_contexts(
    raw_df: pd.DataFrame,
    pitcher_arsenal_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Dict[str, Dict[str, Optional[float]]]:
    contexts: Dict[str, Dict[str, Optional[float]]] = {}
    for column_name in [
        PA_GP_COLUMN,
        K_PA_COLUMN,
        "SO/PA",
        OPP_HAND_K_COLUMN,
        OPP_LAST_5_K_COLUMN,
        OPP_LAST_10_K_COLUMN,
        "K%",
        "K/9",
        "Whiff%",
        "PA",
        "Ks",
    ]:
        series = raw_df[column_name] if column_name in raw_df.columns else pd.Series(dtype="float64")
        contexts[column_name] = _build_numeric_metric_context(series)

    league_averages = {}
    if pitcher_arsenal_lookup:
        candidate = pitcher_arsenal_lookup.get(ARSENAL_META_KEY, {})
        if isinstance(candidate, dict):
            league_averages = candidate
    whiff_league_avg = _to_optional_float(league_averages.get("whiff_percent"))
    if whiff_league_avg is not None and "Whiff%" in contexts:
        contexts["Whiff%"]["mean"] = whiff_league_avg

    return contexts


def _apply_relative_metric_class(
    cells: List[Any],
    column_map: Dict[str, int],
    column_name: str,
    value: Optional[float],
    metric_contexts: Dict[str, Dict[str, Optional[float]]],
) -> None:
    if value is None:
        return

    metric_context = metric_contexts.get(column_name, {})
    mean = _to_optional_float(metric_context.get("mean"))
    strong_band, elite_band = _metric_highlight_bands(metric_context)
    if mean is None or strong_band is None or elite_band is None:
        return

    delta = value - mean
    if delta >= elite_band:
        _add_cell_class(cells, column_map, column_name, "cell-elite")
    elif delta >= strong_band:
        _add_cell_class(cells, column_map, column_name, "cell-strong")
    elif delta <= -elite_band:
        _add_cell_class(cells, column_map, column_name, "cell-weak")


def _classify_matchup_k_percent(k_pct: Optional[float], sample_size: Optional[float]) -> Optional[str]:
    if k_pct is None or sample_size is None:
        return None

    if sample_size < MATCHUP_K_MIN_SAMPLE:
        if k_pct >= MATCHUP_K_STRONG_PCT:
            return "cell-low-confidence"
        return None

    if k_pct < MATCHUP_K_WEAK_PCT:
        return "cell-weak"

    if sample_size < MATCHUP_K_HIGH_CONFIDENCE_SAMPLE:
        if k_pct >= MATCHUP_K_STRONG_PCT:
            return "cell-strong"
        return None

    if k_pct >= MATCHUP_K_ELITE_PCT:
        return "cell-elite"
    if k_pct >= MATCHUP_K_STRONG_PCT:
        return "cell-strong"
    return None


def _classify_matchup_sample_size(sample_size: Optional[float]) -> Optional[str]:
    if sample_size is None:
        return None
    if sample_size >= MATCHUP_K_HIGH_CONFIDENCE_SAMPLE:
        return "cell-confidence-high"
    if sample_size < MATCHUP_K_MIN_SAMPLE:
        return "cell-confidence-low"
    return None


def _build_pitcher_arsenal_payload(
    raw_df: pd.DataFrame,
    pitcher_arsenal_lookup: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if raw_df.empty or not pitcher_arsenal_lookup:
        return {}

    payload: Dict[str, Dict[str, Any]] = {}
    league_averages = pitcher_arsenal_lookup.get(ARSENAL_META_KEY, {})
    if isinstance(league_averages, dict):
        payload[ARSENAL_META_KEY] = {
            "whiff_percent": _to_optional_float(league_averages.get("whiff_percent")),
            "z_swing_miss_percent": _to_optional_float(league_averages.get("z_swing_miss_percent")),
            "oz_swing_miss_percent": _to_optional_float(league_averages.get("oz_swing_miss_percent")),
        }
    names = raw_df["Name"] if "Name" in raw_df.columns else pd.Series(dtype=object)
    for name in names:
        name_text = str(name).strip()
        name_key = _normalize_person_name(name_text)
        if not name_key:
            continue
        details = pitcher_arsenal_lookup.get(name_key)
        if not details:
            continue

        arsenal_items = []
        for pitch in details.get("arsenal", []):
            usage_percent = _to_optional_float(pitch.get("usage_percent"))
            if usage_percent is None:
                continue
            arsenal_items.append(
                {
                    "code": str(pitch.get("code", "")),
                    "label": str(pitch.get("label", "")),
                    "usage_percent": usage_percent,
                }
            )

        payload[name_key] = {
            "name": name_text,
            "whiff_percent": _to_optional_float(details.get("whiff_percent")),
            "z_swing_miss_percent": _to_optional_float(details.get("z_swing_miss_percent")),
            "oz_swing_miss_percent": _to_optional_float(details.get("oz_swing_miss_percent")),
            "fastball_percent": _to_optional_float(details.get("fastball_percent")),
            "arsenal": arsenal_items,
        }

    return payload


def _build_conditional_table_html(
    report_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    pitcher_arsenal_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
) -> str:
    table_html = report_df.to_html(index=False, escape=False, classes="pitchers-table", border=0)
    soup = BeautifulSoup(table_html, "html.parser")

    table = soup.find("table")
    if table is None:
        return table_html
    tbody = table.find("tbody")
    if tbody is None:
        return table_html

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    column_map = {header: idx for idx, header in enumerate(headers)}
    row_tags = tbody.find_all("tr")
    odds_columns = _odds_columns_from_df(raw_df)
    semantic_column_classes = {
        "Name": "column-name",
        "Opponent": "column-opponent",
        BEST_K_ODDS_COLUMN: "column-best-odds",
    }
    for column_name in OPPONENT_K_CONTEXT_COLUMNS:
        semantic_column_classes[column_name] = "column-opp-hand"
    metric_contexts = _build_metric_contexts(raw_df, pitcher_arsenal_lookup=pitcher_arsenal_lookup)

    thead = table.find("thead")
    header_cells = thead.find_all("th") if thead else []
    for col_name, col_class in semantic_column_classes.items():
        col_index = column_map.get(col_name)
        if col_index is None or col_index >= len(header_cells):
            continue
        _add_tag_class(header_cells[col_index], col_class)

    for col_name, col_index in column_map.items():
        if col_index >= len(header_cells):
            continue
        header_cell = header_cells[col_index]
        group_class = _column_group_class(col_name)
        if group_class:
            _add_tag_class(header_cell, group_class)
        tooltip_text = _column_tooltip_text(col_name, col_name in odds_columns)
        if tooltip_text:
            _add_tag_class(header_cell, "stat-tooltip")
            header_cell["title"] = tooltip_text
            header_cell["aria-label"] = f"{col_name}: {tooltip_text}"
        if col_name in PITCHERS_SORTABLE_COLUMNS:
            _add_tag_class(header_cell, "is-sortable")
            header_cell["data-sort-key"] = col_name
            header_cell["data-sort-index"] = str(col_index)
            header_cell["data-sort-direction"] = "default"
            header_cell["tabindex"] = "0"
            header_cell["role"] = "button"
            header_cell["aria-sort"] = "none"

    for row_index, row_tag in enumerate(row_tags):
        if row_index >= len(raw_df):
            break
        row_data = raw_df.iloc[row_index]
        row_classes = row_tag.get("class", [])
        row_tag["data-initial-index"] = str(row_index)
        pitcher_name = str(row_data.get("Name", "")).strip()
        pitcher_key = _normalize_person_name(pitcher_name)
        if pitcher_key:
            row_tag["data-pitcher-key"] = pitcher_key
            if "pitcher-row-selectable" not in row_classes:
                row_classes.append("pitcher-row-selectable")

        status = str(row_data.get("Status", "")).strip()
        if status in NOT_STARTED_STATUSES:
            row_classes.append("row-upcoming")
        elif status == "In Progress":
            row_classes.append("row-live")
        elif status == "Final":
            row_classes.append("row-final")

        k_pct = _to_float(row_data.get("K%"))
        so_pa = _to_float(row_data.get("SO/PA"))
        opp_k_vs_hand = _to_float(row_data.get(OPP_HAND_K_COLUMN))
        opp_last_5_k = _to_float(row_data.get(OPP_LAST_5_K_COLUMN))
        opp_last_10_k = _to_float(row_data.get(OPP_LAST_10_K_COLUMN))
        pa = _to_float(row_data.get("PA"))
        matchup_k_pct_class = _classify_matchup_k_percent(k_pct, pa)
        matchup_pa_class = _classify_matchup_sample_size(pa)
        sample_is_actionable = pa is not None and pa >= MATCHUP_K_MIN_SAMPLE

        if status in NOT_STARTED_STATUSES and k_pct is not None and so_pa is not None:
            k_context = metric_contexts.get("K%", {})
            so_context = metric_contexts.get("SO/PA", {})
            pa_context = metric_contexts.get("PA", {})
            k_mean = _to_optional_float(k_context.get("mean"))
            so_mean = _to_optional_float(so_context.get("mean"))
            pa_mean = _to_optional_float(pa_context.get("mean"))
            k_strong, _ = _metric_highlight_bands(k_context)
            so_strong, _ = _metric_highlight_bands(so_context)
            pa_strong, _ = _metric_highlight_bands(pa_context)

            if (
                k_mean is not None
                and so_mean is not None
                and k_strong is not None
                and so_strong is not None
            ):
                is_target = (
                    sample_is_actionable
                    and k_pct >= (k_mean + k_strong)
                    and so_pa >= (so_mean + so_strong)
                    and (
                        pa is None
                        or pa_mean is None
                        or pa_strong is None
                        or pa >= (pa_mean - pa_strong)
                    )
                )
                is_caution = (k_pct <= (k_mean - k_strong)) or (so_pa <= (so_mean - so_strong))
                if is_target:
                    row_classes.append("row-target")
                elif is_caution:
                    row_classes.append("row-caution")
            else:
                if sample_is_actionable and k_pct >= 25 and so_pa >= 24 and (pa is None or pa >= 20):
                    row_classes.append("row-target")
                elif k_pct <= 18 or so_pa <= 20:
                    row_classes.append("row-caution")

        ks = _to_float(row_data.get("Ks"))
        if status in COMPLETED_STATUSES and ks is not None:
            ks_context = metric_contexts.get("Ks", {})
            ks_mean = _to_optional_float(ks_context.get("mean"))
            _, ks_elite = _metric_highlight_bands(ks_context)
            if ks_mean is not None and ks_elite is not None:
                if ks >= (ks_mean + ks_elite):
                    row_classes.append("row-ks-hot")
                elif ks <= (ks_mean - ks_elite):
                    row_classes.append("row-ks-cold")
            else:
                if ks >= 8:
                    row_classes.append("row-ks-hot")
                elif ks <= 3:
                    row_classes.append("row-ks-cold")

        if row_classes:
            row_tag["class"] = row_classes

        cells = row_tag.find_all("td")
        for col_name, col_class in semantic_column_classes.items():
            col_index = column_map.get(col_name)
            if col_index is None or col_index >= len(cells):
                continue
            _add_tag_class(cells[col_index], col_class)

        for col_name, col_index in column_map.items():
            if col_index >= len(cells):
                continue
            group_class = _column_group_class(col_name)
            if group_class:
                _add_tag_class(cells[col_index], group_class)
            if col_name in PITCHERS_SORTABLE_COLUMNS:
                cells[col_index]["data-sort-key"] = col_name
                sort_value = _to_float(row_data.get(col_name))
                if sort_value is not None:
                    cells[col_index]["data-sort-value"] = f"{sort_value:.12g}"

        k_pa = _to_float(row_data.get(K_PA_COLUMN))
        pa_per_game = _to_float(row_data.get(PA_GP_COLUMN))
        _apply_relative_metric_class(cells, column_map, K_PA_COLUMN, k_pa, metric_contexts)
        _apply_relative_metric_class(cells, column_map, PA_GP_COLUMN, pa_per_game, metric_contexts)
        _apply_relative_metric_class(cells, column_map, "SO/PA", so_pa, metric_contexts)
        _apply_relative_metric_class(cells, column_map, OPP_HAND_K_COLUMN, opp_k_vs_hand, metric_contexts)
        _apply_relative_metric_class(cells, column_map, OPP_LAST_5_K_COLUMN, opp_last_5_k, metric_contexts)
        _apply_relative_metric_class(cells, column_map, OPP_LAST_10_K_COLUMN, opp_last_10_k, metric_contexts)
        if matchup_k_pct_class:
            _add_cell_class(cells, column_map, "K%", matchup_k_pct_class)
        if matchup_pa_class:
            _add_cell_class(cells, column_map, "PA", matchup_pa_class)

        k_per_nine = _to_float(row_data.get("K/9"))
        _apply_relative_metric_class(cells, column_map, "K/9", k_per_nine, metric_contexts)

        whiff_pct = _to_float(row_data.get("Whiff%"))
        _apply_relative_metric_class(cells, column_map, "Whiff%", whiff_pct, metric_contexts)

        rank = _to_float(row_data.get("r"))
        if rank is not None:
            if rank <= 5:
                _add_cell_class(cells, column_map, "r", "cell-top-rank")
            elif rank >= 24:
                _add_cell_class(cells, column_map, "r", "cell-low-rank")

        if ks is not None and status in COMPLETED_STATUSES:
            _apply_relative_metric_class(cells, column_map, "Ks", ks, metric_contexts)

    return str(table)


def _format_for_report_table(df: pd.DataFrame) -> pd.DataFrame:
    report_df = df.copy()
    odds_columns = _odds_columns_from_df(report_df)
    if "Name" in report_df.columns:
        report_df["Name"] = report_df.apply(
            lambda row: _render_pitcher_name_cell(
                row.get("Name"),
                row.get("Hand"),
                row.get(RECENT_PITCHER_GAMES_COLUMN),
            ),
            axis=1,
        )
    if "Ks" in report_df.columns and "Status" in report_df.columns:
        status_text = report_df["Status"].astype(str).str.strip()
        report_df.loc[~status_text.isin(COMPLETED_STATUSES), "Ks"] = ""
    report_df[BEST_K_ODDS_COLUMN] = report_df.apply(
        lambda row: _render_best_k_odds_cell(row, odds_columns),
        axis=1,
    )

    format_map = {
        "SO/PA": "{:.2f}",
        OPP_HAND_K_COLUMN: "{:.2f}",
        OPP_LAST_5_K_COLUMN: "{:.2f}",
        OPP_LAST_10_K_COLUMN: "{:.2f}",
        PA_GP_COLUMN: "{:.1f}",
        K_PA_COLUMN: "{:.2f}",
        "K%": "{:.1f}",
        "PA": "{:.0f}",
        "r": "{:.0f}",
        "K/9": "{:.1f}",
        "Whiff%": "{:.1f}",
    }
    for col, fmt in format_map.items():
        if col in report_df.columns:
            numeric_col = pd.to_numeric(report_df[col], errors="coerce")
            report_df[col] = numeric_col.apply(lambda val: fmt.format(val) if pd.notna(val) else "-")

    if OPP_HAND_K_COLUMN in report_df.columns and OPP_HAND_K_RANK_COLUMN in report_df.columns:
        report_df[OPP_HAND_K_COLUMN] = report_df.apply(
            lambda row: _render_opponent_hand_k_with_rank(
                row.get(OPP_HAND_K_COLUMN),
                row.get(OPP_HAND_K_RANK_COLUMN),
            ),
            axis=1,
        )
    if "K%" in report_df.columns and MATCHUP_SOURCE_COLUMN in report_df.columns:
        report_df["K%"] = report_df.apply(
            lambda row: _annotate_k_percent_with_source(
                row.get("K%"),
                row.get(MATCHUP_SOURCE_COLUMN),
                row.get(MATCHUP_LINES_COLUMN),
            ),
            axis=1,
        )
    if "Opponent" in report_df.columns:
        report_df["Opponent"] = report_df.apply(
            lambda row: _render_opponent_with_start(
                row.get("Opponent"),
                row.get(START_TIME_COLUMN),
                row.get("Status"),
            ),
            axis=1,
        )

    report_df.drop(
        columns=["Status", MATCHUP_SOURCE_COLUMN, START_TIME_COLUMN, *odds_columns, *INTERNAL_ONLY_COLUMNS],
        inplace=True,
        errors="ignore",
    )
    ordered_columns = [col for col in REPORT_COLUMN_ORDER if col in report_df.columns]
    remaining_columns = [col for col in report_df.columns if col not in ordered_columns]
    report_df = report_df[ordered_columns + remaining_columns]
    return report_df.fillna("-")


def _build_report_tabs(active_tab: str, pitcher_href: str, batter_href: str, matchup_href: str) -> str:
    tabs = [
        ("pitchers", "Pitchers", pitcher_href),
        ("batters", "Batters", batter_href),
        ("matchups", "Matchups", matchup_href),
    ]
    links: List[str] = []
    for tab_key, label, href in tabs:
        classes = ["report-tab"]
        if tab_key == active_tab:
            classes.append("active")
        links.append(
            '<a class="' + " ".join(classes) + '" href="' + escape(href, quote=True) + '">'
            + escape(label)
            + "</a>"
        )
    return '<nav class="report-tabs" aria-label="Report pages">' + "".join(links) + "</nav>"


def write_to_html(
    final_df: pd.DataFrame,
    report_key: str,
    display_date: str,
    pitcher_arsenal_lookup: Optional[Dict[str, Dict[str, Any]]] = None,
    *,
    write_root: bool = True,
) -> Path:
    print("\033[92mWriting to HTML....\033[0m")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    report_df = _format_for_report_table(final_df)
    table_html = _build_conditional_table_html(
        report_df,
        final_df,
        pitcher_arsenal_lookup=pitcher_arsenal_lookup,
    )
    arsenal_payload = _build_pitcher_arsenal_payload(final_df, pitcher_arsenal_lookup or {})
    arsenal_payload_json = json.dumps(arsenal_payload).replace("</", "<\\/")
    updated_at = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    root_tabs_html = build_report_tabs("pitchers", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_tabs_html = build_report_tabs("pitchers", display_date, root_page=False, reports_dir=REPORTS_DIR)
    root_date_nav_html = build_date_nav_html("pitchers", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_date_nav_html = build_date_nav_html("pitchers", display_date, root_page=False, reports_dir=REPORTS_DIR)

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLB Pitcher Report {display_date}</title>
  <link rel="icon" href="__FAVICON_HREF__" type="image/svg+xml">
  <style>
    :root {{
      --bg: #f3f6fb;
      --panel: #ffffff;
      --text: #0f172a;
      --muted: #475569;
      --accent: #0f766e;
      --line: #dbe3ee;
      --header: #e5eef9;
      --group-pitcher: #0f766e;
      --group-opponent: #b45309;
      --group-savant: #0369a1;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
      background: radial-gradient(circle at top, #ffffff 0%, var(--bg) 45%);
      color: var(--text);
      padding: 24px;
    }}
    .layout {{
      max-width: 1800px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .hero {{
      background: linear-gradient(120deg, #0f766e 0%, #0369a1 100%);
      color: white;
      padding: 20px 24px;
      border-radius: 14px;
      box-shadow: 0 10px 35px rgba(3, 105, 161, 0.2);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: 0.2px;
    }}
    .hero .updated-at {{
      margin: 0;
      opacity: 0.95;
      font-size: 13px;
    }}
    .hero-nav-row {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px 14px;
      margin-top: 14px;
    }}
    .report-tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 0;
    }}
    .report-tab {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 8px 14px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.24);
      background: rgba(255, 255, 255, 0.12);
      color: #ffffff;
      text-decoration: none;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.01em;
      transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease;
    }}
    .report-tab:hover {{
      background: rgba(255, 255, 255, 0.20);
    }}
    .report-tab.active {{
      background: #ffffff;
      border-color: #ffffff;
      color: #0f4c81;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.14);
    }}
    .report-tab.disabled {{
      opacity: 0.58;
      cursor: not-allowed;
      pointer-events: none;
    }}
    .date-nav {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 0;
      width: auto;
      justify-content: flex-end;
    }}
    .date-pill {{
      display: inline-flex;
      align-items: center;
      gap: 0;
      padding: 6px 10px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.24);
      background: rgba(255, 255, 255, 0.12);
      color: #ffffff;
      text-decoration: none;
      font-size: 11px;
      font-weight: 700;
      letter-spacing: 0.01em;
      transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease;
    }}
    .date-pill:hover {{
      background: rgba(255, 255, 255, 0.20);
    }}
    .date-pill.active {{
      background: rgba(15, 23, 42, 0.18);
      border-color: rgba(255, 255, 255, 0.38);
    }}
    .date-pill.disabled {{
      opacity: 0.58;
      cursor: not-allowed;
      pointer-events: none;
    }}
    .date-pill-label {{
      display: none;
    }}
    .date-pill-date {{
      opacity: 0.94;
      font-size: 11px;
      font-variant-numeric: tabular-nums;
    }}
    .panel {{
      background: var(--panel);
      border-radius: 14px;
      border: 1px solid var(--line);
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
      overflow: hidden;
    }}
    .table-wrap {{
      overflow-y: auto;
      overflow-x: auto;
      max-height: 78vh;
    }}
    .table-toolbar {{
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px 12px;
      padding: 8px 12px 0;
    }}
    .table-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      align-items: center;
      font-size: 11px;
      color: #334155;
      flex: 1 1 720px;
      min-width: 0;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
      font-weight: 600;
    }}
    .legend-swatch {{
      width: 11px;
      height: 11px;
      border-radius: 3px;
      border: 1px solid rgba(15, 23, 42, 0.22);
      display: inline-block;
    }}
    .legend-pitcher .legend-swatch {{
      background: color-mix(in srgb, var(--group-pitcher) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-pitcher) 62%, #ffffff);
    }}
    .legend-opponent .legend-swatch {{
      background: color-mix(in srgb, var(--group-opponent) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-opponent) 62%, #ffffff);
    }}
    .legend-savant .legend-swatch {{
      background: color-mix(in srgb, var(--group-savant) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-savant) 62%, #ffffff);
    }}
    .legend-note {{
      color: #475569;
      font-size: 11px;
    }}
    .table-controls {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      align-items: center;
      justify-content: flex-end;
      margin-left: auto;
      flex: 0 0 auto;
    }}
    .table-controls-label {{
      color: #334155;
      font-size: 10px;
      font-weight: 800;
      letter-spacing: 0.05em;
      text-transform: uppercase;
    }}
    .toggle-chip {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid #d1dbe7;
      background: #f8fbff;
      color: #0f172a;
      font-size: 10px;
      font-weight: 700;
      line-height: 1.1;
    }}
    .toggle-chip input {{
      margin: 0;
      accent-color: #0f766e;
      width: 12px;
      height: 12px;
    }}
    table.pitchers-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      font-size: 10px;
      table-layout: auto;
      min-width: 900px;
    }}
    table.pitchers-table thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--header);
      border-bottom: 1px solid var(--line);
      color: #0b2540;
      padding: 5px 3px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.08;
    }}
    table.pitchers-table thead th.stat-tooltip {{
      cursor: help;
      text-decoration: underline dotted rgba(15, 23, 42, 0.35);
      text-underline-offset: 2px;
    }}
    table.pitchers-table thead th.is-sortable {{
      cursor: pointer;
      user-select: none;
    }}
    table.pitchers-table thead th.is-sortable::after {{
      content: "\\2195";
      display: inline-block;
      margin-left: 5px;
      color: #64748b;
      font-size: 10px;
      font-weight: 700;
    }}
    table.pitchers-table thead th.is-sortable[data-sort-direction="desc"]::after {{
      content: "\\2193";
      color: #0f4c81;
    }}
    table.pitchers-table thead th.is-sortable[data-sort-direction="asc"]::after {{
      content: "\\2191";
      color: #0f4c81;
    }}
    table.pitchers-table thead th.is-sortable:focus-visible {{
      outline: 2px solid #0f4c81;
      outline-offset: -2px;
    }}
    table.pitchers-table thead th.group-pitcher {{
      border-top: 4px solid var(--group-pitcher);
      background: color-mix(in srgb, var(--group-pitcher) 10%, var(--header));
    }}
    table.pitchers-table thead th.group-opponent {{
      border-top: 4px solid var(--group-opponent);
      background: color-mix(in srgb, var(--group-opponent) 11%, var(--header));
    }}
    table.pitchers-table thead th.group-savant {{
      border-top: 4px solid var(--group-savant);
      background: color-mix(in srgb, var(--group-savant) 11%, var(--header));
    }}
    table.pitchers-table thead th.column-best-odds {{
      min-width: 156px;
      white-space: normal;
      line-height: 1.05;
    }}
    table.pitchers-table tbody td {{
      border-bottom: 1px solid var(--line);
      padding: 4px 3px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.08;
      vertical-align: middle;
    }}
    table.pitchers-table tbody td.group-pitcher {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-pitcher) 28%, #ffffff);
    }}
    table.pitchers-table tbody td.group-opponent {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-opponent) 28%, #ffffff);
    }}
    table.pitchers-table tbody td.group-savant {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-savant) 30%, #ffffff);
    }}
    table.pitchers-table tbody td.group-pitcher:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-pitcher) 4%, #ffffff);
    }}
    table.pitchers-table tbody td.group-opponent:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-opponent) 5%, #ffffff);
    }}
    table.pitchers-table tbody td.group-savant:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-savant) 5%, #ffffff);
    }}
    table.pitchers-table th.column-name,
    table.pitchers-table td.column-name {{
      min-width: 118px;
      white-space: normal;
      overflow-wrap: anywhere;
    }}
    table.pitchers-table td.column-name {{
      overflow: visible;
      position: relative;
    }}
    table.pitchers-table th.column-opponent,
    table.pitchers-table td.column-opponent {{
      min-width: 72px;
      white-space: nowrap;
    }}
    table.pitchers-table th.column-opp-hand,
    table.pitchers-table td.column-opp-hand {{
      min-width: 70px;
    }}
    table.pitchers-table th.column-opp-hand {{
      white-space: normal;
      line-height: 1.05;
    }}
    table.pitchers-table td.column-best-odds {{
      min-width: 156px;
      white-space: normal;
    }}
    table.pitchers-table tbody tr:nth-child(even) {{
      background: #f8fbff;
    }}
    table.pitchers-table tbody tr:hover {{
      background: #eef7ff;
    }}
    table.pitchers-table tbody tr.pitcher-row-selectable {{
      cursor: pointer;
    }}
    table.pitchers-table tbody tr.pitcher-row-selectable.row-selected {{
      outline: 2px solid #0f766e;
      outline-offset: -2px;
    }}
    table.pitchers-table tbody tr.row-upcoming {{
      background: linear-gradient(to right, rgba(15, 118, 110, 0.10), transparent);
    }}
    table.pitchers-table tbody tr.row-live {{
      background: linear-gradient(to right, rgba(249, 115, 22, 0.14), transparent);
    }}
    table.pitchers-table tbody tr.row-final {{
      background: linear-gradient(to right, rgba(100, 116, 139, 0.10), transparent);
    }}
    table.pitchers-table tbody tr.row-target {{
      box-shadow: inset 4px 0 0 #16a34a;
    }}
    table.pitchers-table tbody tr.row-caution {{
      box-shadow: inset 4px 0 0 #dc2626;
    }}
    table.pitchers-table tbody tr.row-ks-hot {{
      box-shadow: inset 4px 0 0 #7c3aed;
    }}
    table.pitchers-table tbody tr.row-ks-cold {{
      box-shadow: inset 4px 0 0 #64748b;
    }}
    table.pitchers-table td.cell-elite {{
      background: #dcfce7;
      color: #14532d;
      font-weight: 700;
    }}
    table.pitchers-table td.cell-strong {{
      background: #fef9c3;
      color: #713f12;
      font-weight: 600;
    }}
    table.pitchers-table td.cell-weak {{
      background: #fee2e2;
      color: #7f1d1d;
      font-weight: 600;
    }}
    table.pitchers-table td.cell-low-confidence {{
      background: #f1f5f9;
      color: #475569;
      font-weight: 600;
    }}
    table.pitchers-table td.cell-confidence-high {{
      background: #dbeafe;
      color: #1d4ed8;
      font-weight: 600;
    }}
    table.pitchers-table td.cell-confidence-low {{
      background: #f8fafc;
      color: #64748b;
      font-weight: 600;
    }}
    table.pitchers-table td.cell-top-rank {{
      background: #dbeafe;
      color: #1e3a8a;
      font-weight: 700;
    }}
    table.pitchers-table td.cell-low-rank {{
      background: #f1f5f9;
      color: #334155;
    }}
    table.pitchers-table td.odds-plus {{
      font-weight: 700;
    }}
    table.pitchers-table td.odds-even {{
      font-weight: 600;
    }}
    table.pitchers-table td.odds-juice {{
      font-weight: 600;
    }}
    table.pitchers-table td.odds-missing {{
      color: #64748b;
      background: #f8fafc;
    }}
    .opp-cell {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      flex-wrap: nowrap;
      gap: 4px;
      line-height: 1.08;
      text-align: center;
    }}
    .opp-cell .opp-team {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .opp-cell .opp-team a {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 30px;
      height: 30px;
      border-radius: 5px;
      background: #ffffff;
      box-shadow: inset 0 0 0 1px #e2e8f0;
    }}
    .opp-logo {{
      display: block;
      width: 26px;
      height: 26px;
      object-fit: contain;
    }}
    .opp-time {{
      display: inline-block;
      padding: 1px 5px;
      border-radius: 999px;
      background: #f1f5f9;
      border: 1px solid #cbd5e1;
      color: #334155;
      font-size: 8.5px;
      font-weight: 700;
      letter-spacing: 0.01em;
    }}
    .opp-time-upcoming {{
      background: #ecfeff;
      border-color: #a5f3fc;
      color: #155e75;
    }}
    .opp-time-live {{
      background: #fff7ed;
      border-color: #fdba74;
      color: #9a3412;
    }}
    .opp-time-final {{
      background: #f1f5f9;
      border-color: #cbd5e1;
      color: #334155;
    }}
    .k-src-gap {{
      display: inline-block;
      width: 4px;
    }}
    .k-src-marker {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 13px;
      height: 13px;
      padding: 0 3px;
      border-radius: 999px;
      border: 1px solid transparent;
      font-size: 7.5px;
      font-weight: 800;
      line-height: 1;
      vertical-align: middle;
      transform: translateY(-1px);
      letter-spacing: 0.03em;
    }}
    .k-src-marker.src-espn {{
      background: #dbeafe;
      color: #1d4ed8;
      border-color: #93c5fd;
    }}
    .k-src-marker.src-previous-lineup {{
      background: #fef3c7;
      color: #92400e;
      border-color: #fbbf24;
    }}
    .k-src-marker.src-savant {{
      background: #dcfce7;
      color: #166534;
      border-color: #86efac;
    }}
    .opp-hand-k-cell {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      white-space: nowrap;
    }}
    .opp-hand-rank-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 16px;
      height: 14px;
      padding: 0 4px;
      border-radius: 999px;
      background: hsl(var(--rank-hue, 140) 78% 92%);
      border: 1px solid hsl(var(--rank-hue, 140) 58% 68%);
      color: hsl(var(--rank-hue, 140) 72% 24%);
      font-size: 8px;
      font-weight: 800;
      line-height: 1;
      vertical-align: middle;
      transform: translateY(-1px);
    }}
    .matchup-k-cell {{
      position: relative;
      display: inline-flex;
      align-items: center;
      justify-content: center;
    }}
    .matchup-k-cell.matchup-k-has-popup {{
      cursor: help;
    }}
    .matchup-k-popup {{
      visibility: hidden;
      opacity: 0;
      display: grid;
      gap: 2px;
      position: absolute;
      top: calc(100% + 6px);
      left: 50%;
      z-index: 80;
      min-width: max-content;
      padding: 6px 8px;
      transform: translateX(-50%);
      border-radius: 6px;
      background: #0f172a;
      color: #ffffff;
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.22);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 10px;
      font-weight: 700;
      line-height: 1.25;
      text-align: left;
      pointer-events: none;
      transition: opacity 90ms ease;
    }}
    .matchup-k-popup::before {{
      content: "";
      position: absolute;
      top: -5px;
      left: 50%;
      transform: translateX(-50%);
      border-left: 5px solid transparent;
      border-right: 5px solid transparent;
      border-bottom: 5px solid #0f172a;
    }}
    .matchup-k-cell.matchup-k-has-popup:hover .matchup-k-popup,
    .matchup-k-cell.matchup-k-has-popup:focus-within .matchup-k-popup {{
      visibility: visible;
      opacity: 1;
    }}
    .matchup-k-source-line {{
      display: inline-flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
    }}
    .matchup-k-source-text {{
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      font-size: 10px;
      font-weight: 800;
    }}
    .matchup-k-line {{
      display: block;
      white-space: nowrap;
    }}
    .pitcher-name-cell {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      flex-wrap: wrap;
      position: relative;
    }}
    .pitcher-name-cell.pitcher-has-recent {{
      cursor: help;
    }}
    .pitcher-recent-popup {{
      visibility: hidden;
      opacity: 0;
      display: grid;
      gap: 2px;
      position: absolute;
      top: calc(100% + 6px);
      left: 50%;
      z-index: 80;
      min-width: max-content;
      padding: 6px 8px;
      transform: translateX(-50%);
      border-radius: 6px;
      background: #0f172a;
      color: #ffffff;
      box-shadow: 0 10px 24px rgba(15, 23, 42, 0.22);
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 10px;
      font-weight: 700;
      line-height: 1.25;
      text-align: left;
      pointer-events: none;
      transition: opacity 90ms ease;
    }}
    .pitcher-recent-popup::before {{
      content: "";
      position: absolute;
      top: -5px;
      left: 50%;
      transform: translateX(-50%);
      border-left: 5px solid transparent;
      border-right: 5px solid transparent;
      border-bottom: 5px solid #0f172a;
    }}
    .pitcher-name-cell.pitcher-has-recent:hover .pitcher-recent-popup,
    .pitcher-name-cell.pitcher-has-recent:focus-within .pitcher-recent-popup,
    table.pitchers-table td.column-name:hover .pitcher-name-cell.pitcher-has-recent .pitcher-recent-popup {{
      visibility: visible;
      opacity: 1;
    }}
    .pitcher-recent-line {{
      display: block;
      white-space: nowrap;
    }}
    .hand-marker {{
      transform: none;
      min-width: 15px;
      height: 15px;
      padding: 0 4px;
      font-size: 7.5px;
    }}
    .hand-marker.hand-right {{
      background: #dbeafe;
      color: #1d4ed8;
      border-color: #93c5fd;
    }}
    .hand-marker.hand-left {{
      background: #fee2e2;
      color: #b91c1c;
      border-color: #fca5a5;
    }}
    .hand-marker.hand-tbd {{
      background: #f1f5f9;
      color: #475569;
      border-color: #cbd5e1;
    }}
    .best-odds-cell {{
      display: inline-grid;
      grid-template-columns: minmax(0, 1fr) auto;
      column-gap: 2px;
      align-items: center;
      justify-content: center;
      line-height: 1.1;
    }}
    .best-odds-cell-empty {{
      grid-template-columns: auto;
    }}
    .best-odds-cell-missing-side {{
      display: grid;
      width: 100%;
      justify-content: stretch;
    }}
    .best-odds-summary {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 3px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 9px;
      font-weight: 700;
      white-space: nowrap;
    }}
    .best-odds-cell-missing-side .best-odds-summary {{
      justify-content: flex-start;
    }}
    .best-odds-point {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 2.5rem;
      padding: 2px 6px;
      border-radius: 999px;
      border: 1px solid transparent;
      color: #0f172a;
      font-weight: 800;
    }}
    .best-odds-point-elite {{
      background: #dcfce7;
      border-color: #86efac;
      color: #14532d;
    }}
    .best-odds-point-strong {{
      background: #fef9c3;
      border-color: #fde68a;
      color: #713f12;
    }}
    .best-odds-point-neutral {{
      background: #f1f5f9;
      border-color: #cbd5e1;
      color: #334155;
    }}
    .best-odds-point-weak {{
      background: #fee2e2;
      border-color: #fca5a5;
      color: #7f1d1d;
    }}
    .best-odds-divider {{
      color: #94a3b8;
      font-weight: 700;
    }}
    .best-odds-side {{
      display: inline-flex;
      align-items: center;
      gap: 2px;
    }}
    .best-odds-side-label {{
      font-size: 8px;
      color: #64748b;
      font-weight: 800;
      letter-spacing: 0.03em;
    }}
    .odds-over, .odds-under {{
      text-align: right;
      padding: 0 1px;
      border-radius: 4px;
    }}
    .sportsbook-badge {{
      transform: none;
      border-color: color-mix(in srgb, var(--sportsbook-color) 36%, #ffffff);
      background: color-mix(in srgb, var(--sportsbook-color) 16%, #ffffff);
      color: var(--sportsbook-color);
    }}
    .sportsbook-badge-summary {{
      min-width: 18px;
      height: 14px;
      padding: 0 4px;
      font-size: 7.5px;
    }}
    .sportsbook-badge-detail {{
      min-width: 20px;
      height: 15px;
      padding: 0 4px;
      font-size: 7.5px;
      justify-self: start;
    }}
    .odds-details {{
      font-size: 9.5px;
      color: #334155;
      display: inline-block;
      position: relative;
      margin: 0;
    }}
    .odds-details summary {{
      cursor: pointer;
      color: #334155;
      list-style: none;
      outline: none;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 11px;
      height: 11px;
      border-radius: 50%;
      background: rgba(148, 163, 184, 0.18);
      border: 1px solid rgba(100, 116, 139, 0.35);
      padding: 0;
    }}
    .odds-details summary::-webkit-details-marker {{ display: none; }}
    .odds-arrow {{
      font-size: 7px;
      line-height: 1;
      transform: translateY(-1px);
      transition: transform 120ms ease;
    }}
    .odds-details[open] .odds-arrow {{
      transform: rotate(180deg) translateY(1px);
    }}
    .odds-details > ul {{
      margin: 0;
      padding: 5px 7px;
      list-style: none;
      display: grid;
      gap: 2px;
      position: absolute;
      right: 0;
      top: calc(100% + 4px);
      z-index: 30;
      background: #ffffff;
      border: 1px solid #cbd5e1;
      border-radius: 8px;
      box-shadow: 0 8px 22px rgba(15, 23, 42, 0.15);
      min-width: max-content;
    }}
    .odds-details-list,
    .odds-book-list {{
      list-style: none;
      margin: 0;
      padding: 0;
    }}
    .odds-line-group {{
      display: grid;
      gap: 3px;
      opacity: 0.95;
      white-space: nowrap;
    }}
    .odds-line-label {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      font-size: 9px;
      font-weight: 800;
      color: #0f172a;
    }}
    .odds-book-list {{
      display: grid;
      gap: 2px;
    }}
    .odds-book-row {{
      display: grid;
      grid-template-columns: 22px minmax(0, 1fr) minmax(0, 1fr);
      align-items: center;
      column-gap: 4px;
      opacity: 0.9;
      white-space: nowrap;
    }}
    .odds-arrow-placeholder {{
      width: 12px;
      height: 12px;
      display: inline-block;
      opacity: 0;
    }}
    .odds-over.best-over {{
      color: #15803d;
      font-weight: 800;
    }}
    .odds-under.best-under {{
      color: #1d4ed8;
      font-weight: 800;
    }}
    table.pitchers-table a {{
      color: #0f4c81;
      text-decoration: none;
      font-weight: 600;
      overflow-wrap: anywhere;
    }}
    table.pitchers-table a:hover {{
      text-decoration: underline;
    }}
    .arsenal-card {{
      padding: 20px;
      display: grid;
      gap: 14px;
    }}
    .arsenal-card h2 {{
      margin: 0 0 4px;
      font-size: 22px;
      color: #0b2540;
    }}
    .arsenal-subtitle {{
      margin: 0;
      color: var(--muted);
      font-size: 13px;
    }}
    .arsenal-metrics {{
      display: grid;
      grid-template-columns: repeat(4, minmax(120px, 1fr));
      gap: 10px;
    }}
    .arsenal-metric {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      background: #f8fbff;
      display: grid;
      gap: 4px;
      transition: background-color 120ms ease, border-color 120ms ease, color 120ms ease;
    }}
    .arsenal-metric.metric-above {{
      background: #dcfce7;
      border-color: #86efac;
    }}
    .arsenal-metric.metric-below {{
      background: #fee2e2;
      border-color: #fca5a5;
    }}
    .arsenal-metric.metric-neutral {{
      background: #f8fbff;
      border-color: var(--line);
    }}
    .arsenal-metric-label {{
      font-size: 11px;
      color: #64748b;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }}
    .arsenal-metric-value {{
      font-size: 20px;
      font-weight: 700;
      color: #0f172a;
      line-height: 1.1;
    }}
    .arsenal-metric-meta {{
      font-size: 11px;
      color: #475569;
      line-height: 1.1;
      min-height: 12px;
    }}
    .arsenal-pitches {{
      display: grid;
      gap: 8px;
      min-height: 42px;
    }}
    .arsenal-pitch-row {{
      display: grid;
      grid-template-columns: 90px 56px minmax(120px, 1fr);
      gap: 8px;
      align-items: center;
      font-size: 12px;
      color: #0f172a;
    }}
    .arsenal-pitch-label {{
      font-weight: 600;
    }}
    .arsenal-pitch-value {{
      text-align: right;
      color: #334155;
      font-variant-numeric: tabular-nums;
    }}
    .arsenal-pitch-track {{
      height: 8px;
      border-radius: 999px;
      background: #dbeafe;
      overflow: hidden;
    }}
    .arsenal-pitch-fill {{
      height: 100%;
      background: linear-gradient(90deg, #0f766e, #0284c7);
      border-radius: inherit;
    }}
    .arsenal-empty {{
      color: #64748b;
      font-size: 12px;
      border-top: 1px dashed #cbd5e1;
      padding-top: 10px;
    }}
    .arsenal-future-slot {{
      border: 1px dashed #cbd5e1;
      border-radius: 10px;
      padding: 10px 12px;
      color: #64748b;
      font-size: 12px;
      background: #fafcff;
    }}
    @media (max-width: 900px) {{
      body {{ padding: 12px; }}
      .hero h1 {{ font-size: 23px; }}
      .hero-nav-row {{
        align-items: stretch;
      }}
      .report-tabs {{
        width: 100%;
      }}
      .date-nav {{
        width: 100%;
      }}
      .table-toolbar {{
        align-items: stretch;
      }}
      .table-controls {{
        width: 100%;
        justify-content: flex-start;
        margin-left: 0;
      }}
      .arsenal-metrics {{
        grid-template-columns: repeat(2, minmax(120px, 1fr));
      }}
      .arsenal-pitch-row {{
        grid-template-columns: 78px 56px minmax(100px, 1fr);
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <header class="hero">
      <h1>MLB Pitcher Strikeout Report - {display_date}</h1>
      <p class="updated-at">Last updated: {updated_at}</p>
      <div class="hero-nav-row">
        __TABS_HTML__
        __DATE_NAV_HTML__
      </div>
    </header>
    <section class="panel">
      <div class="table-toolbar">
        <div class="table-legend">
          <span class="legend-item legend-pitcher"><span class="legend-swatch"></span>Pitcher Stats</span>
          <span class="legend-item legend-opponent"><span class="legend-swatch"></span>Opponent/Matchup Stats</span>
          <span class="legend-item legend-savant"><span class="legend-swatch"></span>Savant Stats</span>
          <span class="legend-note">Hover K% for source: E = ESPN confirmed lineup, P = previous lineup BvP, S = Savant fallback.</span>
        </div>
        <div class="table-controls" aria-label="Pitcher table controls">
          <span class="table-controls-label">Show</span>
          <label class="toggle-chip" for="show-live-toggle">
            <input type="checkbox" id="show-live-toggle">
            In Progress
          </label>
          <label class="toggle-chip" for="show-final-toggle">
            <input type="checkbox" id="show-final-toggle">
            Final
          </label>
        </div>
      </div>
      <div class="table-wrap">
        {table_html}
      </div>
    </section>
    <section class="panel">
      <div class="arsenal-card">
        <div>
          <h2 id="arsenal-name">Pitcher Arsenal Snapshot</h2>
          <p class="arsenal-subtitle">Select a pitcher row above to inspect swing-and-miss profile and pitch mix.</p>
        </div>
        <div class="arsenal-metrics">
          <div class="arsenal-metric metric-neutral" id="arsenal-whiff-box">
            <span class="arsenal-metric-label">Whiff%</span>
            <span class="arsenal-metric-value" id="arsenal-whiff">-</span>
            <span class="arsenal-metric-meta" id="arsenal-whiff-avg">Lg Avg: -</span>
          </div>
          <div class="arsenal-metric metric-neutral" id="arsenal-z-miss-box">
            <span class="arsenal-metric-label">Z-Swing Miss%</span>
            <span class="arsenal-metric-value" id="arsenal-z-miss">-</span>
            <span class="arsenal-metric-meta" id="arsenal-z-miss-avg">Lg Avg: -</span>
          </div>
          <div class="arsenal-metric metric-neutral" id="arsenal-oz-miss-box">
            <span class="arsenal-metric-label">OZ-Swing Miss%</span>
            <span class="arsenal-metric-value" id="arsenal-oz-miss">-</span>
            <span class="arsenal-metric-meta" id="arsenal-oz-miss-avg">Lg Avg: -</span>
          </div>
          <div class="arsenal-metric metric-neutral" id="arsenal-fastball-box">
            <span class="arsenal-metric-label">Fastball%</span>
            <span class="arsenal-metric-value" id="arsenal-fastball">-</span>
            <span class="arsenal-metric-meta" id="arsenal-fastball-avg"></span>
          </div>
        </div>
        <div class="arsenal-pitches" id="arsenal-pitches"></div>
        <div class="arsenal-empty" id="arsenal-empty">
          Arsenal details are only available for pitchers returned by the Savant qualified leaderboard feed.
        </div>
        <div class="arsenal-future-slot">
          Reserved for additional pitcher detail modules.
        </div>
      </div>
    </section>
  </div>
  <script>
    const arsenalData = {arsenal_payload_json};
    const leagueAverages = arsenalData["__league_averages__"] || {{}};
    const tableBody = document.querySelector("table.pitchers-table tbody");
    const pitcherRows = Array.from(document.querySelectorAll("table.pitchers-table tbody tr[data-pitcher-key]"));
    const sortableHeaders = Array.from(document.querySelectorAll("table.pitchers-table thead th.is-sortable"));
    const showLiveToggle = document.getElementById("show-live-toggle");
    const showFinalToggle = document.getElementById("show-final-toggle");
    const nameEl = document.getElementById("arsenal-name");
    const whiffBoxEl = document.getElementById("arsenal-whiff-box");
    const whiffEl = document.getElementById("arsenal-whiff");
    const whiffAvgEl = document.getElementById("arsenal-whiff-avg");
    const zMissBoxEl = document.getElementById("arsenal-z-miss-box");
    const zMissEl = document.getElementById("arsenal-z-miss");
    const zMissAvgEl = document.getElementById("arsenal-z-miss-avg");
    const ozMissBoxEl = document.getElementById("arsenal-oz-miss-box");
    const ozMissEl = document.getElementById("arsenal-oz-miss");
    const ozMissAvgEl = document.getElementById("arsenal-oz-miss-avg");
    const fastballBoxEl = document.getElementById("arsenal-fastball-box");
    const fastballEl = document.getElementById("arsenal-fastball");
    const fastballAvgEl = document.getElementById("arsenal-fastball-avg");
    const pitchesEl = document.getElementById("arsenal-pitches");
    const emptyEl = document.getElementById("arsenal-empty");
    let selectedPitcherKey = null;
    let sortState = {{ key: null, index: null, direction: null }};

    function toNumberOrNull(value) {{
      const numeric = Number(value);
      return Number.isFinite(numeric) ? numeric : null;
    }}

    function formatPercent(value) {{
      const numeric = toNumberOrNull(value);
      if (numeric === null) {{
        return "-";
      }}
      return `${{numeric.toFixed(1)}}%`;
    }}

    function setMetricTile(boxEl, valueEl, avgEl, value, leagueAvg, useComparison = true) {{
      const numericValue = toNumberOrNull(value);
      const numericLeagueAvg = toNumberOrNull(leagueAvg);

      valueEl.textContent = formatPercent(numericValue);

      if (avgEl) {{
        avgEl.textContent = useComparison ? `Lg Avg: ${{formatPercent(numericLeagueAvg)}}` : "";
      }}

      boxEl.classList.remove("metric-above", "metric-below", "metric-neutral");
      if (!useComparison || numericValue === null || numericLeagueAvg === null) {{
        boxEl.classList.add("metric-neutral");
        return;
      }}

      const delta = numericValue - numericLeagueAvg;
      if (delta >= 1.0) {{
        boxEl.classList.add("metric-above");
      }} else if (delta <= -1.0) {{
        boxEl.classList.add("metric-below");
      }} else {{
        boxEl.classList.add("metric-neutral");
      }}
    }}

    function resetArsenalPanel(label = "Pitcher Arsenal Snapshot") {{
      const whiffLeagueAvg = toNumberOrNull(leagueAverages.whiff_percent);
      const zMissLeagueAvg = toNumberOrNull(leagueAverages.z_swing_miss_percent);
      const ozMissLeagueAvg = toNumberOrNull(leagueAverages.oz_swing_miss_percent);
      nameEl.textContent = label;
      setMetricTile(whiffBoxEl, whiffEl, whiffAvgEl, null, whiffLeagueAvg, true);
      setMetricTile(zMissBoxEl, zMissEl, zMissAvgEl, null, zMissLeagueAvg, true);
      setMetricTile(ozMissBoxEl, ozMissEl, ozMissAvgEl, null, ozMissLeagueAvg, true);
      setMetricTile(fastballBoxEl, fastballEl, fastballAvgEl, null, null, false);
      clearPitches();
      emptyEl.style.display = "block";
    }}

    function clearPitches() {{
      while (pitchesEl.firstChild) {{
        pitchesEl.removeChild(pitchesEl.firstChild);
      }}
    }}

    function renderPitchMix(arsenalList) {{
      clearPitches();
      if (!Array.isArray(arsenalList) || arsenalList.length === 0) {{
        return false;
      }}

      arsenalList.forEach((pitch) => {{
        const usage = Number(pitch.usage_percent);
        if (Number.isNaN(usage)) {{
          return;
        }}

        const row = document.createElement("div");
        row.className = "arsenal-pitch-row";

        const label = document.createElement("span");
        label.className = "arsenal-pitch-label";
        label.textContent = pitch.label || pitch.code || "Pitch";

        const value = document.createElement("span");
        value.className = "arsenal-pitch-value";
        value.textContent = formatPercent(usage);

        const track = document.createElement("div");
        track.className = "arsenal-pitch-track";
        const fill = document.createElement("div");
        fill.className = "arsenal-pitch-fill";
        fill.style.width = `${{Math.max(0, Math.min(100, usage))}}%`;
        track.appendChild(fill);

        row.appendChild(label);
        row.appendChild(value);
        row.appendChild(track);
        pitchesEl.appendChild(row);
      }});

      return pitchesEl.children.length > 0;
    }}

    function applyRowSelection() {{
      pitcherRows.forEach((row) => {{
        row.classList.toggle("row-selected", selectedPitcherKey !== null && row.dataset.pitcherKey === selectedPitcherKey);
      }});
    }}

    function renderSelectedPitcher() {{
      const selectedRow = pitcherRows.find((row) => row.dataset.pitcherKey === selectedPitcherKey);
      const whiffLeagueAvg = toNumberOrNull(leagueAverages.whiff_percent);
      const zMissLeagueAvg = toNumberOrNull(leagueAverages.z_swing_miss_percent);
      const ozMissLeagueAvg = toNumberOrNull(leagueAverages.oz_swing_miss_percent);
      if (!selectedPitcherKey || !selectedRow) {{
        resetArsenalPanel();
        return;
      }}

      const details = arsenalData[selectedPitcherKey];
      const fallbackName = selectedRow.querySelector("td")?.innerText?.trim() || "Pitcher Arsenal Snapshot";

      if (!details) {{
        nameEl.textContent = fallbackName || "Pitcher Arsenal Snapshot";
        setMetricTile(whiffBoxEl, whiffEl, whiffAvgEl, null, whiffLeagueAvg, true);
        setMetricTile(zMissBoxEl, zMissEl, zMissAvgEl, null, zMissLeagueAvg, true);
        setMetricTile(ozMissBoxEl, ozMissEl, ozMissAvgEl, null, ozMissLeagueAvg, true);
        setMetricTile(fastballBoxEl, fastballEl, fastballAvgEl, null, null, false);
        clearPitches();
        emptyEl.style.display = "block";
        return;
      }}

      nameEl.textContent = details.name || fallbackName || "Pitcher Arsenal Snapshot";
      setMetricTile(whiffBoxEl, whiffEl, whiffAvgEl, details.whiff_percent, whiffLeagueAvg, true);
      setMetricTile(zMissBoxEl, zMissEl, zMissAvgEl, details.z_swing_miss_percent, zMissLeagueAvg, true);
      setMetricTile(ozMissBoxEl, ozMissEl, ozMissAvgEl, details.oz_swing_miss_percent, ozMissLeagueAvg, true);
      setMetricTile(fastballBoxEl, fastballEl, fastballAvgEl, details.fastball_percent, null, false);

      const hasPitches = renderPitchMix(details.arsenal);
      emptyEl.style.display = hasPitches ? "none" : "block";
    }}

    function selectPitcher(pitcherKey) {{
      selectedPitcherKey = pitcherKey || null;
      applyRowSelection();
      renderSelectedPitcher();
    }}

    function visiblePitcherRows() {{
      return pitcherRows.filter((row) => !row.hidden);
    }}

    function syncSelectedPitcher() {{
      const visibleRows = visiblePitcherRows();
      if (selectedPitcherKey) {{
        const activeRow = visibleRows.find((row) => row.dataset.pitcherKey === selectedPitcherKey);
        if (activeRow) {{
          applyRowSelection();
          renderSelectedPitcher();
          return;
        }}
      }}

      const preferredRow =
        visibleRows.find((row) => Object.prototype.hasOwnProperty.call(arsenalData, row.dataset.pitcherKey))
        || visibleRows[0];
      if (preferredRow) {{
        selectPitcher(preferredRow.dataset.pitcherKey);
        return;
      }}
      selectPitcher(null);
    }}

    function readCellSortValue(row, columnIndex) {{
      const cell = row.children[columnIndex];
      if (!cell) {{
        return null;
      }}
      const rawValue = cell.dataset.sortValue;
      if (rawValue === undefined || rawValue === "") {{
        return null;
      }}
      const numeric = Number(rawValue);
      return Number.isFinite(numeric) ? numeric : null;
    }}

    function compareRows(a, b) {{
      const columnIndex = sortState.index;
      const aValue = readCellSortValue(a, columnIndex);
      const bValue = readCellSortValue(b, columnIndex);
      const aHasValue = aValue !== null;
      const bHasValue = bValue !== null;
      if (aHasValue !== bHasValue) {{
        return aHasValue ? -1 : 1;
      }}
      if (aHasValue && bHasValue && aValue !== bValue) {{
        return sortState.direction === "asc" ? aValue - bValue : bValue - aValue;
      }}
      return Number(a.dataset.initialIndex) - Number(b.dataset.initialIndex);
    }}

    function syncSortHeaders() {{
      sortableHeaders.forEach((header) => {{
        let direction = "default";
        if (sortState.key === header.dataset.sortKey && sortState.direction) {{
          direction = sortState.direction;
        }}
        header.dataset.sortDirection = direction;
        header.setAttribute(
          "aria-sort",
          direction === "asc" ? "ascending" : direction === "desc" ? "descending" : "none",
        );
      }});
    }}

    function applySort() {{
      const rowsToRender = pitcherRows.slice();
      if (sortState.key && sortState.direction && Number.isInteger(sortState.index)) {{
        rowsToRender.sort(compareRows);
      }} else {{
        rowsToRender.sort((a, b) => Number(a.dataset.initialIndex) - Number(b.dataset.initialIndex));
      }}
      rowsToRender.forEach((row) => tableBody.appendChild(row));
      syncSortHeaders();
    }}

    function applyVisibility() {{
      const showLive = Boolean(showLiveToggle?.checked);
      const showFinal = Boolean(showFinalToggle?.checked);
      pitcherRows.forEach((row) => {{
        const isLive = row.classList.contains("row-live");
        const isFinal = row.classList.contains("row-final");
        row.hidden = (!showLive && isLive) || (!showFinal && isFinal);
      }});
    }}

    function applyTableState() {{
      applySort();
      applyVisibility();
      syncSelectedPitcher();
    }}

    function cycleSort(header) {{
      const sortKey = header.dataset.sortKey || null;
      const sortIndex = Number(header.dataset.sortIndex);
      if (!sortKey || !Number.isInteger(sortIndex)) {{
        return;
      }}

      if (sortState.key !== sortKey) {{
        sortState = {{ key: sortKey, index: sortIndex, direction: "desc" }};
      }} else if (sortState.direction === "desc") {{
        sortState = {{ key: sortKey, index: sortIndex, direction: "asc" }};
      }} else if (sortState.direction === "asc") {{
        sortState = {{ key: null, index: null, direction: null }};
      }} else {{
        sortState = {{ key: sortKey, index: sortIndex, direction: "desc" }};
      }}

      applyTableState();
    }}

    pitcherRows.forEach((row) => {{
      row.addEventListener("click", (event) => {{
        if (event.target.closest("a")) {{
          return;
        }}
        selectPitcher(row.dataset.pitcherKey);
      }});
    }});

    sortableHeaders.forEach((header) => {{
      header.addEventListener("click", () => cycleSort(header));
      header.addEventListener("keydown", (event) => {{
        if (event.key !== "Enter" && event.key !== " ") {{
          return;
        }}
        event.preventDefault();
        cycleSort(header);
      }});
    }});

    showLiveToggle?.addEventListener("change", applyTableState);
    showFinalToggle?.addEventListener("change", applyTableState);

    applyTableState();
  </script>
</body>
</html>
"""

    output_path = REPORTS_DIR / f"report-{report_key}.html"
    archive_html_content = (
        html_content
        .replace("__TABS_HTML__", archive_tabs_html)
        .replace("__DATE_NAV_HTML__", archive_date_nav_html)
        .replace("__FAVICON_HREF__", "../favicon.svg")
    )
    root_html_content = (
        html_content
        .replace("__TABS_HTML__", root_tabs_html)
        .replace("__DATE_NAV_HTML__", root_date_nav_html)
        .replace("__FAVICON_HREF__", "./favicon.svg")
    )
    output_path.write_text(archive_html_content, encoding="utf-8")
    if write_root:
        ROOT_INDEX_FILE.write_text(root_html_content, encoding="utf-8")
    print(output_path.resolve().as_uri())
    if write_root:
        print(ROOT_INDEX_FILE.resolve().as_uri())
    return output_path


def resolve_date_input(raw_input: str) -> str:
    today = datetime.datetime.now()
    input_lower = raw_input.lower()
    if input_lower == "today":
        return today.strftime("%m/%d/%Y")
    if input_lower == "tmrw":
        return (today + datetime.timedelta(days=1)).strftime("%m/%d/%Y")

    if raw_input.count("/") == 2:
        datetime.datetime.strptime(raw_input, "%m/%d/%Y")
        return raw_input
    if raw_input.count("/") == 1:
        return f"{raw_input}/{today.year}"
    raise ValueError("Date must be 'today', 'tmrw', 'MM/DD', or 'MM/DD/YYYY'.")


def _next_report_date(report_date: str) -> str:
    date_obj = datetime.datetime.strptime(report_date, "%m/%d/%Y")
    return (date_obj + datetime.timedelta(days=1)).strftime("%m/%d/%Y")


def _has_not_started_games(schedule: Sequence[Dict[str, Any]]) -> bool:
    return any(game.get("status") in NOT_STARTED_STATUSES for game in schedule)


def resolve_effective_report_date_and_schedule(
    report_date: str,
    *,
    allow_roll_forward: bool = True,
) -> Tuple[str, List[Dict[str, Any]]]:
    schedule = fetch_schedule(report_date)
    if _has_not_started_games(schedule) or not allow_roll_forward:
        return report_date, schedule

    next_report_date = _next_report_date(report_date)
    next_schedule = fetch_schedule(next_report_date)
    print(
        "\033[93mNo games remain in a not-started state on "
        f"{report_date}. Rolling report forward to {next_report_date}.\033[0m"
    )
    return next_report_date, next_schedule


def main(
    report_date: str,
    odds: str,
    *,
    allow_roll_forward: bool = True,
    write_root: bool = True,
) -> None:
    report_date, schedule = resolve_effective_report_date_and_schedule(
        report_date,
        allow_roll_forward=allow_roll_forward,
    )
    report_key = report_date.replace("/", "")
    print((REPORTS_DIR / f"report-{report_key}.html").resolve().as_uri())

    pitcher_tasks = get_pitcher_tasks(schedule)
    results = fetch_pitcher_stats_concurrently(pitcher_tasks)
    if not results:
        print("\033[93mNo probable pitchers found for the selected date.\033[0m")
        empty_df = pd.DataFrame(columns=REPORT_COLUMN_ORDER)
        write_to_html(empty_df, report_key, report_date, pitcher_arsenal_lookup={}, write_root=write_root)
        return

    report_year = datetime.datetime.strptime(report_date, "%m/%d/%Y").year
    team_batting_df = prepare_team_batting_df(report_year)
    merged_df = merge_pitcher_with_batting_data(results, team_batting_df)
    opp_df = get_opp_data(report_date, schedule)
    pitchers = merge_with_opponent_data(merged_df, opp_df)
    arsenal_lookup = prepare_pitcher_arsenal_lookup(report_year)
    whiff_lookup = {
        name_key: details["whiff_percent"]
        for name_key, details in arsenal_lookup.items()
        if name_key != ARSENAL_META_KEY
        if details.get("whiff_percent") is not None
    }
    pitchers = add_pitcher_whiff_percent(pitchers, whiff_lookup)
    opponent_hand_lookup = build_opponent_hand_k_lookup(schedule, report_year)
    pitchers = add_opponent_hand_matchup_k_percent(pitchers, opponent_hand_lookup)
    report_date_obj = datetime.datetime.strptime(report_date, "%m/%d/%Y").date()
    opponent_recent_lookup = build_opponent_recent_k_lookup(schedule, report_year, report_date_obj)
    pitchers = add_opponent_recent_k_percent(pitchers, opponent_recent_lookup)
    pitchers = calculate_additional_metrics(report_date, pitchers)
    pitchers = add_pitcher_recent_game_logs(pitchers, report_year, report_date_obj)

    final_df = pitchers
    if odds.lower() == "n":
        print("NO ODDS")
    else:
        try:
            final_df = merge_with_odds_data(pitchers, report_date)
        except Exception as exc:
            print(f"No Odds Found {exc}")
            final_df = pitchers

    final_df = sort_pitchers_for_report(final_df)

    write_to_html(
        final_df,
        report_key,
        report_date,
        pitcher_arsenal_lookup=arsenal_lookup,
        write_root=write_root,
    )


def _parse_cli_args(argv: Sequence[str]) -> Tuple[str, str, bool, bool]:
    if len(argv) < 3:
        print("Usage: python3 Pitchers.py <today|tmrw|MM/DD|MM/DD/YYYY> <y|n> [--exact] [--no-root]")
        sys.exit(1)

    date_input = str(argv[1])
    odds = str(argv[2]).lower()
    if odds not in {"y", "n"}:
        print("Second argument must be 'y' or 'n'.")
        sys.exit(1)

    supported_flags = {"--exact", "--no-root"}
    raw_flags = [str(flag) for flag in argv[3:]]
    unexpected_flags = [flag for flag in raw_flags if flag not in supported_flags]
    if unexpected_flags:
        print(f"Unsupported flags: {', '.join(unexpected_flags)}")
        sys.exit(1)

    return date_input, odds, "--exact" in raw_flags, "--no-root" in raw_flags


if __name__ == "__main__":
    date_input, odds, exact_mode, no_root = _parse_cli_args(sys.argv)

    try:
        report_date = resolve_date_input(date_input)
    except ValueError as exc:
        print(f"\033[91m{exc}\033[0m")
        sys.exit(1)

    print(f"\033[94mRunning at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\033[0m")
    main(report_date, odds, allow_roll_forward=not exact_mode, write_root=not no_root)
