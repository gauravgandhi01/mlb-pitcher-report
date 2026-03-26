from __future__ import annotations

import datetime
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote

import gspread
import pandas as pd
import requests
import statsapi
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from pybaseball import team_batting

from oddapi import ALT_LINES_TOKEN, get_pitcher_odds_by_team

REPORTS_DIR = Path("reports")
ROOT_INDEX_FILE = Path(__file__).resolve().parent / "index.html"
SHEETS_CREDS_FILE = Path("sheets_creds.json")
SPREADSHEET_NAME = "MLB Sheet"
SCHEDULE_STATUSES = {"Pre-Game", "Scheduled", "Warmup", "Final", "In Progress"}
NOT_STARTED_STATUSES = {"Pre-Game", "Scheduled", "Warmup"}
COMPLETED_STATUSES = {"Final", "In Progress"}
PREFERRED_ODDS_COLUMNS = ["FanDuel", "BetRivers", "Novig", "ProphetX","DraftKings"]
REPORT_COLUMN_ORDER = [
    "Name",
    "Hand",
    "GP",
    "AB",
    "K",
    "BB",
    "AVG",
    "AB/GP",
    "K/9",
    "K/AB",
    "K%",
    "PA",
    "SO/PA",
    "r",
    "Opponent",
    "Status",
    "Ks",
]
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


def fetch_pitcher_stats(name: str, team: str, opponent: str, status: str) -> Dict[str, Any]:
    try:
        player = statsapi.lookup_player(name)
        if not player:
            raise ValueError(f"Player {name} not found")
        player_id = player[0]["id"]
        stats = statsapi.player_stats(player_id, group="[pitching]", type="season")
        pitcher_stats = parse_pitcher_stats(stats, name)
        pitcher_stats["Opponent"] = opponent
        pitcher_stats["Status"] = status
        return pitcher_stats
    except Exception as exc:
        return {"Name": name, "Team": team, "Opponent": opponent, "Error": str(exc)}


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


def make_opponent_hyperlink(team: str) -> str:
    safe_team = quote(str(team))
    return f'<a href="https://statmuse.com/mlb/ask/{safe_team}-k-per-pa-log">{team}</a>'


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


def get_opp_data(date: str) -> pd.DataFrame:
    date_obj = datetime.datetime.strptime(date, "%m/%d/%Y")
    converted_date = date_obj.strftime("%Y-%m-%d")
    url = f"https://baseballsavant.mlb.com/probable-pitchers?date={converted_date}"
    data: List[Dict[str, Any]] = []

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"\033[91mFailed to retrieve probable pitcher data: {exc}\033[0m")
        return pd.DataFrame(columns=["Pitcher", "Hand", "PA", "K%"])

    soup = BeautifulSoup(response.content, "html.parser")
    blocks = soup.find_all("div", class_="mod")
    for block in blocks:
        cols = block.find_all("div", class_="col")
        for col in cols:
            try:
                name, pa, k_percentage, handedness = get_pitcher_data(col)
                data.append({"Pitcher": name, "Hand": handedness, "PA": pa, "K%": k_percentage})
            except Exception:
                data.append({"Pitcher": "TBD", "Hand": "TBD", "PA": 0, "K%": 0})

    return pd.DataFrame(data)


def fetch_schedule(date: str) -> List[Dict[str, Any]]:
    sched = statsapi.schedule(start_date=date, end_date=date)
    return [game for game in sched if game.get("status") in SCHEDULE_STATUSES]


def get_pitcher_tasks(schedule: Sequence[Dict[str, Any]]) -> List[Tuple[str, str, str, str]]:
    pitcher_tasks: List[Tuple[str, str, str, str]] = []
    for game in schedule:
        status = game.get("status", "")
        away_team, home_team = game.get("away_name", ""), game.get("home_name", "")
        away_pitcher = game.get("away_probable_pitcher")
        home_pitcher = game.get("home_probable_pitcher")
        if away_pitcher:
            pitcher_tasks.append((away_pitcher, away_team, home_team, status))
        if home_pitcher:
            pitcher_tasks.append((home_pitcher, home_team, away_team, status))
    return pitcher_tasks


def fetch_pitcher_stats_concurrently(pitcher_tasks: Sequence[Tuple[str, str, str, str]]) -> List[Dict[str, Any]]:
    if not pitcher_tasks:
        return []
    max_workers = min(16, len(pitcher_tasks))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(fetch_pitcher_stats, pitcher, team, opponent, status)
            for pitcher, team, opponent, status in pitcher_tasks
        ]
        return [future.result() for future in futures]


def prepare_team_batting_df(year: int) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for candidate_year in [year, year - 1]:
        try:
            df = team_batting(candidate_year)
            df["SO/PA"] = 100 * df["SO"] / df["PA"]
            df = df[["Team", "SO/PA"]].sort_values(by="SO/PA", ascending=False)
            df["Team"] = df["Team"].apply(get_team_full_name)
            return df
        except Exception as exc:
            last_error = exc
    print(f"\033[91mFailed to load team batting data: {last_error}\033[0m")
    return pd.DataFrame(columns=["Team", "SO/PA"])


def merge_pitcher_with_batting_data(
    results: Sequence[Dict[str, Any]],
    team_batting_df: pd.DataFrame,
) -> pd.DataFrame:
    main_df = pd.DataFrame(results)
    return pd.merge(main_df, team_batting_df, left_on="Opponent", right_on="Team", how="left")


def merge_with_opponent_data(merged_df: pd.DataFrame, opp_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(merged_df, opp_df, left_on="Name", right_on="Pitcher", how="left")


def sort_pitchers_for_report(pitchers: pd.DataFrame) -> pd.DataFrame:
    if pitchers.empty:
        return pitchers

    sorted_df = pitchers.copy()
    status_series = (
        sorted_df["Status"].astype(str).str.strip()
        if "Status" in sorted_df.columns
        else pd.Series([""] * len(sorted_df), index=sorted_df.index)
    )
    kab_series = (
        pd.to_numeric(sorted_df["K/AB"], errors="coerce")
        if "K/AB" in sorted_df.columns
        else pd.Series([pd.NA] * len(sorted_df), index=sorted_df.index, dtype="object")
    )

    sorted_df["__status_sort"] = status_series.map(lambda status: 0 if status in NOT_STARTED_STATUSES else 1)
    sorted_df["__kab_missing"] = kab_series.isna().astype(int)
    sorted_df["__kab_sort"] = kab_series.fillna(float("-inf"))
    if "Name" in sorted_df.columns:
        sorted_df["__name_sort"] = sorted_df["Name"].astype(str)
        sort_by = ["__status_sort", "__kab_missing", "__kab_sort", "__name_sort"]
        ascending = [True, True, False, True]
    else:
        sort_by = ["__status_sort", "__kab_missing", "__kab_sort"]
        ascending = [True, True, False]

    sorted_df = sorted_df.sort_values(by=sort_by, ascending=ascending, kind="mergesort")
    return sorted_df.drop(columns=["__status_sort", "__kab_missing", "__kab_sort", "__name_sort"], errors="ignore")


def calculate_additional_metrics(date: str, pitchers: pd.DataFrame) -> pd.DataFrame:
    pitchers = pitchers.copy()

    for col in ["AB", "GP", "K", "BB", "SO/PA", "K%", "PA", "K/9"]:
        if col not in pitchers.columns:
            pitchers[col] = pd.NA

    ab = pd.to_numeric(pitchers["AB"], errors="coerce")
    gp = pd.to_numeric(pitchers["GP"], errors="coerce")
    k = pd.to_numeric(pitchers["K"], errors="coerce")
    bb = pd.to_numeric(pitchers["BB"], errors="coerce")
    so_pa = pd.to_numeric(pitchers["SO/PA"], errors="coerce")
    k_ab_denominator = ab + bb

    pitchers["AB/GP"] = ab / gp
    pitchers["K/AB"] = 100 * (k / k_ab_denominator)
    pitchers["Ks"] = [get_strikeouts_by_player_name(date, name) for name in pitchers["Name"]]
    pitchers["r"] = so_pa.rank(ascending=False)

    for col in REPORT_COLUMN_ORDER:
        if col not in pitchers.columns:
            pitchers[col] = pd.NA

    pitchers = pitchers[REPORT_COLUMN_ORDER].copy()
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
    return [col for col in df.columns if col not in REPORT_COLUMN_ORDER]


def _split_primary_and_alts(value: Any) -> Tuple[str, List[str]]:
    text = str(value).strip() if value is not None else ""
    if not text:
        return "", []
    if ALT_LINES_TOKEN not in text:
        return text, []
    primary_line, raw_alts = text.split(ALT_LINES_TOKEN, 1)
    alternate_lines = [line.strip() for line in raw_alts.split(";") if line.strip()]
    return primary_line.strip(), alternate_lines


def _render_odds_line_html(line_text: str, line_class: str) -> str:
    match = PRIMARY_ODDS_PATTERN.match(line_text.strip())
    if not match:
        return f'<span class="{line_class}">{escape(line_text)}</span>'

    point_text = escape(match.group(1))
    over_text = escape(match.group(2))
    under_text = escape(match.group(3))
    return (
        f'<span class="{line_class} odds-line">'
        f'<span class="odds-point">{point_text}</span>'
        f'<span class="odds-sep">:</span>'
        f'<span class="odds-over">{over_text}</span>'
        f'<span class="odds-pipe">|</span>'
        f'<span class="odds-under">{under_text}</span>'
        f"</span>"
    )


def _extract_primary_odds_tuple(value: Any) -> Optional[Tuple[str, Optional[int], Optional[int]]]:
    primary_line, _ = _split_primary_and_alts(value)
    if not primary_line:
        return None

    match = PRIMARY_ODDS_PATTERN.match(primary_line)
    if not match:
        return None

    point = match.group(1)
    over_price = _safe_int(match.group(2))
    under_price = _safe_int(match.group(3))
    if over_price is None and under_price is None:
        return None
    return (point, over_price, under_price)


def _render_odds_cell(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    if text in {"", "-", "N/A", "nan", "None"}:
        return "-"

    primary_line, alternate_lines = _split_primary_and_alts(text)
    if not primary_line:
        return "-"

    primary_html = _render_odds_line_html(primary_line, "odds-main")
    if not alternate_lines:
        return (
            f'<div class="odds-cell">'
            f"{primary_html}"
            f'<span class="odds-arrow-placeholder" aria-hidden="true"></span>'
            f"</div>"
        )

    alt_items = "".join(
        f"<li>{_render_odds_line_html(line, 'odds-alt-line')}</li>" for line in alternate_lines
    )
    return (
        f'<div class="odds-cell">'
        f"{primary_html}"
        f'<details class="odds-details">'
        f'<summary aria-label="Show alternate odds" title="Show alternate odds">'
        f'<span class="odds-arrow">&#9662;</span>'
        f"</summary>"
        f"<ul>{alt_items}</ul>"
        f"</details>"
        f"</div>"
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


def _status_badge(status: Any) -> str:
    status_text = str(status) if status is not None else "-"
    status_slug = status_text.lower().replace(" ", "-")
    return f'<span class="status-pill status-{status_slug}">{status_text}</span>'


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


def _build_conditional_table_html(report_df: pd.DataFrame, raw_df: pd.DataFrame) -> str:
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
    odds_col_colors = {odds_col: _sportsbook_color_for_column(odds_col) for odds_col in odds_columns}
    semantic_column_classes = {
        "Name": "column-name",
        "Opponent": "column-opponent",
        "Status": "column-status",
    }

    thead = table.find("thead")
    header_cells = thead.find_all("th") if thead else []
    for col_name, col_class in semantic_column_classes.items():
        col_index = column_map.get(col_name)
        if col_index is None or col_index >= len(header_cells):
            continue
        _add_tag_class(header_cells[col_index], col_class)

    for odds_col in odds_columns:
        col_index = column_map.get(odds_col)
        if col_index is None or col_index >= len(header_cells):
            continue
        header_cell = header_cells[col_index]
        _add_tag_class(header_cell, "sportsbook-column")
        _set_tag_style_var(header_cell, "--sportsbook-color", odds_col_colors[odds_col])

    for row_index, row_tag in enumerate(row_tags):
        if row_index >= len(raw_df):
            break
        row_data = raw_df.iloc[row_index]
        row_classes = row_tag.get("class", [])

        status = str(row_data.get("Status", "")).strip()
        if status in NOT_STARTED_STATUSES:
            row_classes.append("row-upcoming")
        elif status == "In Progress":
            row_classes.append("row-live")
        elif status == "Final":
            row_classes.append("row-final")

        k_pct = _to_float(row_data.get("K%"))
        so_pa = _to_float(row_data.get("SO/PA"))
        pa = _to_float(row_data.get("PA"))
        if status in NOT_STARTED_STATUSES and k_pct is not None and so_pa is not None:
            if k_pct >= 25 and so_pa >= 24 and (pa is None or pa >= 20):
                row_classes.append("row-target")
            elif k_pct <= 18 or so_pa <= 20:
                row_classes.append("row-caution")

        ks = _to_float(row_data.get("Ks"))
        if status in COMPLETED_STATUSES and ks is not None:
            if ks >= 8:
                row_classes.append("row-ks-hot")
            elif ks <= 3:
                row_classes.append("row-ks-cold")

        if row_classes:
            row_tag["class"] = row_classes

        primary_odds_by_col: Dict[str, Tuple[str, Optional[int], Optional[int]]] = {}
        best_by_point: Dict[str, Dict[str, Optional[int]]] = {}
        for odds_col in odds_columns:
            parsed_odds = _extract_primary_odds_tuple(row_data.get(odds_col))
            if not parsed_odds:
                continue

            point, over_price, under_price = parsed_odds
            primary_odds_by_col[odds_col] = parsed_odds

            point_best = best_by_point.setdefault(point, {"over": None, "under": None})
            if over_price is not None and (
                point_best["over"] is None or over_price > point_best["over"]
            ):
                point_best["over"] = over_price
            if under_price is not None and (
                point_best["under"] is None or under_price > point_best["under"]
            ):
                point_best["under"] = under_price

        cells = row_tag.find_all("td")
        for col_name, col_class in semantic_column_classes.items():
            col_index = column_map.get(col_name)
            if col_index is None or col_index >= len(cells):
                continue
            _add_tag_class(cells[col_index], col_class)

        for odds_col in odds_columns:
            col_index = column_map.get(odds_col)
            if col_index is None or col_index >= len(cells):
                continue
            odds_cell = cells[col_index]
            _add_tag_class(odds_cell, "sportsbook-column")
            _set_tag_style_var(odds_cell, "--sportsbook-color", odds_col_colors[odds_col])

        k_ab = _to_float(row_data.get("K/AB"))
        if k_ab is not None:
            if k_ab >= 30:
                _add_cell_class(cells, column_map, "K/AB", "cell-elite")
            elif k_ab >= 24:
                _add_cell_class(cells, column_map, "K/AB", "cell-strong")
            elif k_ab <= 16:
                _add_cell_class(cells, column_map, "K/AB", "cell-weak")

        if so_pa is not None:
            if so_pa >= 25:
                _add_cell_class(cells, column_map, "SO/PA", "cell-elite")
            elif so_pa >= 23:
                _add_cell_class(cells, column_map, "SO/PA", "cell-strong")
            elif so_pa <= 20:
                _add_cell_class(cells, column_map, "SO/PA", "cell-weak")

        if k_pct is not None:
            if k_pct >= 27:
                _add_cell_class(cells, column_map, "K%", "cell-elite")
            elif k_pct >= 23:
                _add_cell_class(cells, column_map, "K%", "cell-strong")
            elif k_pct <= 18:
                _add_cell_class(cells, column_map, "K%", "cell-weak")

        k_per_nine = _to_float(row_data.get("K/9"))
        if k_per_nine is not None:
            if k_per_nine >= 11:
                _add_cell_class(cells, column_map, "K/9", "cell-strong")
            elif k_per_nine <= 7:
                _add_cell_class(cells, column_map, "K/9", "cell-weak")

        rank = _to_float(row_data.get("r"))
        if rank is not None:
            if rank <= 5:
                _add_cell_class(cells, column_map, "r", "cell-top-rank")
            elif rank >= 24:
                _add_cell_class(cells, column_map, "r", "cell-low-rank")

        if ks is not None and status in COMPLETED_STATUSES:
            if ks >= 8:
                _add_cell_class(cells, column_map, "Ks", "cell-elite")
            elif ks <= 3:
                _add_cell_class(cells, column_map, "Ks", "cell-weak")

        for odds_col in odds_columns:
            odds_col_index = column_map.get(odds_col)
            if odds_col_index is None or odds_col_index >= len(cells):
                continue
            odds_class = _classify_odds_cell(cells[odds_col_index].get_text(strip=True))
            _add_cell_class(cells, column_map, odds_col, odds_class)

            parsed_odds = primary_odds_by_col.get(odds_col)
            if not parsed_odds:
                continue
            point, over_price, under_price = parsed_odds
            point_best = best_by_point.get(point, {})
            cell = cells[odds_col_index]

            if over_price is not None and point_best.get("over") is not None and over_price == point_best["over"]:
                over_span = cell.find("span", class_="odds-over")
                if over_span is not None:
                    over_classes = over_span.get("class", [])
                    if "best-over" not in over_classes:
                        over_classes.append("best-over")
                        over_span["class"] = over_classes

            if under_price is not None and point_best.get("under") is not None and under_price == point_best["under"]:
                under_span = cell.find("span", class_="odds-under")
                if under_span is not None:
                    under_classes = under_span.get("class", [])
                    if "best-under" not in under_classes:
                        under_classes.append("best-under")
                        under_span["class"] = under_classes

    return str(table)


def _format_for_report_table(df: pd.DataFrame) -> pd.DataFrame:
    report_df = df.copy()
    odds_columns = _odds_columns_from_df(report_df)
    if "Name" in report_df.columns:
        report_df["Name"] = report_df["Name"].apply(make_pitcher_hyperlink)
    if "Opponent" in report_df.columns:
        report_df["Opponent"] = report_df["Opponent"].apply(make_opponent_hyperlink)
    if "Status" in report_df.columns:
        report_df["Status"] = report_df["Status"].apply(_status_badge)
    for col in odds_columns:
        report_df[col] = report_df[col].apply(_render_odds_cell)

    format_map = {
        "SO/PA": "{:.2f}",
        "AB/GP": "{:.1f}",
        "K/AB": "{:.2f}",
        "r": "{:.0f}",
        "K/9": "{:.1f}",
    }
    for col, fmt in format_map.items():
        if col in report_df.columns:
            numeric_col = pd.to_numeric(report_df[col], errors="coerce")
            report_df[col] = numeric_col.apply(lambda val: fmt.format(val) if pd.notna(val) else "-")

    return report_df.fillna("-")


def write_to_html(final_df: pd.DataFrame, report_key: str, display_date: str) -> Path:
    print("\033[92mWriting to HTML....\033[0m")
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    report_df = _format_for_report_table(final_df)
    table_html = _build_conditional_table_html(report_df, final_df)
    updated_at = datetime.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLB Pitcher Report {display_date}</title>
  <style>
    :root {{
      --bg: #f3f6fb;
      --panel: #ffffff;
      --text: #0f172a;
      --muted: #475569;
      --accent: #0f766e;
      --line: #dbe3ee;
      --header: #e5eef9;
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
    table.pitchers-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      font-size: 10.5px;
      table-layout: auto;
      min-width: 980px;
    }}
    table.pitchers-table thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--header);
      border-bottom: 1px solid var(--line);
      color: #0b2540;
      padding: 6px 4px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.15;
    }}
    table.pitchers-table thead th.sportsbook-column {{
      border-top: 4px solid var(--sportsbook-color);
      color: var(--sportsbook-color);
      font-weight: 700;
    }}
    table.pitchers-table tbody td {{
      border-bottom: 1px solid var(--line);
      padding: 5px 4px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.12;
      vertical-align: middle;
    }}
    table.pitchers-table th.column-name,
    table.pitchers-table td.column-name {{
      min-width: 140px;
      white-space: normal;
      overflow-wrap: anywhere;
    }}
    table.pitchers-table th.column-opponent,
    table.pitchers-table td.column-opponent {{
      min-width: 130px;
      white-space: normal;
      overflow-wrap: anywhere;
    }}
    table.pitchers-table th.column-status,
    table.pitchers-table td.column-status {{
      min-width: 74px;
    }}
    table.pitchers-table th.sportsbook-column,
    table.pitchers-table td.sportsbook-column {{
      min-width: 112px;
    }}
    table.pitchers-table tbody td.sportsbook-column {{
      box-shadow: inset 3px 0 0 var(--sportsbook-color);
      background: color-mix(in srgb, var(--sportsbook-color) 10%, #ffffff);
    }}
    table.pitchers-table tbody tr:nth-child(even) {{
      background: #f8fbff;
    }}
    table.pitchers-table tbody tr:hover {{
      background: #eef7ff;
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
    table.pitchers-table td.sportsbook-column.odds-missing {{
      background: color-mix(in srgb, var(--sportsbook-color) 6%, #f8fafc);
    }}
    .odds-cell {{
      display: inline-grid;
      grid-template-columns: minmax(0, 1fr) auto;
      column-gap: 3px;
      align-items: center;
      justify-content: center;
      line-height: 1.1;
    }}
    .odds-line {{
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
      display: grid;
      grid-template-columns: minmax(0, 1fr) 6px minmax(0, 1fr) 6px minmax(0, 1fr);
      align-items: center;
      justify-content: center;
      column-gap: 1px;
      white-space: nowrap;
      font-size: 9.5px;
    }}
    .odds-point {{
      text-align: right;
      opacity: 0.9;
    }}
    .odds-over, .odds-under {{
      text-align: right;
      padding: 0 2px;
      border-radius: 4px;
    }}
    .odds-main {{
      font-weight: 700;
    }}
    .odds-details {{
      font-size: 10px;
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
      width: 12px;
      height: 12px;
      border-radius: 50%;
      background: rgba(148, 163, 184, 0.18);
      border: 1px solid rgba(100, 116, 139, 0.35);
      padding: 0;
    }}
    .odds-details summary::-webkit-details-marker {{ display: none; }}
    .odds-arrow {{
      font-size: 8px;
      line-height: 1;
      transform: translateY(-1px);
      transition: transform 120ms ease;
    }}
    .odds-details[open] .odds-arrow {{
      transform: rotate(180deg) translateY(1px);
    }}
    .odds-details ul {{
      margin: 0;
      padding: 6px 8px;
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
    .odds-details li {{
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
      background: #bbf7d0;
      color: #14532d;
      font-weight: 800;
      display: inline-block;
      padding: 1px 4px;
      border-radius: 999px;
      box-shadow: inset 0 0 0 1px #16a34a;
    }}
    .odds-under.best-under {{
      background: #bfdbfe;
      color: #1e3a8a;
      font-weight: 800;
      display: inline-block;
      padding: 1px 4px;
      border-radius: 999px;
      box-shadow: inset 0 0 0 1px #2563eb;
    }}
    .status-pill {{
      display: inline-block;
      padding: 2px 8px;
      border-radius: 999px;
      font-weight: 600;
      font-size: 12px;
      border: 1px solid transparent;
    }}
    .status-pill.status-pre-game,
    .status-pill.status-scheduled,
    .status-pill.status-warmup {{
      background: #ecfeff;
      color: #155e75;
      border-color: #a5f3fc;
    }}
    .status-pill.status-in-progress {{
      background: #fff7ed;
      color: #9a3412;
      border-color: #fdba74;
    }}
    .status-pill.status-final {{
      background: #f1f5f9;
      color: #334155;
      border-color: #cbd5e1;
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
    @media (max-width: 900px) {{
      body {{ padding: 12px; }}
      .hero h1 {{ font-size: 23px; }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <header class="hero">
      <h1>MLB Pitcher Strikeout Report - {display_date}</h1>
      <p class="updated-at">Last updated: {updated_at}</p>
    </header>
    <section class="panel">
      <div class="table-wrap">
        {table_html}
      </div>
    </section>
  </div>
</body>
</html>
"""

    output_path = REPORTS_DIR / f"report-{report_key}.html"
    output_path.write_text(html_content, encoding="utf-8")
    ROOT_INDEX_FILE.write_text(html_content, encoding="utf-8")
    print(output_path.resolve().as_uri())
    print(ROOT_INDEX_FILE.resolve().as_uri())
    return output_path


def write_to_google_sheet(final_df: pd.DataFrame, sheet_name: str, report_date: str) -> None:
    print("\033[92mWriting to Google Sheet....\033[0m")
    if not SHEETS_CREDS_FILE.exists():
        raise FileNotFoundError(f"{SHEETS_CREDS_FILE} not found.")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(str(SHEETS_CREDS_FILE), scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open(sheet_name)

    sheet_tab_name = report_date.replace("/", "-")
    try:
        sheet = spreadsheet.worksheet(sheet_tab_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_tab_name, rows="100", cols="40")

    sheet.clear()
    set_with_dataframe(sheet, final_df)

    if report_date == datetime.datetime.now().strftime("%m/%d/%Y"):
        today_sheet = spreadsheet.sheet1
        current_time = datetime.datetime.now().strftime("%H:%M")
        today_sheet.update_title(f"TODAY: as of {current_time}")
        today_sheet.clear()
        set_with_dataframe(today_sheet, final_df)


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


def main(report_date: str, odds: str) -> None:
    report_key = report_date.replace("/", "")
    print((REPORTS_DIR / f"report-{report_key}.html").resolve().as_uri())

    schedule = fetch_schedule(report_date)
    pitcher_tasks = get_pitcher_tasks(schedule)
    results = fetch_pitcher_stats_concurrently(pitcher_tasks)
    if not results:
        print("\033[93mNo probable pitchers found for the selected date.\033[0m")
        empty_df = pd.DataFrame(columns=REPORT_COLUMN_ORDER)
        write_to_html(empty_df, report_key, report_date)
        return

    report_year = datetime.datetime.strptime(report_date, "%m/%d/%Y").year
    team_batting_df = prepare_team_batting_df(report_year)
    merged_df = merge_pitcher_with_batting_data(results, team_batting_df)
    opp_df = get_opp_data(report_date)
    pitchers = merge_with_opponent_data(merged_df, opp_df)
    pitchers = calculate_additional_metrics(report_date, pitchers)

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

    if odds.lower() != "n":
        try:
            write_to_google_sheet(final_df, SPREADSHEET_NAME, report_date)
        except Exception as exc:
            print(f"\033[91mGoogle Sheet write failed: {exc}\033[0m")

    write_to_html(final_df, report_key, report_date)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 Pitchers.py <today|tmrw|MM/DD|MM/DD/YYYY> <y|n>")
        sys.exit(1)

    date_input = str(sys.argv[1])
    odds = str(sys.argv[2]).lower()
    if odds not in {"y", "n"}:
        print("Second argument must be 'y' or 'n'.")
        sys.exit(1)

    try:
        report_date = resolve_date_input(date_input)
    except ValueError as exc:
        print(f"\033[91m{exc}\033[0m")
        sys.exit(1)

    print(f"\033[94mRunning at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\033[0m")
    main(report_date, odds)
