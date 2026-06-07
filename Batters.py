from __future__ import annotations

import datetime as dt
import sys
from html import escape
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
import statsapi
from bs4 import BeautifulSoup
from unidecode import unidecode

REPORTS_DIR = Path("reports")
ROOT_BATTERS_FILE = Path(__file__).resolve().parent / "batters.html"
SCHEDULE_STATUSES = {"Pre-Game", "Scheduled", "Warmup", "Final", "In Progress"}
NOT_STARTED_STATUSES = {"Pre-Game", "Scheduled", "Warmup"}
SOURCE_ESPN = "ESPN Confirmed"
SOURCE_ACTIVE = "Active Roster"
RECENT_GAMES = 7
RECENT_WINDOW_DAYS = 14
FALLBACK_POOL_LIMIT = 12
STREAK_SECTION_MIN = 3
ACTIVE_STREAK_SECTION_MIN = 6
MATCHUP_MIN_PA = 4
HOT_MATCHUP_AVG_FLOOR = 0.275
GOOD_MATCHUP_AVG_FLOOR = 0.300
ACTIVE_STREAK_SECTION_LIMIT = 20
HOT_SECTION_LIMIT = 20
MATCHUP_SECTION_LIMIT = 30
REQUEST_TIMEOUT_SECONDS = 20
REPORT_COLUMNS = [
    "Batter",
    "Team",
    "Opponent",
    "Pitcher",
    "Total",
    "Status",
    "Hit Stk",
    f"Last {RECENT_GAMES} AVG",
    f"Last {RECENT_GAMES} H-AB",
    "Season AVG",
    "Season H-AB",
    "VsP AVG",
    "VsP H-AB",
]

TEAM_META_CACHE: Dict[int, Dict[str, Any]] = {}
TEAM_ROSTER_CACHE: Dict[int, List[Dict[str, Any]]] = {}
PITCHER_LOOKUP_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
ESPN_SUMMARY_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
LINEUP_NAME_LOOKUP_CACHE: Dict[Tuple[int, str], Optional[int]] = {}


def _normalize_person_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


def _normalize_team_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


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


def _to_float(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def _to_int(value: Any) -> Optional[int]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    return int(numeric)


def _parse_date(value: Any) -> Optional[dt.date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def _safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


def _format_local_start_time(game_datetime: Any) -> str:
    text = str(game_datetime or "").strip()
    if not text:
        return ""
    try:
        game_dt = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return ""
    local_time = game_dt.astimezone().strftime("%I:%M %p").lstrip("0")
    return local_time.replace(" AM", "a").replace(" PM", "p")


def resolve_date_input(raw_input: str) -> str:
    today = dt.datetime.now()
    input_lower = raw_input.lower()
    if input_lower == "today":
        return today.strftime("%m/%d/%Y")
    if input_lower == "tmrw":
        return (today + dt.timedelta(days=1)).strftime("%m/%d/%Y")

    if raw_input.count("/") == 2:
        dt.datetime.strptime(raw_input, "%m/%d/%Y")
        return raw_input
    if raw_input.count("/") == 1:
        return f"{raw_input}/{today.year}"
    raise ValueError("Date must be 'today', 'tmrw', 'MM/DD', or 'MM/DD/YYYY'.")


def _next_report_date(report_date: str) -> str:
    date_obj = dt.datetime.strptime(report_date, "%m/%d/%Y")
    return (date_obj + dt.timedelta(days=1)).strftime("%m/%d/%Y")


def fetch_schedule(date: str) -> List[Dict[str, Any]]:
    schedule = statsapi.schedule(start_date=date, end_date=date)
    return [game for game in schedule if game.get("status") in SCHEDULE_STATUSES]


def _has_not_started_games(schedule: Sequence[Dict[str, Any]]) -> bool:
    return any(game.get("status") in NOT_STARTED_STATUSES for game in schedule)


def resolve_effective_report_date_and_schedule(report_date: str) -> Tuple[str, List[Dict[str, Any]]]:
    schedule = fetch_schedule(report_date)
    if _has_not_started_games(schedule):
        return report_date, schedule

    next_report_date = _next_report_date(report_date)
    next_schedule = fetch_schedule(next_report_date)
    print(
        "\033[93mNo games remain in a not-started state on "
        f"{report_date}. Rolling report forward to {next_report_date}.\033[0m"
    )
    return next_report_date, next_schedule


def fetch_team_meta(team_id: int) -> Dict[str, Any]:
    if team_id in TEAM_META_CACHE:
        return TEAM_META_CACHE[team_id]

    data = statsapi.get("team", {"teamId": team_id})
    team = (data.get("teams") or [{}])[0]
    TEAM_META_CACHE[team_id] = team
    return team


def fetch_team_roster(team_id: int) -> List[Dict[str, Any]]:
    if team_id in TEAM_ROSTER_CACHE:
        return TEAM_ROSTER_CACHE[team_id]

    roster_data = statsapi.get("team_roster", {"teamId": team_id})
    roster = roster_data.get("roster") or []
    TEAM_ROSTER_CACHE[team_id] = roster
    return roster


def fetch_pitcher_context(pitcher_name: str) -> Optional[Dict[str, Any]]:
    key = _normalize_person_name(pitcher_name)
    if key in PITCHER_LOOKUP_CACHE:
        return PITCHER_LOOKUP_CACHE[key]

    players = statsapi.lookup_player(pitcher_name)
    player = _choose_best_player_match(players or [], pitcher_name)
    if not player:
        PITCHER_LOOKUP_CACHE[key] = None
        return None

    data = statsapi.get("people", {"personIds": player["id"]}, force=True)
    people = data.get("people") or []
    if not people:
        PITCHER_LOOKUP_CACHE[key] = None
        return None

    person = people[0]
    result = {
        "id": int(person["id"]),
        "name": str(person.get("fullName") or pitcher_name),
        "hand": str(((person.get("pitchHand") or {}).get("code") or "")).upper() or None,
    }
    PITCHER_LOOKUP_CACHE[key] = result
    return result


def _fetch_espn_scoreboard_events(date: str) -> List[Dict[str, Any]]:
    date_obj = dt.datetime.strptime(date, "%m/%d/%Y")
    url = (
        "https://site.web.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard"
        f"?dates={date_obj.strftime('%Y%m%d')}"
    )
    response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    payload = response.json()
    return payload.get("events") or []


def build_espn_event_lookup(date: str) -> Dict[Tuple[str, str], str]:
    lookup: Dict[Tuple[str, str], str] = {}
    for event in _fetch_espn_scoreboard_events(date):
        event_id = str(event.get("id", "")).strip()
        competition = (event.get("competitions") or [{}])[0]
        competitors = competition.get("competitors") or []
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
        if event_id and away_name and home_name:
            lookup[(_normalize_team_name(away_name), _normalize_team_name(home_name))] = event_id
    return lookup


def fetch_espn_summary(event_id: str) -> Optional[Dict[str, Any]]:
    if not event_id:
        return None
    if event_id in ESPN_SUMMARY_CACHE:
        return ESPN_SUMMARY_CACHE[event_id]

    url = f"https://site.web.api.espn.com/apis/site/v2/sports/baseball/mlb/summary?event={event_id}"
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        summary = response.json()
    except requests.RequestException:
        summary = None
    ESPN_SUMMARY_CACHE[event_id] = summary
    return summary


def extract_espn_game_total(summary_data: Optional[Dict[str, Any]]) -> Optional[float]:
    if not summary_data:
        return None

    for pickcenter in summary_data.get("pickcenter") or []:
        total = _to_float(pickcenter.get("overUnder"))
        if total is not None:
            return total

    for odds_entry in summary_data.get("odds") or []:
        total = _to_float(odds_entry.get("overUnder"))
        if total is not None:
            return total

    return None


def extract_confirmed_espn_lineup(summary_data: Dict[str, Any], team_abbrev: str) -> List[Dict[str, Any]]:
    target = str(team_abbrev or "").strip().upper()
    for roster_block in summary_data.get("rosters") or []:
        roster_team = roster_block.get("team") or {}
        if str(roster_team.get("abbreviation") or "").strip().upper() != target:
            continue

        roster = roster_block.get("roster") or []
        ordered = [player for player in roster if _to_int(player.get("batOrder")) is not None]
        ordered = sorted(ordered, key=lambda player: _to_int(player.get("batOrder")) or 999)
        starters = [player for player in ordered if player.get("starter")]
        lineup = starters if len(starters) >= 9 else ordered
        if len(lineup) < 9:
            return []
        return [
            {
                "name": str((entry.get("athlete") or {}).get("fullName", "")).strip(),
                "order": int(_to_int(entry.get("batOrder")) or 0),
            }
            for entry in lineup[:9]
            if str((entry.get("athlete") or {}).get("fullName", "")).strip()
        ]
    return []


def filter_active_hitters(roster_entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    hitters: List[Dict[str, Any]] = []
    for entry in roster_entries:
        person = entry.get("person") or {}
        position = entry.get("position") or {}
        status = entry.get("status") or {}
        position_type = str(position.get("type") or "").strip().lower()
        if not person.get("id"):
            continue
        if str(status.get("code") or "").strip().upper() not in {"A", ""}:
            continue
        if position_type == "pitcher":
            continue
        hitters.append(entry)
    return hitters


def _chunked(values: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_people_stats_map(
    person_ids: Sequence[int],
    season: int,
    pitch_hand: Optional[str],
    pitcher_id: int,
) -> Dict[int, Dict[str, Any]]:
    if not person_ids:
        return {}

    sit_code = "vl" if str(pitch_hand or "").upper() == "L" else "vr"
    hydrate = (
        "stats(group=[hitting],type=[season,gameLog,statSplits,vsPlayer],"
        f"sitCodes=[{sit_code}],opposingPlayerId={pitcher_id},season={season})"
    )
    people_by_id: Dict[int, Dict[str, Any]] = {}
    for chunk in _chunked(list(dict.fromkeys(person_ids)), 8):
        payload = statsapi.get(
            "people",
            {"personIds": ",".join(str(person_id) for person_id in chunk), "hydrate": hydrate},
            force=True,
        )
        for person in payload.get("people") or []:
            people_by_id[int(person["id"])] = person
    return people_by_id


def index_stat_blocks(person: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    indexed: Dict[str, List[Dict[str, Any]]] = {}
    for block in person.get("stats") or []:
        display_name = str((block.get("type") or {}).get("displayName") or "").strip()
        if display_name:
            indexed.setdefault(display_name, []).append(block)
    return indexed


def first_stat_split(blocks: Sequence[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not blocks:
        return None
    splits = blocks[0].get("splits") or []
    if not splits:
        return None
    return splits[0]


def extract_game_logs(person: Dict[str, Any]) -> List[Dict[str, Any]]:
    indexed = index_stat_blocks(person)
    logs: List[Dict[str, Any]] = []
    for block in indexed.get("gameLog", []):
        for split in block.get("splits") or []:
            game_date = _parse_date(split.get("date"))
            if game_date is None:
                continue
            stat = split.get("stat") or {}
            game_info = split.get("game") or {}
            logs.append(
                {
                    "date": game_date,
                    "gamePk": _to_int(game_info.get("gamePk")) or 0,
                    "atBats": _to_int(stat.get("atBats")) or 0,
                    "hits": _to_int(stat.get("hits")) or 0,
                    "walks": _to_int(stat.get("baseOnBalls")) or 0,
                    "hitByPitch": _to_int(stat.get("hitByPitch")) or 0,
                    "sacFlies": _to_int(stat.get("sacFlies")) or 0,
                    "plateAppearances": _to_int(stat.get("plateAppearances")) or 0,
                    "totalBases": _to_int(stat.get("totalBases")) or 0,
                    "strikeOuts": _to_int(stat.get("strikeOuts")) or 0,
                    "homeRuns": _to_int(stat.get("homeRuns")) or 0,
                    "rbi": _to_int(stat.get("rbi")) or 0,
                }
            )
    logs.sort(key=lambda row: (row["date"], row["gamePk"]), reverse=True)
    return logs


def compute_recent_metrics(
    game_logs: Sequence[Dict[str, Any]],
    report_date: dt.date,
    *,
    max_games: Optional[int] = None,
    window_days: Optional[int] = None,
) -> Dict[str, Any]:
    start_date = report_date - dt.timedelta(days=window_days or 0) if window_days is not None else None
    filtered = []
    for row in game_logs:
        row_date = row.get("date")
        if not isinstance(row_date, dt.date) or row_date >= report_date:
            continue
        if start_date is not None and row_date < start_date:
            continue
        filtered.append(row)

    filtered.sort(key=lambda row: (row["date"], row.get("gamePk", 0)), reverse=True)
    if max_games is not None:
        filtered = filtered[:max_games]

    totals = {
        "games": len(filtered),
        "PA": 0,
        "AB": 0,
        "H": 0,
        "BB": 0,
        "HBP": 0,
        "SF": 0,
        "TB": 0,
        "K": 0,
        "HR": 0,
        "RBI": 0,
    }
    for row in filtered:
        totals["PA"] += int(row.get("plateAppearances") or 0)
        totals["AB"] += int(row.get("atBats") or 0)
        totals["H"] += int(row.get("hits") or 0)
        totals["BB"] += int(row.get("walks") or 0)
        totals["HBP"] += int(row.get("hitByPitch") or 0)
        totals["SF"] += int(row.get("sacFlies") or 0)
        totals["TB"] += int(row.get("totalBases") or 0)
        totals["K"] += int(row.get("strikeOuts") or 0)
        totals["HR"] += int(row.get("homeRuns") or 0)
        totals["RBI"] += int(row.get("rbi") or 0)

    obp_denom = totals["AB"] + totals["BB"] + totals["HBP"] + totals["SF"]
    avg = _safe_ratio(totals["H"], totals["AB"])
    obp = _safe_ratio(totals["H"] + totals["BB"] + totals["HBP"], obp_denom)
    slg = _safe_ratio(totals["TB"], totals["AB"])
    k_pct = _safe_ratio(totals["K"] * 100.0, totals["PA"])
    ops = None if obp is None or slg is None else obp + slg
    totals.update(
        {
            "AVG": avg,
            "OBP": obp,
            "SLG": slg,
            "OPS": ops,
            "K%": k_pct,
        }
    )
    return totals


def compute_hit_streak(game_logs: Sequence[Dict[str, Any]], report_date: dt.date) -> int:
    filtered = [
        row
        for row in game_logs
        if isinstance(row.get("date"), dt.date) and row["date"] < report_date
    ]
    filtered.sort(key=lambda row: (row["date"], row.get("gamePk", 0)), reverse=True)

    streak = 0
    for row in filtered:
        at_bats = int(row.get("atBats") or 0)
        hits = int(row.get("hits") or 0)
        if at_bats <= 0:
            continue
        if hits >= 1:
            streak += 1
            continue
        break
    return streak


def parse_vs_pitcher_stats(indexed_blocks: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    total_split = first_stat_split(indexed_blocks.get("vsPlayerTotal", []))
    if total_split is not None:
        stat = total_split.get("stat") or {}
        return {
            "PA": _to_int(stat.get("plateAppearances")) or 0,
            "AB": _to_int(stat.get("atBats")) or 0,
            "H": _to_int(stat.get("hits")) or 0,
            "HR": _to_int(stat.get("homeRuns")) or 0,
            "RBI": _to_int(stat.get("rbi")) or 0,
            "AVG": _to_float(stat.get("avg")),
            "OPS": _to_float(stat.get("ops")),
            "K%": _safe_ratio((_to_int(stat.get("strikeOuts")) or 0) * 100.0, _to_int(stat.get("plateAppearances")) or 0),
        }

    splits = []
    for block in indexed_blocks.get("vsPlayer", []):
        splits.extend(block.get("splits") or [])
    if not splits:
        return {"PA": 0, "AB": 0, "H": 0, "HR": 0, "RBI": 0, "AVG": None, "OPS": None, "K%": None}

    at_bats = 0
    hits = 0
    walks = 0
    hit_by_pitch = 0
    sac_flies = 0
    total_bases = 0
    strikeouts = 0
    plate_appearances = 0
    home_runs = 0
    rbi = 0
    for split in splits:
        stat = split.get("stat") or {}
        at_bats += _to_int(stat.get("atBats")) or 0
        hits += _to_int(stat.get("hits")) or 0
        walks += _to_int(stat.get("baseOnBalls")) or 0
        hit_by_pitch += _to_int(stat.get("hitByPitch")) or 0
        sac_flies += _to_int(stat.get("sacFlies")) or 0
        total_bases += _to_int(stat.get("totalBases")) or 0
        strikeouts += _to_int(stat.get("strikeOuts")) or 0
        plate_appearances += _to_int(stat.get("plateAppearances")) or 0
        home_runs += _to_int(stat.get("homeRuns")) or 0
        rbi += _to_int(stat.get("rbi")) or 0

    obp_denom = at_bats + walks + hit_by_pitch + sac_flies
    avg = _safe_ratio(hits, at_bats)
    obp = _safe_ratio(hits + walks + hit_by_pitch, obp_denom)
    slg = _safe_ratio(total_bases, at_bats)
    ops = None if obp is None or slg is None else obp + slg
    k_pct = _safe_ratio(strikeouts * 100.0, plate_appearances)
    return {"PA": plate_appearances, "AB": at_bats, "H": hits, "HR": home_runs, "RBI": rbi, "AVG": avg, "OPS": ops, "K%": k_pct}


def _extract_season_stat(person: Dict[str, Any]) -> Dict[str, Any]:
    indexed = index_stat_blocks(person)
    split = first_stat_split(indexed.get("season", []))
    stat = (split or {}).get("stat") or {}
    return {
        "PA": _to_int(stat.get("plateAppearances")) or 0,
        "AB": _to_int(stat.get("atBats")) or 0,
        "H": _to_int(stat.get("hits")) or 0,
        "AVG": _to_float(stat.get("avg")),
        "OBP": _to_float(stat.get("obp")),
        "SLG": _to_float(stat.get("slg")),
        "OPS": _to_float(stat.get("ops")),
    }


def resolve_lineup_player_ids(
    lineup_entries: Sequence[Dict[str, Any]],
    roster_entries: Sequence[Dict[str, Any]],
    team_id: int,
) -> List[int]:
    roster_name_lookup = {
        _normalize_person_name((entry.get("person") or {}).get("fullName")): int((entry.get("person") or {}).get("id"))
        for entry in roster_entries
        if (entry.get("person") or {}).get("fullName") and (entry.get("person") or {}).get("id")
    }
    resolved_ids: List[int] = []
    for lineup_entry in lineup_entries:
        player_name = str(lineup_entry.get("name") or "").strip()
        cache_key = (team_id, _normalize_person_name(player_name))
        if cache_key in LINEUP_NAME_LOOKUP_CACHE:
            player_id = LINEUP_NAME_LOOKUP_CACHE[cache_key]
        else:
            player_id = roster_name_lookup.get(cache_key[1])
            if player_id is None and player_name:
                candidates = statsapi.lookup_player(player_name)
                for candidate in candidates or []:
                    current_team = (candidate.get("currentTeam") or {}).get("id")
                    if int(current_team or 0) == int(team_id):
                        player_id = int(candidate["id"])
                        break
            LINEUP_NAME_LOOKUP_CACHE[cache_key] = player_id
        if player_id is None:
            return []
        resolved_ids.append(int(player_id))
    return resolved_ids


def build_candidate_rows(
    team_id: int,
    team_name: str,
    team_abbrev: str,
    opponent_id: int,
    opponent_name: str,
    opponent_abbrev: str,
    pitcher_name: str,
    game_total: Optional[float],
    pitch_hand: Optional[str],
    start_time: str,
    status: str,
    roster_entries: Sequence[Dict[str, Any]],
    people_by_id: Dict[int, Dict[str, Any]],
    report_date: dt.date,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for roster_entry in roster_entries:
        person_info = roster_entry.get("person") or {}
        person_id = _to_int(person_info.get("id"))
        if person_id is None:
            continue

        person = people_by_id.get(person_id)
        if person is None:
            continue

        indexed = index_stat_blocks(person)
        season = _extract_season_stat(person)
        game_logs = extract_game_logs(person)
        recent7 = compute_recent_metrics(game_logs, report_date, max_games=RECENT_GAMES)
        recent14 = compute_recent_metrics(game_logs, report_date, window_days=RECENT_WINDOW_DAYS)
        vsp = parse_vs_pitcher_stats(indexed)
        hit_streak = compute_hit_streak(game_logs, report_date)

        row = {
            "Batter": str(person_info.get("fullName") or person.get("fullName") or "").strip(),
            "Team Id": team_id,
            "Team": team_name,
            "Team Abbrev": team_abbrev,
            "Opponent Id": opponent_id,
            "Opponent": opponent_name,
            "Opponent Abbrev": opponent_abbrev,
            "Pitcher": pitcher_name,
            "Total": game_total,
            "Pitch Hand": pitch_hand or "",
            "Source": SOURCE_ACTIVE,
            "Pool Rank": pd.NA,
            "Hot Score": None,
            "Hit Stk": hit_streak,
            "Recent PA": recent7["PA"],
            "Recent AB": recent7["AB"],
            "Recent H": recent7["H"],
            "Recent AVG": recent7["AVG"],
            "Recent OBP": recent7["OBP"],
            "Recent SLG": recent7["SLG"],
            "Recent OPS": recent7["OPS"],
            "Recent HR": recent7["HR"],
            "Recent RBI": recent7["RBI"],
            "Recent K%": recent7["K%"],
            "VsP PA": vsp["PA"],
            "VsP AB": vsp["AB"],
            "VsP H": vsp["H"],
            "VsP HR": vsp["HR"],
            "VsP RBI": vsp["RBI"],
            "VsP AVG": vsp["AVG"],
            "VsP OPS": vsp["OPS"],
            "VsP K%": vsp["K%"],
            "Season PA": season["PA"],
            "Season AB": season["AB"],
            "Season H": season["H"],
            "Season AVG": season["AVG"],
            "Season OBP": season["OBP"],
            "Season SLG": season["SLG"],
            "Season OPS": season["OPS"],
            "Start": start_time,
            "Status": status,
            "__player_id": person_id,
            "__recent14d_pa": recent14["PA"],
            "__recent14d_ops": recent14["OPS"],
            "__season_pa": season["PA"],
            "__season_ops": season["OPS"],
            "__low_sample": recent7["PA"] < 8,
        }
        rows.append(row)
    return rows


def rank_active_roster_candidates(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            -int(row.get("__recent14d_pa") or 0),
            -int(row.get("__season_pa") or 0),
            -(float(row.get("__recent14d_ops")) if row.get("__recent14d_ops") is not None else -1.0),
            -(float(row.get("__season_ops")) if row.get("__season_ops") is not None else -1.0),
            str(row.get("Batter") or ""),
        ),
    )


def select_offense_rows(
    candidate_rows: Sequence[Dict[str, Any]],
    lineup_player_ids: Sequence[int],
) -> List[Dict[str, Any]]:
    rows_by_id = {int(row["__player_id"]): dict(row) for row in candidate_rows}
    if len(lineup_player_ids) >= 9 and all(player_id in rows_by_id for player_id in lineup_player_ids[:9]):
        selected_rows: List[Dict[str, Any]] = []
        for order, player_id in enumerate(lineup_player_ids[:9], start=1):
            row = dict(rows_by_id[player_id])
            row["Source"] = SOURCE_ESPN
            row["Pool Rank"] = order
            selected_rows.append(row)
        return selected_rows

    ranked_rows = rank_active_roster_candidates(candidate_rows)[:FALLBACK_POOL_LIMIT]
    selected_rows = []
    for order, row in enumerate(ranked_rows, start=1):
        updated = dict(row)
        updated["Source"] = SOURCE_ACTIVE
        updated["Pool Rank"] = order
        selected_rows.append(updated)
    return selected_rows


def _percentile_series(values: pd.Series, *, lower_is_better: bool = False) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce").fillna(0.0)
    percentiles = numeric.rank(pct=True, method="average")
    if lower_is_better:
        return 1 - percentiles
    return percentiles


def apply_hot_scores(rows: Sequence[Dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    ops_pct = _percentile_series(df["Recent OPS"])
    obp_pct = _percentile_series(df["Recent OBP"])
    slg_pct = _percentile_series(df["Recent SLG"])
    inv_k_pct = _percentile_series(df["Recent K%"], lower_is_better=True)
    df["Hot Score"] = 0.40 * ops_pct + 0.25 * obp_pct + 0.25 * slg_pct + 0.10 * inv_k_pct
    df["__low_sample"] = pd.to_numeric(df["Recent PA"], errors="coerce").fillna(0) < 8
    return df


def _status_sort_value(status: Any) -> int:
    status_text = str(status or "").strip()
    if status_text in NOT_STARTED_STATUSES:
        return 0
    if status_text == "In Progress":
        return 2
    return 1


def sort_batters_for_report(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    sorted_df = df.copy()
    sorted_df["__status_sort"] = sorted_df["Status"].astype(str).map(_status_sort_value)
    sorted_df["__vsp_ops_sort"] = pd.to_numeric(sorted_df["VsP OPS"], errors="coerce").fillna(-1.0)
    sorted_df["__season_ops_sort"] = pd.to_numeric(sorted_df["Season OPS"], errors="coerce").fillna(-1.0)
    sorted_df["__hot_score_sort"] = pd.to_numeric(sorted_df["Hot Score"], errors="coerce").fillna(-1.0)
    sorted_df["__pool_rank_sort"] = pd.to_numeric(sorted_df["Pool Rank"], errors="coerce").fillna(999)
    sorted_df = sorted_df.sort_values(
        by=[
            "__status_sort",
            "__low_sample",
            "__hot_score_sort",
            "__vsp_ops_sort",
            "__season_ops_sort",
            "__pool_rank_sort",
            "Batter",
        ],
        ascending=[True, True, False, False, False, True, True],
        kind="mergesort",
    )
    return sorted_df.drop(
        columns=["__status_sort", "__vsp_ops_sort", "__season_ops_sort", "__hot_score_sort", "__pool_rank_sort"],
        errors="ignore",
    )


def build_active_hit_streak_section(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    filtered = df.copy()
    filtered["__status_sort"] = filtered["Status"].astype(str).map(_status_sort_value)
    filtered["__hit_stk"] = pd.to_numeric(filtered["Hit Stk"], errors="coerce").fillna(0)
    filtered["__recent_avg"] = pd.to_numeric(filtered["Recent AVG"], errors="coerce")
    filtered["__season_avg"] = pd.to_numeric(filtered["Season AVG"], errors="coerce")
    filtered["__vsp_avg"] = pd.to_numeric(filtered["VsP AVG"], errors="coerce")

    streaks = filtered[filtered["__hit_stk"] >= ACTIVE_STREAK_SECTION_MIN].copy()
    if streaks.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    streaks = streaks.sort_values(
        by=["__status_sort", "__hit_stk", "__recent_avg", "__season_avg", "__vsp_avg", "Batter"],
        ascending=[True, False, False, False, False, True],
        kind="mergesort",
    )
    return streaks.head(ACTIVE_STREAK_SECTION_LIMIT)


def build_hot_streak_matchup_section(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    filtered = df.copy()
    filtered["__status_sort"] = filtered["Status"].astype(str).map(_status_sort_value)
    filtered["__hit_stk"] = pd.to_numeric(filtered["Hit Stk"], errors="coerce").fillna(0)
    filtered["__vsp_pa"] = pd.to_numeric(filtered["VsP PA"], errors="coerce").fillna(0)
    filtered["__vsp_avg"] = pd.to_numeric(filtered["VsP AVG"], errors="coerce")
    filtered["__season_avg"] = pd.to_numeric(filtered["Season AVG"], errors="coerce")
    filtered["__recent_avg"] = pd.to_numeric(filtered["Recent AVG"], errors="coerce")

    hot = filtered[
        (filtered["__hit_stk"] >= STREAK_SECTION_MIN)
        & (filtered["__vsp_pa"] >= MATCHUP_MIN_PA)
        & (filtered["__vsp_avg"].fillna(0) >= HOT_MATCHUP_AVG_FLOOR)
        & (filtered["__vsp_avg"].fillna(0) >= filtered["__season_avg"].fillna(0))
    ].copy()
    if hot.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    hot = hot.sort_values(
        by=["__status_sort", "__hit_stk", "__vsp_avg", "__recent_avg", "__vsp_pa", "Batter"],
        ascending=[True, False, False, False, False, True],
        kind="mergesort",
    )
    return hot.head(HOT_SECTION_LIMIT)


def build_good_matchups_section(df: pd.DataFrame, hot_df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    filtered = df.copy()
    filtered["__status_sort"] = filtered["Status"].astype(str).map(_status_sort_value)
    filtered["__vsp_pa"] = pd.to_numeric(filtered["VsP PA"], errors="coerce").fillna(0)
    filtered["__vsp_avg"] = pd.to_numeric(filtered["VsP AVG"], errors="coerce")
    filtered["__recent_avg"] = pd.to_numeric(filtered["Recent AVG"], errors="coerce")
    filtered["__hit_stk"] = pd.to_numeric(filtered["Hit Stk"], errors="coerce").fillna(0)

    if not hot_df.empty:
        hot_keys = set(zip(hot_df["Batter"], hot_df["Pitcher"], hot_df["Team"]))
        filtered = filtered[
            ~filtered.apply(lambda row: (row["Batter"], row["Pitcher"], row["Team"]) in hot_keys, axis=1)
        ].copy()

    good = filtered[
        (filtered["__vsp_pa"] >= MATCHUP_MIN_PA)
        & (filtered["__vsp_avg"].fillna(0) >= GOOD_MATCHUP_AVG_FLOOR)
    ].copy()
    if good.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    good = good.sort_values(
        by=["__status_sort", "__vsp_avg", "__vsp_pa", "__recent_avg", "__hit_stk", "Batter"],
        ascending=[True, False, False, False, False, True],
        kind="mergesort",
    )
    return good.head(MATCHUP_SECTION_LIMIT)


def _format_rate(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.3f}"


def _format_pct(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.1f}"


def _format_score(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return f"{numeric * 100:.1f}"


def _format_int(value: Any) -> str:
    numeric = _to_int(value)
    if numeric is None:
        return ""
    return str(numeric)


def _format_hit_ab(hits: Any, at_bats: Any) -> str:
    hits_value = _to_int(hits) or 0
    at_bats_value = _to_int(at_bats) or 0
    if at_bats_value <= 0:
        return ""
    return f"{hits_value}-{at_bats_value}"


def _team_logo_url(team_id: Any) -> str:
    team_id_value = _to_int(team_id)
    if team_id_value is None:
        return ""
    return f"https://www.mlbstatic.com/team-logos/{team_id_value}.svg"


def _status_badge(status: Any) -> str:
    status_text = str(status or "").strip() or "Unknown"
    status_slug = (
        status_text.lower()
        .replace(" ", "-")
        .replace("/", "-")
    )
    return f'<span class="status-pill status-{escape(status_slug, quote=True)}">{escape(status_text)}</span>'


def _render_team_cell(team_name: Any, team_abbrev: Any, team_id: Any) -> str:
    name_text = str(team_name or "").strip()
    abbrev_text = str(team_abbrev or "").strip() or name_text[:3].upper()
    logo_url = escape(_team_logo_url(team_id), quote=True)
    title = escape(name_text or abbrev_text, quote=True)
    return (
        '<span class="team-cell" title="'
        + title
        + '"><span class="team-badge"><img class="team-logo" src="'
        + logo_url
        + '" alt="'
        + title
        + ' logo"></span><span class="team-abbrev">'
        + escape(abbrev_text)
        + "</span></span>"
    )


def _render_opponent_cell(
    opponent_name: Any,
    opponent_abbrev: Any,
    opponent_id: Any,
    start_time: Any,
) -> str:
    name_text = str(opponent_name or "").strip()
    abbrev_text = str(opponent_abbrev or "").strip() or name_text[:3].upper()
    logo_url = escape(_team_logo_url(opponent_id), quote=True)
    title = escape(name_text or abbrev_text, quote=True)
    time_text = str(start_time or "").strip()
    time_html = (
        '<span class="opp-time">' + escape(time_text) + "</span>"
        if time_text
        else ""
    )
    return (
        '<span class="opp-cell" title="'
        + title
        + '"><span class="opp-team"><span class="team-badge"><img class="team-logo" src="'
        + logo_url
        + '" alt="'
        + title
        + ' logo"></span><span class="team-abbrev">'
        + escape(abbrev_text)
        + "</span></span>"
        + time_html
        + "</span>"
    )


def _add_tag_class(tag: Any, class_name: str) -> None:
    tag_classes = tag.get("class", [])
    if class_name not in tag_classes:
        tag_classes.append(class_name)
        tag["class"] = tag_classes


def _add_cell_class(cells: Sequence[Any], column_map: Dict[str, int], column_name: str, class_name: str) -> None:
    col_index = column_map.get(column_name)
    if col_index is None or col_index >= len(cells):
        return
    _add_tag_class(cells[col_index], class_name)


def _classify_avg_cell(value: Any, *, elite: float, strong: float, weak: float) -> Optional[str]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    if numeric >= elite:
        return "cell-elite"
    if numeric >= strong:
        return "cell-strong"
    if numeric <= weak:
        return "cell-weak"
    return None


def _classify_total_cell(value: Any) -> Optional[str]:
    numeric = _to_float(value)
    if numeric is None:
        return None
    if numeric >= 9.0:
        return "cell-elite"
    if numeric >= 8.5:
        return "cell-strong"
    if numeric <= 7.5:
        return "cell-weak"
    return None


def _format_pitcher_last_name(name: Any) -> str:
    parts = [part for part in str(name or "").strip().split() if part]
    if not parts:
        return ""
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    last_token = parts[-1]
    if len(parts) >= 2 and last_token.lower().rstrip(".") in suffixes:
        return parts[-2]
    return last_token


def _format_total(value: Any) -> str:
    numeric = _to_float(value)
    if numeric is None:
        return ""
    return f"{numeric:.1f}"


def format_report_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=REPORT_COLUMNS)

    formatted = df.copy()
    formatted["Batter"] = formatted["Batter"].apply(
        lambda value: '<span class="batter-name">' + escape(str(value or "")) + "</span>"
    )
    formatted["Pitcher"] = formatted["Pitcher"].apply(
        lambda value: '<span class="pitcher-name">' + escape(_format_pitcher_last_name(value)) + "</span>"
    )
    formatted["Team"] = [
        _render_team_cell(team_name, team_abbrev, team_id)
        for team_name, team_abbrev, team_id in zip(
            formatted["Team"],
            formatted["Team Abbrev"],
            formatted["Team Id"],
        )
    ]
    formatted["Opponent"] = [
        _render_opponent_cell(opponent_name, opponent_abbrev, opponent_id, start_time)
        for opponent_name, opponent_abbrev, opponent_id, start_time in zip(
            formatted["Opponent"],
            formatted["Opponent Abbrev"],
            formatted["Opponent Id"],
            formatted["Start"],
        )
    ]
    formatted["Status"] = formatted["Status"].apply(_status_badge)
    formatted["Total"] = formatted["Total"].apply(_format_total)
    formatted["Hit Stk"] = formatted["Hit Stk"].apply(_format_int)
    formatted[f"Last {RECENT_GAMES} AVG"] = formatted["Recent AVG"].apply(_format_rate)
    for column in ["VsP AVG", "Season AVG"]:
        formatted[column] = formatted[column].apply(_format_rate)
    formatted[f"Last {RECENT_GAMES} H-AB"] = [
        _format_hit_ab(hits, at_bats)
        for hits, at_bats in zip(formatted["Recent H"], formatted["Recent AB"])
    ]
    formatted["VsP H-AB"] = [
        _format_hit_ab(hits, at_bats)
        for hits, at_bats in zip(formatted["VsP H"], formatted["VsP AB"])
    ]
    formatted["Season H-AB"] = [
        _format_hit_ab(hits, at_bats)
        for hits, at_bats in zip(formatted["Season H"], formatted["Season AB"])
    ]
    return formatted[REPORT_COLUMNS]


def format_focus_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return format_report_dataframe(df)


def _build_focus_table_html(report_df: pd.DataFrame, raw_df: pd.DataFrame) -> str:
    if report_df.empty:
        return '<p class="empty-state">No rows available.</p>'

    table_html = report_df.to_html(index=False, escape=False, classes="pitchers-table batters-table", border=0)
    soup = BeautifulSoup(table_html, "html.parser")
    table = soup.find("table")
    if table is None:
        return table_html

    headers = [th.get_text(strip=True) for th in table.find_all("th")]
    column_map = {header: idx for idx, header in enumerate(headers)}
    header_group_map = {
        "Batter": "group-context",
        "Team": "group-context",
        "Opponent": "group-context",
        "Pitcher": "group-context",
        "Total": "group-context",
        "Status": "group-context",
        "Hit Stk": "group-batter",
        f"Last {RECENT_GAMES} AVG": "group-batter",
        f"Last {RECENT_GAMES} H-AB": "group-batter",
        "Season AVG": "group-batter",
        "Season H-AB": "group-batter",
        "VsP AVG": "group-matchup",
        "VsP H-AB": "group-matchup",
    }
    semantic_classes = {
        "Batter": "column-name",
        "Team": "column-team",
        "Opponent": "column-opponent",
        "Pitcher": "column-pitcher",
        "Total": "column-total",
        "Status": "column-status",
    }

    thead = table.find("thead")
    tbody = table.find("tbody")
    if thead is None or tbody is None:
        return table_html

    header_cells = thead.find_all("th")
    for col_name, col_index in column_map.items():
        if col_index >= len(header_cells):
            continue
        header_cell = header_cells[col_index]
        group_class = header_group_map.get(col_name)
        if group_class:
            _add_tag_class(header_cell, group_class)
        semantic_class = semantic_classes.get(col_name)
        if semantic_class:
            _add_tag_class(header_cell, semantic_class)

    row_tags = tbody.find_all("tr")
    for row_index, row_tag in enumerate(row_tags):
        if row_index >= len(raw_df):
            break
        row_data = raw_df.iloc[row_index]
        row_classes = row_tag.get("class", [])
        status_text = str(row_data.get("Status") or "").strip()
        if status_text in NOT_STARTED_STATUSES:
            row_classes.append("row-upcoming")
        elif status_text == "In Progress":
            row_classes.append("row-live")
        elif status_text == "Final":
            row_classes.append("row-final")
        if str(row_data.get("Source") or "").strip() == SOURCE_ACTIVE:
            row_classes.append("row-fallback")
        vsp_avg = _to_float(row_data.get("VsP AVG")) or 0.0
        recent_avg = _to_float(row_data.get("Recent AVG")) or 0.0
        hit_streak = _to_float(row_data.get("Hit Stk")) or 0.0
        if vsp_avg >= 0.350 or (hit_streak >= 5 and recent_avg >= 0.300):
            row_classes.append("row-target")
        elif recent_avg <= 0.220 and vsp_avg < 0.250:
            row_classes.append("row-caution")
        if row_classes:
            row_tag["class"] = row_classes

        cells = row_tag.find_all("td")
        for col_name, semantic_class in semantic_classes.items():
            _add_cell_class(cells, column_map, col_name, semantic_class)
        for col_name, group_class in header_group_map.items():
            _add_cell_class(cells, column_map, col_name, group_class)

        total_class = _classify_total_cell(row_data.get("Total"))
        if total_class:
            _add_cell_class(cells, column_map, "Total", total_class)

        streak_value = _to_float(row_data.get("Hit Stk"))
        if streak_value is not None:
            if streak_value >= 8:
                _add_cell_class(cells, column_map, "Hit Stk", "cell-elite")
            elif streak_value >= 5:
                _add_cell_class(cells, column_map, "Hit Stk", "cell-strong")

        for column_name, thresholds, raw_key in (
            (f"Last {RECENT_GAMES} AVG", (0.330, 0.280, 0.220), "Recent AVG"),
            ("Season AVG", (0.300, 0.270, 0.230), "Season AVG"),
            ("VsP AVG", (0.360, 0.300, 0.220), "VsP AVG"),
        ):
            avg_class = _classify_avg_cell(
                row_data.get(raw_key),
                elite=thresholds[0],
                strong=thresholds[1],
                weak=thresholds[2],
            )
            if avg_class:
                _add_cell_class(cells, column_map, column_name, avg_class)

    return str(table)


def _build_report_tabs(active_tab: str, pitcher_href: str, batter_href: str) -> str:
    tabs = [
        ("pitchers", "Pitchers", pitcher_href),
        ("batters", "Batters", batter_href),
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


def write_html(
    streak_df: pd.DataFrame,
    hot_df: pd.DataFrame,
    matchup_df: pd.DataFrame,
    report_key: str,
    display_date: str,
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    updated_at = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    streak_table = _build_focus_table_html(format_focus_dataframe(streak_df), streak_df)
    hot_table = _build_focus_table_html(format_focus_dataframe(hot_df), hot_df)
    matchup_table = _build_focus_table_html(format_focus_dataframe(matchup_df), matchup_df)
    tabs_html = _build_report_tabs("batters", "__PITCHER_HREF__", "__BATTER_HREF__")
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLB Batter Report {escape(display_date)}</title>
  <style>
    :root {{
      --bg: #eef4f9;
      --panel: #ffffff;
      --header: #edf4fa;
      --text: #0f172a;
      --muted: #475569;
      --line: #d8e2ec;
      --group-context: #0f766e;
      --group-batter: #1d4ed8;
      --group-matchup: #c2410c;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      padding: 18px;
      background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
      color: var(--text);
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
    }}
    .layout {{
      max-width: 1560px;
      margin: 0 auto;
      display: grid;
      gap: 18px;
    }}
    .hero {{
      background: linear-gradient(135deg, #0f766e 0%, #0369a1 100%);
      color: #ffffff;
      border-radius: 14px;
      padding: 22px 24px;
      box-shadow: 0 10px 35px rgba(3, 105, 161, 0.20);
    }}
    .hero h1 {{
      margin: 0 0 8px;
      font-size: 28px;
      letter-spacing: 0.2px;
    }}
    .hero p {{
      margin: 0;
      opacity: 0.95;
      font-size: 13px;
    }}
    .report-tabs {{
      display: inline-flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 14px;
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
    .panel {{
      background: var(--panel);
      border-radius: 14px;
      border: 1px solid var(--line);
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
      overflow: hidden;
    }}
    .panel-legend {{
      padding: 10px 14px;
    }}
    .panel-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 16px 10px;
      border-bottom: 1px solid var(--line);
      background: #fbfdff;
    }}
    .panel-header.section-hot {{
      background: #edf9f2;
      border-bottom-color: #cfe7d7;
    }}
    .panel-header.section-matchup {{
      background: #fff7ea;
      border-bottom-color: #efd9b6;
    }}
    .panel-header.section-streak {{
      background: #eef6ff;
      border-bottom-color: #d5e2f4;
    }}
    .panel-header h2 {{
      margin: 0;
      font-size: 18px;
    }}
    .panel-header .note {{
      color: var(--muted);
      font-size: 12px;
      text-align: right;
    }}
    .table-wrap {{
      overflow-y: auto;
      overflow-x: auto;
      max-height: 78vh;
    }}
    .table-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px 14px;
      align-items: center;
      padding: 8px 12px 0;
      font-size: 11px;
      color: #334155;
    }}
    .page-legend {{
      padding: 0;
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
    .legend-context .legend-swatch {{
      background: color-mix(in srgb, var(--group-context) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-context) 62%, #ffffff);
    }}
    .legend-batter .legend-swatch {{
      background: color-mix(in srgb, var(--group-batter) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-batter) 62%, #ffffff);
    }}
    .legend-matchup .legend-swatch {{
      background: color-mix(in srgb, var(--group-matchup) 22%, #ffffff);
      border-color: color-mix(in srgb, var(--group-matchup) 62%, #ffffff);
    }}
    .legend-note {{
      color: #475569;
      font-size: 11px;
    }}
    table.pitchers-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      font-size: 11px;
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
    table.pitchers-table thead th.group-context {{
      border-top: 4px solid var(--group-context);
      background: color-mix(in srgb, var(--group-context) 10%, var(--header));
    }}
    table.pitchers-table thead th.group-batter {{
      border-top: 4px solid var(--group-batter);
      background: color-mix(in srgb, var(--group-batter) 10%, var(--header));
    }}
    table.pitchers-table thead th.group-matchup {{
      border-top: 4px solid var(--group-matchup);
      background: color-mix(in srgb, var(--group-matchup) 11%, var(--header));
    }}
    table.pitchers-table tbody td {{
      border-bottom: 1px solid var(--line);
      padding: 6px 5px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.12;
      vertical-align: middle;
    }}
    table.pitchers-table tbody td.group-context {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-context) 28%, #ffffff);
    }}
    table.pitchers-table tbody td.group-batter {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-batter) 28%, #ffffff);
    }}
    table.pitchers-table tbody td.group-matchup {{
      box-shadow: inset 2px 0 0 color-mix(in srgb, var(--group-matchup) 30%, #ffffff);
    }}
    table.pitchers-table tbody td.group-context:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-context) 4%, #ffffff);
    }}
    table.pitchers-table tbody td.group-batter:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-batter) 4%, #ffffff);
    }}
    table.pitchers-table tbody td.group-matchup:not(.cell-elite):not(.cell-strong):not(.cell-weak) {{
      background: color-mix(in srgb, var(--group-matchup) 5%, #ffffff);
    }}
    table.pitchers-table tbody tr.row-live td {{
      background-image: linear-gradient(rgba(128, 92, 62, 0.22), rgba(128, 92, 62, 0.22));
      background-blend-mode: multiply;
      color: #5b4637;
    }}
    table.pitchers-table tbody tr.row-final td {{
      background-image: linear-gradient(rgba(71, 85, 105, 0.22), rgba(71, 85, 105, 0.22));
      background-blend-mode: multiply;
      color: #475569;
    }}
    table.pitchers-table tbody tr.row-live td.group-context {{
      box-shadow: inset 3px 0 0 #fb923c;
    }}
    table.pitchers-table tbody tr.row-final td.group-context {{
      box-shadow: inset 3px 0 0 #94a3b8;
    }}
    table.pitchers-table tbody tr.row-live td.cell-elite,
    table.pitchers-table tbody tr.row-live td.cell-strong,
    table.pitchers-table tbody tr.row-live td.cell-weak,
    table.pitchers-table tbody tr.row-final td.cell-elite,
    table.pitchers-table tbody tr.row-final td.cell-strong,
    table.pitchers-table tbody tr.row-final td.cell-weak {{
      color: #374151;
    }}
    table.pitchers-table tbody tr.row-live td .status-pill,
    table.pitchers-table tbody tr.row-final td .status-pill {{
      filter: saturate(0.8) brightness(0.96);
    }}
    table.pitchers-table th.column-name,
    table.pitchers-table td.column-name {{
      min-width: 136px;
      text-align: center;
      white-space: normal;
      overflow-wrap: anywhere;
    }}
    table.pitchers-table th.column-team,
    table.pitchers-table td.column-team {{
      min-width: 68px;
    }}
    table.pitchers-table th.column-opponent,
    table.pitchers-table td.column-opponent {{
      min-width: 84px;
    }}
    table.pitchers-table th.column-pitcher,
    table.pitchers-table td.column-pitcher {{
      min-width: 76px;
      max-width: 84px;
      padding-left: 3px;
      padding-right: 3px;
      text-align: center;
      white-space: nowrap;
      overflow-wrap: normal;
    }}
    table.pitchers-table th.column-total,
    table.pitchers-table td.column-total {{
      min-width: 54px;
      max-width: 62px;
      padding-left: 3px;
      padding-right: 3px;
    }}
    table.pitchers-table th.column-status,
    table.pitchers-table td.column-status {{
      min-width: 78px;
    }}
    table.pitchers-table tbody tr:nth-child(even) {{
      background: #f8fbff;
    }}
    table.pitchers-table tbody tr:hover {{
      background: #eef7ff;
    }}
    table.pitchers-table tbody tr.row-target {{
      box-shadow: inset 4px 0 0 #16a34a;
    }}
    table.pitchers-table tbody tr.row-caution {{
      box-shadow: inset 4px 0 0 #dc2626;
    }}
    table.pitchers-table tbody tr.row-fallback {{
      box-shadow: inset 4px 0 0 #94a3b8;
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
    .batter-name,
    .pitcher-name {{
      font-weight: 600;
      color: #0f172a;
    }}
    .batter-name {{
      display: block;
      text-align: center;
    }}
    .pitcher-name {{
      font-size: 12px;
      letter-spacing: -0.01em;
    }}
    .team-cell,
    .opp-cell {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 5px;
      line-height: 1.08;
      text-align: center;
    }}
    .opp-cell {{
      flex-wrap: nowrap;
    }}
    .team-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 30px;
      height: 30px;
      border-radius: 6px;
      background: #ffffff;
      box-shadow: inset 0 0 0 1px #e2e8f0;
      flex: 0 0 auto;
    }}
    .team-logo {{
      display: block;
      width: 26px;
      height: 26px;
      object-fit: contain;
    }}
    .team-abbrev {{
      font-weight: 700;
      letter-spacing: 0.03em;
      color: #0f172a;
    }}
    .opp-team {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 5px;
    }}
    .opp-time {{
      display: inline-block;
      padding: 1px 6px;
      border-radius: 999px;
      background: #f1f5f9;
      color: #334155;
      font-size: 9px;
      font-weight: 700;
      letter-spacing: 0.01em;
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
    .empty-state {{
      margin: 0;
      padding: 18px;
      color: var(--muted);
      font-size: 14px;
    }}
    @media (max-width: 900px) {{
      body {{
        padding: 12px;
      }}
      .hero {{
        padding: 18px;
      }}
      .hero h1 {{
        font-size: 23px;
      }}
      .panel-header {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .panel-header .note {{
        text-align: left;
      }}
      table.pitchers-table {{
        min-width: 860px;
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <section class="hero">
      <h1>MLB Daily Batter Report</h1>
      <p>{escape(display_date)} slate. Updated {escape(updated_at)}.</p>
      {tabs_html}
    </section>

    <section class="panel panel-legend">
      <div class="table-legend page-legend">
        <span class="legend-item legend-context"><span class="legend-swatch"></span>Context</span>
        <span class="legend-item legend-batter"><span class="legend-swatch"></span>Form</span>
        <span class="legend-item legend-matchup"><span class="legend-swatch"></span>Matchup</span>
        <span class="legend-note">Gray rail = fallback pool. Orange/gray rows = started or final. Green/yellow/red = stronger to weaker totals or AVG.</span>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header section-hot">
        <h2>Hot Streaks With Pitcher History</h2>
        <div class="note">{STREAK_SECTION_MIN}+ hit streak, {MATCHUP_MIN_PA}+ PA vs pitcher.</div>
      </div>
      <div class="table-wrap">
        {hot_table}
      </div>
    </section>

    <section class="panel">
      <div class="panel-header section-matchup">
        <h2>Good Historical Matchups</h2>
        <div class="note">Best direct AVG vs scheduled pitcher.</div>
      </div>
      <div class="table-wrap">
        {matchup_table}
      </div>
    </section>

    <section class="panel">
      <div class="panel-header section-streak">
        <h2>Active Hit Streaks 6+ Games</h2>
        <div class="note">{ACTIVE_STREAK_SECTION_MIN}+ active hit streak.</div>
      </div>
      <div class="table-wrap">
        {streak_table}
      </div>
    </section>
  </div>
</body>
</html>
"""

    output_path = REPORTS_DIR / f"batters-report-{report_key}.html"
    archive_html_content = html_content.replace("__PITCHER_HREF__", "../index.html").replace("__BATTER_HREF__", "../batters.html")
    root_html_content = html_content.replace("__PITCHER_HREF__", "./index.html").replace("__BATTER_HREF__", "./batters.html")
    output_path.write_text(archive_html_content, encoding="utf-8")
    ROOT_BATTERS_FILE.write_text(root_html_content, encoding="utf-8")
    print(output_path.resolve().as_uri())
    return output_path


def build_report_rows(schedule: Sequence[Dict[str, Any]], report_date: str) -> List[Dict[str, Any]]:
    report_date_obj = dt.datetime.strptime(report_date, "%m/%d/%Y").date()
    report_year = report_date_obj.year
    espn_event_lookup = build_espn_event_lookup(report_date)
    team_ids = {
        int(game[side])
        for game in schedule
        for side in ("away_id", "home_id")
        if game.get(side) is not None
    }
    team_meta_map = {team_id: fetch_team_meta(team_id) for team_id in team_ids}

    rows: List[Dict[str, Any]] = []
    for game in schedule:
        away_team = str(game.get("away_name") or "").strip()
        home_team = str(game.get("home_name") or "").strip()
        away_team_id = _to_int(game.get("away_id"))
        home_team_id = _to_int(game.get("home_id"))
        if away_team_id is None or home_team_id is None:
            continue

        start_time = _format_local_start_time(game.get("game_datetime"))
        status = str(game.get("status") or "").strip()
        event_id = espn_event_lookup.get((_normalize_team_name(away_team), _normalize_team_name(home_team)), "")
        espn_summary = fetch_espn_summary(event_id) if event_id else None
        game_total = extract_espn_game_total(espn_summary)
        offense_configs = [
            {
                "team_id": away_team_id,
                "team_name": away_team,
                "team_abbrev": str((team_meta_map.get(away_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "opponent_id": home_team_id,
                "opponent_name": home_team,
                "opponent_abbrev": str((team_meta_map.get(home_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "pitcher_name": str(game.get("home_probable_pitcher") or "").strip(),
            },
            {
                "team_id": home_team_id,
                "team_name": home_team,
                "team_abbrev": str((team_meta_map.get(home_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "opponent_id": away_team_id,
                "opponent_name": away_team,
                "opponent_abbrev": str((team_meta_map.get(away_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "pitcher_name": str(game.get("away_probable_pitcher") or "").strip(),
            },
        ]

        for offense in offense_configs:
            pitcher_name = offense["pitcher_name"]
            if not pitcher_name:
                continue

            pitcher_context = fetch_pitcher_context(pitcher_name)
            if not pitcher_context or not pitcher_context.get("id"):
                continue

            team_id = int(offense["team_id"])
            team_name = str(offense["team_name"])
            opponent_name = str(offense["opponent_name"])
            team_meta = team_meta_map[team_id]
            team_abbrev = str(team_meta.get("abbreviation") or "").strip().upper()
            roster_entries = filter_active_hitters(fetch_team_roster(team_id))
            if not roster_entries:
                continue

            person_ids = [
                int((entry.get("person") or {}).get("id"))
                for entry in roster_entries
                if (entry.get("person") or {}).get("id") is not None
            ]
            people_by_id = fetch_people_stats_map(
                person_ids,
                season=report_year,
                pitch_hand=pitcher_context.get("hand"),
                pitcher_id=int(pitcher_context["id"]),
            )
            candidate_rows = build_candidate_rows(
                team_id=team_id,
                team_name=team_name,
                team_abbrev=str(offense["team_abbrev"] or ""),
                opponent_id=int(offense["opponent_id"]),
                opponent_name=opponent_name,
                opponent_abbrev=str(offense["opponent_abbrev"] or ""),
                pitcher_name=str(pitcher_context.get("name") or pitcher_name),
                game_total=game_total,
                pitch_hand=str(pitcher_context.get("hand") or ""),
                start_time=start_time,
                status=status,
                roster_entries=roster_entries,
                people_by_id=people_by_id,
                report_date=report_date_obj,
            )
            if not candidate_rows:
                continue

            lineup_entries = extract_confirmed_espn_lineup(espn_summary, team_abbrev) if espn_summary and team_abbrev else []
            lineup_player_ids = resolve_lineup_player_ids(lineup_entries, roster_entries, team_id) if lineup_entries else []
            selected_rows = select_offense_rows(candidate_rows, lineup_player_ids)
            rows.extend(selected_rows)

    return rows


def main(raw_date_input: str) -> None:
    report_date = resolve_date_input(raw_date_input)
    report_date, schedule = resolve_effective_report_date_and_schedule(report_date)
    report_key = report_date.replace("/", "")
    rows = build_report_rows(schedule, report_date)
    final_df = sort_batters_for_report(apply_hot_scores(rows))
    streak_df = build_active_hit_streak_section(final_df)
    hot_df = build_hot_streak_matchup_section(final_df)
    matchup_df = build_good_matchups_section(final_df, hot_df)
    write_html(streak_df, hot_df, matchup_df, report_key, report_date)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 Batters.py <today|tmrw|MM/DD|MM/DD/YYYY>")
        sys.exit(1)

    main(str(sys.argv[1]))
