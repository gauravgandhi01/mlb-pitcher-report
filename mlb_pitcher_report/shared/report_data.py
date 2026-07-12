from __future__ import annotations

import datetime as dt
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import statsapi
from unidecode import unidecode

SCHEDULE_STATUSES = {"Pre-Game", "Scheduled", "Warmup", "Final", "In Progress"}
NOT_STARTED_STATUSES = {"Pre-Game", "Scheduled", "Warmup"}
REQUEST_TIMEOUT_SECONDS = 20

TEAM_META_CACHE: Dict[int, Dict[str, Any]] = {}
TEAM_ROSTER_CACHE: Dict[int, List[Dict[str, Any]]] = {}
PITCHER_LOOKUP_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
ESPN_SUMMARY_CACHE: Dict[str, Optional[Dict[str, Any]]] = {}
LINEUP_NAME_LOOKUP_CACHE: Dict[Tuple[int, str], Optional[int]] = {}
LAST_GAME_LINEUP_CACHE: Dict[int, List[int]] = {}
TEAM_HAND_SPLIT_CACHE: Dict[Tuple[int, int], Dict[str, Any]] = {}
MLB_TEAM_IDS_CACHE: Dict[int, List[int]] = {}
TEAM_HAND_RANK_CACHE: Dict[Tuple[int, str], Dict[int, Dict[str, int]]] = {}
PITCHER_SEASON_RANK_CACHE: Dict[Tuple[int, int], Dict[int, Dict[str, int]]] = {}
PARK_WEATHER_CACHE: Dict[Tuple[int, str], Optional[Dict[str, Any]]] = {}
GAME_BVP_LINE_CACHE: Dict[Tuple[int, int], Dict[int, Dict[str, Any]]] = {}
GAME_BATTER_LINE_CACHE: Dict[int, Dict[int, Dict[str, Any]]] = {}
PITCHER_DEBUT_YEAR_CACHE: Dict[int, int] = {}
PITCHER_GAME_LOG_CACHE: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
BATTER_GAME_LOG_CACHE: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
PITCHER_HISTORICAL_BVP_CACHE: Dict[Tuple[int, dt.date], Dict[int, Dict[str, Any]]] = {}

MLB_PARK_METADATA: Dict[int, Dict[str, Any]] = {
    1: {"name": "Angel Stadium", "lat": 33.8003, "lon": -117.8827, "roof_type": "open"},
    2: {"name": "Oriole Park at Camden Yards", "lat": 39.2839, "lon": -76.6217, "roof_type": "open"},
    3: {"name": "Fenway Park", "lat": 42.3467, "lon": -71.0972, "roof_type": "open"},
    4: {"name": "Rate Field", "lat": 41.8299, "lon": -87.6338, "roof_type": "open"},
    5: {"name": "Progressive Field", "lat": 41.4962, "lon": -81.6852, "roof_type": "open"},
    7: {"name": "Kauffman Stadium", "lat": 39.0517, "lon": -94.4803, "roof_type": "open"},
    12: {"name": "Tropicana Field", "lat": 27.7682, "lon": -82.6534, "roof_type": "indoor"},
    14: {"name": "Rogers Centre", "lat": 43.6414, "lon": -79.3894, "roof_type": "retractable"},
    15: {"name": "Chase Field", "lat": 33.4453, "lon": -112.0667, "roof_type": "retractable"},
    17: {"name": "Wrigley Field", "lat": 41.9484, "lon": -87.6553, "roof_type": "open"},
    19: {"name": "Coors Field", "lat": 39.7559, "lon": -104.9942, "roof_type": "open"},
    22: {"name": "UNIQLO Field at Dodger Stadium", "lat": 34.0739, "lon": -118.24, "roof_type": "open"},
    31: {"name": "PNC Park", "lat": 40.4469, "lon": -80.0057, "roof_type": "open"},
    32: {"name": "American Family Field", "lat": 43.0280, "lon": -87.9712, "roof_type": "retractable"},
    680: {"name": "T-Mobile Park", "lat": 47.5914, "lon": -122.3325, "roof_type": "retractable"},
    2392: {"name": "Daikin Park", "lat": 29.7572, "lon": -95.3552, "roof_type": "retractable"},
    2394: {"name": "Comerica Park", "lat": 42.3390, "lon": -83.0485, "roof_type": "open"},
    2395: {"name": "Oracle Park", "lat": 37.7786, "lon": -122.3893, "roof_type": "open"},
    2529: {"name": "Sutter Health Park", "lat": 38.5802, "lon": -121.5136, "roof_type": "open"},
    2602: {"name": "Great American Ball Park", "lat": 39.0979, "lon": -84.5082, "roof_type": "open"},
    2680: {"name": "Petco Park", "lat": 32.7073, "lon": -117.1566, "roof_type": "open"},
    2681: {"name": "Citizens Bank Park", "lat": 39.9057, "lon": -75.1665, "roof_type": "open"},
    2889: {"name": "Busch Stadium", "lat": 38.6226, "lon": -90.1928, "roof_type": "open"},
    3289: {"name": "Citi Field", "lat": 40.7571, "lon": -73.8458, "roof_type": "open"},
    3309: {"name": "Nationals Park", "lat": 38.8729, "lon": -77.0074, "roof_type": "open"},
    3312: {"name": "Target Field", "lat": 44.9817, "lon": -93.2776, "roof_type": "open"},
    3313: {"name": "Yankee Stadium", "lat": 40.8296, "lon": -73.9262, "roof_type": "open"},
    4169: {"name": "loanDepot park", "lat": 25.7781, "lon": -80.2197, "roof_type": "retractable"},
    4705: {"name": "Truist Park", "lat": 33.8907, "lon": -84.4677, "roof_type": "open"},
    5325: {"name": "Globe Life Field", "lat": 32.7473, "lon": -97.0847, "roof_type": "retractable"},
}


def normalize_person_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


def normalize_team_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


def choose_best_player_match(players: List[Dict[str, Any]], player_name: str) -> Optional[Dict[str, Any]]:
    if not players:
        return None

    target = normalize_person_name(player_name)
    if not target:
        return players[0]

    exact_full = [player for player in players if normalize_person_name(player.get("fullName")) == target]
    if exact_full:
        return exact_full[0]

    exact_first_last = [
        player
        for player in players
        if normalize_person_name(player.get("firstLastName")) == target
        or normalize_person_name(player.get("nameFirstLast")) == target
    ]
    if exact_first_last:
        return exact_first_last[0]

    return players[0]


def to_float(value: Any) -> Optional[float]:
    numeric = pd.to_numeric(value, errors="coerce")
    if pd.isna(numeric):
        return None
    return float(numeric)


def to_int(value: Any) -> Optional[int]:
    numeric = to_float(value)
    if numeric is None:
        return None
    return int(numeric)


def parse_date(value: Any) -> Optional[dt.date]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return dt.datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None


def safe_ratio(numerator: float, denominator: float) -> Optional[float]:
    if denominator <= 0:
        return None
    return numerator / denominator


def format_local_start_time(game_datetime: Any) -> str:
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
    if not next_schedule:
        print(
            "\033[93mNo games remain in a not-started state on "
            f"{report_date}, and no reportable games were found on {next_report_date}. "
            "Keeping the current slate.\033[0m"
        )
        return report_date, schedule

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


def get_park_metadata(venue_id: Any) -> Optional[Dict[str, Any]]:
    venue_id_value = to_int(venue_id)
    if venue_id_value is None:
        return None
    metadata = MLB_PARK_METADATA.get(venue_id_value)
    return dict(metadata) if metadata else None


def _wind_direction_cardinal(degrees: Any) -> Optional[str]:
    numeric = to_float(degrees)
    if numeric is None:
        return None
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    index = int(((numeric % 360.0) + 22.5) // 45.0) % len(directions)
    return directions[index]


def extract_open_meteo_hourly_park_context(
    payload: Optional[Dict[str, Any]],
    game_datetime: Any,
) -> Optional[Dict[str, Any]]:
    if not payload or not game_datetime:
        return None

    timezone_name = str(payload.get("timezone") or "").strip() or "UTC"
    try:
        venue_timezone = ZoneInfo(timezone_name)
    except Exception:
        venue_timezone = ZoneInfo("UTC")

    try:
        target_dt = dt.datetime.fromisoformat(str(game_datetime).replace("Z", "+00:00")).astimezone(venue_timezone)
    except ValueError:
        return None

    hourly = payload.get("hourly") or {}
    times = hourly.get("time") or []
    temps = hourly.get("temperature_2m") or []
    wind_speeds = hourly.get("wind_speed_10m") or []
    wind_directions = hourly.get("wind_direction_10m") or []
    precip_probs = hourly.get("precipitation_probability") or []

    best_index = None
    best_delta = None
    for index, time_text in enumerate(times):
        try:
            local_dt = dt.datetime.fromisoformat(str(time_text)).replace(tzinfo=venue_timezone)
        except ValueError:
            continue
        delta_seconds = abs((local_dt - target_dt).total_seconds())
        if best_delta is None or delta_seconds < best_delta:
            best_index = index
            best_delta = delta_seconds

    if best_index is None:
        return None

    return {
        "temp_f": to_float(temps[best_index]) if best_index < len(temps) else None,
        "wind_mph": to_float(wind_speeds[best_index]) if best_index < len(wind_speeds) else None,
        "wind_dir": _wind_direction_cardinal(wind_directions[best_index]) if best_index < len(wind_directions) else None,
        "precip_pct": to_float(precip_probs[best_index]) if best_index < len(precip_probs) else None,
        "source": "Open-Meteo",
    }


def _fetch_open_meteo_hourly_payload(venue_id: int, report_date: str) -> Optional[Dict[str, Any]]:
    cache_key = (venue_id, report_date)
    if cache_key in PARK_WEATHER_CACHE:
        return PARK_WEATHER_CACHE[cache_key]

    metadata = get_park_metadata(venue_id)
    if not metadata:
        PARK_WEATHER_CACHE[cache_key] = None
        return None

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": metadata["lat"],
        "longitude": metadata["lon"],
        "hourly": "temperature_2m,precipitation_probability,wind_speed_10m,wind_direction_10m",
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "timezone": "auto",
        "start_date": dt.datetime.strptime(report_date, "%m/%d/%Y").strftime("%Y-%m-%d"),
        "end_date": dt.datetime.strptime(report_date, "%m/%d/%Y").strftime("%Y-%m-%d"),
    }
    try:
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError):
        payload = None

    PARK_WEATHER_CACHE[cache_key] = payload
    return payload


def fetch_park_context(venue_id: Any, game_datetime: Any, report_date: str) -> Optional[Dict[str, Any]]:
    metadata = get_park_metadata(venue_id)
    if not metadata:
        return None

    context = {
        "roof_type": str(metadata.get("roof_type") or "").strip() or None,
        "temp_f": None,
        "wind_mph": None,
        "wind_dir": None,
        "precip_pct": None,
        "source": "Static",
    }
    if context["roof_type"] in {"indoor", "retractable"}:
        return context

    payload = _fetch_open_meteo_hourly_payload(int(venue_id), report_date)
    weather = extract_open_meteo_hourly_park_context(payload, game_datetime)
    if not weather:
        return context

    context.update(weather)
    return context


def fetch_pitcher_context(pitcher_name: str) -> Optional[Dict[str, Any]]:
    key = normalize_person_name(pitcher_name)
    if key in PITCHER_LOOKUP_CACHE:
        return PITCHER_LOOKUP_CACHE[key]

    players = statsapi.lookup_player(pitcher_name)
    player = choose_best_player_match(players or [], pitcher_name)
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


def extract_espn_scoreboard_snapshot(event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    event_id = str(event.get("id") or "").strip()
    competition = (event.get("competitions") or [{}])[0]
    competitors = competition.get("competitors") or []
    status = competition.get("status") or event.get("status") or {}
    status_type = status.get("type") or {}
    away_name = ""
    home_name = ""
    away_score = None
    home_score = None

    for competitor in competitors:
        team = competitor.get("team") or {}
        display_name = str(team.get("displayName") or "").strip()
        home_away = str(competitor.get("homeAway") or "").strip().lower()
        score = to_int(competitor.get("score"))
        if home_away == "away":
            away_name = display_name
            away_score = score
        elif home_away == "home":
            home_name = display_name
            home_score = score

    if not event_id or not away_name or not home_name:
        return None

    detail = str(status_type.get("detail") or status.get("detail") or "").strip() or None
    short_detail = str(status_type.get("shortDetail") or status.get("shortDetail") or "").strip() or None
    return {
        "event_id": event_id,
        "away_name": away_name,
        "home_name": home_name,
        "away_score": away_score,
        "home_score": home_score,
        "status_state": str(status_type.get("state") or "").strip().lower() or None,
        "status_detail": detail,
        "status_short_detail": short_detail,
    }


def build_espn_event_snapshot_lookup(date: str) -> Dict[Tuple[str, str], Dict[str, Any]]:
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for event in _fetch_espn_scoreboard_events(date):
        snapshot = extract_espn_scoreboard_snapshot(event)
        if not snapshot:
            continue
        lookup[
            (
                normalize_team_name(snapshot["away_name"]),
                normalize_team_name(snapshot["home_name"]),
            )
        ] = snapshot
    return lookup


def build_espn_event_lookup(date: str) -> Dict[Tuple[str, str], str]:
    lookup: Dict[Tuple[str, str], str] = {}
    for key, snapshot in build_espn_event_snapshot_lookup(date).items():
        event_id = str(snapshot.get("event_id") or "").strip()
        if event_id:
            lookup[key] = event_id
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


def extract_espn_odds(summary_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    empty = {
        "provider": None,
        "details": None,
        "total": None,
        "spread": None,
        "over_odds": None,
        "under_odds": None,
        "away_moneyline": None,
        "home_moneyline": None,
    }
    if not summary_data:
        return empty

    for key in ("pickcenter", "odds"):
        for entry in summary_data.get(key) or []:
            provider = entry.get("provider") or {}
            away_team_odds = entry.get("awayTeamOdds") or {}
            home_team_odds = entry.get("homeTeamOdds") or {}
            total = to_float(entry.get("overUnder"))
            away_moneyline = to_int(away_team_odds.get("moneyLine"))
            home_moneyline = to_int(home_team_odds.get("moneyLine"))
            if (
                total is None
                and away_moneyline is None
                and home_moneyline is None
                and to_float(entry.get("spread")) is None
            ):
                continue
            return {
                "provider": str(provider.get("name") or "").strip() or None,
                "details": str(entry.get("details") or "").strip() or None,
                "total": total,
                "spread": to_float(entry.get("spread")),
                "over_odds": to_int(entry.get("overOdds")),
                "under_odds": to_int(entry.get("underOdds")),
                "away_moneyline": away_moneyline,
                "home_moneyline": home_moneyline,
            }
    return empty


def extract_espn_game_total(summary_data: Optional[Dict[str, Any]]) -> Optional[float]:
    return extract_espn_odds(summary_data).get("total")


def extract_confirmed_espn_lineup(summary_data: Dict[str, Any], team_abbrev: str) -> List[Dict[str, Any]]:
    target = str(team_abbrev or "").strip().upper()
    for roster_block in summary_data.get("rosters") or []:
        roster_team = roster_block.get("team") or {}
        if str(roster_team.get("abbreviation") or "").strip().upper() != target:
            continue

        roster = roster_block.get("roster") or []
        ordered = [player for player in roster if to_int(player.get("batOrder")) is not None]
        ordered = sorted(ordered, key=lambda player: to_int(player.get("batOrder")) or 999)
        starters = [player for player in ordered if player.get("starter")]
        lineup = starters if len(starters) >= 9 else ordered
        if len(lineup) < 9:
            return []
        return [
            {
                "name": str((entry.get("athlete") or {}).get("fullName", "")).strip(),
                "order": int(to_int(entry.get("batOrder")) or 0),
            }
            for entry in lineup[:9]
            if str((entry.get("athlete") or {}).get("fullName", "")).strip()
        ]
    return []


def resolve_lineup_player_ids(
    lineup_entries: Sequence[Dict[str, Any]],
    roster_entries: Sequence[Dict[str, Any]],
    team_id: int,
) -> List[int]:
    roster_name_lookup = {
        normalize_person_name((entry.get("person") or {}).get("fullName")): int((entry.get("person") or {}).get("id"))
        for entry in roster_entries
        if (entry.get("person") or {}).get("fullName") and (entry.get("person") or {}).get("id")
    }
    resolved_ids: List[int] = []
    for lineup_entry in lineup_entries:
        player_name = str(lineup_entry.get("name") or "").strip()
        cache_key = (team_id, normalize_person_name(player_name))
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


def extract_last_game_lineup_player_ids_from_boxscore(boxscore_data: Dict[str, Any], team_id: int) -> List[int]:
    for side in ("home", "away"):
        team_block = boxscore_data.get(side) or {}
        team = team_block.get("team") or {}
        if to_int(team.get("id")) != int(team_id):
            continue

        batting_order = team_block.get("battingOrder") or []
        player_ids = [int(player_id) for player_id in batting_order if to_int(player_id) is not None]
        return player_ids[:9] if len(player_ids) >= 9 else []
    return []


def fetch_last_game_lineup_player_ids(team_id: int) -> List[int]:
    cached = LAST_GAME_LINEUP_CACHE.get(team_id)
    if cached is not None:
        return cached

    try:
        game_id = statsapi.last_game(team_id)
        if not game_id:
            LAST_GAME_LINEUP_CACHE[team_id] = []
            return []
        boxscore_data = statsapi.boxscore_data(game_id)
        lineup_ids = extract_last_game_lineup_player_ids_from_boxscore(boxscore_data, team_id)
    except Exception:
        lineup_ids = []

    LAST_GAME_LINEUP_CACHE[team_id] = lineup_ids
    return lineup_ids


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


def chunked(values: Sequence[int], size: int) -> Iterable[Sequence[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_people_stats_map(
    person_ids: Sequence[int],
    season: int,
    pitch_hand: Optional[str],
    pitcher_id: Optional[int],
    stats_end_date: Optional[dt.date] = None,
) -> Dict[int, Dict[str, Any]]:
    if not person_ids:
        return {}

    sit_code = None
    if str(pitch_hand or "").upper() == "L":
        sit_code = "vl"
    elif str(pitch_hand or "").upper() == "R":
        sit_code = "vr"

    hydrate_parts = ["season", "gameLog", "statSplits"]
    hydrate_tail = f",season={season}"
    if stats_end_date is not None:
        hydrate_tail = f"{hydrate_tail},endDate={stats_end_date.strftime('%Y-%m-%d')}"
    if sit_code:
        hydrate_tail = f",sitCodes=[{sit_code}]{hydrate_tail}"
    if pitcher_id:
        hydrate_parts.append("vsPlayer")
        hydrate_tail = f"{hydrate_tail},opposingPlayerId={pitcher_id}"
    hydrate = f"stats(group=[hitting],type=[{','.join(hydrate_parts)}]{hydrate_tail})"

    people_by_id: Dict[int, Dict[str, Any]] = {}
    for chunk in chunked(list(dict.fromkeys(int(person_id) for person_id in person_ids)), 8):
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
            game_date = parse_date(split.get("date"))
            if game_date is None:
                continue
            stat = split.get("stat") or {}
            game_info = split.get("game") or {}
            logs.append(
                {
                    "date": game_date,
                    "gamePk": to_int(game_info.get("gamePk")) or 0,
                    "atBats": to_int(stat.get("atBats")) or 0,
                    "hits": to_int(stat.get("hits")) or 0,
                    "walks": to_int(stat.get("baseOnBalls")) or 0,
                    "hitByPitch": to_int(stat.get("hitByPitch")) or 0,
                    "sacFlies": to_int(stat.get("sacFlies")) or 0,
                    "plateAppearances": to_int(stat.get("plateAppearances")) or 0,
                    "totalBases": to_int(stat.get("totalBases")) or 0,
                    "strikeOuts": to_int(stat.get("strikeOuts")) or 0,
                    "homeRuns": to_int(stat.get("homeRuns")) or 0,
                    "rbi": to_int(stat.get("rbi")) or 0,
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

    return aggregate_stat_lines([totals], preserve_games=len(filtered))


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


def aggregate_stat_lines(
    lines: Sequence[Dict[str, Any]],
    *,
    preserve_games: Optional[int] = None,
) -> Dict[str, Any]:
    totals = {
        "games": preserve_games or 0,
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
    for line in lines:
        totals["games"] += int(line.get("games") or 0) if preserve_games is None else 0
        totals["PA"] += int(line.get("PA") or 0)
        totals["AB"] += int(line.get("AB") or 0)
        totals["H"] += int(line.get("H") or 0)
        totals["BB"] += int(line.get("BB") or 0)
        totals["HBP"] += int(line.get("HBP") or 0)
        totals["SF"] += int(line.get("SF") or 0)
        totals["TB"] += int(line.get("TB") or 0)
        totals["K"] += int(line.get("K") or 0)
        totals["HR"] += int(line.get("HR") or 0)
        totals["RBI"] += int(line.get("RBI") or 0)

    obp_denom = totals["AB"] + totals["BB"] + totals["HBP"] + totals["SF"]
    avg = safe_ratio(totals["H"], totals["AB"])
    obp = safe_ratio(totals["H"] + totals["BB"] + totals["HBP"], obp_denom)
    slg = safe_ratio(totals["TB"], totals["AB"])
    ops = None if obp is None or slg is None else obp + slg
    k_pct = safe_ratio(totals["K"] * 100.0, totals["PA"])
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


def _stat_line_from_values(
    *,
    pa: int = 0,
    ab: int = 0,
    hits: int = 0,
    walks: int = 0,
    hit_by_pitch: int = 0,
    sac_flies: int = 0,
    total_bases: int = 0,
    strike_outs: int = 0,
    home_runs: int = 0,
    rbi: int = 0,
) -> Dict[str, int]:
    return {
        "PA": max(pa, 0),
        "AB": max(ab, 0),
        "H": max(hits, 0),
        "BB": max(walks, 0),
        "HBP": max(hit_by_pitch, 0),
        "SF": max(sac_flies, 0),
        "TB": max(total_bases, 0),
        "K": max(strike_outs, 0),
        "HR": max(home_runs, 0),
        "RBI": max(rbi, 0),
    }


def _stat_line_from_split_stat(stat: Dict[str, Any]) -> Dict[str, int]:
    return _stat_line_from_values(
        pa=to_int(stat.get("plateAppearances")) or 0,
        ab=to_int(stat.get("atBats")) or 0,
        hits=to_int(stat.get("hits")) or 0,
        walks=to_int(stat.get("baseOnBalls")) or 0,
        hit_by_pitch=to_int(stat.get("hitByPitch")) or 0,
        sac_flies=to_int(stat.get("sacFlies")) or 0,
        total_bases=to_int(stat.get("totalBases")) or 0,
        strike_outs=to_int(stat.get("strikeOuts")) or 0,
        home_runs=to_int(stat.get("homeRuns")) or 0,
        rbi=to_int(stat.get("rbi")) or 0,
    )


def _subtract_stat_line(base_line: Dict[str, Any], adjustment_line: Optional[Dict[str, Any]]) -> Dict[str, int]:
    if not adjustment_line:
        return _stat_line_from_values(
            pa=to_int(base_line.get("PA")) or 0,
            ab=to_int(base_line.get("AB")) or 0,
            hits=to_int(base_line.get("H")) or 0,
            walks=to_int(base_line.get("BB")) or 0,
            hit_by_pitch=to_int(base_line.get("HBP")) or 0,
            sac_flies=to_int(base_line.get("SF")) or 0,
            total_bases=to_int(base_line.get("TB")) or 0,
            strike_outs=to_int(base_line.get("K")) or 0,
            home_runs=to_int(base_line.get("HR")) or 0,
            rbi=to_int(base_line.get("RBI")) or 0,
        )

    return _stat_line_from_values(
        pa=(to_int(base_line.get("PA")) or 0) - (to_int(adjustment_line.get("PA")) or 0),
        ab=(to_int(base_line.get("AB")) or 0) - (to_int(adjustment_line.get("AB")) or 0),
        hits=(to_int(base_line.get("H")) or 0) - (to_int(adjustment_line.get("H")) or 0),
        walks=(to_int(base_line.get("BB")) or 0) - (to_int(adjustment_line.get("BB")) or 0),
        hit_by_pitch=(to_int(base_line.get("HBP")) or 0) - (to_int(adjustment_line.get("HBP")) or 0),
        sac_flies=(to_int(base_line.get("SF")) or 0) - (to_int(adjustment_line.get("SF")) or 0),
        total_bases=(to_int(base_line.get("TB")) or 0) - (to_int(adjustment_line.get("TB")) or 0),
        strike_outs=(to_int(base_line.get("K")) or 0) - (to_int(adjustment_line.get("K")) or 0),
        home_runs=(to_int(base_line.get("HR")) or 0) - (to_int(adjustment_line.get("HR")) or 0),
        rbi=(to_int(base_line.get("RBI")) or 0) - (to_int(adjustment_line.get("RBI")) or 0),
    )


def _nested_person_id(split: Dict[str, Any], key: str) -> Optional[int]:
    return to_int((split.get(key) or {}).get("id"))


def _split_matches_batter_pitcher(
    split: Dict[str, Any],
    *,
    batter_id: Optional[int] = None,
    pitcher_id: Optional[int] = None,
) -> bool:
    if batter_id is not None:
        split_batter_id = _nested_person_id(split, "batter")
        if split_batter_id is None:
            split_batter_id = _nested_person_id(split, "player")
        if split_batter_id != int(batter_id):
            return False

    if pitcher_id is not None:
        split_pitcher_id = _nested_person_id(split, "pitcher")
        if split_pitcher_id is not None and split_pitcher_id != int(pitcher_id):
            return False

    return True


def extract_batter_vs_pitcher_stat_lines_from_plays(
    plays: Sequence[Dict[str, Any]],
    pitcher_id: int,
) -> Dict[int, Dict[str, Any]]:
    stat_lines: Dict[int, Dict[str, Any]] = {}
    non_at_bat_events = {"walk", "intent_walk", "hit_by_pitch", "sac_fly", "sac_bunt", "catcher_interf"}
    hit_event_bases = {"single": 1, "double": 2, "triple": 3, "home_run": 4}
    strikeout_events = {"strikeout", "strikeout_double_play"}

    for play in plays:
        if str((play.get("result") or {}).get("type") or "").strip() != "atBat":
            continue
        matchup = play.get("matchup") or {}
        batter_id = to_int((matchup.get("batter") or {}).get("id"))
        play_pitcher_id = to_int((matchup.get("pitcher") or {}).get("id"))
        if batter_id is None or play_pitcher_id != int(pitcher_id):
            continue

        event_type = str((play.get("result") or {}).get("eventType") or "").strip().lower()
        if not event_type:
            continue

        line = stat_lines.setdefault(batter_id, _stat_line_from_values())
        line["PA"] += 1
        if event_type not in non_at_bat_events:
            line["AB"] += 1

        bases = hit_event_bases.get(event_type, 0)
        if bases > 0:
            line["H"] += 1
            line["TB"] += bases
            if event_type == "home_run":
                line["HR"] += 1
        if event_type in {"walk", "intent_walk"}:
            line["BB"] += 1
        if event_type == "hit_by_pitch":
            line["HBP"] += 1
        if event_type == "sac_fly":
            line["SF"] += 1
        if event_type in strikeout_events:
            line["K"] += 1

        line["RBI"] += to_int((play.get("result") or {}).get("rbi")) or 0

    return stat_lines


def fetch_game_batter_vs_pitcher_stat_lines(game_id: int, pitcher_id: int) -> Dict[int, Dict[str, Any]]:
    cache_key = (int(game_id), int(pitcher_id))
    cached = GAME_BVP_LINE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        payload = statsapi.get("game_playByPlay", {"gamePk": int(game_id)})
    except Exception:
        payload = {}

    stat_lines = extract_batter_vs_pitcher_stat_lines_from_plays(payload.get("allPlays") or [], int(pitcher_id))
    GAME_BVP_LINE_CACHE[cache_key] = stat_lines
    return stat_lines


def fetch_pitcher_debut_year(pitcher_id: int, fallback_year: int) -> int:
    pitcher_id = int(pitcher_id)
    cached = PITCHER_DEBUT_YEAR_CACHE.get(pitcher_id)
    if cached is not None:
        return cached

    debut_year = int(fallback_year)
    try:
        payload = statsapi.get("people", {"personIds": pitcher_id}, force=True)
    except Exception:
        payload = {}

    people = payload.get("people") or []
    if people:
        debut_date = parse_date(people[0].get("mlbDebutDate"))
        if debut_date is not None:
            debut_year = min(debut_date.year, int(fallback_year))

    PITCHER_DEBUT_YEAR_CACHE[pitcher_id] = debut_year
    return debut_year


def fetch_pitcher_game_log_splits(pitcher_id: int, season: int) -> List[Dict[str, Any]]:
    cache_key = (int(pitcher_id), int(season))
    cached = PITCHER_GAME_LOG_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        payload = statsapi.get(
            "people",
            {
                "personIds": int(pitcher_id),
                "hydrate": f"stats(group=[pitching],type=[gameLog],season={int(season)})",
            },
            force=True,
        )
    except Exception:
        payload = {}

    people = payload.get("people") or []
    splits: List[Dict[str, Any]] = []
    for block in (people[0].get("stats") if people else []) or []:
        if str((block.get("type") or {}).get("displayName") or "").strip() != "gameLog":
            continue
        splits.extend(block.get("splits") or [])

    PITCHER_GAME_LOG_CACHE[cache_key] = splits
    return splits


def fetch_batter_game_log_splits(batter_id: int, season: int) -> List[Dict[str, Any]]:
    cache_key = (int(batter_id), int(season))
    cached = BATTER_GAME_LOG_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        payload = statsapi.get(
            "people",
            {
                "personIds": int(batter_id),
                "hydrate": f"stats(group=[hitting],type=[gameLog],season={int(season)})",
            },
            force=True,
        )
    except Exception:
        payload = {}

    people = payload.get("people") or []
    splits: List[Dict[str, Any]] = []
    for block in (people[0].get("stats") if people else []) or []:
        if str((block.get("type") or {}).get("displayName") or "").strip() != "gameLog":
            continue
        splits.extend(block.get("splits") or [])

    BATTER_GAME_LOG_CACHE[cache_key] = splits
    return splits


def fetch_pitcher_historical_batter_vs_pitcher_stat_lines(
    pitcher_id: int,
    report_date: dt.date,
    batter_ids: Optional[Sequence[int]] = None,
) -> Dict[int, Dict[str, Any]]:
    cache_key = (int(pitcher_id), report_date)
    target_batter_ids = {int(batter_id) for batter_id in batter_ids or []}
    cached = PITCHER_HISTORICAL_BVP_CACHE.get(cache_key)
    if cached is not None:
        if target_batter_ids:
            return {
                batter_id: line
                for batter_id, line in cached.items()
                if batter_id in target_batter_ids
            }
        return cached

    debut_year = fetch_pitcher_debut_year(int(pitcher_id), report_date.year)
    game_ids: List[int] = []
    game_dates_by_id: Dict[int, dt.date] = {}
    seen_game_ids = set()
    for season in range(debut_year, report_date.year + 1):
        for split in fetch_pitcher_game_log_splits(int(pitcher_id), season):
            game_date = parse_date(split.get("date"))
            if game_date is None or game_date >= report_date:
                continue
            game_id = to_int((split.get("game") or {}).get("gamePk"))
            if game_id is None or int(game_id) in seen_game_ids:
                continue
            seen_game_ids.add(int(game_id))
            game_ids.append(int(game_id))
            game_dates_by_id[int(game_id)] = game_date

    if target_batter_ids:
        overlapping_game_ids = set()
        for batter_id in target_batter_ids:
            for season in range(debut_year, report_date.year + 1):
                for split in fetch_batter_game_log_splits(int(batter_id), season):
                    game_date = parse_date(split.get("date"))
                    if game_date is None or game_date >= report_date:
                        continue
                    game_id = to_int((split.get("game") or {}).get("gamePk"))
                    if game_id is not None and int(game_id) in game_dates_by_id:
                        overlapping_game_ids.add(int(game_id))
        game_ids = [game_id for game_id in game_ids if game_id in overlapping_game_ids]

    lines_by_batter: Dict[int, List[Dict[str, Any]]] = {}
    game_line_maps: List[Dict[int, Dict[str, Any]]] = []
    if game_ids:
        max_workers = min(8, len(game_ids))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_game_id = {
                executor.submit(fetch_game_batter_vs_pitcher_stat_lines, game_id, int(pitcher_id)): game_id
                for game_id in game_ids
            }
            for future in as_completed(future_to_game_id):
                try:
                    game_line_maps.append(future.result())
                except Exception:
                    continue

    for game_line_map in game_line_maps:
        for batter_id, line in game_line_map.items():
            if target_batter_ids and int(batter_id) not in target_batter_ids:
                continue
            lines_by_batter.setdefault(int(batter_id), []).append(line)

    stat_lines = {
        batter_id: aggregate_stat_lines(lines)
        for batter_id, lines in lines_by_batter.items()
    }
    if not target_batter_ids:
        PITCHER_HISTORICAL_BVP_CACHE[cache_key] = stat_lines
    return stat_lines


def extract_game_batter_stat_lines_from_boxscore(boxscore_data: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    stat_lines: Dict[int, Dict[str, Any]] = {}
    for side in ("away", "home"):
        team_block = boxscore_data.get(side) or {}
        players = team_block.get("players") or {}
        for raw_player_id in team_block.get("batters") or []:
            player_id = to_int(raw_player_id)
            if player_id is None:
                continue
            batting = ((players.get(f"ID{player_id}") or {}).get("stats") or {}).get("batting") or {}
            if not batting:
                continue
            stat_lines[int(player_id)] = {
                "AB": to_int(batting.get("atBats")) or 0,
                "H": to_int(batting.get("hits")) or 0,
                "HR": to_int(batting.get("homeRuns")) or 0,
            }
    return stat_lines


def fetch_game_batter_stat_lines(game_id: int) -> Dict[int, Dict[str, Any]]:
    game_id = int(game_id)
    cached = GAME_BATTER_LINE_CACHE.get(game_id)
    if cached is not None:
        return cached

    try:
        boxscore_data = statsapi.boxscore_data(game_id)
    except Exception:
        boxscore_data = {}

    stat_lines = extract_game_batter_stat_lines_from_boxscore(boxscore_data)
    GAME_BATTER_LINE_CACHE[game_id] = stat_lines
    return stat_lines


def parse_vs_pitcher_stats(
    indexed_blocks: Dict[str, List[Dict[str, Any]]],
    *,
    batter_id: Optional[int] = None,
    pitcher_id: Optional[int] = None,
    report_date: Optional[dt.date] = None,
    same_day_line: Optional[Dict[str, Any]] = None,
    subtract_same_day_from_season_splits: bool = True,
) -> Dict[str, Any]:
    report_year = report_date.year if report_date is not None else None

    total_splits = []
    for block in indexed_blocks.get("vsPlayerTotal", []):
        for split in block.get("splits") or []:
            if _split_matches_batter_pitcher(split, batter_id=batter_id, pitcher_id=pitcher_id):
                total_splits.append(split)
    if total_splits:
        line = aggregate_stat_lines([_stat_line_from_split_stat(split.get("stat") or {}) for split in total_splits])
        if subtract_same_day_from_season_splits and same_day_line:
            line = _subtract_stat_line(line, same_day_line)
        totals = aggregate_stat_lines([line])
        if totals.get("AVG") is None and len(total_splits) == 1 and not same_day_line:
            totals["AVG"] = to_float((total_splits[0].get("stat") or {}).get("avg"))
        if totals.get("OPS") is None and len(total_splits) == 1 and not same_day_line:
            totals["OPS"] = to_float((total_splits[0].get("stat") or {}).get("ops"))
        return totals

    season_splits = []
    for block in indexed_blocks.get("vsPlayer", []):
        for split in block.get("splits") or []:
            if _split_matches_batter_pitcher(split, batter_id=batter_id, pitcher_id=pitcher_id):
                season_splits.append(split)
    if season_splits:
        rows = []
        for split in season_splits:
            season_value = to_int(split.get("season"))
            if report_year is not None and season_value is not None and season_value > report_year:
                continue
            line = _stat_line_from_split_stat(split.get("stat") or {})
            if (
                subtract_same_day_from_season_splits
                and report_year is not None
                and season_value == report_year
            ):
                line = _subtract_stat_line(line, same_day_line)
            rows.append(line)
        return aggregate_stat_lines(rows)

    return aggregate_stat_lines([])


def extract_season_hitting_stats(person: Dict[str, Any]) -> Dict[str, Any]:
    indexed = index_stat_blocks(person)
    split = first_stat_split(indexed.get("season", []))
    stat = (split or {}).get("stat") or {}
    return aggregate_stat_lines(
        [
            {
                "PA": to_int(stat.get("plateAppearances")) or 0,
                "AB": to_int(stat.get("atBats")) or 0,
                "H": to_int(stat.get("hits")) or 0,
                "BB": to_int(stat.get("baseOnBalls")) or 0,
                "HBP": to_int(stat.get("hitByPitch")) or 0,
                "SF": to_int(stat.get("sacFlies")) or 0,
                "TB": to_int(stat.get("totalBases")) or 0,
                "K": to_int(stat.get("strikeOuts")) or 0,
                "HR": to_int(stat.get("homeRuns")) or 0,
                "RBI": to_int(stat.get("rbi")) or 0,
            }
        ]
    )


def parse_team_split_stats(stat: Dict[str, Any]) -> Dict[str, Any]:
    return aggregate_stat_lines(
        [
            {
                "PA": to_int(stat.get("plateAppearances")) or 0,
                "AB": to_int(stat.get("atBats")) or 0,
                "H": to_int(stat.get("hits")) or 0,
                "BB": to_int(stat.get("baseOnBalls")) or 0,
                "HBP": to_int(stat.get("hitByPitch")) or 0,
                "SF": to_int(stat.get("sacFlies")) or 0,
                "TB": to_int(stat.get("totalBases")) or 0,
                "K": to_int(stat.get("strikeOuts")) or 0,
                "HR": to_int(stat.get("homeRuns")) or 0,
                "RBI": to_int(stat.get("rbi")) or 0,
            }
        ]
    )


def build_metric_rank_index(
    rows: Sequence[Dict[str, Any]],
    *,
    identifier_key: str,
    metric_directions: Dict[str, bool],
) -> Dict[int, Dict[str, int]]:
    rank_index: Dict[int, Dict[str, int]] = {}
    for metric, higher_is_better in metric_directions.items():
        ranked_rows = []
        for row in rows:
            row_id = to_int(row.get(identifier_key))
            value = to_float(row.get(metric))
            if row_id is None or value is None:
                continue
            ranked_rows.append((row_id, value))
        ranked_rows.sort(key=lambda item: item[1], reverse=higher_is_better)
        for position, (row_id, _) in enumerate(ranked_rows, start=1):
            rank_index.setdefault(int(row_id), {})[metric] = position
    return rank_index


def _fetch_team_handedness_split(team_id: int, season: int, sit_code: str) -> Optional[Dict[str, Any]]:
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
    parsed = parse_team_split_stats(stat)
    if parsed["PA"] <= 0:
        return None
    return parsed


def fetch_team_handedness_splits(team_id: int, season: int) -> Dict[str, Any]:
    cache_key = (team_id, season)
    cached = TEAM_HAND_SPLIT_CACHE.get(cache_key)
    if cached is not None:
        return cached

    candidate_seasons = [season]
    if season > 1900:
        candidate_seasons.append(season - 1)

    result: Dict[str, Optional[Dict[str, Any]]] = {"season": None, "vs_lhp": None, "vs_rhp": None}
    for candidate_season in candidate_seasons:
        vs_lhp = _fetch_team_handedness_split(team_id, candidate_season, "vl")
        vs_rhp = _fetch_team_handedness_split(team_id, candidate_season, "vr")
        if vs_lhp is not None or vs_rhp is not None:
            result = {"season": candidate_season, "vs_lhp": vs_lhp, "vs_rhp": vs_rhp}
            break

    TEAM_HAND_SPLIT_CACHE[cache_key] = result
    return result


def fetch_mlb_team_ids(season: int) -> List[int]:
    cached = MLB_TEAM_IDS_CACHE.get(season)
    if cached is not None:
        return cached

    try:
        payload = statsapi.get(
            "teams_stats",
            {"season": season, "sportIds": 1, "group": "hitting", "stats": "season"},
        )
    except Exception:
        MLB_TEAM_IDS_CACHE[season] = []
        return []

    team_ids: List[int] = []
    for split in ((payload.get("stats") or [{}])[0].get("splits") or []):
        team_id = to_int(((split.get("team") or {}).get("id")))
        if team_id is None:
            continue
        team_ids.append(team_id)

    deduped = list(dict.fromkeys(team_ids))
    MLB_TEAM_IDS_CACHE[season] = deduped
    return deduped


def fetch_team_handedness_rank_map(season: int, pitcher_hand: Optional[str]) -> Dict[int, Dict[str, int]]:
    hand_code = str(pitcher_hand or "").upper()
    if hand_code == "L":
        split_key = "vs_lhp"
        cache_key = (season, "L")
    elif hand_code == "R":
        split_key = "vs_rhp"
        cache_key = (season, "R")
    else:
        return {}

    cached = TEAM_HAND_RANK_CACHE.get(cache_key)
    if cached is not None:
        return cached

    rows: List[Dict[str, Any]] = []
    for team_id in fetch_mlb_team_ids(season):
        split_bundle = fetch_team_handedness_splits(team_id, season)
        split_stats = split_bundle.get(split_key)
        if not split_stats or (to_int(split_stats.get("PA")) or 0) <= 0:
            continue
        rows.append({"id": team_id, **split_stats})

    rank_map = build_metric_rank_index(
        rows,
        identifier_key="id",
        metric_directions={"OPS": True, "AVG": True, "K%": False, "HR": True},
    )
    TEAM_HAND_RANK_CACHE[cache_key] = rank_map
    return rank_map


def fetch_pitcher_season_rank_map(season: int, minimum_starts: int = 5) -> Dict[int, Dict[str, int]]:
    cache_key = (season, minimum_starts)
    cached = PITCHER_SEASON_RANK_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        payload = statsapi.get(
            "stats",
            {
                "stats": "season",
                "group": "pitching",
                "playerPool": "ALL",
                "sportIds": 1,
                "season": season,
                "limit": 2000,
            },
        )
    except Exception:
        PITCHER_SEASON_RANK_CACHE[cache_key] = {}
        return {}

    rows: List[Dict[str, Any]] = []
    stats_blocks = payload.get("stats") or []
    for split in (stats_blocks[0].get("splits") if stats_blocks else []):
        player_id = to_int(((split.get("player") or {}).get("id")))
        stat = split.get("stat") or {}
        games_started = to_int(stat.get("gamesStarted")) or 0
        if player_id is None or games_started < minimum_starts:
            continue
        rows.append(
            {
                "id": player_id,
                "ERA": to_float(stat.get("era")),
                "WHIP": to_float(stat.get("whip")),
                "K/9": to_float(stat.get("strikeoutsPer9Inn")),
                "AVG": to_float(stat.get("avg")),
            }
        )

    rank_map = build_metric_rank_index(
        rows,
        identifier_key="id",
        metric_directions={"ERA": False, "WHIP": False, "K/9": True, "AVG": False},
    )
    PITCHER_SEASON_RANK_CACHE[cache_key] = rank_map
    return rank_map
