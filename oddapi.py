from __future__ import annotations

import json
import time
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
import statsapi
from unidecode import unidecode

ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"
KEYS_FILE = Path("keys.json")
MIN_REQUESTS_REMAINING = 30
REQUEST_TIMEOUT_SECONDS = 12
IGNORED_BOOKMAKERS = {"mybookieag", "betmgm", "superbook", "bovada"}
MIN_REQUEST_INTERVAL_SECONDS = 0.35
MAX_429_RETRIES = 4
MAX_BACKOFF_SECONDS = 12.0

_cached_api_key: Optional[str] = None
_last_request_ts = 0.0
_events_cache: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
_event_id_cache: Dict[Tuple[str, str, str], Optional[str]] = {}
_event_data_cache: Dict[str, Dict[str, Any]] = {}
_event_pitcher_odds_cache: Dict[str, pd.DataFrame] = {}
_pitcher_team_cache: Dict[str, Optional[str]] = {}
ALT_LINES_TOKEN = " || ALT: "


def _normalize_person_name(name: Any) -> str:
    text = unidecode(str(name or "")).lower().strip()
    text = text.replace(".", "").replace("'", "")
    return " ".join(text.split())


def _choose_best_player_match(players: List[Dict[str, Any]], pitcher_name: str) -> Optional[Dict[str, Any]]:
    if not players:
        return None

    target = _normalize_person_name(pitcher_name)
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


def _is_429_error(exc: requests.exceptions.RequestException) -> bool:
    return bool(
        isinstance(exc, requests.exceptions.HTTPError)
        and exc.response is not None
        and exc.response.status_code == 429
    )


def _safe_parse_retry_after(header_value: Optional[str]) -> Optional[float]:
    if not header_value:
        return None

    try:
        return max(float(header_value), 0.0)
    except (TypeError, ValueError):
        pass

    try:
        retry_at = parsedate_to_datetime(header_value)
        now = datetime.now(timezone.utc)
        return max((retry_at - now).total_seconds(), 0.0)
    except (TypeError, ValueError, OverflowError):
        return None


def _throttle_requests() -> None:
    global _last_request_ts
    now = time.monotonic()
    wait = MIN_REQUEST_INTERVAL_SECONDS - (now - _last_request_ts)
    if wait > 0:
        time.sleep(wait)
    _last_request_ts = time.monotonic()


def _request_with_backoff(url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
    last_exc: Optional[requests.exceptions.RequestException] = None

    for attempt in range(MAX_429_RETRIES + 1):
        _throttle_requests()
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            if attempt >= MAX_429_RETRIES:
                raise
            delay = min(1.0 * (2**attempt), MAX_BACKOFF_SECONDS)
            print(f"\033[93mRequest failed ({exc}). Retrying in {delay:.1f}s...\033[0m")
            time.sleep(delay)
            continue

        if response.status_code != 429:
            response.raise_for_status()
            return response

        delay = _safe_parse_retry_after(response.headers.get("Retry-After"))
        if delay is None:
            delay = min(1.5 * (2**attempt), MAX_BACKOFF_SECONDS)
        delay = min(max(delay, 1.0), MAX_BACKOFF_SECONDS)

        if attempt >= MAX_429_RETRIES:
            response.raise_for_status()
        print(f"\033[93mOdds API rate limited (429). Retrying in {delay:.1f}s...\033[0m")
        time.sleep(delay)

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Unexpected request failure.")


def load_api_keys(keys_file: Path = KEYS_FILE) -> List[str]:
    if not keys_file.exists():
        return []
    try:
        with keys_file.open("r", encoding="utf-8") as config_file:
            config = json.load(config_file)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"\033[91mFailed to load {keys_file}: {exc}\033[0m")
        return []
    return [key for key in config.get("api_keys", []) if key]


def check_api_requests_remaining() -> Optional[str]:
    api_keys = load_api_keys()
    if not api_keys:
        print("\033[91mNo API keys found in keys.json.\033[0m")
        return None

    for key in api_keys:
        url = f"{ODDS_API_BASE_URL}/sports/"
        try:
            response = _request_with_backoff(url, params={"apiKey": key})
            requests_remaining = response.headers.get("X-Requests-Remaining")
            if requests_remaining is None:
                print(f"API Key: {key} | X-Requests-Remaining header not found.")
                continue

            remaining = int(requests_remaining)
            if remaining > MIN_REQUESTS_REMAINING:
                print(
                    f"\033[92mUsing API Key: {key} | "
                    f"Requests remaining: {remaining}\033[0m"
                )
                return key
        except requests.exceptions.RequestException as exc:
            print(f"An error occurred while checking API key {key}: {exc}")

    print("No API keys with sufficient requests remaining.")
    return None


def get_api_key(force_refresh: bool = False) -> Optional[str]:
    global _cached_api_key
    if force_refresh or _cached_api_key is None:
        _cached_api_key = check_api_requests_remaining()
    return _cached_api_key


def _fetch_events_for_date(api_key: str, date: str) -> List[Dict[str, Any]]:
    cache_key = (api_key, date)
    if cache_key in _events_cache:
        return _events_cache[cache_key]

    url = f"{ODDS_API_BASE_URL}/sports/baseball_mlb/events"
    response = _request_with_backoff(url, params={"apiKey": api_key})
    games = response.json()
    _events_cache[cache_key] = games
    return games


def get_event_id_by_team(team_name: str, api_key: Optional[str], date: str) -> Optional[str]:
    if not api_key:
        return None
    cache_key = (api_key, date, team_name)
    if cache_key in _event_id_cache:
        return _event_id_cache[cache_key]

    try:
        event_id = request_event_id(team_name, api_key, date, {})
        _event_id_cache[cache_key] = event_id
        return event_id
    except requests.exceptions.RequestException as exc:
        print(f"An error occurred while requesting the API: {exc}")
        if _is_429_error(exc):
            refreshed_key = get_api_key(force_refresh=True)
            if refreshed_key and refreshed_key != api_key:
                retry_cache_key = (refreshed_key, date, team_name)
                try:
                    event_id = request_event_id(team_name, refreshed_key, date, {})
                    _event_id_cache[retry_cache_key] = event_id
                    return event_id
                except requests.exceptions.RequestException as retry_exc:
                    print(f"Retry with refreshed API key failed: {retry_exc}")
        return None


def request_event_id(
    team_name: str,
    api_key: str,
    date: str,
    event_data: Dict[str, Any],
) -> Optional[str]:
    del event_data
    games = _fetch_events_for_date(api_key, date)
    for game in games:
        if team_name in (game.get("home_team"), game.get("away_team")):
            return game.get("id")
    return None


def fetch_game_data(event_id: str, api_key: str) -> Dict[str, Any]:
    if event_id in _event_data_cache:
        return _event_data_cache[event_id]

    url = f"{ODDS_API_BASE_URL}/sports/baseball_mlb/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us,us_ex",
        "markets": "pitcher_strikeouts",
        "oddsFormat": "american",
    }
    response = _request_with_backoff(url, params=params)
    data = response.json()
    _event_data_cache[event_id] = data
    return data


def collect_pitcher_points(
    game_data: Dict[str, Any],
    ignored_bookmakers: set[str],
) -> Dict[str, List[float]]:
    pitcher_points: Dict[str, List[float]] = {}
    for bookmaker in game_data.get("bookmakers", []):
        if bookmaker.get("key") in ignored_bookmakers:
            continue
        for market in bookmaker.get("markets", []):
            if market.get("key") != "pitcher_strikeouts":
                continue
            for outcome in market.get("outcomes", []):
                pitcher = outcome.get("description")
                point = outcome.get("point")
                if pitcher is None or point is None:
                    continue
                pitcher_points.setdefault(pitcher, []).append(point)
    return pitcher_points


def determine_most_common_points(pitcher_points: Dict[str, List[float]]) -> Dict[str, float]:
    most_common: Dict[str, float] = {}
    for pitcher, points in pitcher_points.items():
        if points:
            most_common[pitcher] = Counter(points).most_common(1)[0][0]
    return most_common


def _format_price(price: Any) -> str:
    if not isinstance(price, (int, float)):
        return "N/A"
    return f"+{int(price)}" if price >= 0 else str(int(price))


def _to_int_price(price: Any) -> Optional[int]:
    try:
        if price is None:
            return None
        return int(float(price))
    except (TypeError, ValueError):
        return None


def _american_to_implied_probability(price: Optional[int]) -> Optional[float]:
    if price is None or price == 0:
        return None
    if price > 0:
        return 100.0 / (price + 100.0)
    return (-price) / ((-price) + 100.0)


def _line_balance_score(over_price: Optional[int], under_price: Optional[int]) -> Tuple[float, float]:
    over_prob = _american_to_implied_probability(over_price)
    under_prob = _american_to_implied_probability(under_price)
    if over_prob is None or under_prob is None:
        return (9_999.0, 9_999.0)

    # Lower is better: first minimize skew, then minimize total market distortion.
    return (abs(over_prob - under_prob), abs((over_prob + under_prob) - 1.0))


def _format_line(point: float, over_price: Optional[int], under_price: Optional[int]) -> str:
    over_text = _format_price(over_price) if over_price is not None else "N/A"
    under_text = _format_price(under_price) if under_price is not None else "N/A"
    point_text = str(int(point)) if float(point).is_integer() else str(point)
    return f"{point_text}: {over_text}|{under_text}"


def _build_best_and_alts_line(line_entries: List[Dict[str, Any]]) -> Optional[str]:
    if not line_entries:
        return None

    ranked = sorted(
        line_entries,
        key=lambda entry: (
            entry["balance_score"][0],
            entry["balance_score"][1],
            entry["point"],
        ),
    )
    primary = ranked[0]["formatted"]
    alternates = [entry["formatted"] for entry in ranked[1:] if entry["formatted"] != primary]
    if not alternates:
        return primary
    return primary + ALT_LINES_TOKEN + "; ".join(alternates)


def process_bookmaker_outcomes(
    bookmaker: Dict[str, Any],
    ignored_bookmakers: set[str],
) -> List[Dict[str, str]]:
    if bookmaker.get("key") in ignored_bookmakers:
        return []

    data: List[Dict[str, str]] = []
    for market in bookmaker.get("markets", []):
        if market.get("key") != "pitcher_strikeouts":
            continue

        line_map: Dict[Tuple[str, float], Dict[str, Optional[int]]] = {}
        for outcome in market.get("outcomes", []):
            pitcher = outcome.get("description")
            point = outcome.get("point")
            side = outcome.get("name")
            if pitcher is None or point is None or side not in {"Over", "Under"}:
                continue
            entry = line_map.setdefault((pitcher, float(point)), {"Over": None, "Under": None})
            entry[side] = _to_int_price(outcome.get("price"))

        lines_by_pitcher: Dict[str, List[Dict[str, Any]]] = {}
        for (pitcher, point), odds in line_map.items():
            over_price = odds.get("Over")
            under_price = odds.get("Under")
            lines_by_pitcher.setdefault(pitcher, []).append(
                {
                    "point": point,
                    "over_price": over_price,
                    "under_price": under_price,
                    "balance_score": _line_balance_score(over_price, under_price),
                    "formatted": _format_line(point, over_price, under_price),
                }
            )

        for pitcher, line_entries in lines_by_pitcher.items():
            best_and_alts = _build_best_and_alts_line(line_entries)
            if not best_and_alts:
                continue
            data.append(
                {
                    "pitcher": pitcher,
                    bookmaker.get("title", "Unknown Book"): best_and_alts,
                }
            )
    return data


def build_event_odds_dataframe(data: List[Dict[str, str]]) -> pd.DataFrame:
    if not data:
        return pd.DataFrame()
    df = pd.DataFrame(data)
    if df.empty or "pitcher" not in df.columns:
        return pd.DataFrame()
    return df.pivot_table(index="pitcher", aggfunc="first").reset_index()


def _filter_pitcher_from_event_df(event_df: pd.DataFrame, pitcher_name: str) -> pd.DataFrame:
    if event_df.empty or "pitcher" not in event_df.columns:
        return pd.DataFrame()

    clean_pitcher_name = unidecode(pitcher_name)
    if clean_pitcher_name:
        df_filtered = event_df[event_df["pitcher"] == clean_pitcher_name].copy()
    else:
        df_filtered = event_df.copy()

    if df_filtered.empty:
        return pd.DataFrame()

    df_filtered.loc[:, "pitcher"] = pitcher_name
    return df_filtered


def build_dataframe(data: List[Dict[str, str]], pitcher_name: str) -> pd.DataFrame:
    event_df = build_event_odds_dataframe(data)
    return _filter_pitcher_from_event_df(event_df, pitcher_name)


def get_pitcher_odds(
    event_id: str,
    api_key: str,
    pitcher_name: str,
    allow_key_refresh: bool = True,
) -> pd.DataFrame:
    try:
        if event_id in _event_pitcher_odds_cache:
            event_df = _event_pitcher_odds_cache[event_id]
        else:
            game_data = fetch_game_data(event_id, api_key)
            if not game_data.get("bookmakers"):
                return pd.DataFrame()

            data: List[Dict[str, str]] = []
            for bookmaker in game_data.get("bookmakers", []):
                data.extend(process_bookmaker_outcomes(bookmaker, IGNORED_BOOKMAKERS))
            event_df = build_event_odds_dataframe(data)
            _event_pitcher_odds_cache[event_id] = event_df

        return _filter_pitcher_from_event_df(event_df, pitcher_name)
    except requests.exceptions.RequestException as exc:
        print(f"\033[91mAn error occurred while requesting the API: {exc}\033[0m")
        if allow_key_refresh and _is_429_error(exc):
            refreshed_key = get_api_key(force_refresh=True)
            if refreshed_key and refreshed_key != api_key:
                return get_pitcher_odds(event_id, refreshed_key, pitcher_name, allow_key_refresh=False)
        return pd.DataFrame()


def get_pitcher_team(pitcher_name: str) -> Optional[str]:
    if pitcher_name in _pitcher_team_cache:
        return _pitcher_team_cache[pitcher_name]

    try:
        players = statsapi.lookup_player(pitcher_name)
        player = _choose_best_player_match(players or [], pitcher_name)
        if not player:
            _pitcher_team_cache[pitcher_name] = None
            return None

        current_team = player.get("currentTeam") or {}
        team_id = current_team.get("id")
        if team_id is None:
            _pitcher_team_cache[pitcher_name] = None
            return None
        team_name = statsapi.get("team", {"teamId": team_id})["teams"][0]["name"]
        if team_name == "Athletics":
            team_name = "Oakland Athletics"
        _pitcher_team_cache[pitcher_name] = team_name
        return team_name
    except Exception as exc:
        print(f"\033[91mCould not get pitcher {pitcher_name} team with error: {exc}\033[0m")
        _pitcher_team_cache[pitcher_name] = None
        return None


def get_pitcher_odds_by_team(pitcher_name: str, date: str) -> Optional[pd.DataFrame]:
    try:
        team_name = get_pitcher_team(pitcher_name)
        if not team_name:
            return None

        api_key = get_api_key()
        if not api_key:
            print("\033[91mNo valid Odds API key available.\033[0m")
            return None

        event_id = get_event_id_by_team(team_name, api_key, date)
        if not event_id:
            print(f"\033[91mEvent ID not found for team: {team_name}\033[0m")
            return None

        pitcher_odds = get_pitcher_odds(event_id, api_key, pitcher_name)
        if pitcher_odds.empty:
            print(f"\033[93mOdds not found for pitcher: {pitcher_name}\033[0m")
            return None
        return pitcher_odds
    except Exception as exc:
        print(f"An error occurred: {pitcher_name}, {exc}")
        return None


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Please provide the first and last name of the pitcher as arguments.")
    else:
        name = f"{sys.argv[1]} {sys.argv[2]}"
        pitcher_odds = get_pitcher_odds_by_team(name, datetime.now().strftime("%Y-%m-%d"))
        if pitcher_odds is not None and not pitcher_odds.empty:
            print(pitcher_odds)
