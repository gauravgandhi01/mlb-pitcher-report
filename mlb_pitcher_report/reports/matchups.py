from __future__ import annotations

import datetime as dt
import sys
from dataclasses import dataclass, field
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import statsapi

from mlb_pitcher_report.shared.report_data import (
    aggregate_stat_lines,
    build_espn_event_snapshot_lookup,
    compute_recent_metrics,
    extract_confirmed_espn_lineup,
    extract_espn_odds,
    fetch_game_batter_vs_pitcher_stat_lines,
    extract_game_logs,
    extract_last_game_lineup_player_ids_from_boxscore,
    fetch_last_game_lineup_player_ids,
    fetch_park_context,
    fetch_people_stats_map,
    fetch_pitcher_context,
    fetch_pitcher_season_rank_map,
    fetch_team_handedness_splits,
    fetch_team_handedness_rank_map,
    fetch_team_meta,
    fetch_team_roster,
    fetch_espn_summary,
    filter_active_hitters,
    first_stat_split,
    format_local_start_time,
    index_stat_blocks,
    parse_date,
    parse_team_split_stats,
    parse_vs_pitcher_stats,
    resolve_date_input,
    resolve_effective_report_date_and_schedule,
    resolve_lineup_player_ids,
    safe_ratio,
    to_float,
    to_int,
    normalize_team_name,
)
from mlb_pitcher_report.shared.site_nav import build_date_nav_html, build_report_tabs
from mlb_pitcher_report.shared.team_logos import get_team_logo_src
from mlb_pitcher_report.reports.matchup_styles import _detail_page_css, _summary_page_css

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = Path("reports")
ROOT_MATCHUPS_FILE = PROJECT_ROOT / "matchups.html"
ROOT_MATCHUPS_DETAIL_FILE = PROJECT_ROOT / "matchups-detail.html"
NOT_STARTED_STATUSES = {"Pre-Game", "Scheduled", "Warmup"}

LINEUP_SOURCE_CONFIRMED = "ESPN Confirmed"
LINEUP_SOURCE_LAST_GAME = "Last Game Lineup"
LINEUP_SOURCE_ROSTER = "Roster Fallback"
RECENT_GAMES = 7
RECENT_WINDOW_DAYS = 14
LOW_SAMPLE_PA = 18

POSITIVE_BADGES = {"Strong BvP", "Strong vs Hand", "Lineup Hot", "Pitcher Cold"}
NEGATIVE_BADGES = {"Weak BvP", "Weak vs Hand", "Lineup Cold", "Pitcher Hot"}
NEUTRAL_BADGES = {"Low Sample", "Pitcher TBD"}
WARNING_BADGES = {"Low Sample"}

SUMMARY_CHIP_MAP: Dict[str, Dict[str, str]] = {
    "Strong BvP": {"code": "B+", "title": "Strong BvP", "tooltip": "Strong lineup batter-vs-pitcher history.", "tone": "positive"},
    "Weak BvP": {"code": "B-", "title": "Weak BvP", "tooltip": "Weak lineup batter-vs-pitcher history.", "tone": "negative"},
    "Strong vs Hand": {"code": "H+", "title": "Strong vs Hand", "tooltip": "Team has strong season results versus this starter hand.", "tone": "positive"},
    "Weak vs Hand": {"code": "H-", "title": "Weak vs Hand", "tooltip": "Team has weak season results versus this starter hand.", "tone": "negative"},
    "Lineup Hot": {"code": "F+", "title": "Lineup Hot", "tooltip": "Lineup has been hitting well recently.", "tone": "positive"},
    "Lineup Cold": {"code": "F-", "title": "Lineup Cold", "tooltip": "Lineup has been cold recently.", "tone": "negative"},
    "Pitcher Cold": {"code": "P-", "title": "Pitcher Cold", "tooltip": "Opposing pitcher is in poor recent form.", "tone": "positive"},
    "Pitcher Hot": {"code": "P+", "title": "Pitcher Hot", "tooltip": "Opposing pitcher is in strong recent form.", "tone": "negative"},
    "Low Sample": {"code": "LS", "title": "Low Sample", "tooltip": "Very small direct matchup sample.", "tone": "warning"},
    "Pitcher TBD": {"code": "TBD", "title": "Pitcher TBD", "tooltip": "Opposing starting pitcher is not confirmed.", "tone": "neutral"},
}

SUMMARY_SIGNAL_WEIGHTS = {
    "Strong BvP": 3,
    "Strong vs Hand": 2,
    "Lineup Hot": 2,
    "Pitcher Cold": 2,
    "Weak BvP": -3,
    "Weak vs Hand": -2,
    "Lineup Cold": -2,
    "Pitcher Hot": -2,
    "Low Sample": -1,
    "Pitcher TBD": -1,
}


@dataclass
class ParkContext:
    roof_type: Optional[str] = None
    temp_f: Optional[float] = None
    wind_mph: Optional[float] = None
    wind_dir: Optional[str] = None
    precip_pct: Optional[float] = None
    source: Optional[str] = None


@dataclass
class BestSpot:
    anchor_id: str
    display_label: str
    score: int
    chips: List[Dict[str, str]]
    order: int


@dataclass
class OffenseMatchup:
    team_id: int
    team_name: str
    team_abbrev: str
    opponent_id: int
    opponent_name: str
    opponent_abbrev: str
    pitcher_name: str
    pitcher_hand: Optional[str]
    lineup_source: str
    selected_player_ids: List[int]
    lineup_names: List[str]
    matchup_stats: Dict[str, Any]
    recent7_stats: Dict[str, Any]
    recent14_stats: Dict[str, Any]
    hand_split_stats: Optional[Dict[str, Any]]
    hand_split_ranks: Dict[str, int]
    pitcher_id: Optional[int]
    pitcher_season: Dict[str, Any]
    pitcher_season_ranks: Dict[str, int]
    pitcher_recent: Dict[str, Any]
    badges: List[str]
    summary_chips: List[Dict[str, str]] = field(default_factory=list)
    summary_lean: str = ""
    summary_score: int = 0


@dataclass
class GameMatchup:
    event_id: str
    away_team_id: int
    away_team_name: str
    away_team_abbrev: str
    home_team_id: int
    home_team_name: str
    home_team_abbrev: str
    start_time: str
    status: str
    odds: Dict[str, Any]
    away_offense: OffenseMatchup
    home_offense: OffenseMatchup
    status_state: str = ""
    status_detail: str = ""
    away_score: Optional[int] = None
    home_score: Optional[int] = None
    sort_datetime: str = ""
    venue_id: Optional[int] = None
    venue_name: str = ""
    park_context: Optional[ParkContext] = None


def innings_string_to_outs(value: Any) -> int:
    text = str(value or "").strip()
    if not text:
        return 0
    if "." not in text:
        try:
            return int(text) * 3
        except ValueError:
            return 0
    whole_text, remainder_text = text.split(".", 1)
    try:
        whole_innings = int(whole_text)
        remainder_outs = int(remainder_text[:1] or "0")
    except ValueError:
        return 0
    return (whole_innings * 3) + min(max(remainder_outs, 0), 2)


def build_pitcher_recent_form(
    game_log_splits: Sequence[Dict[str, Any]],
    report_date: dt.date,
    limit: int = 5,
) -> Dict[str, Any]:
    starts: List[Dict[str, Any]] = []
    for split in game_log_splits:
        game_date = parse_date(split.get("date"))
        if game_date is None or game_date >= report_date:
            continue
        stat = split.get("stat") or {}
        if to_int(stat.get("gamesStarted")) != 1:
            continue
        game_info = split.get("game") or {}
        outs = to_int(stat.get("outs"))
        if outs is None:
            outs = innings_string_to_outs(stat.get("inningsPitched"))
        starts.append(
            {
                "date": game_date,
                "gamePk": to_int(game_info.get("gamePk")) or 0,
                "outs": outs or 0,
                "earnedRuns": to_int(stat.get("earnedRuns")) or 0,
                "hits": to_int(stat.get("hits")) or 0,
                "walks": to_int(stat.get("baseOnBalls")) or 0,
                "strikeOuts": to_int(stat.get("strikeOuts")) or 0,
                "atBats": to_int(stat.get("atBats")) or 0,
            }
        )

    starts.sort(key=lambda row: (row["date"], row["gamePk"]), reverse=True)
    starts = starts[:limit]
    if not starts:
        return {"Starts": 0, "IP": None, "IP/start": None, "ERA": None, "WHIP": None, "K/9": None, "BB/9": None, "AVG": None}

    total_outs = sum(start["outs"] for start in starts)
    total_er = sum(start["earnedRuns"] for start in starts)
    total_hits = sum(start["hits"] for start in starts)
    total_walks = sum(start["walks"] for start in starts)
    total_strikeouts = sum(start["strikeOuts"] for start in starts)
    total_at_bats = sum(start["atBats"] for start in starts)
    innings_pitched = total_outs / 3.0 if total_outs > 0 else 0.0
    starts_count = len(starts)

    return {
        "Starts": starts_count,
        "IP": innings_pitched if total_outs > 0 else None,
        "IP/start": innings_pitched / starts_count if total_outs > 0 and starts_count > 0 else None,
        "ERA": safe_ratio(total_er * 27.0, total_outs),
        "WHIP": safe_ratio((total_hits + total_walks) * 3.0, total_outs),
        "K/9": safe_ratio(total_strikeouts * 27.0, total_outs),
        "BB/9": safe_ratio(total_walks * 27.0, total_outs),
        "AVG": safe_ratio(total_hits, total_at_bats),
    }


def extract_pitcher_season_stats(person: Dict[str, Any]) -> Dict[str, Any]:
    indexed = index_stat_blocks(person)
    split = first_stat_split(indexed.get("season", []))
    stat = (split or {}).get("stat") or {}
    innings_text = str(stat.get("inningsPitched") or "").strip() or None
    outs = to_int(stat.get("outs"))
    if outs is None:
        outs = innings_string_to_outs(innings_text)
    games_started = to_int(stat.get("gamesStarted")) or 0
    return {
        "GS": games_started,
        "IP": innings_text,
        "ERA": to_float(stat.get("era")),
        "WHIP": to_float(stat.get("whip")),
        "K/9": to_float(stat.get("strikeoutsPer9Inn")),
        "BB/9": to_float(stat.get("walksPer9Inn")),
        "AVG": to_float(stat.get("avg")),
        "IP/start": ((outs / 3.0) / games_started) if outs and games_started > 0 else None,
    }


def _status_badge(status: str) -> str:
    status_text = str(status or "").strip() or "Unknown"
    status_slug = status_text.lower().replace(" ", "-").replace("/", "-")
    return f'<span class="status-pill status-{escape(status_slug, quote=True)}">{escape(status_text)}</span>'


def _normalized_status_state(status_state: str, status: str) -> str:
    state = str(status_state or "").strip().lower()
    if state in {"pre", "in", "post"}:
        return state

    status_text = str(status or "").strip()
    if status_text in NOT_STARTED_STATUSES:
        return "pre"
    if status_text == "In Progress":
        return "in"
    if status_text == "Final":
        return "post"
    return ""


def _format_rate(value: Any) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    return f"{numeric:.3f}"


def _format_pct(value: Any) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    return f"{numeric:.1f}%"


def _format_total(value: Any) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    return f"{numeric:.1f}"


def _format_moneyline(value: Any) -> str:
    numeric = to_int(value)
    if numeric is None:
        return "-"
    return f"{numeric:+d}"


def _format_int(value: Any) -> str:
    numeric = to_int(value)
    if numeric is None:
        return "-"
    return str(numeric)


def _format_hit_ab(stats: Dict[str, Any]) -> str:
    at_bats = to_int(stats.get("AB")) or 0
    hits = to_int(stats.get("H")) or 0
    if at_bats <= 0:
        return "-"
    return f"{hits}-{at_bats}"


def _format_innings(value: Any) -> str:
    numeric = to_float(value)
    if numeric is None:
        return "-"
    return f"{numeric:.1f}"


def _format_rank(rank: Any) -> str:
    numeric = to_int(rank)
    if numeric is None or numeric <= 0:
        return ""
    return f"MLB #{numeric}"


def _rank_tone(rank: Any) -> str:
    numeric = to_int(rank)
    if numeric is None or numeric <= 0:
        return ""
    if numeric <= 5:
        return "rank-elite"
    if numeric <= 10:
        return "rank-strong"
    if numeric >= 40:
        return "rank-poor"
    if numeric >= 25:
        return "rank-weak"
    return ""


def _pitcher_last_name(name: str) -> str:
    text = str(name or "").strip()
    if not text or text.upper() == "TBD":
        return "TBD"
    base_name = text.split(" (", 1)[0].strip()
    raw_tokens = [token for token in base_name.split() if token.strip()]
    suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
    while raw_tokens and raw_tokens[-1].strip(",.").lower() in suffixes:
        raw_tokens.pop()
    if not raw_tokens:
        return "TBD"
    return raw_tokens[-1].strip(",.") or "TBD"


def _pitcher_display_name(name: str, hand: Optional[str]) -> str:
    hand_text = f" ({hand})" if str(hand or "").strip() else ""
    if not name:
        return f"TBD{hand_text}"
    return f"{name}{hand_text}"


def _pitcher_chip_label(name: str, hand: Optional[str]) -> str:
    last_name = _pitcher_last_name(name)
    if last_name == "TBD":
        return "TBD"
    hand_text = f" ({hand})" if str(hand or "").strip() else ""
    return f"{last_name}{hand_text}"


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


def _build_matchup_view_tabs(active_view: str, summary_href: str, detail_href: str) -> str:
    tabs = [
        ("summary", "Summary", summary_href),
        ("detail", "Detail", detail_href),
    ]
    links: List[str] = []
    for view_key, label, href in tabs:
        classes = ["matchup-view-tab"]
        if view_key == active_view:
            classes.append("active")
        links.append(
            '<a class="' + " ".join(classes) + '" href="' + escape(href, quote=True) + '">'
            + escape(label)
            + "</a>"
        )
    return '<nav class="matchup-view-tabs" aria-label="Matchup views">' + "".join(links) + "</nav>"


def _slugify_fragment(value: Any) -> str:
    cleaned: List[str] = []
    previous_dash = False
    for char in str(value or "").strip().lower():
        if char.isalnum():
            cleaned.append(char)
            previous_dash = False
            continue
        if not previous_dash:
            cleaned.append("-")
            previous_dash = True
    return "".join(cleaned).strip("-")


def _game_anchor_id(game: GameMatchup) -> str:
    event_id = str(game.event_id or "").strip()
    if event_id:
        return f"game-{event_id}"

    away_slug = _slugify_fragment(game.away_team_abbrev or game.away_team_name)
    home_slug = _slugify_fragment(game.home_team_abbrev or game.home_team_name)
    date_slug = _slugify_fragment(game.sort_datetime or game.start_time or game.status)
    fallback = "-".join(part for part in (away_slug, home_slug, date_slug) if part) or "matchup"
    return f"game-{fallback}"


def _extract_team_pitcher_hand_split(
    team_id: int,
    season: int,
    pitcher_hand: Optional[str],
) -> Optional[Dict[str, Any]]:
    if str(pitcher_hand or "").upper() not in {"L", "R"}:
        return None
    splits = fetch_team_handedness_splits(team_id, season)
    return splits.get("vs_lhp") if str(pitcher_hand).upper() == "L" else splits.get("vs_rhp")


def _fetch_pitcher_person(pitcher_name: str, season: int) -> Optional[Dict[str, Any]]:
    pitcher_context = fetch_pitcher_context(pitcher_name)
    if not pitcher_context or not pitcher_context.get("id"):
        return None
    payload = statsapi.get(
        "people",
        {
            "personIds": pitcher_context["id"],
            "hydrate": f"stats(group=[pitching],type=[season,gameLog],season={season})",
        },
        force=True,
    )
    people = payload.get("people") or []
    return people[0] if people else None


def _build_player_snapshot(person_id: int, person: Dict[str, Any], report_date: dt.date) -> Dict[str, Any]:
    indexed = index_stat_blocks(person)
    game_logs = extract_game_logs(person)
    return {
        "id": person_id,
        "name": str(person.get("fullName") or "").strip(),
        "vsp": aggregate_stat_lines([]),
        "recent7": compute_recent_metrics(game_logs, report_date, max_games=RECENT_GAMES),
        "recent14": compute_recent_metrics(game_logs, report_date, window_days=RECENT_WINDOW_DAYS),
        "season": compute_recent_metrics(game_logs, report_date),
        "__indexed": indexed,
    }


def _rank_roster_fallback_candidates(snapshots: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        snapshots,
        key=lambda row: (
            -(to_int((row.get("recent14") or {}).get("PA")) or 0),
            -(to_int((row.get("season") or {}).get("PA")) or 0),
            -((to_float((row.get("recent14") or {}).get("OPS")) if to_float((row.get("recent14") or {}).get("OPS")) is not None else -1.0)),
            -((to_float((row.get("season") or {}).get("OPS")) if to_float((row.get("season") or {}).get("OPS")) is not None else -1.0)),
            str(row.get("name") or ""),
        ),
    )


def _select_lineup_source(
    confirmed_lineup_ids: Sequence[int],
    last_game_lineup_ids: Sequence[int],
    player_snapshots: Sequence[Dict[str, Any]],
) -> tuple[str, List[Dict[str, Any]]]:
    snapshots_by_id = {int(snapshot["id"]): snapshot for snapshot in player_snapshots}

    if len(confirmed_lineup_ids) >= 9 and all(player_id in snapshots_by_id for player_id in confirmed_lineup_ids[:9]):
        return LINEUP_SOURCE_CONFIRMED, [snapshots_by_id[player_id] for player_id in confirmed_lineup_ids[:9]]

    if len(last_game_lineup_ids) >= 9 and all(player_id in snapshots_by_id for player_id in last_game_lineup_ids[:9]):
        return LINEUP_SOURCE_LAST_GAME, [snapshots_by_id[player_id] for player_id in last_game_lineup_ids[:9]]

    ranked = _rank_roster_fallback_candidates(player_snapshots)[:9]
    return LINEUP_SOURCE_ROSTER, ranked


def _build_badges(
    lineup_source: str,
    matchup_stats: Dict[str, Any],
    recent7_stats: Dict[str, Any],
    hand_split_stats: Optional[Dict[str, Any]],
    pitcher_recent: Dict[str, Any],
    pitcher_name: str,
    pitcher_hand: Optional[str],
) -> List[str]:
    del lineup_source
    badges: List[str] = []
    matchup_pa = to_int(matchup_stats.get("PA")) or 0
    matchup_ops = to_float(matchup_stats.get("OPS"))
    recent7_ops = to_float(recent7_stats.get("OPS"))
    hand_ops = to_float((hand_split_stats or {}).get("OPS"))
    pitcher_recent_era = to_float(pitcher_recent.get("ERA"))
    pitcher_recent_whip = to_float(pitcher_recent.get("WHIP"))
    pitcher_recent_k9 = to_float(pitcher_recent.get("K/9"))
    pitcher_recent_starts = to_int(pitcher_recent.get("Starts")) or 0

    if matchup_pa < LOW_SAMPLE_PA:
        badges.append("Low Sample")
    elif matchup_ops is not None and matchup_ops >= 0.800:
        badges.append("Strong BvP")
    elif matchup_ops is not None and matchup_ops <= 0.650:
        badges.append("Weak BvP")

    if hand_ops is not None and hand_ops >= 0.760:
        badges.append("Strong vs Hand")
    elif hand_ops is not None and hand_ops <= 0.680:
        badges.append("Weak vs Hand")

    if recent7_ops is not None and recent7_ops >= 0.800:
        badges.append("Lineup Hot")
    elif recent7_ops is not None and recent7_ops <= 0.650:
        badges.append("Lineup Cold")

    if not pitcher_name or pitcher_name == "TBD" or not pitcher_hand:
        badges.append("Pitcher TBD")
    elif pitcher_recent_starts >= 3:
        hot = (
            pitcher_recent_era is not None
            and pitcher_recent_era <= 3.0
            and (pitcher_recent_whip is None or pitcher_recent_whip <= 1.15)
        ) or (
            pitcher_recent_k9 is not None
            and pitcher_recent_k9 >= 10.5
            and (pitcher_recent_era is None or pitcher_recent_era <= 3.5)
        )
        cold = (
            pitcher_recent_era is not None
            and pitcher_recent_era >= 5.0
        ) or (
            pitcher_recent_whip is not None
            and pitcher_recent_whip >= 1.40
        )
        if hot:
            badges.append("Pitcher Hot")
        elif cold:
            badges.append("Pitcher Cold")

    seen = set()
    ordered: List[str] = []
    for badge in badges:
        if badge in seen:
            continue
        seen.add(badge)
        ordered.append(badge)
    return ordered


def _matchup_sort_bucket(game: GameMatchup) -> int:
    state = _normalized_status_state(game.status_state, game.status)
    if state == "pre":
        return 0
    if state == "in":
        return 1
    if state == "post":
        return 2
    return 1


def _sort_matchups(matchups: Sequence[GameMatchup]) -> List[GameMatchup]:
    return sorted(
        list(matchups),
        key=lambda game: (
            _matchup_sort_bucket(game),
            str(game.sort_datetime or ""),
            game.away_team_name,
            game.home_team_name,
        ),
    )


def _blank_pitcher_stats() -> Dict[str, Any]:
    return {"GS": None, "IP": None, "ERA": None, "WHIP": None, "K/9": None, "BB/9": None, "AVG": None, "IP/start": None}


def _blank_recent_pitcher_stats() -> Dict[str, Any]:
    return {"Starts": 0, "IP": None, "IP/start": None, "ERA": None, "WHIP": None, "K/9": None, "BB/9": None, "AVG": None}


def _build_offense_matchup(
    *,
    team_id: int,
    team_name: str,
    team_abbrev: str,
    opponent_id: int,
    opponent_name: str,
    opponent_abbrev: str,
    pitcher_name: str,
    game_id: Optional[int],
    start_time: str,
    status: str,
    report_date: dt.date,
    report_year: int,
    espn_summary: Optional[Dict[str, Any]],
) -> OffenseMatchup:
    del start_time

    roster_entries = filter_active_hitters(fetch_team_roster(team_id))
    confirmed_lineup_entries = extract_confirmed_espn_lineup(espn_summary, team_abbrev) if espn_summary and team_abbrev else []
    confirmed_lineup_ids = resolve_lineup_player_ids(confirmed_lineup_entries, roster_entries, team_id) if confirmed_lineup_entries else []
    last_game_lineup_ids = fetch_last_game_lineup_player_ids(team_id)

    active_roster_ids = [
        int((entry.get("person") or {}).get("id"))
        for entry in roster_entries
        if (entry.get("person") or {}).get("id") is not None
    ]
    requested_ids = list(dict.fromkeys(active_roster_ids + confirmed_lineup_ids + last_game_lineup_ids))

    pitcher_context = fetch_pitcher_context(pitcher_name) if pitcher_name else None
    people_by_id = fetch_people_stats_map(
        requested_ids,
        season=report_year,
        pitch_hand=(pitcher_context or {}).get("hand"),
        pitcher_id=(pitcher_context or {}).get("id"),
    )

    player_snapshots = []
    same_day_bvp_lines: Dict[int, Dict[str, Any]] = {}
    if (
        game_id is not None
        and pitcher_context
        and pitcher_context.get("id")
        and str(status or "").strip() not in NOT_STARTED_STATUSES
    ):
        same_day_bvp_lines = fetch_game_batter_vs_pitcher_stat_lines(int(game_id), int(pitcher_context["id"]))

    for player_id in requested_ids:
        if player_id not in people_by_id:
            continue
        snapshot = _build_player_snapshot(player_id, people_by_id[player_id], report_date)
        snapshot["vsp"] = parse_vs_pitcher_stats(
            snapshot.pop("__indexed"),
            batter_id=player_id,
            pitcher_id=(pitcher_context or {}).get("id"),
            report_date=report_date,
            same_day_line=same_day_bvp_lines.get(player_id),
        )
        player_snapshots.append(snapshot)
    lineup_source, selected_snapshots = _select_lineup_source(confirmed_lineup_ids, last_game_lineup_ids, player_snapshots)

    matchup_stats = aggregate_stat_lines([snapshot["vsp"] for snapshot in selected_snapshots])
    recent7_stats = aggregate_stat_lines([snapshot["recent7"] for snapshot in selected_snapshots])
    recent14_stats = aggregate_stat_lines([snapshot["recent14"] for snapshot in selected_snapshots])
    hand_split_stats = _extract_team_pitcher_hand_split(team_id, report_year, (pitcher_context or {}).get("hand"))
    hand_split_ranks = fetch_team_handedness_rank_map(report_year, (pitcher_context or {}).get("hand")).get(team_id, {})

    pitcher_season = _blank_pitcher_stats()
    pitcher_season_ranks: Dict[str, int] = {}
    pitcher_recent = _blank_recent_pitcher_stats()
    pitcher_id = to_int((pitcher_context or {}).get("id"))
    pitcher_hand = (pitcher_context or {}).get("hand")
    pitcher_display_name = str((pitcher_context or {}).get("name") or pitcher_name or "TBD").strip() or "TBD"

    if pitcher_context and pitcher_context.get("id"):
        pitcher_person = _fetch_pitcher_person(str(pitcher_context.get("name") or pitcher_name), report_year)
        if pitcher_person:
            pitcher_season = extract_pitcher_season_stats(pitcher_person)
            if pitcher_id is not None:
                pitcher_season_ranks = fetch_pitcher_season_rank_map(report_year).get(pitcher_id, {})
            indexed = index_stat_blocks(pitcher_person)
            pitcher_recent = build_pitcher_recent_form(
                (indexed.get("gameLog") or [{}])[0].get("splits") or [],
                report_date,
            )

    badges = _build_badges(
        lineup_source,
        matchup_stats,
        recent7_stats,
        hand_split_stats,
        pitcher_recent,
        pitcher_display_name,
        pitcher_hand,
    )
    summary_chips = _build_summary_chips(badges)
    summary_lean = _build_summary_lean(badges, recent7_stats, recent14_stats, hand_split_ranks)
    summary_score = _compute_summary_score(badges)

    return OffenseMatchup(
        team_id=team_id,
        team_name=team_name,
        team_abbrev=team_abbrev,
        opponent_id=opponent_id,
        opponent_name=opponent_name,
        opponent_abbrev=opponent_abbrev,
        pitcher_name=pitcher_display_name,
        pitcher_hand=pitcher_hand,
        lineup_source=lineup_source,
        selected_player_ids=[int(snapshot["id"]) for snapshot in selected_snapshots],
        lineup_names=[str(snapshot["name"] or "").strip() for snapshot in selected_snapshots],
        matchup_stats=matchup_stats,
        recent7_stats=recent7_stats,
        recent14_stats=recent14_stats,
        hand_split_stats=hand_split_stats,
        hand_split_ranks=hand_split_ranks,
        pitcher_id=pitcher_id,
        pitcher_season=pitcher_season,
        pitcher_season_ranks=pitcher_season_ranks,
        pitcher_recent=pitcher_recent,
        badges=badges,
        summary_chips=summary_chips,
        summary_lean=summary_lean,
        summary_score=summary_score,
    )


def build_matchups(schedule: Sequence[Dict[str, Any]], report_date: str) -> List[GameMatchup]:
    report_date_obj = dt.datetime.strptime(report_date, "%m/%d/%Y").date()
    report_year = report_date_obj.year
    espn_event_snapshot_lookup = build_espn_event_snapshot_lookup(report_date)

    team_ids = {
        int(game[side])
        for game in schedule
        for side in ("away_id", "home_id")
        if game.get(side) is not None
    }
    team_meta_map = {team_id: fetch_team_meta(team_id) for team_id in team_ids}
    sorted_games = sorted(schedule, key=lambda game: str(game.get("game_datetime") or ""))

    matchups: List[GameMatchup] = []
    for game in sorted_games:
        away_team_name = str(game.get("away_name") or "").strip()
        home_team_name = str(game.get("home_name") or "").strip()
        away_team_id = to_int(game.get("away_id"))
        home_team_id = to_int(game.get("home_id"))
        if away_team_id is None or home_team_id is None:
            continue

        away_team_abbrev = str((team_meta_map.get(away_team_id) or {}).get("abbreviation") or "").strip().upper()
        home_team_abbrev = str((team_meta_map.get(home_team_id) or {}).get("abbreviation") or "").strip().upper()
        espn_snapshot = espn_event_snapshot_lookup.get((normalize_team_name(away_team_name), normalize_team_name(home_team_name))) or {}
        event_id = str(espn_snapshot.get("event_id") or "").strip()
        espn_summary = fetch_espn_summary(event_id) if event_id else None
        odds = extract_espn_odds(espn_summary)
        status_text = str(game.get("status") or "").strip()
        venue_id = to_int(game.get("venue_id"))
        venue_name = str(game.get("venue_name") or "").strip()
        park_context_data = fetch_park_context(venue_id, game.get("game_datetime"), report_date) if venue_id is not None else None
        park_context = ParkContext(**park_context_data) if park_context_data else None
        away_score = espn_snapshot.get("away_score")
        home_score = espn_snapshot.get("home_score")
        if away_score is None and status_text not in NOT_STARTED_STATUSES:
            away_score = to_int(game.get("away_score"))
        if home_score is None and status_text not in NOT_STARTED_STATUSES:
            home_score = to_int(game.get("home_score"))

        away_offense = _build_offense_matchup(
            team_id=away_team_id,
            team_name=away_team_name,
            team_abbrev=away_team_abbrev,
            opponent_id=home_team_id,
            opponent_name=home_team_name,
            opponent_abbrev=home_team_abbrev,
            pitcher_name=str(game.get("home_probable_pitcher") or "").strip(),
            game_id=to_int(game.get("game_id")),
            start_time=format_local_start_time(game.get("game_datetime")),
            status=status_text,
            report_date=report_date_obj,
            report_year=report_year,
            espn_summary=espn_summary,
        )
        home_offense = _build_offense_matchup(
            team_id=home_team_id,
            team_name=home_team_name,
            team_abbrev=home_team_abbrev,
            opponent_id=away_team_id,
            opponent_name=away_team_name,
            opponent_abbrev=away_team_abbrev,
            pitcher_name=str(game.get("away_probable_pitcher") or "").strip(),
            game_id=to_int(game.get("game_id")),
            start_time=format_local_start_time(game.get("game_datetime")),
            status=status_text,
            report_date=report_date_obj,
            report_year=report_year,
            espn_summary=espn_summary,
        )

        matchups.append(
            GameMatchup(
                event_id=event_id,
                away_team_id=away_team_id,
                away_team_name=away_team_name,
                away_team_abbrev=away_team_abbrev,
                home_team_id=home_team_id,
                home_team_name=home_team_name,
                home_team_abbrev=home_team_abbrev,
                start_time=format_local_start_time(game.get("game_datetime")),
                status=status_text,
                odds=odds,
                away_offense=away_offense,
                home_offense=home_offense,
                status_state=str(espn_snapshot.get("status_state") or "").strip().lower(),
                status_detail=str(espn_snapshot.get("status_short_detail") or espn_snapshot.get("status_detail") or "").strip(),
                away_score=away_score,
                home_score=home_score,
                sort_datetime=str(game.get("game_datetime") or ""),
                venue_id=venue_id,
                venue_name=venue_name,
                park_context=park_context,
            )
        )
    return _sort_matchups(matchups)


def _render_badges(badges: Sequence[str]) -> str:
    if not badges:
        return ""

    return '<div class="badge-row">' + _render_badge_spans(badges) + "</div>"


def _render_badge_spans(badges: Sequence[str]) -> str:
    if not badges:
        return ""

    rendered_badges: List[str] = []
    for badge in badges:
        badge_text = str(badge)
        badge_class = "signal-badge"
        if badge_text in WARNING_BADGES:
            badge_class += " signal-warning"
        elif badge_text in POSITIVE_BADGES:
            badge_class += " signal-positive"
        elif badge_text in NEGATIVE_BADGES:
            badge_class += " signal-negative"
        elif badge_text in NEUTRAL_BADGES:
            badge_class += " signal-neutral"
        rendered_badges.append(f'<span class="{badge_class}">{escape(badge_text)}</span>')
    return "".join(rendered_badges)


SUMMARY_BADGE_PRIORITY = {
    "Strong BvP": 0,
    "Weak BvP": 0,
    "Strong vs Hand": 1,
    "Weak vs Hand": 1,
    "Lineup Hot": 2,
    "Lineup Cold": 2,
    "Pitcher Cold": 3,
    "Pitcher Hot": 3,
    "Low Sample": 4,
    "Pitcher TBD": 5,
}


def _select_summary_badges(badges: Sequence[str], limit: int = 3) -> List[str]:
    ranked = sorted(
        enumerate(str(badge) for badge in badges),
        key=lambda item: (SUMMARY_BADGE_PRIORITY.get(item[1], 999), item[0]),
    )
    return [badge for _, badge in ranked[:limit]]


def _build_summary_chips(badges: Sequence[str]) -> List[Dict[str, str]]:
    chips: List[Dict[str, str]] = []
    for badge in _select_summary_badges(badges, limit=len(list(badges)) or 0):
        chip = SUMMARY_CHIP_MAP.get(str(badge))
        if not chip:
            continue
        chips.append(dict(chip))
    return chips


def _compute_summary_score(badges: Sequence[str]) -> int:
    return sum(SUMMARY_SIGNAL_WEIGHTS.get(str(badge), 0) for badge in badges)


def _build_summary_lean(
    badges: Sequence[str],
    recent7_stats: Dict[str, Any],
    recent14_stats: Dict[str, Any],
    hand_split_ranks: Dict[str, int],
) -> str:
    badge_set = {str(badge) for badge in badges}
    positive_count = sum(1 for badge in badges if badge in POSITIVE_BADGES)
    negative_count = sum(1 for badge in badges if badge in NEGATIVE_BADGES)
    positive_dominant = positive_count >= 2 and negative_count <= 1

    if negative_count >= 2 or ("Pitcher Hot" in badge_set and ("Weak vs Hand" in badge_set or "Lineup Cold" in badge_set)):
        return "Fade"
    if positive_dominant:
        hr_rank = to_int(hand_split_ranks.get("HR"))
        avg_rank = to_int(hand_split_ranks.get("AVG"))
        k_rank = to_int(hand_split_ranks.get("K%"))
        recent7_hr = to_int(recent7_stats.get("HR")) or 0
        recent14_hr = to_int(recent14_stats.get("HR")) or 0
        if (hr_rank is not None and hr_rank <= 10) or recent7_hr >= 7 or recent14_hr >= 14:
            return "Attack: power"
        if avg_rank is not None and avg_rank <= 10 and k_rank is not None and k_rank <= 12:
            return "Attack: contact"
        return "Attack"
    return ""


def _render_summary_chip_spans(chips: Sequence[Dict[str, str]]) -> str:
    rendered: List[str] = []
    for chip in chips:
        code = str(chip.get("code") or "").strip()
        title = str(chip.get("title") or code).strip() or code
        tooltip = str(chip.get("tooltip") or title).strip() or title
        tone = str(chip.get("tone") or "").strip()
        chip_class = "summary-chip"
        if tone == "warning":
            chip_class += " signal-warning"
        elif tone == "positive":
            chip_class += " signal-positive"
        elif tone == "negative":
            chip_class += " signal-negative"
        elif tone == "neutral":
            chip_class += " signal-neutral"
        rendered.append(
            f'<span class="{chip_class}" data-tooltip="{escape(tooltip, quote=True)}" title="{escape(tooltip, quote=True)}" aria-label="{escape(tooltip, quote=True)}">{escape(title)}</span>'
        )
    return "".join(rendered)


def _format_park_chip_text(context: Optional[ParkContext]) -> str:
    if not context:
        return ""
    roof_type = str(context.roof_type or "").strip().lower()
    if roof_type == "indoor":
        return "Indoor"
    if roof_type == "retractable":
        return "Retractable"

    temp_f = to_int(context.temp_f)
    wind_mph = to_int(context.wind_mph)
    if temp_f is None and wind_mph is None:
        return ""
    if temp_f is None:
        return f"W{wind_mph}"
    if wind_mph is None:
        return f"{temp_f}\N{DEGREE SIGN}"
    return f"{temp_f}\N{DEGREE SIGN} W{wind_mph}"


def _render_park_chip(context: Optional[ParkContext]) -> str:
    chip_text = _format_park_chip_text(context)
    if not chip_text:
        return ""

    title_parts: List[str] = []
    if context and context.roof_type:
        title_parts.append(str(context.roof_type).capitalize())
    if context and context.temp_f is not None:
        title_parts.append(f"{to_int(context.temp_f)}°F")
    if context and context.wind_mph is not None:
        wind_text = f"{to_int(context.wind_mph)} mph"
        if context.wind_dir:
            wind_text = f"{context.wind_dir} {wind_text}"
        title_parts.append(wind_text)
    if context and context.precip_pct is not None:
        title_parts.append(f"{to_int(context.precip_pct)}% precip")
    if context and context.source:
        title_parts.append(str(context.source))
    title_text = " | ".join(part for part in title_parts if part)
    title_attr = f' title="{escape(title_text, quote=True)}"' if title_text else ""
    return f'<span class="park-pill"{title_attr}>{escape(chip_text)}</span>'


def _collect_best_spots(matchups: Sequence[GameMatchup]) -> List[BestSpot]:
    spots: List[BestSpot] = []
    for order, game in enumerate(matchups):
        if _normalized_status_state(game.status_state, game.status) != "pre":
            continue
        for offense in (game.away_offense, game.home_offense):
            if offense.summary_score <= 0:
                continue
            spots.append(
                BestSpot(
                    anchor_id=_game_anchor_id(game),
                    display_label=f"{offense.team_abbrev} vs {_pitcher_last_name(offense.pitcher_name)}",
                    score=offense.summary_score,
                    chips=offense.summary_chips[:2],
                    order=order,
                )
            )

    spots.sort(key=lambda spot: (-spot.score, spot.order, spot.display_label))
    return spots[:5]


def _render_best_spots(spots: Sequence[BestSpot], detail_href: str) -> str:
    if not spots:
        return ""

    items = []
    for spot in spots:
        href = f"{detail_href}#{spot.anchor_id}"
        items.append(
            '<a class="best-spot-link" href="'
            + escape(href, quote=True)
            + '"><span class="best-spot-label">'
            + escape(spot.display_label)
            + '</span><span class="best-spot-score">'
            + escape(f"+{spot.score}")
            + '</span><span class="best-spot-chips">'
            + _render_summary_chip_spans(spot.chips)
            + "</span></a>"
        )
    return '<section class="best-spots-panel"><div class="best-spots-head">Best Spots</div><div class="best-spots-grid">' + "".join(items) + "</div></section>"


def _metric_tone(
    value: Any,
    *,
    elite: Optional[float] = None,
    strong: Optional[float] = None,
    weak: Optional[float] = None,
    poor: Optional[float] = None,
    inverse: bool = False,
) -> str:
    numeric = to_float(value)
    if numeric is None:
        return ""
    if inverse:
        if elite is not None and numeric <= elite:
            return "metric-elite"
        if strong is not None and numeric <= strong:
            return "metric-strong"
        if poor is not None and numeric >= poor:
            return "metric-poor"
        if weak is not None and numeric >= weak:
            return "metric-weak"
        return ""
    if elite is not None and numeric >= elite:
        return "metric-elite"
    if strong is not None and numeric >= strong:
        return "metric-strong"
    if poor is not None and numeric <= poor:
        return "metric-poor"
    if weak is not None and numeric <= weak:
        return "metric-weak"
    return ""


def _render_stat_pair(label: str, value: str, value_class: str = "", rank: str = "", rank_class: str = "") -> str:
    class_attr = f' class="stat-value {value_class}"' if value_class else ' class="stat-value"'
    rank_attr = f' class="stat-rank {rank_class}"' if rank and rank_class else ' class="stat-rank"'
    rank_html = f'<span{rank_attr}>{escape(rank)}</span>' if rank else ""
    head_class = "stat-label-row has-rank" if rank else "stat-label-row"
    return (
        '<div class="stat-pair">'
        f'<div class="{head_class}">'
        f'<span class="stat-label">{escape(label)}</span>'
        f"{rank_html}"
        "</div>"
        f"<span{class_attr}>{value}</span>"
        "</div>"
    )


def _render_matchup_section(title: str, stats: Dict[str, Any]) -> str:
    return (
        '<section class="metric-card">'
        f"<h4>{escape(title)}</h4>"
        '<div class="stat-grid">'
        + _render_stat_pair("PA", _format_int(stats.get("PA")), "metric-weak" if (to_int(stats.get("PA")) or 0) < LOW_SAMPLE_PA else "")
        + _render_stat_pair("OPS", _format_rate(stats.get("OPS")), _metric_tone(stats.get("OPS"), elite=0.860, strong=0.780, weak=0.670, poor=0.600))
        + _render_stat_pair("AVG", _format_rate(stats.get("AVG")), _metric_tone(stats.get("AVG"), elite=0.320, strong=0.285, weak=0.225, poor=0.190))
        + _render_stat_pair("K%", _format_pct(stats.get("K%")), _metric_tone(stats.get("K%"), elite=18.0, strong=22.0, weak=28.0, poor=33.0, inverse=True))
        + _render_stat_pair("HR", _format_int(stats.get("HR")), _metric_tone(stats.get("HR"), elite=3.0, strong=2.0))
        + "</div></section>"
    )


def _render_recent_section(recent7_stats: Dict[str, Any], recent14_stats: Dict[str, Any]) -> str:
    return (
        '<section class="metric-card">'
        "<h4>Lineup Form</h4>"
        '<div class="dual-grid">'
        '<div class="sub-grid">'
        '<span class="sub-grid-title">Last 7</span>'
        + _render_stat_pair("OPS", _format_rate(recent7_stats.get("OPS")), _metric_tone(recent7_stats.get("OPS"), elite=0.840, strong=0.760, weak=0.660, poor=0.610))
        + _render_stat_pair("AVG", _format_rate(recent7_stats.get("AVG")), _metric_tone(recent7_stats.get("AVG"), elite=0.305, strong=0.275, weak=0.225, poor=0.205))
        + _render_stat_pair("K%", _format_pct(recent7_stats.get("K%")), _metric_tone(recent7_stats.get("K%"), elite=18.0, strong=22.0, weak=28.0, poor=32.0, inverse=True))
        + _render_stat_pair("HR", _format_int(recent7_stats.get("HR")), _metric_tone(recent7_stats.get("HR"), elite=8.0, strong=5.0))
        + "</div>"
        '<div class="sub-grid">'
        '<span class="sub-grid-title">Last 14d</span>'
        + _render_stat_pair("OPS", _format_rate(recent14_stats.get("OPS")), _metric_tone(recent14_stats.get("OPS"), elite=0.840, strong=0.760, weak=0.660, poor=0.610))
        + _render_stat_pair("AVG", _format_rate(recent14_stats.get("AVG")), _metric_tone(recent14_stats.get("AVG"), elite=0.305, strong=0.275, weak=0.225, poor=0.205))
        + _render_stat_pair("K%", _format_pct(recent14_stats.get("K%")), _metric_tone(recent14_stats.get("K%"), elite=18.0, strong=22.0, weak=28.0, poor=32.0, inverse=True))
        + _render_stat_pair("HR", _format_int(recent14_stats.get("HR")), _metric_tone(recent14_stats.get("HR"), elite=10.0, strong=6.0))
        + "</div>"
        "</div></section>"
    )


def _render_hand_split_section(
    pitcher_hand: Optional[str],
    stats: Optional[Dict[str, Any]],
    ranks: Optional[Dict[str, int]],
) -> str:
    hand_label = "LHP" if str(pitcher_hand or "").upper() == "L" else "RHP" if str(pitcher_hand or "").upper() == "R" else "Starter Hand"
    if not stats:
        return (
            '<section class="metric-card">'
            f"<h4>Team vs {escape(hand_label)}</h4>"
            '<p class="empty-lineup">No handedness split available.</p>'
            "</section>"
        )
    return (
        '<section class="metric-card">'
        f"<h4>Team vs {escape(hand_label)}</h4>"
        '<div class="stat-grid stat-grid-4">'
        + _render_stat_pair("OPS", _format_rate(stats.get("OPS")), _metric_tone(stats.get("OPS"), elite=0.790, strong=0.750, weak=0.680, poor=0.630), _format_rank((ranks or {}).get("OPS")), _rank_tone((ranks or {}).get("OPS")))
        + _render_stat_pair("AVG", _format_rate(stats.get("AVG")), _metric_tone(stats.get("AVG"), elite=0.280, strong=0.260, weak=0.230, poor=0.210), _format_rank((ranks or {}).get("AVG")), _rank_tone((ranks or {}).get("AVG")))
        + _render_stat_pair("K%", _format_pct(stats.get("K%")), _metric_tone(stats.get("K%"), elite=18.0, strong=22.0, weak=26.5, poor=29.5, inverse=True), _format_rank((ranks or {}).get("K%")), _rank_tone((ranks or {}).get("K%")))
        + _render_stat_pair("HR", _format_int(stats.get("HR")), _metric_tone(stats.get("HR"), elite=40.0, strong=24.0), _format_rank((ranks or {}).get("HR")), _rank_tone((ranks or {}).get("HR")))
        + "</div></section>"
    )


def _render_pitcher_section(offense: OffenseMatchup) -> str:
    season = offense.pitcher_season
    recent = offense.pitcher_recent
    return (
        '<section class="metric-card pitcher-card">'
        '<div class="pitcher-card-head">'
        '<span class="pitcher-kicker">Opposing SP</span>'
        f'<div class="pitcher-name">{escape(_pitcher_display_name(offense.pitcher_name, offense.pitcher_hand))}</div>'
        "</div>"
        '<div class="dual-grid">'
        '<div class="sub-grid">'
        '<span class="sub-grid-title">Season</span>'
        + _render_stat_pair("ERA", _format_total(season.get("ERA")), _metric_tone(season.get("ERA"), elite=5.0, strong=4.2, weak=3.2, poor=2.8), _format_rank(offense.pitcher_season_ranks.get("ERA")), _rank_tone(offense.pitcher_season_ranks.get("ERA")))
        + _render_stat_pair("WHIP", _format_total(season.get("WHIP")), _metric_tone(season.get("WHIP"), elite=1.38, strong=1.25, weak=1.10, poor=0.98), _format_rank(offense.pitcher_season_ranks.get("WHIP")), _rank_tone(offense.pitcher_season_ranks.get("WHIP")))
        + _render_stat_pair("K/9", _format_total(season.get("K/9")), _metric_tone(season.get("K/9"), elite=7.0, strong=8.2, weak=10.0, poor=11.0, inverse=True), _format_rank(offense.pitcher_season_ranks.get("K/9")), _rank_tone(offense.pitcher_season_ranks.get("K/9")))
        + _render_stat_pair("AVG", _format_rate(season.get("AVG")), _metric_tone(season.get("AVG"), elite=0.260, strong=0.245, weak=0.220, poor=0.205), _format_rank(offense.pitcher_season_ranks.get("AVG")), _rank_tone(offense.pitcher_season_ranks.get("AVG")))
        + "</div>"
        '<div class="sub-grid">'
        '<span class="sub-grid-title">Last 5 Starts</span>'
        + _render_stat_pair("Starts", _format_int(recent.get("Starts")))
        + _render_stat_pair("ERA", _format_total(recent.get("ERA")), _metric_tone(recent.get("ERA"), elite=5.0, strong=4.2, weak=3.2, poor=2.8))
        + _render_stat_pair("WHIP", _format_total(recent.get("WHIP")), _metric_tone(recent.get("WHIP"), elite=1.38, strong=1.25, weak=1.10, poor=0.98))
        + _render_stat_pair("K/9", _format_total(recent.get("K/9")), _metric_tone(recent.get("K/9"), elite=7.0, strong=8.2, weak=10.0, poor=11.0, inverse=True))
        + _render_stat_pair("AVG", _format_rate(recent.get("AVG")), _metric_tone(recent.get("AVG"), elite=0.260, strong=0.245, weak=0.220, poor=0.205))
        + "</div>"
        "</div></section>"
    )


def _render_offense_panel(offense: OffenseMatchup) -> str:
    source_slug = str(offense.lineup_source).lower().replace(" ", "-")
    return (
        f'<section class="offense-panel source-{escape(source_slug, quote=True)}">'
        '<div class="offense-header">'
        '<div class="team-heading">'
        f'<img class="team-logo" src="{escape(get_team_logo_src(team_id=offense.team_id, team_abbrev=offense.team_abbrev, team_name=offense.team_name), quote=True)}" alt="{escape(offense.team_name, quote=True)} logo">'
        '<div>'
        f'<h3>{escape(offense.team_name)}</h3>'
        "</div></div>"
        "</div>"
        + _render_badges(offense.badges)
        + '<div class="panel-metrics">'
        + _render_matchup_section(f"Vs {_pitcher_last_name(offense.pitcher_name)}", offense.matchup_stats)
        + _render_recent_section(offense.recent7_stats, offense.recent14_stats)
        + _render_hand_split_section(offense.pitcher_hand, offense.hand_split_stats, offense.hand_split_ranks)
        + _render_pitcher_section(offense)
        + "</div>"
        + "</section>"
    )


def _render_team_chip(
    *,
    team_id: int,
    team_name: str,
    team_abbrev: str,
    side_value: str,
    side_value_class: str,
    starter_label: str,
) -> str:
    side_html = f'<span class="{side_value_class}">{escape(side_value)}</span>' if side_value != "-" else ""
    starter_html = f'<span class="team-chip-starter">{escape(starter_label)}</span>' if starter_label else ""
    return (
        f'<span class="team-chip"><img class="team-logo small" src="{escape(get_team_logo_src(team_id=team_id, team_abbrev=team_abbrev, team_name=team_name), quote=True)}" alt="{escape(team_name, quote=True)} logo">'
        '<span class="team-chip-copy">'
        f'<span class="team-chip-main"><span>{escape(team_abbrev)}</span>{side_html}</span>'
        f"{starter_html}"
        "</span></span>"
    )


def _render_summary_offense_row(offense: OffenseMatchup) -> str:
    lean_class = "summary-lean"
    if offense.summary_lean.startswith("Attack"):
        lean_class += " signal-positive"
    elif offense.summary_lean == "Fade":
        lean_class += " signal-negative"
    lean_html = f'<span class="{lean_class}">{escape(offense.summary_lean)}</span>' if offense.summary_lean else ""
    return (
        '<div class="summary-offense-row">'
        f'<span class="summary-offense-team">{escape(offense.team_abbrev)}</span>'
        '<div class="summary-offense-tags">'
        f"{_render_summary_chip_spans(offense.summary_chips[:3])}"
        f"{lean_html}"
        "</div>"
        "</div>"
    )


def _render_summary_card(game: GameMatchup, detail_href: str) -> str:
    detail_link = f"{detail_href}#{_game_anchor_id(game)}"
    state = _normalized_status_state(game.status_state, game.status) or "unknown"
    return (
        f'<a class="summary-card-link" href="{escape(detail_link, quote=True)}" aria-label="{escape(f"View detailed matchup for {game.away_team_name} at {game.home_team_name}", quote=True)}">'
        f'<article class="summary-card game-state-{escape(state, quote=True)}">'
        + _render_game_header(game)
        + '<div class="summary-signals">'
        + _render_summary_offense_row(game.away_offense)
        + _render_summary_offense_row(game.home_offense)
        + "</div>"
        + "</article></a>"
    )


def _render_game_header(game: GameMatchup) -> str:
    odds = game.odds or {}
    away_ml = _format_moneyline(odds.get("away_moneyline"))
    home_ml = _format_moneyline(odds.get("home_moneyline"))
    total = _format_total(odds.get("total"))
    away_starter = _pitcher_chip_label(game.home_offense.pitcher_name, game.home_offense.pitcher_hand)
    home_starter = _pitcher_chip_label(game.away_offense.pitcher_name, game.away_offense.pitcher_hand)
    status_state = _normalized_status_state(game.status_state, game.status)
    is_pregame = status_state == "pre"
    away_side_value = away_ml if is_pregame else _format_int(game.away_score)
    home_side_value = home_ml if is_pregame else _format_int(game.home_score)
    side_value_class = "team-chip-ml" if is_pregame else "team-chip-score"
    total_html = f'<span class="total-pill">{escape(total)}</span>' if total != "-" else ""
    park_html = _render_park_chip(game.park_context)
    detail_text = str(game.status_detail or "").strip()
    show_detail = bool(detail_text) and detail_text.lower() != str(game.status or "").strip().lower()
    meta_prefix = f"<span>{escape(game.start_time or '-')}</span>" if is_pregame else (f"<span>{escape(detail_text)}</span>" if show_detail else "")
    return (
        '<div class="game-header">'
        + '<div class="matchup-title">'
        + '<div class="teams-line">'
        + _render_team_chip(
            team_id=game.away_team_id,
            team_name=game.away_team_name,
            team_abbrev=game.away_team_abbrev,
            side_value=away_side_value,
            side_value_class=side_value_class,
            starter_label=away_starter,
        )
        + '<span class="at-symbol">@</span>'
        + _render_team_chip(
            team_id=game.home_team_id,
            team_name=game.home_team_name,
            team_abbrev=game.home_team_abbrev,
            side_value=home_side_value,
            side_value_class=side_value_class,
            starter_label=home_starter,
        )
        + total_html
        + "</div>"
        + '<div class="game-meta">'
        + f"{meta_prefix}"
        + f"{_status_badge(game.status)}"
        + park_html
        + "</div></div>"
    )


def _render_game_detail_card(game: GameMatchup) -> str:
    state = _normalized_status_state(game.status_state, game.status) or "unknown"
    return (
        f'<article class="game-card game-state-{escape(state, quote=True)}" id="{escape(_game_anchor_id(game), quote=True)}">'
        + _render_game_header(game)
        + '<div class="offense-grid">'
        + _render_offense_panel(game.away_offense)
        + _render_offense_panel(game.home_offense)
        + "</div></article>"
    )


def _render_page_html(
    *,
    title: str,
    heading: str,
    display_date: str,
    updated_at: str,
    description: str,
    tabs_html: str,
    date_nav_html: str,
    view_tabs_html: str,
    legend_text: str,
    lead_html: str,
    cards_html: str,
    cards_section_class: str,
    css: str,
    favicon_href: str,
) -> str:
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape(title)}</title>
  <link rel="icon" href="{escape(favicon_href, quote=True)}" type="image/svg+xml">
  <style>
{css}
  </style>
</head>
<body>
  <div class="layout">
    <section class="hero">
      <h1>{escape(heading)}</h1>
      <p>{escape(display_date)} slate. Updated {escape(updated_at)}. {escape(description)}</p>
      <div class="hero-nav-row">
        {tabs_html}
        {date_nav_html}
      </div>
      {view_tabs_html}
    </section>
    <section class="legend-panel">
      {escape(legend_text)}
    </section>
    {lead_html}
    <section class="{escape(cards_section_class, quote=True)}">
      {cards_html}
    </section>
  </div>
</body>
</html>
"""






def write_html(
    matchups: Sequence[GameMatchup],
    report_key: str,
    display_date: str,
    *,
    write_root: bool = True,
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    updated_at = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

    root_tabs_html = build_report_tabs("matchups", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_tabs_html = build_report_tabs("matchups", display_date, root_page=False, reports_dir=REPORTS_DIR)
    root_summary_date_nav_html = build_date_nav_html("matchups", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_summary_date_nav_html = build_date_nav_html("matchups", display_date, root_page=False, reports_dir=REPORTS_DIR)
    root_detail_date_nav_html = build_date_nav_html("matchups_detail", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_detail_date_nav_html = build_date_nav_html("matchups_detail", display_date, root_page=False, reports_dir=REPORTS_DIR)
    root_summary_view_tabs = _build_matchup_view_tabs("summary", "./matchups.html", "./matchups-detail.html")
    root_detail_view_tabs = _build_matchup_view_tabs("detail", "./matchups.html", "./matchups-detail.html")
    archive_summary_href = f"./matchups-report-{report_key}.html"
    archive_detail_href = f"./matchups-detail-report-{report_key}.html"
    archive_summary_view_tabs = _build_matchup_view_tabs("summary", archive_summary_href, archive_detail_href)
    archive_detail_view_tabs = _build_matchup_view_tabs("detail", archive_summary_href, archive_detail_href)

    best_spots = _collect_best_spots(matchups)
    summary_lead_root = _render_best_spots(best_spots, "./matchups-detail.html")
    summary_lead_archive = _render_best_spots(best_spots, archive_detail_href)
    summary_cards_root = "".join(_render_summary_card(game, "./matchups-detail.html") for game in matchups)
    summary_cards_archive = "".join(_render_summary_card(game, archive_detail_href) for game in matchups)
    detail_cards_html = "".join(_render_game_detail_card(game) for game in matchups)
    if not summary_cards_root:
        summary_cards_root = '<p class="empty-state">No scheduled matchups found for this slate.</p>'
    if not summary_cards_archive:
        summary_cards_archive = '<p class="empty-state">No scheduled matchups found for this slate.</p>'
    if not detail_cards_html:
        detail_cards_html = '<p class="empty-state">No scheduled matchups found for this slate.</p>'

    summary_legend = "Signals-first summary with compact leans, park context, and top pregame spots. Click any matchup card to open the full breakdown."
    detail_legend = "Low-sample badges mark thin batter-vs-pitcher history. Small MLB rank tags only appear on season-based team split and starter metrics."

    summary_root_html = _render_page_html(
        title=f"MLB Matchups {display_date}",
        heading="MLB Daily Matchups",
        display_date=display_date,
        updated_at=updated_at,
        description="ESPN moneylines/totals and compact offense-vs-starter signals.",
        tabs_html=root_tabs_html,
        date_nav_html=root_summary_date_nav_html,
        view_tabs_html=root_summary_view_tabs,
        legend_text=summary_legend,
        lead_html=summary_lead_root,
        cards_html=summary_cards_root,
        cards_section_class="summary-cards",
        css=_summary_page_css(),
        favicon_href="./favicon.svg",
    )
    summary_archive_html = _render_page_html(
        title=f"MLB Matchups {display_date}",
        heading="MLB Daily Matchups",
        display_date=display_date,
        updated_at=updated_at,
        description="ESPN moneylines/totals and compact offense-vs-starter signals.",
        tabs_html=archive_tabs_html,
        date_nav_html=archive_summary_date_nav_html,
        view_tabs_html=archive_summary_view_tabs,
        legend_text=summary_legend,
        lead_html=summary_lead_archive,
        cards_html=summary_cards_archive,
        cards_section_class="summary-cards",
        css=_summary_page_css(),
        favicon_href="../favicon.svg",
    )
    detail_root_html = _render_page_html(
        title=f"MLB Matchup Details {display_date}",
        heading="MLB Matchup Details",
        display_date=display_date,
        updated_at=updated_at,
        description="Full offense-vs-starter breakdowns, team splits, and pitcher context.",
        tabs_html=root_tabs_html,
        date_nav_html=root_detail_date_nav_html,
        view_tabs_html=root_detail_view_tabs,
        legend_text=detail_legend,
        lead_html="",
        cards_html=detail_cards_html,
        cards_section_class="cards",
        css=_detail_page_css(),
        favicon_href="./favicon.svg",
    )
    detail_archive_html = _render_page_html(
        title=f"MLB Matchup Details {display_date}",
        heading="MLB Matchup Details",
        display_date=display_date,
        updated_at=updated_at,
        description="Full offense-vs-starter breakdowns, team splits, and pitcher context.",
        tabs_html=archive_tabs_html,
        date_nav_html=archive_detail_date_nav_html,
        view_tabs_html=archive_detail_view_tabs,
        legend_text=detail_legend,
        lead_html="",
        cards_html=detail_cards_html,
        cards_section_class="cards",
        css=_detail_page_css(),
        favicon_href="../favicon.svg",
    )

    summary_output_path = REPORTS_DIR / f"matchups-report-{report_key}.html"
    detail_output_path = REPORTS_DIR / f"matchups-detail-report-{report_key}.html"
    summary_output_path.write_text(summary_archive_html, encoding="utf-8")
    detail_output_path.write_text(detail_archive_html, encoding="utf-8")
    if write_root:
        ROOT_MATCHUPS_FILE.write_text(summary_root_html, encoding="utf-8")
        ROOT_MATCHUPS_DETAIL_FILE.write_text(detail_root_html, encoding="utf-8")
    print(summary_output_path.resolve().as_uri())
    return summary_output_path


def main(raw_date_input: str, *, allow_roll_forward: bool = True, write_root: bool = True) -> None:
    report_date = resolve_date_input(raw_date_input)
    report_date, schedule = resolve_effective_report_date_and_schedule(
        report_date,
        allow_roll_forward=allow_roll_forward,
    )
    report_key = report_date.replace("/", "")
    matchups = build_matchups(schedule, report_date)
    write_html(matchups, report_key, report_date, write_root=write_root)


def _parse_cli_args(argv: Sequence[str]) -> tuple[str, bool, bool]:
    if len(argv) < 2:
        print("Usage: python3 Matchups.py <today|tmrw|MM/DD|MM/DD/YYYY> [--exact] [--no-root]")
        sys.exit(1)

    supported_flags = {"--exact", "--no-root"}
    raw_flags = [str(flag) for flag in argv[2:]]
    unexpected_flags = [flag for flag in raw_flags if flag not in supported_flags]
    if unexpected_flags:
        print(f"Unsupported flags: {', '.join(unexpected_flags)}")
        sys.exit(1)

    return str(argv[1]), "--exact" in raw_flags, "--no-root" in raw_flags


if __name__ == "__main__":
    raw_date_input, exact_mode, no_root = _parse_cli_args(sys.argv)
    main(raw_date_input, allow_roll_forward=not exact_mode, write_root=not no_root)
