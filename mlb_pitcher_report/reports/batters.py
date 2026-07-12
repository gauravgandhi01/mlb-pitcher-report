from __future__ import annotations

import datetime as dt
import json
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd
from bs4 import BeautifulSoup

from mlb_pitcher_report.shared.report_data import (
    aggregate_stat_lines,
    build_espn_event_snapshot_lookup,
    compute_hit_streak,
    compute_recent_metrics,
    extract_confirmed_espn_lineup,
    extract_espn_game_total,
    fetch_game_batter_stat_lines,
    fetch_game_batter_vs_pitcher_stat_lines,
    extract_game_logs,
    fetch_espn_summary,
    fetch_people_stats_map,
    fetch_pitcher_historical_batter_vs_pitcher_stat_lines,
    fetch_pitcher_context,
    fetch_team_meta,
    fetch_team_roster,
    filter_active_hitters,
    format_local_start_time as _format_local_start_time,
    index_stat_blocks,
    normalize_team_name as _normalize_team_name,
    parse_vs_pitcher_stats,
    resolve_date_input,
    resolve_effective_report_date_and_schedule,
    resolve_lineup_player_ids,
    to_float as _to_float,
    to_int as _to_int,
)
from mlb_pitcher_report.shared.site_nav import build_date_nav_html, build_report_tabs
from mlb_pitcher_report.shared.team_logos import get_team_logo_src

PROJECT_ROOT = Path(__file__).resolve().parents[2]
REPORTS_DIR = Path("reports")
REPORT_STATE_DIR = Path("report_state")
BATTER_LINEUP_LOCKS_FILE = REPORT_STATE_DIR / "batter-lineup-locks.json"
ROOT_BATTERS_FILE = PROJECT_ROOT / "batters.html"
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
HOME_RUN_MIN_HR = 1
HOME_RUN_SECTION_LIMIT = 20
REPORT_COLUMNS = [
    "Batter",
    "Opponent",
    "Pitcher",
    "Hit Stk",
    f"Last {RECENT_GAMES} AVG",
    f"Last {RECENT_WINDOW_DAYS} AVG",
    "Season AVG",
    "VsP AVG",
    "VsP H-AB",
]
HOME_RUN_REPORT_COLUMNS = [
    "Batter",
    "Opponent",
    "Pitcher",
    "VsP HR",
    "VsP PA",
    "VsP HR/PA",
    "VsP H-AB",
]


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
    pitcher_id: Optional[int],
    start_time: str,
    status: str,
    game_id: Optional[int],
    team_score: Optional[int],
    opponent_score: Optional[int],
    team_result: str,
    total_result: str,
    final_total_runs: Optional[int],
    roster_entries: Sequence[Dict[str, Any]],
    people_by_id: Dict[int, Dict[str, Any]],
    report_date: dt.date,
    current_game_batter_lines: Optional[Dict[int, Dict[str, Any]]] = None,
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
        game_logs = extract_game_logs(person)
        season = compute_recent_metrics(game_logs, report_date)
        recent7 = compute_recent_metrics(game_logs, report_date, max_games=RECENT_GAMES)
        recent14 = compute_recent_metrics(game_logs, report_date, window_days=RECENT_WINDOW_DAYS)
        same_day_vsp = None
        if (
            pitcher_id is not None
            and game_id is not None
            and str(status or "").strip() not in NOT_STARTED_STATUSES
        ):
            same_day_vsp = fetch_game_batter_vs_pitcher_stat_lines(int(game_id), int(pitcher_id)).get(person_id)
        vsp = parse_vs_pitcher_stats(
            indexed,
            batter_id=person_id,
            pitcher_id=pitcher_id,
            report_date=report_date,
            same_day_line=same_day_vsp,
            subtract_same_day_from_season_splits=False,
        )
        hit_streak = compute_hit_streak(game_logs, report_date)
        current_game_line = (current_game_batter_lines or {}).get(person_id)
        game_hit_result = (
            _resolve_game_hit_result(game_logs, game_id, current_game_line)
            if str(status or "").strip() == "Final"
            else ""
        )
        game_home_run_result = (
            _resolve_game_home_run_result(game_logs, game_id, current_game_line)
            if str(status or "").strip() == "Final"
            else ""
        )

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
            "Total Result": total_result,
            "Final Total Runs": final_total_runs,
            "Pitch Hand": pitch_hand or "",
            "Source": SOURCE_ACTIVE,
            "Pool Rank": pd.NA,
            "Hot Score": None,
            "Hit Stk": hit_streak,
            "Team Result": team_result,
            "Team Score": team_score,
            "Opponent Score": opponent_score,
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
            f"Last {RECENT_WINDOW_DAYS} AVG": recent14["AVG"],
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
            "Game Hit Result": game_hit_result,
            "Game Home Run Result": game_home_run_result,
            "__player_id": person_id,
            "__pitcher_id": _to_int(pitcher_id),
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


def load_batter_lineup_locks(path: Optional[Path] = None) -> Dict[str, Any]:
    lock_path = path or BATTER_LINEUP_LOCKS_FILE
    if not lock_path.exists():
        return {}
    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def save_batter_lineup_locks(locks: Dict[str, Any], path: Optional[Path] = None) -> None:
    lock_path = path or BATTER_LINEUP_LOCKS_FILE
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(json.dumps(locks, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _coerce_player_ids(values: Any) -> List[int]:
    if values is None or isinstance(values, (str, bytes)):
        return []
    try:
        iterator = iter(values)
    except TypeError:
        return []
    player_ids: List[int] = []
    for value in iterator:
        player_id = _to_int(value)
        if player_id is None:
            continue
        player_ids.append(int(player_id))
    return player_ids


def _lineup_lock_key(game_id: Any, team_id: Any, pitcher_id: Any) -> Optional[str]:
    game_id_value = _to_int(game_id)
    team_id_value = _to_int(team_id)
    pitcher_id_value = _to_int(pitcher_id)
    if game_id_value is None or team_id_value is None or pitcher_id_value is None:
        return None
    return f"{game_id_value}:{team_id_value}:{pitcher_id_value}"


def _utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_lineup_ids_for_game_state(
    *,
    report_date: str,
    game_id: Any,
    team_id: Any,
    pitcher_id: Any,
    status: Any,
    confirmed_lineup_player_ids: Sequence[int],
    lineup_locks: Dict[str, Any],
) -> tuple[List[int], bool]:
    lock_key = _lineup_lock_key(game_id, team_id, pitcher_id)
    confirmed_ids = _coerce_player_ids(confirmed_lineup_player_ids)[:9]
    status_text = str(status or "").strip()

    day_locks = lineup_locks.get(report_date)
    if not isinstance(day_locks, dict):
        day_locks = {}
    existing_lock = day_locks.get(lock_key) if lock_key else None
    locked_ids = _coerce_player_ids((existing_lock or {}).get("player_ids") if isinstance(existing_lock, dict) else None)[:9]

    if status_text in NOT_STARTED_STATUSES:
        if len(confirmed_ids) >= 9:
            if not lock_key:
                return confirmed_ids, False

            if not isinstance(lineup_locks.get(report_date), dict):
                lineup_locks[report_date] = {}
            record = {
                "source": SOURCE_ESPN,
                "player_ids": confirmed_ids,
                "updated_at": _utc_timestamp(),
            }
            current_record = lineup_locks[report_date].get(lock_key)
            if (
                isinstance(current_record, dict)
                and _coerce_player_ids(current_record.get("player_ids"))[:9] == confirmed_ids
                and current_record.get("source") == SOURCE_ESPN
            ):
                return confirmed_ids, False
            lineup_locks[report_date][lock_key] = record
            return confirmed_ids, True

        if len(locked_ids) >= 9:
            return locked_ids, False
        return [], False

    if len(locked_ids) >= 9:
        return locked_ids, False
    return [], False


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


def build_home_run_matchup_section(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=HOME_RUN_REPORT_COLUMNS)

    filtered = df.copy()
    filtered["__status_sort"] = filtered["Status"].astype(str).map(_status_sort_value)
    filtered["__vsp_hr"] = pd.to_numeric(filtered["VsP HR"], errors="coerce").fillna(0)
    filtered["__vsp_pa"] = pd.to_numeric(filtered["VsP PA"], errors="coerce").fillna(0)
    filtered["__vsp_hr_rate"] = filtered["__vsp_hr"].div(filtered["__vsp_pa"].where(filtered["__vsp_pa"] > 0))
    filtered["__recent_hr"] = pd.to_numeric(filtered["Recent HR"], errors="coerce").fillna(0)

    home_run_rows = filtered[
        (filtered["__vsp_hr"] >= HOME_RUN_MIN_HR)
        & (filtered["__vsp_pa"] >= MATCHUP_MIN_PA)
    ].copy()
    if home_run_rows.empty:
        return pd.DataFrame(columns=HOME_RUN_REPORT_COLUMNS)

    home_run_rows = home_run_rows.sort_values(
        by=["__status_sort", "__vsp_hr_rate", "__vsp_hr", "__vsp_pa", "__recent_hr", "Batter"],
        ascending=[True, False, False, False, False, True],
        kind="mergesort",
    )
    return home_run_rows.head(HOME_RUN_SECTION_LIMIT)


def verify_historical_bvp_for_feature_candidates(
    df: pd.DataFrame,
    report_date: dt.date,
    candidate_indices: Optional[Sequence[Any]] = None,
) -> pd.DataFrame:
    if df.empty or "__player_id" not in df.columns or "__pitcher_id" not in df.columns:
        return df

    verified = df.copy()
    if candidate_indices is None:
        vsp_pa = pd.to_numeric(verified["VsP PA"], errors="coerce").fillna(0)
        vsp_ab = pd.to_numeric(verified["VsP AB"], errors="coerce").fillna(0)
        vsp_hr = pd.to_numeric(verified["VsP HR"], errors="coerce").fillna(0)
        candidate_mask = (vsp_pa >= MATCHUP_MIN_PA) | (vsp_ab >= MATCHUP_MIN_PA) | (vsp_hr >= HOME_RUN_MIN_HR)
    else:
        candidate_mask = verified.index.isin(candidate_indices)

    if not candidate_mask.any():
        return verified

    blank_vsp = aggregate_stat_lines([])
    pitcher_ids = sorted(
        int(value)
        for value in pd.to_numeric(verified.loc[candidate_mask, "__pitcher_id"], errors="coerce").dropna().unique()
    )
    historical_lines_by_pitcher: Dict[int, Dict[int, Dict[str, Any]]] = {}
    with ThreadPoolExecutor(max_workers=min(6, len(pitcher_ids) or 1)) as executor:
        future_to_pitcher_id = {}
        for pitcher_id in pitcher_ids:
            pitcher_mask = candidate_mask & (pd.to_numeric(verified["__pitcher_id"], errors="coerce") == pitcher_id)
            player_ids = [
                int(player_id)
                for player_id in pd.to_numeric(verified.loc[pitcher_mask, "__player_id"], errors="coerce").dropna()
            ]
            future = executor.submit(
                fetch_pitcher_historical_batter_vs_pitcher_stat_lines,
                pitcher_id,
                report_date,
                player_ids,
            )
            future_to_pitcher_id[future] = pitcher_id
        for future in as_completed(future_to_pitcher_id):
            pitcher_id = future_to_pitcher_id[future]
            try:
                historical_lines_by_pitcher[pitcher_id] = future.result()
            except Exception:
                historical_lines_by_pitcher[pitcher_id] = {}

    for pitcher_id in pitcher_ids:
        pitcher_mask = candidate_mask & (pd.to_numeric(verified["__pitcher_id"], errors="coerce") == pitcher_id)
        historical_lines = historical_lines_by_pitcher.get(pitcher_id, {})
        for row_index in verified.loc[pitcher_mask].index:
            player_id = _to_int(verified.at[row_index, "__player_id"])
            vsp = historical_lines.get(int(player_id)) if player_id is not None else None
            if vsp is None:
                vsp = blank_vsp
            verified.at[row_index, "VsP PA"] = vsp["PA"]
            verified.at[row_index, "VsP AB"] = vsp["AB"]
            verified.at[row_index, "VsP H"] = vsp["H"]
            verified.at[row_index, "VsP HR"] = vsp["HR"]
            verified.at[row_index, "VsP RBI"] = vsp["RBI"]
            verified.at[row_index, "VsP AVG"] = vsp["AVG"]
            verified.at[row_index, "VsP OPS"] = vsp["OPS"]
            verified.at[row_index, "VsP K%"] = vsp["K%"]

    return verified


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


def _format_percentage_ratio(numerator: Any, denominator: Any) -> str:
    numerator_value = _to_float(numerator)
    denominator_value = _to_float(denominator)
    if numerator_value is None or denominator_value is None or denominator_value <= 0:
        return ""
    return f"{(100.0 * numerator_value / denominator_value):.1f}%"


def _final_team_result(status: Any, team_score: Any, opponent_score: Any) -> str:
    status_text = str(status or "").strip()
    if status_text != "Final":
        return ""
    team_score_value = _to_int(team_score)
    opponent_score_value = _to_int(opponent_score)
    if team_score_value is None or opponent_score_value is None:
        return ""
    if team_score_value > opponent_score_value:
        return "win"
    if team_score_value < opponent_score_value:
        return "loss"
    return ""


def _final_total_result(status: Any, total: Any, away_score: Any, home_score: Any) -> str:
    status_text = str(status or "").strip()
    if status_text != "Final":
        return ""
    total_value = _to_float(total)
    away_score_value = _to_int(away_score)
    home_score_value = _to_int(home_score)
    if total_value is None or away_score_value is None or home_score_value is None:
        return ""
    final_runs = away_score_value + home_score_value
    if final_runs > total_value:
        return "over"
    if final_runs < total_value:
        return "under"
    return "push"


def _render_team_result_badge(result: Any) -> str:
    return ""


def _render_total_cell(total: Any, result: Any = "", final_runs: Any = None) -> str:
    return _format_total(total)


def _render_total_badge(total: Any) -> str:
    total_text = _format_total(total)
    if not total_text:
        return ""
    total_class = _classify_total_cell(total)
    class_suffix = {
        "cell-elite": "total-badge-elite",
        "cell-strong": "total-badge-strong",
        "cell-weak": "total-badge-weak",
    }.get(total_class, "total-badge-neutral")
    return (
        f'<span class="total-badge {escape(class_suffix, quote=True)}" '
        f'title="Game total {escape(total_text, quote=True)}">'
        f"{escape(total_text)}"
        "</span>"
    )


def _status_badge(status: Any) -> str:
    status_text = str(status or "").strip()
    if not status_text:
        return ""
    status_slug = (
        status_text.lower()
        .replace(" ", "-")
        .replace("/", "-")
    )
    label_map = {
        "Pre-Game": "Pre",
        "Scheduled": "Sched",
        "Warmup": "Warm",
        "In Progress": "Live",
        "Final": "Final",
    }
    label = label_map.get(status_text, status_text)
    title = f"Game status: {status_text}"
    return (
        f'<span class="status-pill status-{escape(status_slug, quote=True)}" '
        f'title="{escape(title, quote=True)}" aria-label="{escape(title, quote=True)}">'
        f"{escape(label)}</span>"
    )


def _render_team_cell(team_name: Any, team_abbrev: Any, team_id: Any, result: Any = "") -> str:
    name_text = str(team_name or "").strip()
    abbrev_text = str(team_abbrev or "").strip() or name_text[:3].upper()
    logo_url = escape(get_team_logo_src(team_id=team_id, team_abbrev=abbrev_text, team_name=name_text), quote=True)
    title = escape(name_text or abbrev_text, quote=True)
    result_html = _render_team_result_badge(result)
    return (
        '<span class="team-cell" title="'
        + title
        + '"><span class="team-badge"><img class="team-logo" src="'
        + logo_url
        + '" alt="'
        + title
        + ' logo"></span>'
        + result_html
        + "</span>"
    )


def _render_opponent_cell(
    opponent_name: Any,
    opponent_abbrev: Any,
    opponent_id: Any,
    start_time: Any,
    status: Any = "",
    total: Any = None,
) -> str:
    name_text = str(opponent_name or "").strip()
    abbrev_text = str(opponent_abbrev or "").strip() or name_text[:3].upper()
    logo_url = escape(get_team_logo_src(team_id=opponent_id, team_abbrev=abbrev_text, team_name=name_text), quote=True)
    title = escape(name_text or abbrev_text, quote=True)
    time_text = str(start_time or "").strip()
    time_html = (
        '<span class="opp-time">' + escape(time_text) + "</span>"
        if time_text
        else ""
    )
    status_html = _status_badge(status)
    total_html = _render_total_badge(total)
    meta_html = (
        '<span class="opp-meta">'
        + time_html
        + status_html
        + total_html
        + "</span>"
        if time_html or status_html or total_html
        else ""
    )
    return (
        '<span class="opp-cell" title="'
        + title
        + '"><span class="opp-team"><span class="team-badge"><img class="team-logo" src="'
        + logo_url
        + '" alt="'
        + title
        + ' logo"></span></span>'
        + meta_html
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


def _resolve_game_hit_result(
    game_logs: Sequence[Dict[str, Any]],
    game_id: Optional[int],
    current_game_line: Optional[Dict[str, Any]] = None,
) -> str:
    if current_game_line is not None:
        return "hit" if (_to_int(current_game_line.get("H") or current_game_line.get("hits")) or 0) >= 1 else "no-hit"

    target_game_id = _to_int(game_id)
    if target_game_id is None:
        return ""
    for row in game_logs:
        if _to_int(row.get("gamePk")) != target_game_id:
            continue
        return "hit" if (_to_int(row.get("hits")) or 0) >= 1 else "no-hit"
    return "no-hit"


def _resolve_game_home_run_result(
    game_logs: Sequence[Dict[str, Any]],
    game_id: Optional[int],
    current_game_line: Optional[Dict[str, Any]] = None,
) -> str:
    if current_game_line is not None:
        return (
            "home-run"
            if (_to_int(current_game_line.get("HR") or current_game_line.get("homeRuns")) or 0) >= 1
            else "no-home-run"
        )

    target_game_id = _to_int(game_id)
    if target_game_id is None:
        return ""
    for row in game_logs:
        if _to_int(row.get("gamePk")) != target_game_id:
            continue
        return "home-run" if (_to_int(row.get("homeRuns")) or 0) >= 1 else "no-home-run"
    return "no-home-run"


def _render_batter_name(
    name: Any,
    game_result: Any = "",
    *,
    marker_context: str = "hit",
    team_name: Any = "",
    team_abbrev: Any = "",
    team_id: Any = None,
) -> str:
    name_text = str(name or "").strip()
    result_text = str(game_result or "").strip().lower()
    team_name_text = str(team_name or "").strip()
    team_abbrev_text = str(team_abbrev or "").strip() or team_name_text[:3].upper()
    team_logo = ""
    if team_name_text or team_abbrev_text or team_id is not None:
        logo_url = escape(
            get_team_logo_src(team_id=team_id, team_abbrev=team_abbrev_text, team_name=team_name_text),
            quote=True,
        )
        team_title = escape(team_name_text or team_abbrev_text, quote=True)
        team_logo = (
            '<span class="team-badge batter-team-badge" title="'
            + team_title
            + '"><img class="team-logo" src="'
            + logo_url
            + '" alt="'
            + team_title
            + ' logo"></span>'
        )
    badge = ""
    if marker_context == "home_run":
        if result_text == "home-run":
            badge = '<span class="batter-game-mark batter-game-mark-hit" title="Had a home run in this final game">&#10003;</span>'
        elif result_text == "no-home-run":
            badge = '<span class="batter-game-mark batter-game-mark-no-hit" title="No home run in this final game">X</span>'
    else:
        if result_text == "hit":
            badge = '<span class="batter-game-mark batter-game-mark-hit" title="Had a hit in this final game">&#10003;</span>'
        elif result_text == "no-hit":
            badge = '<span class="batter-game-mark batter-game-mark-no-hit" title="No hit in this final game">X</span>'
    return (
        '<span class="batter-name">'
        + team_logo
        + '<span class="batter-name-text">'
        + escape(name_text)
        + "</span>"
        + badge
        + "</span>"
    )


def format_report_dataframe(
    df: pd.DataFrame,
    columns: Optional[Sequence[str]] = None,
    *,
    game_result_column: str = "Game Hit Result",
    marker_context: str = "hit",
) -> pd.DataFrame:
    report_columns = list(columns or REPORT_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=report_columns)

    formatted = df.copy()
    for column_name, default_value in (
        ("Team Result", ""),
        ("Team Score", pd.NA),
        ("Opponent Score", pd.NA),
        ("Total Result", ""),
        ("Final Total Runs", pd.NA),
    ):
        if column_name not in formatted.columns:
            formatted[column_name] = default_value
    formatted["Batter"] = [
        _render_batter_name(
            batter_name,
            game_result,
            marker_context=marker_context,
            team_name=team_name,
            team_abbrev=team_abbrev,
            team_id=team_id,
        )
        for batter_name, game_result, team_name, team_abbrev, team_id in zip(
            formatted["Batter"],
            formatted.get(game_result_column, pd.Series("", index=formatted.index)),
            formatted.get("Team", pd.Series("", index=formatted.index)),
            formatted.get("Team Abbrev", pd.Series("", index=formatted.index)),
            formatted.get("Team Id", pd.Series(pd.NA, index=formatted.index)),
        )
    ]
    formatted["Pitcher"] = formatted["Pitcher"].apply(
        lambda value: '<span class="pitcher-name">' + escape(_format_pitcher_last_name(value)) + "</span>"
    )
    if "Team" in formatted.columns:
        formatted["Team"] = [
            _render_team_cell(team_name, team_abbrev, team_id, team_result)
            for team_name, team_abbrev, team_id, team_result in zip(
                formatted["Team"],
                formatted.get("Team Abbrev", pd.Series("", index=formatted.index)),
                formatted.get("Team Id", pd.Series(pd.NA, index=formatted.index)),
                formatted.get("Team Result", pd.Series("", index=formatted.index)),
            )
        ]
    formatted["Opponent"] = [
        _render_opponent_cell(opponent_name, opponent_abbrev, opponent_id, start_time, status, total)
        for opponent_name, opponent_abbrev, opponent_id, start_time, status, total in zip(
            formatted["Opponent"],
            formatted["Opponent Abbrev"],
            formatted["Opponent Id"],
            formatted["Start"],
            formatted["Status"],
            formatted.get("Total", pd.Series(pd.NA, index=formatted.index)),
        )
    ]
    formatted["Status"] = formatted["Status"].apply(_status_badge)
    formatted["Total"] = [
        _render_total_cell(total, total_result, final_total_runs)
        for total, total_result, final_total_runs in zip(
            formatted["Total"],
            formatted["Total Result"],
            formatted["Final Total Runs"],
        )
    ]
    if "Hit Stk" in formatted.columns:
        formatted["Hit Stk"] = formatted["Hit Stk"].apply(_format_int)
    if "Recent AVG" in formatted.columns:
        formatted[f"Last {RECENT_GAMES} AVG"] = formatted["Recent AVG"].apply(_format_rate)
    if f"Last {RECENT_WINDOW_DAYS} AVG" in formatted.columns:
        formatted[f"Last {RECENT_WINDOW_DAYS} AVG"] = formatted[f"Last {RECENT_WINDOW_DAYS} AVG"].apply(_format_rate)
    for column in ["VsP AVG", "Season AVG"]:
        if column in formatted.columns:
            formatted[column] = formatted[column].apply(_format_rate)
    if "VsP H" in formatted.columns and "VsP AB" in formatted.columns:
        formatted["VsP H-AB"] = [
            _format_hit_ab(hits, at_bats)
            for hits, at_bats in zip(formatted["VsP H"], formatted["VsP AB"])
        ]
    if "VsP HR" in formatted.columns:
        formatted["VsP HR"] = formatted["VsP HR"].apply(_format_int)
    if "VsP PA" in formatted.columns:
        formatted["VsP PA"] = formatted["VsP PA"].apply(_format_int)
    formatted["VsP HR/PA"] = [
        _format_percentage_ratio(hr, pa)
        for hr, pa in zip(df.get("VsP HR", pd.Series(index=df.index)), df.get("VsP PA", pd.Series(index=df.index)))
    ]
    return formatted[report_columns]


def format_focus_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return format_report_dataframe(df)


def format_home_run_focus_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return format_report_dataframe(
        df,
        columns=HOME_RUN_REPORT_COLUMNS,
        game_result_column="Game Home Run Result",
        marker_context="home_run",
    )


def _build_focus_table_html(report_df: pd.DataFrame, raw_df: pd.DataFrame, *, focus_mode: str = "default") -> str:
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
        f"Last {RECENT_WINDOW_DAYS} AVG": "group-batter",
        "Season AVG": "group-batter",
        "VsP HR": "group-matchup",
        "VsP PA": "group-matchup",
        "VsP HR/PA": "group-matchup",
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
    compact_header_labels = {
        "Opponent": "Opp",
        "Pitcher": "P",
        "Total": "Tot",
        f"Last {RECENT_GAMES} AVG": f"L{RECENT_GAMES} AVG",
        f"Last {RECENT_WINDOW_DAYS} AVG": f"L{RECENT_WINDOW_DAYS} AVG",
        "Season AVG": "Season",
        "VsP AVG": "VsP",
        "VsP HR/PA": "HR/PA",
    }
    for col_name, col_index in column_map.items():
        if col_index >= len(header_cells):
            continue
        header_cell = header_cells[col_index]
        if col_name in compact_header_labels:
            header_cell.string = compact_header_labels[col_name]
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
        if focus_mode == "home_run":
            vsp_hr = _to_float(row_data.get("VsP HR")) or 0.0
            vsp_pa = _to_float(row_data.get("VsP PA")) or 0.0
            hr_rate_pct = (100.0 * vsp_hr / vsp_pa) if vsp_pa > 0 else 0.0
            if vsp_hr >= 2 or hr_rate_pct >= 20.0:
                row_classes.append("row-target")
        else:
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

        vsp_hr_value = _to_float(row_data.get("VsP HR"))
        if vsp_hr_value is not None:
            if vsp_hr_value >= 3:
                _add_cell_class(cells, column_map, "VsP HR", "cell-elite")
            elif vsp_hr_value >= 2:
                _add_cell_class(cells, column_map, "VsP HR", "cell-strong")

        vsp_hr_rate = _to_float(row_data.get("VsP HR"))
        vsp_pa = _to_float(row_data.get("VsP PA"))
        if vsp_hr_rate is not None and vsp_pa is not None and vsp_pa > 0:
            hr_rate_pct = 100.0 * vsp_hr_rate / vsp_pa
            if hr_rate_pct >= 20.0:
                _add_cell_class(cells, column_map, "VsP HR/PA", "cell-elite")
            elif hr_rate_pct >= 10.0:
                _add_cell_class(cells, column_map, "VsP HR/PA", "cell-strong")

        for column_name, thresholds, raw_key in (
            (f"Last {RECENT_GAMES} AVG", (0.330, 0.280, 0.220), "Recent AVG"),
            (f"Last {RECENT_WINDOW_DAYS} AVG", (0.320, 0.275, 0.220), f"Last {RECENT_WINDOW_DAYS} AVG"),
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


def write_html(
    streak_df: pd.DataFrame,
    hot_df: pd.DataFrame,
    home_run_df: pd.DataFrame,
    matchup_df: pd.DataFrame,
    report_key: str,
    display_date: str,
    *,
    write_root: bool = True,
) -> Path:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    updated_at = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    streak_table = _build_focus_table_html(format_focus_dataframe(streak_df), streak_df)
    hot_table = _build_focus_table_html(format_focus_dataframe(hot_df), hot_df)
    home_run_table = _build_focus_table_html(
        format_home_run_focus_dataframe(home_run_df),
        home_run_df,
        focus_mode="home_run",
    )
    matchup_table = _build_focus_table_html(format_focus_dataframe(matchup_df), matchup_df)
    root_tabs_html = build_report_tabs("batters", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_tabs_html = build_report_tabs("batters", display_date, root_page=False, reports_dir=REPORTS_DIR)
    root_date_nav_html = build_date_nav_html("batters", display_date, root_page=True, reports_dir=REPORTS_DIR)
    archive_date_nav_html = build_date_nav_html("batters", display_date, root_page=False, reports_dir=REPORTS_DIR)
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>MLB Batter Report {escape(display_date)}</title>
  <link rel="icon" href="__FAVICON_HREF__" type="image/svg+xml">
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
      padding: 12px;
      background: linear-gradient(180deg, #f8fbff 0%, var(--bg) 100%);
      color: var(--text);
      font-family: "Segoe UI", Tahoma, Geneva, Verdana, sans-serif;
    }}
    .layout {{
      max-width: 1560px;
      margin: 0 auto;
      display: grid;
      gap: 10px;
    }}
    .hero {{
      background: linear-gradient(135deg, #0f766e 0%, #0369a1 100%);
      color: #ffffff;
      border-radius: 10px;
      padding: 14px 16px;
      box-shadow: 0 10px 35px rgba(3, 105, 161, 0.20);
    }}
    .hero h1 {{
      margin: 0 0 4px;
      font-size: 22px;
      letter-spacing: 0;
    }}
    .hero p {{
      margin: 0;
      opacity: 0.95;
      font-size: 11px;
    }}
    .hero-nav-row {{
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 8px 10px;
      margin-top: 10px;
    }}
    .report-tabs {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin-top: 0;
    }}
    .report-tab {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      padding: 6px 11px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.24);
      background: rgba(255, 255, 255, 0.12);
      color: #ffffff;
      text-decoration: none;
      font-size: 12px;
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
      padding: 5px 8px;
      border-radius: 999px;
      border: 1px solid rgba(255, 255, 255, 0.24);
      background: rgba(255, 255, 255, 0.12);
      color: #ffffff;
      text-decoration: none;
      font-size: 10px;
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
      font-size: 10px;
      font-variant-numeric: tabular-nums;
    }}
    .panel {{
      background: var(--panel);
      border-radius: 10px;
      border: 1px solid var(--line);
      box-shadow: 0 6px 20px rgba(15, 23, 42, 0.06);
      overflow: hidden;
    }}
    .panel-legend {{
      padding: 7px 10px;
    }}
    .panel-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      padding: 8px 11px 7px;
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
    .panel-header.section-homer {{
      background: #fff1f2;
      border-bottom-color: #fecdd3;
    }}
    .panel-header.section-streak {{
      background: #eef6ff;
      border-bottom-color: #d5e2f4;
    }}
    .panel-header h2 {{
      margin: 0;
      font-size: 15px;
    }}
    .panel-header .note {{
      color: var(--muted);
      font-size: 10px;
      text-align: right;
    }}
    .table-wrap {{
      overflow-y: auto;
      overflow-x: auto;
      max-height: 82vh;
    }}
    .featured-tables {{
      display: grid;
      grid-template-columns: minmax(0, 1fr) minmax(0, 1fr);
      gap: 10px;
      align-items: start;
    }}
    .featured-column {{
      display: grid;
      gap: 10px;
      align-content: start;
      min-width: 0;
    }}
    .featured-tables .table-wrap {{
      max-height: min(58vh, 460px);
    }}
    .table-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 6px 10px;
      align-items: center;
      padding: 5px 8px 0;
      font-size: 10px;
      color: #334155;
    }}
    .page-legend {{
      padding: 0;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 4px;
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
      font-size: 10px;
    }}
    table.pitchers-table {{
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      font-size: 10px;
      table-layout: auto;
      min-width: 680px;
    }}
    .featured-tables table.pitchers-table {{
      min-width: 0;
      font-size: 9px;
    }}
    table.pitchers-table thead th {{
      position: sticky;
      top: 0;
      z-index: 2;
      background: var(--header);
      border-bottom: 1px solid var(--line);
      color: #0b2540;
      padding: 4px 3px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.05;
    }}
    table.pitchers-table thead th.group-context {{
      border-top: 3px solid var(--group-context);
      background: color-mix(in srgb, var(--group-context) 10%, var(--header));
    }}
    table.pitchers-table thead th.group-batter {{
      border-top: 3px solid var(--group-batter);
      background: color-mix(in srgb, var(--group-batter) 10%, var(--header));
    }}
    table.pitchers-table thead th.group-matchup {{
      border-top: 3px solid var(--group-matchup);
      background: color-mix(in srgb, var(--group-matchup) 11%, var(--header));
    }}
    table.pitchers-table tbody td {{
      border-bottom: 1px solid var(--line);
      padding: 3px 4px;
      text-align: center;
      white-space: nowrap;
      line-height: 1.05;
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
    .featured-tables table.pitchers-table thead th {{
      padding: 3px 2px;
    }}
    .featured-tables table.pitchers-table tbody td {{
      padding: 2px 2px;
    }}
    table.pitchers-table th.column-name,
    table.pitchers-table td.column-name {{
      width: 1%;
      min-width: 0;
      text-align: left;
      white-space: nowrap;
      overflow-wrap: normal;
    }}
    table.pitchers-table th.column-team,
    table.pitchers-table td.column-team {{
      min-width: 36px;
      max-width: 42px;
    }}
    table.pitchers-table th.column-opponent,
    table.pitchers-table td.column-opponent {{
      min-width: 108px;
      max-width: 126px;
    }}
    table.pitchers-table th.column-pitcher,
    table.pitchers-table td.column-pitcher {{
      width: 1%;
      min-width: 0;
      max-width: none;
      padding-left: 2px;
      padding-right: 2px;
      text-align: center;
      white-space: nowrap;
      overflow-wrap: normal;
    }}
    table.pitchers-table th.column-total,
    table.pitchers-table td.column-total {{
      min-width: 50px;
      max-width: 58px;
      padding-left: 2px;
      padding-right: 2px;
    }}
    table.pitchers-table th.column-status,
    table.pitchers-table td.column-status {{
      display: none;
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
      display: inline-flex;
      align-items: center;
      justify-content: flex-start;
      gap: 4px;
      text-align: left;
      width: auto;
      white-space: nowrap;
    }}
    .batter-name-text {{
      min-width: 0;
      white-space: nowrap;
    }}
    .batter-game-mark {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 14px;
      height: 14px;
      border-radius: 999px;
      font-size: 9px;
      font-weight: 800;
      line-height: 1;
      border: 1px solid transparent;
    }}
    .batter-game-mark-hit {{
      background: rgba(22, 163, 74, 0.12);
      border-color: rgba(22, 163, 74, 0.22);
      color: #166534;
    }}
    .batter-game-mark-no-hit {{
      background: rgba(220, 38, 38, 0.10);
      border-color: rgba(220, 38, 38, 0.18);
      color: #991b1b;
    }}
    .pitcher-name {{
      font-size: 9px;
      letter-spacing: 0;
    }}
    .team-cell,
    .opp-cell {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 3px;
      line-height: 1;
      text-align: center;
    }}
    .opp-cell {{
      flex-wrap: nowrap;
    }}
    .team-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      width: 24px;
      height: 24px;
      border-radius: 5px;
      background: #ffffff;
      box-shadow: inset 0 0 0 1px #e2e8f0;
      flex: 0 0 auto;
    }}
    .batter-team-badge {{
      width: 22px;
      height: 22px;
    }}
    .team-logo {{
      display: block;
      width: 21px;
      height: 21px;
      object-fit: contain;
    }}
    .batter-team-badge .team-logo {{
      width: 19px;
      height: 19px;
    }}
    .team-abbrev {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 4px;
      font-weight: 700;
      letter-spacing: 0.03em;
      color: #0f172a;
    }}
    .opp-team {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 3px;
    }}
    .opp-meta {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 2px;
      min-width: 0;
    }}
    .opp-time {{
      display: inline-block;
      padding: 1px 4px;
      border-radius: 999px;
      background: #f1f5f9;
      color: #334155;
      font-size: 8px;
      font-weight: 700;
      letter-spacing: 0.01em;
    }}
    .total-badge {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 25px;
      height: 14px;
      padding: 0 5px;
      border-radius: 999px;
      font-size: 8px;
      font-weight: 800;
      border: 1px solid transparent;
      line-height: 1;
      font-variant-numeric: tabular-nums;
    }}
    .total-badge.total-badge-elite {{
      background: #dcfce7;
      border-color: rgba(22, 163, 74, 0.26);
      color: #14532d;
    }}
    .total-badge.total-badge-strong {{
      background: #fef9c3;
      border-color: rgba(202, 138, 4, 0.26);
      color: #713f12;
    }}
    .total-badge.total-badge-weak {{
      background: #fee2e2;
      border-color: rgba(220, 38, 38, 0.24);
      color: #7f1d1d;
    }}
    .total-badge.total-badge-neutral {{
      background: #f1f5f9;
      border-color: #cbd5e1;
      color: #334155;
    }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 22px;
      height: 14px;
      padding: 0 4px;
      border-radius: 999px;
      font-weight: 800;
      font-size: 8px;
      border: 1px solid transparent;
      line-height: 1;
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
        padding: 12px;
      }}
      .hero h1 {{
        font-size: 20px;
      }}
      .hero-nav-row {{
        align-items: stretch;
      }}
      .report-tabs {{
        width: 100%;
      }}
      .date-nav {{
        width: 100%;
      }}
      .panel-header {{
        align-items: flex-start;
        flex-direction: column;
      }}
      .panel-header .note {{
        text-align: left;
      }}
      .featured-tables {{
        grid-template-columns: 1fr;
      }}
      .featured-column {{
        gap: 10px;
      }}
      .featured-tables .table-wrap {{
        max-height: 72vh;
      }}
      table.pitchers-table {{
        min-width: 640px;
      }}
      .featured-tables table.pitchers-table {{
        min-width: 640px;
      }}
    }}
  </style>
</head>
<body>
  <div class="layout">
    <section class="hero">
      <h1>MLB Daily Batter Report</h1>
      <p>{escape(display_date)} slate. Updated {escape(updated_at)}.</p>
      <div class="hero-nav-row">
        __TABS_HTML__
        __DATE_NAV_HTML__
      </div>
    </section>

    <section class="panel panel-legend">
      <div class="table-legend page-legend">
        <span class="legend-item legend-context"><span class="legend-swatch"></span>Context</span>
        <span class="legend-item legend-batter"><span class="legend-swatch"></span>Form</span>
        <span class="legend-item legend-matchup"><span class="legend-swatch"></span>Matchup</span>
        <span class="legend-note">Gray rail = fallback pool. Orange/gray rows = started or final. Opponent chips show time, status, and game total. Green/yellow/red = stronger to weaker totals or AVG.</span>
      </div>
    </section>

    <div class="featured-tables">
      <div class="featured-column">
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
          <div class="panel-header section-streak">
            <h2>Active Hit Streaks 6+ Games</h2>
            <div class="note">{ACTIVE_STREAK_SECTION_MIN}+ active hit streak.</div>
          </div>
          <div class="table-wrap">
            {streak_table}
          </div>
        </section>
      </div>

      <div class="featured-column">
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
          <div class="panel-header section-homer">
            <h2>Home Run History vs Scheduled Pitcher</h2>
            <div class="note">{HOME_RUN_MIN_HR}+ HR and {MATCHUP_MIN_PA}+ PA vs pitcher, sorted by VsP HR/PA.</div>
          </div>
          <div class="table-wrap">
            {home_run_table}
          </div>
        </section>
      </div>
    </div>
  </div>
</body>
</html>
"""

    output_path = REPORTS_DIR / f"batters-report-{report_key}.html"
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
        ROOT_BATTERS_FILE.write_text(root_html_content, encoding="utf-8")
    print(output_path.resolve().as_uri())
    return output_path


def build_report_rows(schedule: Sequence[Dict[str, Any]], report_date: str) -> List[Dict[str, Any]]:
    report_date_obj = dt.datetime.strptime(report_date, "%m/%d/%Y").date()
    report_year = report_date_obj.year
    stats_end_date = report_date_obj - dt.timedelta(days=1)
    lineup_locks = load_batter_lineup_locks()
    lineup_locks_changed = False
    espn_event_snapshots = build_espn_event_snapshot_lookup(report_date)
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
        game_id = _to_int(game.get("game_id"))
        event_snapshot = espn_event_snapshots.get((_normalize_team_name(away_team), _normalize_team_name(home_team))) or {}
        event_id = str(event_snapshot.get("event_id") or "").strip()
        espn_summary = fetch_espn_summary(event_id) if event_id else None
        game_total = extract_espn_game_total(espn_summary)
        away_score = _to_int(event_snapshot.get("away_score"))
        home_score = _to_int(event_snapshot.get("home_score"))
        total_result = _final_total_result(status, game_total, away_score, home_score)
        final_total_runs = (away_score + home_score) if away_score is not None and home_score is not None else None
        current_game_batter_lines = (
            fetch_game_batter_stat_lines(int(game_id))
            if status == "Final" and game_id is not None
            else {}
        )
        offense_configs = [
            {
                "team_id": away_team_id,
                "team_name": away_team,
                "team_abbrev": str((team_meta_map.get(away_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "opponent_id": home_team_id,
                "opponent_name": home_team,
                "opponent_abbrev": str((team_meta_map.get(home_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "pitcher_name": str(game.get("home_probable_pitcher") or "").strip(),
                "team_score": away_score,
                "opponent_score": home_score,
            },
            {
                "team_id": home_team_id,
                "team_name": home_team,
                "team_abbrev": str((team_meta_map.get(home_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "opponent_id": away_team_id,
                "opponent_name": away_team,
                "opponent_abbrev": str((team_meta_map.get(away_team_id) or {}).get("abbreviation") or "").strip().upper(),
                "pitcher_name": str(game.get("away_probable_pitcher") or "").strip(),
                "team_score": home_score,
                "opponent_score": away_score,
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
                stats_end_date=stats_end_date,
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
                pitcher_id=int(pitcher_context["id"]),
                start_time=start_time,
                status=status,
                game_id=game_id,
                team_score=_to_int(offense.get("team_score")),
                opponent_score=_to_int(offense.get("opponent_score")),
                team_result=_final_team_result(status, offense.get("team_score"), offense.get("opponent_score")),
                total_result=total_result,
                final_total_runs=final_total_runs,
                roster_entries=roster_entries,
                people_by_id=people_by_id,
                report_date=report_date_obj,
                current_game_batter_lines=current_game_batter_lines,
            )
            if not candidate_rows:
                continue

            lineup_entries = extract_confirmed_espn_lineup(espn_summary, team_abbrev) if espn_summary and team_abbrev else []
            lineup_player_ids = resolve_lineup_player_ids(lineup_entries, roster_entries, team_id) if lineup_entries else []
            lineup_player_ids, changed = _resolve_lineup_ids_for_game_state(
                report_date=report_date,
                game_id=game_id,
                team_id=team_id,
                pitcher_id=int(pitcher_context["id"]),
                status=status,
                confirmed_lineup_player_ids=lineup_player_ids,
                lineup_locks=lineup_locks,
            )
            lineup_locks_changed = lineup_locks_changed or changed
            selected_rows = select_offense_rows(candidate_rows, lineup_player_ids)
            rows.extend(selected_rows)

    if lineup_locks_changed:
        save_batter_lineup_locks(lineup_locks)

    return rows


def main(raw_date_input: str, *, allow_roll_forward: bool = True, write_root: bool = True) -> None:
    report_date = resolve_date_input(raw_date_input)
    report_date, schedule = resolve_effective_report_date_and_schedule(
        report_date,
        allow_roll_forward=allow_roll_forward,
    )
    report_key = report_date.replace("/", "")
    rows = build_report_rows(schedule, report_date)
    final_df = sort_batters_for_report(apply_hot_scores(rows))
    report_date_obj = dt.datetime.strptime(report_date, "%m/%d/%Y").date()
    final_df = verify_historical_bvp_for_feature_candidates(final_df, report_date_obj)
    streak_df = build_active_hit_streak_section(final_df)
    hot_df = build_hot_streak_matchup_section(final_df)
    home_run_df = build_home_run_matchup_section(final_df)
    matchup_df = build_good_matchups_section(final_df, hot_df)
    write_html(
        streak_df,
        hot_df,
        home_run_df,
        matchup_df,
        report_key,
        report_date,
        write_root=write_root,
    )


def _parse_cli_args(argv: Sequence[str]) -> tuple[str, bool, bool]:
    if len(argv) < 2:
        print("Usage: python3 Batters.py <today|tmrw|MM/DD|MM/DD/YYYY> [--exact] [--no-root]")
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
