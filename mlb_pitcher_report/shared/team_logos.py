from __future__ import annotations

import base64
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

TEAM_LOGO_ROOT = Path(__file__).resolve().parents[2] / "mlb_teams_logo_svg"
DEFAULT_LOGO_VARIANT = "light"
TEAM_LOGO_DATA_URI_CACHE: Dict[Tuple[str, str], str] = {}

TEAM_LOGO_DEFINITIONS = (
    {"code": "ari", "team_id": 109, "abbrevs": ("ARI",), "names": ("Arizona Diamondbacks", "Diamondbacks")},
    {"code": "atl", "team_id": 144, "abbrevs": ("ATL",), "names": ("Atlanta Braves", "Braves")},
    {"code": "bal", "team_id": 110, "abbrevs": ("BAL",), "names": ("Baltimore Orioles", "Orioles")},
    {"code": "bos", "team_id": 111, "abbrevs": ("BOS",), "names": ("Boston Red Sox", "Red Sox")},
    {"code": "chc", "team_id": 112, "abbrevs": ("CHC", "CCB"), "names": ("Chicago Cubs", "Cubs")},
    {"code": "cin", "team_id": 113, "abbrevs": ("CIN",), "names": ("Cincinnati Reds", "Reds")},
    {"code": "cle", "team_id": 114, "abbrevs": ("CLE",), "names": ("Cleveland Guardians", "Guardians")},
    {"code": "col", "team_id": 115, "abbrevs": ("COL",), "names": ("Colorado Rockies", "Rockies")},
    {"code": "cws", "team_id": 145, "abbrevs": ("CWS", "CHW"), "names": ("Chicago White Sox", "White Sox")},
    {"code": "det", "team_id": 116, "abbrevs": ("DET",), "names": ("Detroit Tigers", "Tigers")},
    {"code": "hou", "team_id": 117, "abbrevs": ("HOU",), "names": ("Houston Astros", "Astros")},
    {"code": "kc", "team_id": 118, "abbrevs": ("KC", "KCR"), "names": ("Kansas City Royals", "Royals")},
    {"code": "laa", "team_id": 108, "abbrevs": ("LAA",), "names": ("Los Angeles Angels", "Angels")},
    {"code": "lad", "team_id": 119, "abbrevs": ("LAD",), "names": ("Los Angeles Dodgers", "Dodgers")},
    {"code": "mia", "team_id": 146, "abbrevs": ("MIA",), "names": ("Miami Marlins", "Marlins")},
    {"code": "mil", "team_id": 158, "abbrevs": ("MIL",), "names": ("Milwaukee Brewers", "Brewers")},
    {"code": "min", "team_id": 142, "abbrevs": ("MIN",), "names": ("Minnesota Twins", "Twins")},
    {"code": "nym", "team_id": 121, "abbrevs": ("NYM",), "names": ("New York Mets", "Mets")},
    {"code": "nyy", "team_id": 147, "abbrevs": ("NYY",), "names": ("New York Yankees", "Yankees")},
    {"code": "oak", "team_id": 133, "abbrevs": ("OAK", "ATH"), "names": ("Oakland Athletics", "Athletics")},
    {"code": "phi", "team_id": 143, "abbrevs": ("PHI",), "names": ("Philadelphia Phillies", "Phillies")},
    {"code": "pit", "team_id": 134, "abbrevs": ("PIT",), "names": ("Pittsburgh Pirates", "Pirates")},
    {"code": "sd", "team_id": 135, "abbrevs": ("SD", "SDP"), "names": ("San Diego Padres", "Padres")},
    {"code": "sea", "team_id": 136, "abbrevs": ("SEA",), "names": ("Seattle Mariners", "Mariners")},
    {"code": "sf", "team_id": 137, "abbrevs": ("SF", "SFG"), "names": ("San Francisco Giants", "Giants")},
    {"code": "stl", "team_id": 138, "abbrevs": ("STL",), "names": ("St. Louis Cardinals", "Cardinals")},
    {"code": "tb", "team_id": 139, "abbrevs": ("TB", "TBR", "TPA"), "names": ("Tampa Bay Rays", "Rays")},
    {"code": "tex", "team_id": 140, "abbrevs": ("TEX",), "names": ("Texas Rangers", "Rangers")},
    {"code": "tor", "team_id": 141, "abbrevs": ("TOR",), "names": ("Toronto Blue Jays", "Blue Jays")},
    {"code": "wsh", "team_id": 120, "abbrevs": ("WSH", "WSN", "WAS"), "names": ("Washington Nationals", "Nationals")},
)

TEAM_LOGO_CODE_BY_ID = {item["team_id"]: item["code"] for item in TEAM_LOGO_DEFINITIONS}
TEAM_LOGO_CODE_BY_ABBREV = {
    abbrev.upper(): item["code"]
    for item in TEAM_LOGO_DEFINITIONS
    for abbrev in item["abbrevs"]
}


def _normalize_team_name(name: Any) -> str:
    text = str(name or "").strip().lower()
    text = text.replace(".", "").replace("'", "")
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(text.split())


TEAM_LOGO_CODE_BY_NAME = {
    _normalize_team_name(name): item["code"]
    for item in TEAM_LOGO_DEFINITIONS
    for name in item["names"]
}


def resolve_team_logo_code(
    *,
    team_id: Any = None,
    team_abbrev: Any = None,
    team_name: Any = None,
) -> Optional[str]:
    try:
        team_id_value = int(team_id)
    except (TypeError, ValueError):
        team_id_value = None

    if team_id_value is not None:
        code = TEAM_LOGO_CODE_BY_ID.get(team_id_value)
        if code:
            return code

    abbrev = str(team_abbrev or "").strip().upper()
    if abbrev:
        code = TEAM_LOGO_CODE_BY_ABBREV.get(abbrev)
        if code:
            return code

    normalized_name = _normalize_team_name(team_name)
    if normalized_name:
        return TEAM_LOGO_CODE_BY_NAME.get(normalized_name)

    return None


def resolve_team_logo_file_path(
    *,
    team_id: Any = None,
    team_abbrev: Any = None,
    team_name: Any = None,
    variant: str = DEFAULT_LOGO_VARIANT,
) -> Optional[Path]:
    code = resolve_team_logo_code(team_id=team_id, team_abbrev=team_abbrev, team_name=team_name)
    if not code:
        return None

    variants = []
    for candidate in (variant, DEFAULT_LOGO_VARIANT, "dark"):
        normalized = str(candidate or "").strip().lower()
        if normalized and normalized not in variants:
            variants.append(normalized)

    for current_variant in variants:
        suffix = "l" if current_variant == "light" else "d"
        path = TEAM_LOGO_ROOT / current_variant / f"{code}_{suffix}.svg"
        if path.exists():
            return path

    return None


def get_team_logo_src(
    *,
    team_id: Any = None,
    team_abbrev: Any = None,
    team_name: Any = None,
    variant: str = DEFAULT_LOGO_VARIANT,
) -> str:
    code = resolve_team_logo_code(team_id=team_id, team_abbrev=team_abbrev, team_name=team_name)
    if not code:
        return ""

    cache_key = (str(variant or DEFAULT_LOGO_VARIANT).strip().lower(), code)
    cached = TEAM_LOGO_DATA_URI_CACHE.get(cache_key)
    if cached is not None:
        return cached

    path = resolve_team_logo_file_path(
        team_id=team_id,
        team_abbrev=team_abbrev,
        team_name=team_name,
        variant=variant,
    )
    if path is None:
        TEAM_LOGO_DATA_URI_CACHE[cache_key] = ""
        return ""

    data_uri = "data:image/svg+xml;base64," + base64.b64encode(path.read_bytes()).decode("ascii")
    TEAM_LOGO_DATA_URI_CACHE[cache_key] = data_uri
    return data_uri
