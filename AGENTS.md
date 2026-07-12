# MLB Pitcher Report Agent Guide

## Project Purpose

This project generates static daily MLB report pages:

- `index.html`: pitcher strikeout report.
- `batters.html`: batter streak and matchup report.
- `matchups.html`: matchup summary page.
- `matchups-detail.html`: matchup detail page.
- `reports/*.html`: date-stamped archive pages for the rolling publish window.

The reports are intended for GitHub Pages style publishing. The root HTML files are the current public pages, and the archive files support yesterday/today/tomorrow navigation.

## Source Layout

- `Pitchers.py`, `Batters.py`, `Matchups.py`: compatibility entrypoints. Keep these names working for cron and manual usage.
- `mlb_pitcher_report/reports/`: report-specific builders and renderers.
- `mlb_pitcher_report/shared/`: shared schedule, StatsAPI, ESPN, weather, navigation, and logo helpers.
- `mlb_pitcher_report/odds/`: odds API integration used by the pitcher report.
- `scripts/run_pitcher_cron.sh`: production publish workflow.
- `scripts/cron_publish_utils.py`: publish-change normalization helpers.
- `tests/`: behavior and rendering regression coverage.
- `mlb_teams_logo_svg/`: runtime logo assets used by `team_logos`.

## Generation Flow

1. Resolve the requested date (`today`, `tmrw`, `MM/DD`, or `MM/DD/YYYY`).
2. Fetch MLB schedule and roll forward when allowed if the selected slate has no not-started games.
3. Build report-specific datasets from MLB StatsAPI, ESPN scoreboard/summary data, Open-Meteo park weather, pybaseball, and odds data where enabled.
4. Render the root page and a matching archive page under `reports/`.
5. Cron stages and publishes only root files, the rolling archive window, and `report_state/batter-lineup-locks.json` when substantive content changed.

## Common Commands

Run the full test suite:

```bash
python3 -m pytest
```

Generate current root pages:

```bash
python3 Pitchers.py today y
python3 Batters.py today
python3 Matchups.py today
```

Generate archive-only pages for an exact date:

```bash
python3 Pitchers.py 07/12/2026 y --exact --no-root
python3 Batters.py 07/12/2026 --exact --no-root
python3 Matchups.py 07/12/2026 --exact --no-root
```

Dry-run the cron workflow:

```bash
CRON_DRY_RUN=1 CRON_LOG_TO_FILE=0 CRON_SKIP_GIT_SYNC=1 scripts/run_pitcher_cron.sh today y frequent
```

## Artifact Policy

Keep source, tests, root generated pages, `favicon.svg`, `requirements.txt`, `report_state/batter-lineup-locks.json`, logo SVGs, and the rolling archive window. Treat stale historical `reports/*.html`, logs, caches, local docs dumps, and secret/key files as disposable local artifacts.

`reports/` is ignored because the cron workflow force-adds the current publish window. Do not assume every file under `reports/` is source.

## Compatibility Rules

- Do not change report scoring, odds selection, lineup selection, roll-forward behavior, filenames, CLI arguments, or link structure during cleanup-only work.
- Keep the top-level module names importable. Tests and ad-hoc workflows import private helpers from `Pitchers`, `Batters`, and `Matchups`.
- Prefer small, mechanical refactors with `python3 -m pytest` after each risky move.
- If rendered HTML changes are intentional, explain the user-visible reason and add or update tests. Cleanup refactors should avoid intentional output changes.

