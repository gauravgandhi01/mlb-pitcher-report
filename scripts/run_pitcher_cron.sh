#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="/Users/ggandhi001/mlb-pitcher-report"
BRANCH="master"
REPORT_DATE="${1:-today}"
INCLUDE_ODDS="${2:-y}"

# Keep cron environment predictable.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

cd "$REPO_DIR"

# If a virtualenv exists, use it.
if [[ -f "$REPO_DIR/venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$REPO_DIR/venv/bin/activate"
fi

# Rebase onto remote to avoid non-fast-forward push errors.
git fetch origin "$BRANCH"
git pull --rebase --autostash origin "$BRANCH"

ARCHIVE_DATES=()
while IFS= read -r ARCHIVE_DATE; do
  if [[ -n "$ARCHIVE_DATE" ]]; then
    ARCHIVE_DATES+=("$ARCHIVE_DATE")
  fi
done < <(python3 - <<'PY'
import datetime as dt

today = dt.date.today()
for offset in (-1, 0, 1):
    print((today + dt.timedelta(days=offset)).strftime("%m/%d/%Y"))
PY
)

TODAY_ARCHIVE_DATE="${ARCHIVE_DATES[1]}"

for ARCHIVE_DATE in "${ARCHIVE_DATES[@]}"; do
  ARCHIVE_ODDS="n"
  if [[ "$ARCHIVE_DATE" == "$TODAY_ARCHIVE_DATE" ]]; then
    ARCHIVE_ODDS="$INCLUDE_ODDS"
  fi

  python3 Pitchers.py "$ARCHIVE_DATE" "$ARCHIVE_ODDS" --exact --no-root
  python3 Batters.py "$ARCHIVE_DATE" --exact --no-root
  python3 Matchups.py "$ARCHIVE_DATE" --exact --no-root
done

python3 Pitchers.py "$REPORT_DATE" "$INCLUDE_ODDS"
python3 Batters.py "$REPORT_DATE"
python3 Matchups.py "$REPORT_DATE"

ROOT_FILES=(index.html batters.html matchups.html matchups-detail.html)
ARCHIVE_FILES=()
for ARCHIVE_DATE in "${ARCHIVE_DATES[@]}"; do
  REPORT_KEY="${ARCHIVE_DATE//\//}"
  ARCHIVE_FILES+=(
    "reports/report-${REPORT_KEY}.html"
    "reports/batters-report-${REPORT_KEY}.html"
    "reports/matchups-report-${REPORT_KEY}.html"
    "reports/matchups-detail-report-${REPORT_KEY}.html"
  )
done

archive_file_is_expected() {
  local candidate="$1"
  local expected_file
  for expected_file in "${ARCHIVE_FILES[@]}"; do
    if [[ "$expected_file" == "$candidate" ]]; then
      return 0
    fi
  done
  return 1
}

TRACKED_ARCHIVE_FILES=()
while IFS= read -r TRACKED_FILE; do
  if [[ -n "$TRACKED_FILE" ]]; then
    TRACKED_ARCHIVE_FILES+=("$TRACKED_FILE")
  fi
done < <(git ls-files 'reports/*.html')

STALE_ARCHIVE_FILES=()
for TRACKED_FILE in "${TRACKED_ARCHIVE_FILES[@]}"; do
  if ! archive_file_is_expected "$TRACKED_FILE"; then
    STALE_ARCHIVE_FILES+=("$TRACKED_FILE")
  fi
done

if [[ ${#STALE_ARCHIVE_FILES[@]} -gt 0 ]]; then
  git rm --cached --ignore-unmatch -- "${STALE_ARCHIVE_FILES[@]}"
fi

# Publish the root entrypoints and the rolling 3-day archive window.
git add "${ROOT_FILES[@]}"
git add -f "${ARCHIVE_FILES[@]}"
if git diff --cached --quiet; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] No GH Pages root HTML change to commit."
  exit 0
fi

COMMIT_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
git commit -m "auto: update GH Pages reports (${COMMIT_TS})"
git push origin "$BRANCH"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Root pages and 3-day archive reports updated and pushed."
