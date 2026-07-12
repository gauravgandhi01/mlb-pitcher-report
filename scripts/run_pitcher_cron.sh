#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="${CRON_REPO_DIR:-/Users/ggandhi001/mlb-pitcher-report}"
BRANCH="${CRON_BRANCH:-master}"
REPORT_DATE="${1:-today}"
INCLUDE_ODDS="${2:-y}"
MODE="${3:-frequent}"
LOG_FILE="${CRON_LOG_FILE:-$REPO_DIR/cron.log}"
LOG_TO_FILE="${CRON_LOG_TO_FILE:-1}"
DRY_RUN="${CRON_DRY_RUN:-0}"
SKIP_GIT_SYNC="${CRON_SKIP_GIT_SYNC:-0}"

# Keep cron environment predictable.
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  echo "[$(timestamp)] $*"
}

is_nonnegative_int() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

rotate_log() {
  local max_bytes="${CRON_LOG_MAX_BYTES:-5242880}"
  local backups="${CRON_LOG_BACKUPS:-5}"

  if ! is_nonnegative_int "$max_bytes" || ! is_nonnegative_int "$backups"; then
    echo "Invalid CRON_LOG_MAX_BYTES or CRON_LOG_BACKUPS." >&2
    exit 2
  fi
  if [[ "$max_bytes" == "0" || ! -f "$LOG_FILE" ]]; then
    return
  fi

  local current_size
  current_size="$(wc -c < "$LOG_FILE" | tr -d ' ')"
  if (( current_size < max_bytes )); then
    return
  fi

  if (( backups == 0 )); then
    : > "$LOG_FILE"
    return
  fi

  rm -f "$LOG_FILE.$backups"
  local index
  for ((index = backups - 1; index >= 1; index--)); do
    if [[ -f "$LOG_FILE.$index" ]]; then
      mv "$LOG_FILE.$index" "$LOG_FILE.$((index + 1))"
    fi
  done
  mv "$LOG_FILE" "$LOG_FILE.1"
}

setup_logging() {
  if [[ "$LOG_TO_FILE" != "1" ]]; then
    return
  fi
  mkdir -p "$(dirname "$LOG_FILE")"
  rotate_log
  exec >> "$LOG_FILE" 2>&1
}

run_cmd() {
  if [[ "$DRY_RUN" == "1" ]]; then
    printf 'DRY RUN:'
    printf ' %q' "$@"
    printf '\n'
    return 0
  fi
  "$@"
}

date_for_offset() {
  local offset="$1"
  python3 - "$offset" <<'PY'
import datetime as dt
import os
import sys

raw_today = os.environ.get("CRON_TODAY")
if raw_today:
    today = dt.datetime.strptime(raw_today, "%m/%d/%Y").date()
else:
    today = dt.date.today()
print((today + dt.timedelta(days=int(sys.argv[1]))).strftime("%m/%d/%Y"))
PY
}

report_key() {
  local date_text="$1"
  echo "${date_text//\//}"
}

archive_files_for_date() {
  local date_text="$1"
  local key
  key="$(report_key "$date_text")"
  printf '%s\n' \
    "reports/report-${key}.html" \
    "reports/batters-report-${key}.html" \
    "reports/matchups-report-${key}.html" \
    "reports/matchups-detail-report-${key}.html"
}

append_archive_files() {
  local date_text file
  for date_text in "$@"; do
    while IFS= read -r file; do
      ARCHIVE_FILES+=("$file")
    done < <(archive_files_for_date "$date_text")
  done
}

file_in_list() {
  local needle="$1"
  local candidate
  shift
  for candidate in "$@"; do
    if [[ "$candidate" == "$needle" ]]; then
      return 0
    fi
  done
  return 1
}

run_root_generation() {
  run_cmd python3 Pitchers.py "$REPORT_DATE" "$INCLUDE_ODDS"
  run_cmd python3 Batters.py "$REPORT_DATE"
  run_cmd python3 Matchups.py "$REPORT_DATE"
}

run_archive_generation() {
  local archive_date="$1"
  run_cmd python3 Pitchers.py "$archive_date" "$INCLUDE_ODDS" --exact --no-root
  run_cmd python3 Batters.py "$archive_date" --exact --no-root
  run_cmd python3 Matchups.py "$archive_date" --exact --no-root
}

stage_existing_files() {
  local existing_files=()
  local file
  for file in "$@"; do
    if [[ -e "$file" ]]; then
      existing_files+=("$file")
      PUBLISH_FILES+=("$file")
    fi
  done
  if (( ${#existing_files[@]} > 0 )); then
    run_cmd git add -f -- "${existing_files[@]}"
  fi
}

revert_publish_files() {
  if (( ${#PUBLISH_FILES[@]} == 0 )); then
    return
  fi
  git restore --staged --worktree -- "${PUBLISH_FILES[@]}" 2>/dev/null || true
}

setup_logging
log "Starting MLB report cron mode=$MODE report_date=$REPORT_DATE odds=$INCLUDE_ODDS dry_run=$DRY_RUN"

if [[ "$INCLUDE_ODDS" != "y" && "$INCLUDE_ODDS" != "n" ]]; then
  log "INCLUDE_ODDS must be y or n."
  exit 2
fi
if [[ "$MODE" != "frequent" && "$MODE" != "archive" ]]; then
  log "MODE must be frequent or archive."
  exit 2
fi

cd "$REPO_DIR"

if [[ "$SKIP_GIT_SYNC" == "1" ]]; then
  log "Skipping git sync because CRON_SKIP_GIT_SYNC=1."
else
  run_cmd git fetch origin "$BRANCH"
  run_cmd git pull --rebase --autostash origin "$BRANCH"
fi

YESTERDAY_DATE="$(date_for_offset -1)"
TODAY_DATE="$(date_for_offset 0)"
TOMORROW_DATE="$(date_for_offset 1)"
ROOT_FILES=(index.html batters.html matchups.html matchups-detail.html)
LINEUP_LOCK_FILE="report_state/batter-lineup-locks.json"
PUBLISH_FILES=()
ARCHIVE_FILES=()
FULL_WINDOW_ARCHIVE_FILES=()
STALE_ARCHIVE_FILES=()

if [[ "$MODE" == "frequent" ]]; then
  run_root_generation
  append_archive_files "$TODAY_DATE" "$TOMORROW_DATE"
else
  for ARCHIVE_DATE in "$YESTERDAY_DATE" "$TOMORROW_DATE"; do
    run_archive_generation "$ARCHIVE_DATE"
  done
  append_archive_files "$YESTERDAY_DATE" "$TOMORROW_DATE"
fi

while IFS= read -r file; do
  FULL_WINDOW_ARCHIVE_FILES+=("$file")
done < <(
  archive_files_for_date "$YESTERDAY_DATE"
  archive_files_for_date "$TODAY_DATE"
  archive_files_for_date "$TOMORROW_DATE"
)

while IFS= read -r TRACKED_FILE; do
  if [[ -n "$TRACKED_FILE" ]] && ! file_in_list "$TRACKED_FILE" "${FULL_WINDOW_ARCHIVE_FILES[@]}"; then
    STALE_ARCHIVE_FILES+=("$TRACKED_FILE")
  fi
done < <(git ls-files 'reports/*.html')

if [[ "$DRY_RUN" == "1" ]]; then
  log "Dry run complete before staging and publishing."
  exit 0
fi

if ! git diff --cached --quiet; then
  log "Refusing to publish because pre-existing staged changes are present."
  exit 1
fi

if (( ${#STALE_ARCHIVE_FILES[@]} > 0 )); then
  git rm --cached --ignore-unmatch -- "${STALE_ARCHIVE_FILES[@]}"
  PUBLISH_FILES+=("${STALE_ARCHIVE_FILES[@]}")
fi

if [[ "$MODE" == "frequent" ]]; then
  stage_existing_files "${ROOT_FILES[@]}"
fi
stage_existing_files "${ARCHIVE_FILES[@]}"
if [[ -f "$LINEUP_LOCK_FILE" ]]; then
  stage_existing_files "$LINEUP_LOCK_FILE"
fi

if git diff --cached --quiet; then
  log "No GH Pages report changes to commit."
  exit 0
fi

if ! python3 scripts/cron_publish_utils.py staged-has-substantive-change --repo "$REPO_DIR" "${PUBLISH_FILES[@]}"; then
  log "Only timestamp changes detected; reverting generated publish artifacts."
  revert_publish_files
  exit 0
fi

COMMIT_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
git commit -m "auto: update GH Pages reports (${COMMIT_TS})"
git push origin "$BRANCH"

log "Root pages and archive reports updated and pushed."
