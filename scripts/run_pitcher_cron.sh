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

python3 Pitchers.py "$REPORT_DATE" "$INCLUDE_ODDS"

# Only publish the GH Pages entrypoint.
git add index.html
if git diff --cached --quiet; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] No index.html change to commit."
  exit 0
fi

COMMIT_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
git commit -m "auto: update index.html (${COMMIT_TS})"
git push origin "$BRANCH"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] index.html updated and pushed."
