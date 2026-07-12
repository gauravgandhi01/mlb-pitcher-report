from __future__ import annotations

import argparse
import re
import subprocess
from pathlib import Path
from typing import Iterable, Optional

PITCHER_UPDATED_RE = re.compile(
    r'(<p class="updated-at">Last updated: )[^<]+(</p>)'
)
SLATE_UPDATED_RE = re.compile(
    r"(Updated )\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [A-Za-z_/+-]+"
)
JSON_UPDATED_AT_RE = re.compile(
    r'("updated_at"\s*:\s*")[^"]+(")'
)


def normalize_publish_content(content: str) -> str:
    normalized = PITCHER_UPDATED_RE.sub(r"\1__TIMESTAMP__\2", content)
    normalized = SLATE_UPDATED_RE.sub(r"\1__TIMESTAMP__", normalized)
    normalized = JSON_UPDATED_AT_RE.sub(r'\1__TIMESTAMP__\2', normalized)
    return normalized


def _git_blob(repo: Path, ref: str, file_path: str) -> Optional[str]:
    blob_ref = f":{file_path}" if ref == ":" else f"{ref}:{file_path}"
    result = subprocess.run(
        ["git", "show", blob_ref],
        cwd=repo,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return result.stdout


def staged_file_has_substantive_change(repo: Path, file_path: str) -> bool:
    head_content = _git_blob(repo, "HEAD", file_path)
    staged_content = _git_blob(repo, ":", file_path)
    if head_content is None and staged_content is None:
        return False
    if head_content is None or staged_content is None:
        return True
    return normalize_publish_content(head_content) != normalize_publish_content(staged_content)


def staged_files_have_substantive_changes(repo: Path, file_paths: Iterable[str]) -> bool:
    return any(staged_file_has_substantive_change(repo, file_path) for file_path in file_paths)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cron publish helpers.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    normalize_parser = subparsers.add_parser("normalize")
    normalize_parser.add_argument("file", nargs="?")

    staged_parser = subparsers.add_parser("staged-has-substantive-change")
    staged_parser.add_argument("files", nargs="+")
    staged_parser.add_argument("--repo", default=".")

    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.command == "normalize":
        if args.file:
            content = Path(args.file).read_text(encoding="utf-8")
        else:
            import sys

            content = sys.stdin.read()
        print(normalize_publish_content(content), end="")
        return 0

    if args.command == "staged-has-substantive-change":
        has_change = staged_files_have_substantive_changes(Path(args.repo), args.files)
        return 0 if has_change else 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
