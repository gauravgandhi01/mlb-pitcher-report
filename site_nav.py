from __future__ import annotations

import datetime as dt
from html import escape
from pathlib import Path
from typing import Dict, List

DATE_PILL_SPECS = [(-1, "Yesterday"), (0, "Today"), (1, "Tomorrow")]
REPORT_TAB_SPECS = [
    ("pitchers", "Pitchers"),
    ("batters", "Batters"),
    ("matchups", "Matchups"),
]
PAGE_CONFIGS: Dict[str, Dict[str, str]] = {
    "pitchers": {
        "root_href": "./index.html",
        "archive_template": "report-{report_key}.html",
    },
    "batters": {
        "root_href": "./batters.html",
        "archive_template": "batters-report-{report_key}.html",
    },
    "matchups": {
        "root_href": "./matchups.html",
        "archive_template": "matchups-report-{report_key}.html",
    },
    "matchups_detail": {
        "root_href": "./matchups-detail.html",
        "archive_template": "matchups-detail-report-{report_key}.html",
    },
}


def parse_report_date(report_date: str) -> dt.date:
    return dt.datetime.strptime(report_date, "%m/%d/%Y").date()


def format_report_date(value: dt.date) -> str:
    return value.strftime("%m/%d/%Y")


def report_key_for_date(report_date: str) -> str:
    return report_date.replace("/", "")


def archive_filename(page_key: str, report_date: str) -> str:
    config = PAGE_CONFIGS[page_key]
    return config["archive_template"].format(report_key=report_key_for_date(report_date))


def archive_output_path(reports_dir: Path, page_key: str, report_date: str) -> Path:
    return reports_dir / archive_filename(page_key, report_date)


def _archive_href(page_key: str, report_date: str, *, root_page: bool) -> str:
    file_name = archive_filename(page_key, report_date)
    if root_page:
        return f"./reports/{file_name}"
    return f"./{file_name}"


def _date_short_label(value: dt.date) -> str:
    return value.strftime("%m/%d")


def _is_current_archive_window(report_date: str) -> bool:
    target_date = parse_report_date(report_date)
    today = dt.date.today()
    return today - dt.timedelta(days=1) <= target_date <= today + dt.timedelta(days=1)


def archive_target_available(reports_dir: Path, page_key: str, report_date: str) -> bool:
    return archive_output_path(reports_dir, page_key, report_date).exists() or _is_current_archive_window(report_date)


def _render_nav_item(
    *,
    classes: List[str],
    label: str,
    href: str | None,
    title: str,
) -> str:
    class_attr = " ".join(classes)
    if href:
        return (
            f'<a class="{escape(class_attr, quote=True)}" href="{escape(href, quote=True)}" '
            f'title="{escape(title, quote=True)}">{label}</a>'
        )
    return f'<span class="{escape(class_attr, quote=True)}" title="{escape(title, quote=True)}">{label}</span>'


def build_report_tabs(active_tab: str, report_date: str, *, root_page: bool, reports_dir: Path) -> str:
    items: List[str] = []
    for page_key, label_text in REPORT_TAB_SPECS:
        classes = ["report-tab"]
        label = escape(label_text)
        title = f"{label_text} report for {report_date}"
        if page_key == active_tab:
            classes.append("active")
            items.append(_render_nav_item(classes=classes, label=label, href=None, title=title))
            continue

        if root_page:
            href = PAGE_CONFIGS[page_key]["root_href"]
            items.append(_render_nav_item(classes=classes, label=label, href=href, title=title))
            continue

        if archive_target_available(reports_dir, page_key, report_date):
            href = _archive_href(page_key, report_date, root_page=False)
            items.append(_render_nav_item(classes=classes, label=label, href=href, title=title))
            continue

        classes.append("disabled")
        items.append(_render_nav_item(classes=classes, label=label, href=None, title=f"{title} unavailable"))

    return '<nav class="report-tabs" aria-label="Report pages">' + "".join(items) + "</nav>"


def build_date_nav_html(page_key: str, display_date: str, *, root_page: bool, reports_dir: Path) -> str:
    display_value = parse_report_date(display_date)
    today = dt.date.today()
    items: List[str] = []
    for offset, label_text in DATE_PILL_SPECS:
        target_date = today + dt.timedelta(days=offset)
        target_report_date = format_report_date(target_date)
        classes = ["date-pill"]
        label = (
            f'<span class="date-pill-label">{escape(label_text)}</span>'
            f'<span class="date-pill-date">{escape(_date_short_label(target_date))}</span>'
        )
        title = f"{label_text} archive ({target_report_date})"
        if target_date == display_value:
            classes.append("active")
            items.append(_render_nav_item(classes=classes, label=label, href=None, title=title))
            continue

        if archive_target_available(reports_dir, page_key, target_report_date):
            href = _archive_href(page_key, target_report_date, root_page=root_page)
            items.append(_render_nav_item(classes=classes, label=label, href=href, title=title))
            continue

        classes.append("disabled")
        items.append(_render_nav_item(classes=classes, label=label, href=None, title=f"{title} unavailable"))

    return '<nav class="date-nav" aria-label="Archive dates">' + "".join(items) + "</nav>"
