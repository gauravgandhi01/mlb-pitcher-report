import datetime as dt
import unittest
from pathlib import Path

from site_nav import build_date_nav_html, build_report_tabs, format_report_date, report_key_for_date


class SiteNavTests(unittest.TestCase):
    def test_build_date_nav_uses_root_and_archive_relative_paths(self) -> None:
        today = dt.date.today()
        yesterday = today - dt.timedelta(days=1)
        tomorrow = today + dt.timedelta(days=1)
        display_date = format_report_date(today)

        root_html = build_date_nav_html("pitchers", display_date, root_page=True, reports_dir=Path("/tmp/missing"))
        archive_html = build_date_nav_html("pitchers", display_date, root_page=False, reports_dir=Path("/tmp/missing"))

        self.assertIn(f'href="./reports/report-{report_key_for_date(format_report_date(yesterday))}.html"', root_html)
        self.assertIn(f'href="./reports/report-{report_key_for_date(format_report_date(tomorrow))}.html"', root_html)
        self.assertIn(f'href="./report-{report_key_for_date(format_report_date(yesterday))}.html"', archive_html)
        self.assertIn(f'href="./report-{report_key_for_date(format_report_date(tomorrow))}.html"', archive_html)
        self.assertIn('class="date-pill active"', root_html)

    def test_build_report_tabs_disable_old_archive_targets_outside_window(self) -> None:
        old_date = format_report_date(dt.date.today() - dt.timedelta(days=10))
        old_key = report_key_for_date(old_date)

        html = build_report_tabs("pitchers", old_date, root_page=False, reports_dir=Path("/tmp/missing"))

        self.assertIn('class="report-tab disabled"', html)
        self.assertNotIn(f'href="./batters-report-{old_key}.html"', html)
        self.assertNotIn(f'href="./matchups-report-{old_key}.html"', html)


if __name__ == "__main__":
    unittest.main()
