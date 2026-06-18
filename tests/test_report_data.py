import unittest
from unittest.mock import patch

from report_data import resolve_effective_report_date_and_schedule


class ReportDataDateResolutionTests(unittest.TestCase):
    def test_resolve_effective_report_date_rolls_forward_by_default(self) -> None:
        completed_schedule = [{"status": "Final"}]
        next_schedule = [{"status": "Scheduled"}]

        with patch("report_data.fetch_schedule", side_effect=[completed_schedule, next_schedule]):
            report_date, schedule = resolve_effective_report_date_and_schedule("06/17/2026")

        self.assertEqual(report_date, "06/18/2026")
        self.assertEqual(schedule, next_schedule)

    def test_resolve_effective_report_date_respects_exact_mode(self) -> None:
        completed_schedule = [{"status": "Final"}]

        with patch("report_data.fetch_schedule", return_value=completed_schedule):
            report_date, schedule = resolve_effective_report_date_and_schedule(
                "06/17/2026",
                allow_roll_forward=False,
            )

        self.assertEqual(report_date, "06/17/2026")
        self.assertEqual(schedule, completed_schedule)


if __name__ == "__main__":
    unittest.main()
