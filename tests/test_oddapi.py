import unittest
from unittest.mock import patch

import oddapi


class OddsApiTests(unittest.TestCase):
    def test_request_event_id_matches_athletics_alias(self) -> None:
        games = [
            {
                "id": "game-123",
                "away_team": "Athletics",
                "home_team": "Los Angeles Angels",
            }
        ]

        with patch.object(oddapi, "_fetch_events_for_date", return_value=games):
            self.assertEqual(
                oddapi.request_event_id("Oakland Athletics", "fake-key", "2026-06-27", {}),
                "game-123",
            )
            self.assertEqual(
                oddapi.request_event_id("Athletics", "fake-key", "2026-06-27", {}),
                "game-123",
            )


if __name__ == "__main__":
    unittest.main()
