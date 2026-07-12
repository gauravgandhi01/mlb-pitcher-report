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

    def test_process_bookmaker_outcomes_includes_betonlineag(self) -> None:
        bookmaker = {
            "key": "betonlineag",
            "title": "BetOnline",
            "markets": [
                {
                    "key": "pitcher_strikeouts",
                    "outcomes": [
                        {
                            "description": "Alpha Ace",
                            "name": "Over",
                            "point": 5.5,
                            "price": 110,
                        },
                        {
                            "description": "Alpha Ace",
                            "name": "Under",
                            "point": 5.5,
                            "price": -130,
                        },
                    ],
                }
            ],
        }

        self.assertEqual(
            oddapi.process_bookmaker_outcomes(bookmaker, oddapi.IGNORED_BOOKMAKERS),
            [{"pitcher": "Alpha Ace", "BetOnline.ag": "5.5: +110|-130"}],
        )


if __name__ == "__main__":
    unittest.main()
