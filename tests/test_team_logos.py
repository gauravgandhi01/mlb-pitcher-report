import unittest

from Batters import _render_team_cell
from Matchups import _render_team_chip
from Pitchers import _render_opponent_with_start
from team_logos import get_team_logo_src, resolve_team_logo_file_path


class TeamLogoTests(unittest.TestCase):
    def test_resolve_team_logo_file_path_handles_abbreviation_aliases(self) -> None:
        self.assertEqual(
            resolve_team_logo_file_path(team_abbrev="SFG").name,
            "sf_l.svg",
        )
        self.assertEqual(
            resolve_team_logo_file_path(team_abbrev="CHW").name,
            "cws_l.svg",
        )
        self.assertEqual(
            resolve_team_logo_file_path(team_abbrev="WSN").name,
            "wsh_l.svg",
        )
        self.assertEqual(
            resolve_team_logo_file_path(team_abbrev="ATH").name,
            "oak_l.svg",
        )

    def test_get_team_logo_src_uses_local_svg_assets(self) -> None:
        src = get_team_logo_src(team_name="Boston Red Sox")
        self.assertTrue(src.startswith("data:image/svg+xml;base64,"))
        self.assertNotIn("mlbstatic.com/team-logos", src)

    def test_pitchers_render_uses_local_logo_source(self) -> None:
        html = _render_opponent_with_start("Washington Nationals", "7:10p", "Scheduled")
        self.assertIn("data:image/svg+xml;base64,", html)
        self.assertNotIn("mlbstatic.com/team-logos", html)

    def test_batters_render_uses_local_logo_source(self) -> None:
        html = _render_team_cell("Chicago White Sox", "CHW", 145)
        self.assertIn("data:image/svg+xml;base64,", html)
        self.assertNotIn("mlbstatic.com/team-logos", html)

    def test_matchups_render_uses_local_logo_source(self) -> None:
        html = _render_team_chip(
            team_id=120,
            team_name="Washington Nationals",
            team_abbrev="WSN",
            side_value="-",
            side_value_class="side-value",
            starter_label="Irvin (L)",
        )
        self.assertIn("data:image/svg+xml;base64,", html)
        self.assertNotIn("mlbstatic.com/team-logos", html)


if __name__ == "__main__":
    unittest.main()
