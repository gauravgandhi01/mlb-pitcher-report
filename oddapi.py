import requests
import pandas as pd
from typing import Dict, List, Any
import statsapi
import sys
import json
from datetime import datetime
from unidecode import unidecode

# Constants
IGNORED_BOOKMAKERS = ['mybookieag', 'betmgm', 'superbook', 'bovada', 'prophetx']
API_BASE_URL = "https://api.the-odds-api.com/v4/sports"

def load_api_keys() -> List[str]:
    """Load API keys from the configuration file."""
    with open("/Users/ggandhi001/Documents/MLB_2024/keys.json", "r") as config_file:
        config = json.load(config_file)
        return config["api_keys"]

def make_api_request(url: str, params: Dict[str, Any] = None) -> requests.Response:
    """Make an API request and handle errors."""
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        print(f"\033[91mAPI request failed: {e}\033[0m")
        raise

def check_api_requests_remaining() -> str:
    """Check which API key has sufficient requests remaining."""
    api_keys = load_api_keys()
    for key in api_keys:
        url = f"{API_BASE_URL}/?apiKey={key}"
        try:
            response = make_api_request(url)
            requests_remaining = response.headers.get("X-Requests-Remaining")
            if requests_remaining and int(requests_remaining) > 30:
                print(f"\033[92mUsing API Key: {key} | Requests remaining: {requests_remaining}\033[0m")
                return key
        except Exception:
            continue
    print("No API keys with sufficient requests remaining.")
    return None

api_key = check_api_requests_remaining()

def get_event_id_by_team(team_name: str, api_key: str, date: str) -> str:
    """Retrieve the event ID for a given team."""
    url = f"{API_BASE_URL}/baseball_mlb/events?apiKey={api_key}"
    try:
        response = make_api_request(url)
        games = response.json()
        for game in games:
            if team_name in (game["home_team"], game["away_team"]):
                return game["id"]
    except Exception as e:
        print(f"An error occurred while fetching event ID: {e}")
    return None

def fetch_game_data(event_id: str, api_key: str) -> Dict[str, Any]:
    """Fetch game data for a specific event."""
    url = f"{API_BASE_URL}/baseball_mlb/events/{event_id}/odds"
    params = {
        "apiKey": api_key,
        "regions": "us,us_ex",
        "markets": "pitcher_strikeouts",
        "oddsFormat": "american"
    }
    response = make_api_request(url, params)
    return response.json()

def collect_pitcher_points(game_data: Dict[str, Any]) -> Dict[str, List[float]]:
    """Collect all points for each pitcher."""
    pitcher_points: Dict[str, List[float]] = {}
    for bookmaker in game_data.get('bookmakers', []):
        if bookmaker['key'] in IGNORED_BOOKMAKERS:
            continue
        for market in bookmaker['markets']:
            if market['key'] == 'pitcher_strikeouts':
                for outcome in market['outcomes']:
                    pitcher = outcome['description']
                    point = outcome['point']
                    pitcher_points.setdefault(pitcher, []).append(point)
    return pitcher_points

def determine_most_common_points(pitcher_points: Dict[str, List[float]]) -> Dict[str, float]:
    """Determine the most common point for each pitcher."""
    return {pitcher: max(set(points), key=points.count) for pitcher, points in pitcher_points.items()}

def process_bookmaker_outcomes(
    bookmaker: Dict[str, Any], 
    most_common_points: Dict[str, float]
) -> List[Dict[str, str]]:
    """Process outcomes for a single bookmaker."""
    if bookmaker['key'] in IGNORED_BOOKMAKERS:
        return []

    data: List[Dict[str, str]] = []
    for market in bookmaker['markets']:
        if market['key'] == 'pitcher_strikeouts':
            over_under_dict: Dict[tuple, Dict[str, str]] = {}
            for outcome in market['outcomes']:
                pitcher = outcome['description']
                point = outcome['point']
                price = f'+{outcome["price"]}' if outcome["price"] >= 0 else str(outcome["price"])

                if point == most_common_points.get(pitcher):
                    over_under_dict.setdefault((pitcher, point), {})[outcome['name']] = price

            if not over_under_dict:
                for outcome in market['outcomes']:
                    pitcher = outcome['description']
                    point = outcome['point']
                    price = f'+{outcome["price"]}' if outcome["price"] >= 0 else str(outcome["price"])
                    over_under_dict.setdefault((pitcher, point), {})[outcome['name']] = price

            for (pitcher, point), odds in over_under_dict.items():
                data.append({
                    'pitcher': pitcher,
                    f'{bookmaker["title"]}': f"{point}: {odds.get('Over', 'N/A')}|{odds.get('Under', 'N/A')}"
                })
    return data

def build_dataframe(data: List[Dict[str, str]], pitcher_name: str) -> pd.DataFrame:
    """Build and filter the DataFrame."""
    df = pd.DataFrame(data)
    df_pivot = df.pivot_table(index='pitcher', aggfunc='first').reset_index()
    clean_pitcher_name = unidecode(pitcher_name)
    df_filtered = df_pivot[df_pivot['pitcher'] == clean_pitcher_name] if clean_pitcher_name else df_pivot
    df_filtered = df_filtered.copy()  # Avoid SettingWithCopyWarning
    df_filtered.loc[:, 'pitcher'] = pitcher_name
    return df_filtered

def get_pitcher_odds(event_id: str, api_key: str, pitcher_name: str) -> pd.DataFrame:
    """Main function to get pitcher odds."""
    try:
        game_data = fetch_game_data(event_id, api_key)
        if not game_data.get('bookmakers'):
            return pd.DataFrame()

        pitcher_points = collect_pitcher_points(game_data)
        most_common_points = determine_most_common_points(pitcher_points)

        data: List[Dict[str, str]] = []
        for bookmaker in game_data['bookmakers']:
            data.extend(process_bookmaker_outcomes(bookmaker, most_common_points))

        return build_dataframe(data, pitcher_name)
    except Exception as e:
        print(f"\033[91mAn error occurred while fetching pitcher odds: {e}\033[0m")
        return pd.DataFrame()

def get_pitcher_team(pitcher_name: str) -> str:
    """Retrieve the team name for a given pitcher."""
    try:
        player = statsapi.lookup_player(pitcher_name)
        team_id = player[0]['currentTeam']['id']
        team_name = statsapi.get('team', {'teamId': team_id})['teams'][0]['name']
        return "Oakland Athletics" if team_name == "Athletics" else team_name
    except Exception as e:
        print(f"\033[91mCould not get team for pitcher {pitcher_name}: {e}\033[0m")
        return None

def get_pitcher_odds_by_team(pitcher_name: str, date: str) -> pd.DataFrame:
    """Retrieve pitcher odds by team."""
    try:
        team_name = get_pitcher_team(pitcher_name)
        if not team_name:
            return None

        event_id = get_event_id_by_team(team_name, api_key, date)
        if not event_id:
            print(f"\033[91mEvent ID not found for team: {team_name}\033[0m")
            return None

        pitcher_odds = get_pitcher_odds(event_id, api_key, pitcher_name)
        if pitcher_odds.empty:
            print(f"\033[93mOdds not found for pitcher: {pitcher_name}\033[0m")
            return None

        return pitcher_odds
    except Exception as e:
        print(f"An error occurred: {pitcher_name}, {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Please provide the first and last name of the pitcher as arguments.")
    else:
        name = f"{sys.argv[1]} {sys.argv[2]}"
        pitcher_odds = get_pitcher_odds_by_team(name, datetime.now().strftime("%Y-%m-%d"))
        if pitcher_odds is not None and not pitcher_odds.empty:
            print(pitcher_odds)