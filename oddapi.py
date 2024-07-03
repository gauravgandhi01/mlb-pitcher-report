import requests
import pandas as pd
import statsapi
import sys
import re
from unidecode import unidecode
import os

# set api_key to an os env
api_key = os.getenv("ODDS_API_KEY")
if not api_key:
    api_key="731c329d776683129f78ed81e3ab9184"

def get_event_id_by_team(team_name, api_key):
    api_url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey={api_key}" 
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        games = response.json()

        for game in games:
            if team_name in (game["home_team"], game["away_team"]):
                return game["id"]
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while requesting the API: {e}")
        return None

def get_pitcher_odds(event_id, apiKey, pitcher_name):
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds"
    params = {
        "apiKey": apiKey,
        "regions": "us",
        "markets": "pitcher_strikeouts",
        "oddsFormat": "american"
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        game_data = response.json()

        if not game_data.get('bookmakers'):
            return pd.DataFrame()  # Return an empty DataFrame if no bookmakers are found
        
        data = []
        ignored_bookmakers = ['mybookieag', 'betrivers', 'superbook', 'bovada', 'betonlineag']
        
        for bookmaker in game_data['bookmakers']:
            if bookmaker['key'] in ignored_bookmakers:
                continue

            for market in bookmaker['markets']:
                if market['key'] == 'pitcher_strikeouts':
                    over_under_dict = {}
                    for outcome in market['outcomes']:
                        pitcher = outcome['description']
                        point = outcome['point']
                        if (pitcher, point) not in over_under_dict:
                            over_under_dict[(pitcher, point)] = {}

                        price = outcome['price']
                        price = f'+{price}' if price >= 0 else str(price)

                        over_under_dict[(pitcher, point)][outcome['name']] = price
                    
                    for (pitcher, point), odds in over_under_dict.items():
                        data.append({
                            'pitcher': pitcher,
                            f'{bookmaker["title"]}': f"{point}: {odds.get('Over', 'N/A')}|{odds.get('Under', 'N/A')}"
                        })

        df = pd.DataFrame(data)
        df_pivot = df.pivot_table(index='pitcher', aggfunc='first').reset_index()
        
        clean_pitcher_name = unidecode(pitcher_name)
    
        if clean_pitcher_name:
            df_filtered = df_pivot[df_pivot['pitcher'] == clean_pitcher_name]
        else:
            df_filtered = df_pivot

        df_filtered.loc[:, 'pitcher'] = pitcher_name

        return df_filtered
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while requesting the API: {e}")
        return pd.DataFrame()

def get_pitcher_team(pitcher_name):
    try:
        player = statsapi.lookup_player(pitcher_name)
        team_id = player[0]['currentTeam']['id']
        team_name = statsapi.get('team', {'teamId': team_id})['teams'][0]['name']
        return team_name
    except Exception as e:
        print(f"Could not get pitcher {pitcher_name} team with error: {e}")
        return None

def get_pitcher_odds_by_team(pitcher_name):
    try:
        team_name = get_pitcher_team(pitcher_name)
        if not team_name:
            print(f"Team not found for pitcher: {pitcher_name}")
            return None

        event_id = get_event_id_by_team(team_name, api_key)
        if not event_id:
            print(f"Event ID not found for team: {team_name}")
            return None

        pitcher_odds = get_pitcher_odds(event_id, api_key, pitcher_name)
        if pitcher_odds.empty:
            print(f"Odds not found for pitcher: {pitcher_name}")
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
        pitcher_odds = get_pitcher_odds_by_team(name)
        if pitcher_odds is not None and not pitcher_odds.empty:
            print(pitcher_odds)
