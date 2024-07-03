import requests
import pandas as pd
import statsapi
import sys

def get_event_id_by_team(team_name,apiKey):
    api_url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey={apiKey}" 
    try:
        # Make an API request to get the data
        response = requests.get(api_url)
        response.raise_for_status()
        
        # Parse the JSON response
        games = response.json()

        # Search for the team in the list of games
        for game in games:
            if game["home_team"] == team_name or game["away_team"] == team_name:
                return game["id"]
        
        # If the team is not found, return None or raise an error
        return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while requesting the API: {e}")
        return None


def get_pitcher_odds(event_id, apiKey, pitcher_name):
    # Define the URL template
    url_template = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds?apiKey={apiKey}&regions=us&markets=pitcher_strikeouts&oddsFormat=american"
    try:
        # Make an API request to get the data
        response = requests.get(url_template)
        response.raise_for_status()

        # Parse the JSON response
        game_data = response.json()

        # Initialize an empty list to collect rows of data
        data = []

        ignored_bookmakers = ['mybookieag', 'betrivers', 'superbook','bovada','betonlineag']

        # Iterate over each bookmaker in the response
        for bookmaker in game_data['bookmakers']:
            # Skip the bookmaker with the key 'mybookieag'
            if bookmaker['key'] in ignored_bookmakers:
                continue

            # Iterate over each market in the bookmaker
            for market in bookmaker['markets']:
                if market['key'] == 'pitcher_strikeouts':
                    # Iterate over each outcome in the market
                    outcomes = market['outcomes']
                    over_under_dict = {}
                    for outcome in outcomes:
                        pitcher = outcome['description']
                        point = outcome['point']
                        if (pitcher, point) not in over_under_dict:
                            over_under_dict[(pitcher, point)] = {}
                        over_under_dict[(pitcher, point)][outcome['name']] = outcome['price']
                    
                    # Append the relevant data to the list
                    for (pitcher, point), odds in over_under_dict.items():
                        data.append({
                            'pitcher': pitcher,
                            'point': point,
                            f'{bookmaker["title"]}': f"{odds.get('Over', 'N/A')}/{odds.get('Under', 'N/A')}"
                        })

        # Create a DataFrame from the collected data
        df = pd.DataFrame(data)

        # Pivot the DataFrame to combine all bookmakers' odds into columns
        df_pivot = df.pivot_table(
            index=['pitcher', 'point'],
            aggfunc='first'
        ).reset_index()

        if pitcher_name:
            df_filtered = df_pivot[df_pivot['pitcher'] == pitcher_name]
        else:
            df_filtered = df_pivot
        
        return df_filtered

        
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while requesting the API: {e}")
        return None
    
def get_pitcher_team(pitcher_name):
    try:
        player = statsapi.lookup_player(pitcher_name)
        team_id = player[0]['currentTeam']['id']
        team_name = (statsapi.get('team', {'teamId':team_id}))['teams'][0]['name']
        return team_name
    except Exception as e:
        print(f"Could not get pitcher {pitcher_name} team with error: {e}")
        
def get_pitcher_odds_by_team(pitcher_name):
    apiKey = "902e39fd15173a0d554965e843efcfd2"
    team_name = get_pitcher_team(pitcher_name)
    try:
        # Step 1: Get the event ID for the specified team
        event_id = get_event_id_by_team(team_name, apiKey)
        if event_id is None:
            print(f"Event ID not found for team: {team_name}")
            return None
        
        # Step 2: Get the pitcher odds for the retrieved event ID
        pitcher_odds = get_pitcher_odds(event_id, apiKey, pitcher_name)
        
        if pitcher_odds is None:
            print(f"odds not found: {pitcher_name}")
            return None
            
        return pitcher_odds
    
    except Exception as e:
        print(f"An error occurred: {pitcher_name}, {e}")

