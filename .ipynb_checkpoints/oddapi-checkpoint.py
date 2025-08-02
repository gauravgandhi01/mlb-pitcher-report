import requests
import pandas as pd
import statsapi
import sys
import re
import json
from datetime import datetime
from unidecode import unidecode
import os

api_keys = ["0bf29db6f2a47d9e964a60de5bf76403", "0c200413868939ad4b64f2e3d5ce62f4", "6f4d0c92ba85b73e0fd67ac845029811",
            "f8f4c83dd169a44d1608277864b09d5a","731c329d776683129f78ed81e3ab9184","b8317555a3744dce345d6db9cb7800b6",
            "00fda838241863db7f1fd1ea9fdbd78a","55ec4bd87aea046fc351c8d68e4dd328","3e88a28a2bc2444f465a6e963361d1da",
            "cffd30ea013eaa0747127817dde562c2", "c922bea515dbcef837e52114e6964396", "c324cbb60ac6a5e14881647005a37811",
            "1b3109ba63e8d64f5cb4e2f17966f64f", "10e223a68ab7d271c639ade9fd85fb97", "a24289e659ac1876bae5f18f69ee713c",
            "1077dd7546b262bcc9f78cfb256fa221", "902e39fd15173a0d554965e843efcfd2", "ac43375f6bc4c7a7dec46b44c7616f04",
            "1e602cfb3705b78dabb152e3546e43fd", "d3abf07c56764e34b789e7b2ecd99a29", "cfb930450ccbe0a6d4a379bc9b6ae487",
            "35d29fe90c6cf778f44be0c3aae146d1", "4b294e2edfb1498c7e8691bde772d5b6","cdcd73c9d6427d8ccadb3a6752a542d6",
            "035e8bde68020eed6101ac865ef4bf5d", "17238192c09bd284bd2d4737c0a54093", "8aafcbbb8a85af34706a51eed7eb685c",
            "b3680bec01e055359448418e881b2902","0e15983d57f797e9dc39f4d45eb019ff", "fcd2708ef53e658a3e03230237bc8e48",
            "90b0c4eac63044381bcd27770d8b1e13", "a85ade7f4d8e56e2eaaeb61eb7687c8f", "731cdf707f49672eb389e7a26ab22463",
            "073dd447e04726f1088bfd18b7ac8ca7", "f06fecb84bc2aa397ef1c3ce47e0fc41", "269eb487b1e5c6dc2dd5611e839f17fa",
            "0cd7e6a57989de7ded48a44b79e99110","3c654b8fb42902b915c504fa1331771c", "122047bcedcbedced72b4b6b4c871ac9",
            "1d67241326df96c5ded305e06966f813", "3b2bedf6aa3eaa2aeae32b9749331122", "d4168975303ea7a9f9c1b26893641eb0"]

def check_api_requests_remaining(api_keys):
    for key in api_keys:
        url = f"https://api.the-odds-api.com/v4/sports/?apiKey={key}"
        try:
            response = requests.get(url)
            response.raise_for_status()

            # Get the X-Requests-Remaining header
            requests_remaining = response.headers.get("X-Requests-Remaining")

            if requests_remaining is not None:
                requests_remaining = int(requests_remaining)
                if requests_remaining > 30:
                    print(f"\033[92mUsing API Key: {key} | Requests remaining: {requests_remaining}\033[0m")
                    return key  # Return the current key if it has sufficient requests
            else:
                print(f"API Key: {key} | X-Requests-Remaining header not found.")
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while checking API key {key}: {e}")

    print("No API keys with sufficient requests remaining.")
    return None

api_key = check_api_requests_remaining(api_keys)

def get_event_id_by_team(team_name, api_key, date):
    output_file = "ids/event_ids.json"

    if os.path.exists(output_file):
        with open(output_file, "r") as file:
            event_data = json.load(file)
    else:
        event_data = {}

    # if date in event_data and team_name in event_data[date]:
    #     return event_data[date][team_name]

    try:
        event_id = request_event_id(team_name, api_key, date, event_data)
        return event_id
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while requesting the API: {e}")
        return None

def request_event_id(team_name, api_key, date, event_data):

    api_url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey={api_key}" 


    response = requests.get(api_url)
    response.raise_for_status()
    games = response.json()

    for game in games:
        if team_name in (game["home_team"], game["away_team"]):
            event_id = game["id"]

            # Save the result to the dictionary
            if date not in event_data:
                event_data[date] = {}
            event_data[date][team_name] = event_id

            # Write the updated dictionary to the file
            with open("ids/event_ids.json", "w") as file:
                json.dump(event_data, file, indent=4)

            return event_id


def get_pitcher_odds(event_id, apiKey, pitcher_name):
    url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds"
    params = {
        "apiKey": apiKey,
        "regions": "us,us_ex",
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
        ignored_bookmakers = ['mybookieag','betmgm', 'superbook', 'bovada','prophetx']

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
        print(f"\033[91mAn error occurred while requesting the API: {e}\033[0m")
        return pd.DataFrame()

def get_pitcher_team(pitcher_name):
    try:
        player = statsapi.lookup_player(pitcher_name)
        team_id = player[0]['currentTeam']['id']
        team_name = statsapi.get('team', {'teamId': team_id})['teams'][0]['name']
        if team_name == "Athletics":
            team_name = "Oakland Athletics"
        return team_name
    except Exception as e:
        print(f"\033[91mCould not get pitcher {pitcher_name} team with error: {e}\033[0m")
        return None

def get_pitcher_odds_by_team(pitcher_name, date):
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
