import pandas as pd
from bs4 import BeautifulSoup
import requests
import statsapi
from concurrent.futures import ThreadPoolExecutor
from pybaseball import team_batting
import sys
import datetime
from oddapi import get_pitcher_odds_by_team
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials


def parse_pitcher_stats(raw_data, name):
    lines = [line.strip() for line in raw_data.split('\n') if line.strip()]
    relevant_lines = lines[2:]
    data = {}
    for line in relevant_lines:
        key, value = line.split(':')
        data[key.strip()] = value.strip()
    specific_fields = {
        "Name": name,
        "GP": data.get("gamesPlayed"),
        "AB": data.get("atBats"),
        "BB": data.get("baseOnBalls"),
        "AVG": data.get("avg"),
        "K": data.get("strikeOuts"),
        "K/9": data.get("strikeoutsPer9Inn")
    }
    return specific_fields
    
def get_game_id_by_probable_pitcher(date, pitcher_name):
    games = statsapi.schedule(start_date=date, end_date=date)
    for game in games:
        if game['status'] in ['Pre-Game', 'Scheduled', 'Warmup']:
            return None
        else:
            if (game['away_probable_pitcher'] == pitcher_name or
                    game['home_probable_pitcher'] == pitcher_name):
                return game.get('game_id')
    return None

def get_strikeouts_by_player_name(date, full_name):
    id = get_game_id_by_probable_pitcher(date, full_name)
    if not id:
        return "N/A"
    data = statsapi.boxscore_data(id, timecode=None)
    # Find the player ID for the given full name
    player_info = data.get('playerInfo', {})
    player_id = None
    for pid, player_data in player_info.items():
        if player_data.get('fullName') == full_name:
            player_id = pid
            break
    
    if not player_id:
        return None  # Return None if the player is not found
    
    # Retrieve the strikeouts for the found player ID
    players = data.get('home', {}).get('players', {})
    player_data = players.get(player_id, {})
    if not player_data:
        players = data.get('away', {}).get('players', {})
        player_data = players.get(player_id, {})
    k = player_data.get('stats', {}).get('pitching', {}).get('strikeOuts', 0)
    return k

def fetch_pitcher_stats(name, team, opponent, status):
    try:
        player = statsapi.lookup_player(name)
        if not player:
            raise ValueError(f"Player {name} not found")
        player_id = player[0]['id']
        stats = statsapi.player_stats(player_id, group="[pitching]", type="season")
        pitcher_stats = parse_pitcher_stats(stats, name)
        pitcher_stats["Opponent"] = opponent
        pitcher_stats["Status"] = status
        return pitcher_stats
    except Exception as e:
        return {"Name": name, "Team": team, "Opponent": opponent, "Error": str(e)}

def fetch_pitcher_odds(name):
    try:
     return get_pitcher_odds_by_team(name, date)
    except Exception as e:
        return {"Name": name, "Error": str(e)}

def get_team_full_name(abbreviation):
    team_mapping = {
        "SEA": "Seattle Mariners",
        "OAK": "Oakland Athletics",
        "ATH": "Athletics",
        "CIN": "Cincinnati Reds",
        "BOS": "Boston Red Sox",
        "COL": "Colorado Rockies",
        "PIT": "Pittsburgh Pirates",
        "TBR": "Tampa Bay Rays",
        "DET": "Detroit Tigers",
        "MIN": "Minnesota Twins",
        "CHC": "Chicago Cubs",
        "ATL": "Atlanta Braves",
        "MIL": "Milwaukee Brewers",
        "CHW": "Chicago White Sox",
        "LAA": "Los Angeles Angels",
        "STL": "St. Louis Cardinals",
        "WSN": "Washington Nationals",
        "LAD": "Los Angeles Dodgers",
        "PHI": "Philadelphia Phillies",
        "BAL": "Baltimore Orioles",
        "SFG": "San Francisco Giants",
        "MIA": "Miami Marlins",
        "TEX": "Texas Rangers",
        "NYM": "New York Mets",
        "ARI": "Arizona Diamondbacks",
        "CLE": "Cleveland Guardians",
        "TOR": "Toronto Blue Jays",
        "NYY": "New York Yankees",
        "SDP": "San Diego Padres",
        "KCR": "Kansas City Royals",
        "HOU": "Houston Astros"
    }
    return team_mapping.get(abbreviation, "Unknown")

def make_hyperlink(name):
    url = f"https://statmuse.com/mlb/ask/{name.replace(' ', '%20')}-k-log"
    return f'<a href="{url}">{name}</a>'

def make_hyperlink_2(team):
    url = f"https://statmuse.com/mlb/ask/{team.replace(' ', '%20')}-k-per-pa-log"
    return f'<a href="{url}">{team}</a>'

def get_pitcher_data(pitcher_div):
    player_info = pitcher_div.find('div', class_='player-info')
    if not player_info:
        return 'Unknown',"N/A","N/A"
    hand = player_info.find('span', class_='throws').get_text(strip=True)
    handedness = "R" if hand == "Throws: Right" else "L"
    name_tag = player_info.find('h3').find('a')
    name = name_tag.get_text(strip=True) if name_tag else 'Unknown'
    
    probable_stats = pitcher_div.find('p', class_='probable-stats')
    if probable_stats:
        table = probable_stats.find('table', class_='pitcher-stats')
        if table:
            rows = table.find_all('tr')
            if len(rows) > 1:
                data_row = rows[1].find_all('td')
                if len(data_row) >= 2:
                    pa = data_row[0].get_text(strip=True)
                    k_percentage = data_row[1].get_text(strip=True)
                    return name, pa, k_percentage, handedness
    return name, 0, 0, handedness  

def get_opp_data(date):
    date_obj = datetime.datetime.strptime(date, "%m/%d/%Y")
    converted_date = date_obj.strftime("%Y-%m-%d")
    url = f"https://baseballsavant.mlb.com/probable-pitchers?date={converted_date}"
    response = requests.get(url)

    data = []

    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        blocks = soup.find_all('div', class_='mod')

        for block in blocks:
            cols = block.find_all('div', class_='col')
            for col in cols:
                try:
                    name, pa, k_percentage, handedness = get_pitcher_data(col)
                    data.append({"Pitcher": name, "Hand": handedness, "PA": pa, "K%": k_percentage})
                except Exception as e:
                    print("An error occurred")
                    data.append({"Pitcher": "TBD", "Hand": "TBD", "PA": 0, "K%": 0})

    else:
        print(f"\033[91mFailed to retrieve the webpage. Status code: {response.status_code}\033[0m")

    df = pd.DataFrame(data)
    return df

def fetch_schedule(date):
    sched = statsapi.schedule(start_date=date, end_date=date)
    # print(sched)
    return [game for game in sched if game['status'] in ['Pre-Game', 'Scheduled', 'Warmup', "Final", "In Progress"]]

def get_pitcher_tasks(sched):
    pitcher_tasks = []
    for game in sched:
        status = game['status']
        away_team, home_team = game['away_name'], game['home_name']
        if game['away_probable_pitcher']:
            pitcher_tasks.append((game['away_probable_pitcher'], away_team, home_team, status))
        if game['home_probable_pitcher']:
            pitcher_tasks.append((game['home_probable_pitcher'], home_team, away_team, status))
    return pitcher_tasks

def fetch_pitcher_stats_concurrently(pitcher_tasks):
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(fetch_pitcher_stats, pitcher, team, opponent, status) 
                   for pitcher, team, opponent, status in pitcher_tasks]
        return [future.result() for future in futures]

def prepare_team_batting_df(year):
    df = team_batting(year)
    df['SO/PA'] = (100 * df['SO'] / df['PA'])
    df = df[['Team', 'SO/PA']].sort_values(by='SO/PA', ascending=False)
    df['Team'] = df['Team'].apply(get_team_full_name)
    return df

def merge_pitcher_with_batting_data(results, team_batting_df):
    main_df = pd.DataFrame(results)
    return pd.merge(main_df, team_batting_df, left_on='Opponent', right_on='Team', how='left')

def merge_with_opponent_data(merged_df, opp_df):
    return pd.merge(merged_df, opp_df, left_on='Name', right_on='Pitcher', how='left')

def calculate_additional_metrics(date, pitchers):
    pitchers['AB/GP'] = pitchers['AB'].astype(float) / pitchers['GP'].astype(float)
    pitchers['K/AB'] = 100 * (pitchers['K'].astype(float) / (pitchers['AB'].astype(float) + pitchers['BB'].astype(float)))
    pitchers['Ks'] = [get_strikeouts_by_player_name(date, name) for name in pitchers['Name']]
    pitchers['r'] = pitchers['SO/PA'].rank(ascending=False)
    pitchers = pitchers[['Name', 'Hand', 'GP', 'AB', 'K', 'BB', 'AVG', 'AB/GP', 'K/9', 'K/AB', 'K%', 'PA', 
                         'SO/PA', 'r', 'Opponent', 'Status', 'Ks']]         
    return pitchers.sort_values(by=['Ks', 'K/AB'], ascending=[False, False])
def merge_with_odds_data(pitchers):
    odds_data = [fetch_pitcher_odds(name) for name, status in zip(pitchers['Name'], pitchers['Status']) if status not in ['Final', 'In Progress']]
    if odds_data:
        odds_df_all = pd.concat(odds_data, ignore_index=True)
        try:
            odds_df_all = odds_df_all[['pitcher','FanDuel', 'Caesars', 'betrivers', 'BetOnline.ag', 'DraftKings', 'Novig']]
        except:    
            odds_df_all = odds_df_all
        final_df = pd.merge(pitchers, odds_df_all, left_on='Name', right_on='pitcher', how='left')
        final_df.drop(columns=['pitcher'], inplace=True)
    else:
        final_df = pitchers
    return final_df

def style_dataframe(df):
    df['Name'] = df['Name'].apply(make_hyperlink)
    df['Opponent'] = df['Opponent'].apply(make_hyperlink_2)

    def highlight_rows(row):
        return ['background-color: lightgreen' if float(row['K%']) > 25 and float(row['SO/PA']) > 25 and int(row['PA']) > 20 and col == 'Name' else '' for col in row.index]

    def highlight_columns(s, color):
        return f'background-color: {color}'

    styled_df = df.style\
        .background_gradient(cmap='YlGnBu', subset=['SO/PA', 'PA'])\
        .background_gradient(cmap='YlOrRd', subset=['K/AB', 'K%', 'K/9'], vmin=0, vmax=40)\
        .background_gradient(cmap='YlOrRd', subset=['K/9'])\
        .map(lambda x: highlight_columns(x, 'lightblue'), subset=['Name'])\
        .apply(highlight_rows, axis=1)\
        .format({'SO/PA': '{:.2f}', 'AB/GP': '{:.1f}', 'K/AB': '{:.2f}', 'r': '{:.0f}', 'point': '{:.1f}'})\
        .set_properties(**{'text-align': 'center'})\
        .set_table_styles([
            {'selector': 'th', 'props': [('font-size', '14px'), ('background-color', '#f4f4f4')]},
            {'selector': 'td', 'props': [('padding', '6px'), ('border', '1px solid #ddd')]}
        ])
    return styled_df


def write_to_html(styled_pitchers, date):
    print("\033[92mWriting to HTML....\033[0m")
    html_content = f'''
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta http-equiv="X-UA-Compatible" content="IE=edge">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Report {date}</title>
        </head>
        <body>
            {styled_pitchers.to_html(index=False)}
        </body>
        </html>
    '''
    with open(f'reports/report-{date}.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f'file:///Users/ggandhi001/Documents/MLB_2024/reports/report-{date}.html')

def write_to_google_sheet(final_df, sheet_name):
    print("\033[92mWriting to Google Sheet....\033[0m")
    # Define the required scopes
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    creds = Credentials.from_service_account_file('sheets_creds.json', scopes=scopes)

    client = gspread.authorize(creds)
    
    spreadsheet = client.open(sheet_name)  

    sheet_tab_name = date.replace("/", "-")  
    try:
        sheet = spreadsheet.worksheet(sheet_tab_name)
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=sheet_tab_name, rows="100", cols="20")

    sheet.clear()
    set_with_dataframe(sheet, final_df)

    # if date is today open, clear and writee to today
    if date == datetime.datetime.now().strftime("%m/%d/%Y"):
        today = client.open(sheet_name).sheet1
        current_time = datetime.datetime.now().strftime("%H:%M")
        new_title = f"TODAY: as of {current_time}"
        today.update_title(new_title)
        today.clear()
        set_with_dataframe(today, final_df)

def main(date, odds):

    stripped_date = date.replace("/", "")
    print(f'file:///Users/ggandhi001/Documents/MLB_2024/reports/report-{stripped_date}.html')

    sched = fetch_schedule(date)

    pitcher_tasks = get_pitcher_tasks(sched)

    results = fetch_pitcher_stats_concurrently(pitcher_tasks)

    team_batting_df = prepare_team_batting_df(2025)

    merged_df = merge_pitcher_with_batting_data(results, team_batting_df)

    opp_df = get_opp_data(date)

    pitchers = merge_with_opponent_data(merged_df, opp_df)

    pitchers = calculate_additional_metrics(date, pitchers)

    if(odds =="n"):
        print("NO ODDS")            
        styled_pitchers = style_dataframe(pitchers)
    else:
        try:
            final_df = merge_with_odds_data(pitchers)
            write_to_google_sheet(final_df, "MLB Sheet")
            styled_pitchers = style_dataframe(final_df)
        except Exception as e:
            print("No Odds Found", e)
            write_to_google_sheet(pitchers, "MLB Sheet")
            styled_pitchers = style_dataframe(pitchers)
    
    write_to_html(styled_pitchers, stripped_date)

if __name__ == "__main__":
    input = str(sys.argv[1])
    odds = str(sys.argv[2])
    if input.lower() == "today":
        # Set date to today's date in MM/DD/YYYY format
        date = datetime.datetime.now().strftime("%m/%d/%Y")
    elif input.lower() == "tmrw":
        # Set date to tomorrow's date in MM/DD/YYYY format
        date = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%m/%d/%Y")    
    else:
        # Assume input_arg is in MM/DD format and append the year
        date = input + "/2025"
    print(f"\033[94mRunning at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\033[0m")    
    main(date, odds)