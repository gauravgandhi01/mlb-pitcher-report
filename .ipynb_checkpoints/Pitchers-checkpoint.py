import pandas as pd
from bs4 import BeautifulSoup
import requests
import statsapi
from concurrent.futures import ThreadPoolExecutor
from pybaseball import team_batting
import sys
import datetime

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
        "AVG": data.get("avg"),
        "S%": data.get("strikePercentage"),
        "P/I": data.get("pitchesPerInning"),
        "K": data.get("strikeOuts"),
        "K/9": data.get("strikeoutsPer9Inn")
    }
    return specific_fields

def fetch_pitcher_stats(name, team, opponent):
    try:
        player = statsapi.lookup_player(name)
        if not player:
            raise ValueError(f"Player {name} not found")
        player_id = player[0]['id']
        stats = statsapi.player_stats(player_id, group="[pitching]", type="season")
        pitcher_stats = parse_pitcher_stats(stats, name)
        pitcher_stats["Opponent"] = opponent
        return pitcher_stats
    except Exception as e:
        return {"Name": name, "Team": team, "Opponent": opponent, "Error": str(e)}

def get_team_full_name(abbreviation):
    team_mapping = {
        "SEA": "Seattle Mariners",
        "OAK": "Oakland Athletics",
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

def get_pitcher_data(pitcher_div):
    player_info = pitcher_div.find('div', class_='player-info')
    if not player_info:
        return 'Unknown',"N/A","N/A"
    
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
                    return name, pa, k_percentage
    return name, 0, 0 

def getOppData(date):
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
                name, pa, k_percentage = get_pitcher_data(col)
                data.append({"Pitcher": name, "PA": pa, "K%": k_percentage})
    else:
        print(f"Failed to retrieve the webpage. Status code: {response.status_code}")

    df = pd.DataFrame(data)
    return df

def main(date):
    # Fetch the schedule for the given date
    sched = statsapi.schedule(start_date=date, end_date=date)
    pitcher_tasks = []

    # Collect tasks for fetching pitcher stats
    for game in sched:
        away_team, home_team = game['away_name'], game['home_name']
        if game['away_probable_pitcher']:
            pitcher_tasks.append((game['away_probable_pitcher'], away_team, home_team))
        if game['home_probable_pitcher']:
            pitcher_tasks.append((game['home_probable_pitcher'], home_team, away_team))

    # Fetch pitcher stats using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        results = [future.result() for future in 
                   [executor.submit(fetch_pitcher_stats, pitcher, team, opponent) 
                    for pitcher, team, opponent in pitcher_tasks]]

    # Prepare team batting dataframe
    df = team_batting(2024)
    df['SO/AB'] = (100 * df['SO'] / df['AB'])
    df = df[['Team', 'SO/AB']].sort_values(by='SO/AB', ascending=False)
    df['Team'] = df['Team'].apply(get_team_full_name)

    # Merge pitcher stats with team batting data
    main_df = pd.DataFrame(results)
    merged = pd.merge(main_df, df, left_on='Opponent', right_on='Team', how='left')
    
    # Get opponent data and merge it
    opp_df = getOppData(date)
    pitchers = pd.merge(merged, opp_df, left_on='Name', right_on='Pitcher', how='left')
    pitchers.drop(columns=['Team'], inplace=True)
    
    # Calculate additional metrics
    pitchers['AB/GP'] = pitchers['AB'].astype(float) / pitchers['GP'].astype(float)
    pitchers['K/AB'] = 100 * (pitchers['K'].astype(float) / pitchers['AB'].astype(float))
    pitchers = pitchers.sort_values(by=['SO/AB'], ascending=False)
    
    # Select and style the final columns
    pitchers = pitchers[['Name', 'GP', 'AB', 'K', 'AVG', 'S%', 'P/I', 'K/9', 'AB/GP', 'K/AB', 'PA', 'K%', 'SO/AB', 'Opponent']]
    
    # Define a function to ensure text color contrast
    def apply_text_color(value):
        try:
            value = float(value)
            return 'color: black' if value < 30 else 'color: white'
        except ValueError:
            return ''
    
    styled_pitchers = pitchers.style.background_gradient(cmap='YlGnBu', subset=['SO/AB', "AB/GP", "K/AB", 'K%', 'PA'])
    styled_pitchers = (styled_pitchers
                       .format({'SO/AB': '{:.2f}', 'AB/GP': '{:.1f}', 'K/AB': '{:.2f}'})
                       .set_properties(**{'text-align': 'center'})
                       .set_table_styles(
                           [{'selector': 'th', 'props': [('font-size', '14px'), ('background-color', '#f4f4f4')]},
                            {'selector': 'td', 'props': [('padding', '6px'), ('border', '1px solid #ddd')]}]))
    
    # Write the styled dataframe to an HTML file
    stripped_date = date.replace("/", "")
    with open(f'reports/report-{stripped_date}.html', 'w') as f:
        f.write(styled_pitchers.to_html(index=False))


if __name__ == "__main__":
    input = str(sys.argv[1])
    date = input+"/2024"
    main(date)
