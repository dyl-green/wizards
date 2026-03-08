from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from scrape_all_links import played_urls 
import json

# ADDED: function to format URL, used in main loop
def format_url(url):
    url = url[0:25] + "boxscore" + url[29:]
    return url

game_count = 0
final_data = {}

# ADDED: function to count DNP entries, used in both home and away team functions
def dnp_count(soup):
    flex = soup.find_all("div", class_="flex")
    dnp = soup.find_all("td", class_="tc td BoxscoreItem__DNP Table__TD")
    return len(dnp)

def awayteam(soup):
    print(f"Processing away team data for {url}...")
    flex = soup.find_all("div", class_="flex")
    player_name = flex[1].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
    dnp = dnp_count(soup)
    stats = soup.find_all("tbody", class_="Table__TBODY")
    players_data = []  # FIX: was {}, must be a list

    for player in range(1, 6):
        data = stats[1].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
        get_name = player_name[player].find_all("span")
        if not get_name:
            print(f"Warning: No span found for player {player} in away team data for {url}")
            continue
        if player >= len(data):          # ADDED bounds check
            continue
        data = data[player].find_all("td")
        if len(data) < 14:               # UPDATED bounds check
            continue
        name = get_name[0].get_text()
        jersey = get_name[2].get_text()
        player_stats = { 
            'player': name,
            'jersey': jersey,
            'minutes': data[0].get_text(),
            'points': data[1].get_text(),
            'field_goal_percentage': data[2].get_text(),
            'three_point_percentage': data[3].get_text(),
            'free_throw_percentage': data[4].get_text(),
            'rebounds': data[5].get_text(),
            'assists': data[6].get_text(),
            'turnovers': data[7].get_text(),
            'steals': data[8].get_text(),
            'blocks': data[9].get_text(),
            'o_rebounds': data[10].get_text(),
            'd_rebounds': data[11].get_text(),
            'fouls': data[12].get_text(),
            'plus_minus': data[13].get_text(),
            'Starter': True
        }
        players_data.append(player_stats)  # FIX: was players_data = player_stats

    for player in range(7, 17-dnp):
        data = stats[1].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
        get_name = player_name[player].find_all("span")
        if not get_name:
            print(f"Warning: No span found for player {player} in away team data for {url}")
            continue
        if player >= len(data):          # ADDED bounds check
            continue
        data = data[player].find_all("td")
        if len(data) < 14:               # UPDATED bounds check
            continue
        name = get_name[0].get_text()
        jersey = get_name[2].get_text()
        player_stats = {
            'player': name,
            'jersey': jersey,
            'minutes': data[0].get_text(),
            'points': data[1].get_text(),
            'field_goal_percentage': data[2].get_text(),
            'three_point_percentage': data[3].get_text(),
            'free_throw_percentage': data[4].get_text(),
            'rebounds': data[5].get_text(),
            'assists': data[6].get_text(),
            'turnovers': data[7].get_text(),
            'steals': data[8].get_text(),
            'blocks': data[9].get_text(),
            'o_rebounds': data[10].get_text(),
            'd_rebounds': data[11].get_text(),
            'fouls': data[12].get_text(),
            'plus_minus': data[13].get_text(),
            'Starter': False
        }
        players_data.append(player_stats)  # FIX: was players_data = player_stats
    return players_data

def hometeam(soup):
    print(f"Processing home team data for {url}...")
    flex = soup.find_all("div", class_="Wrapper")
    player_name = flex[2].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
    dnp = dnp_count(soup)
    stats = soup.find_all("tbody", class_="Table__TBODY")
    players_data = []  # FIX: was {}, must be a list

    for player in range(1, 6):
        data = stats[3].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
        get_name = player_name[player].find_all("span")
        if not get_name:
            print(f"Warning: No span found for player {player} in home team data for {url}")
            continue
        if player >= len(data):          # ADDED bounds check
            continue
        data = data[player].find_all("td")
        if len(data) < 14:               # UPDATED bounds check
            continue
        name = get_name[0].get_text()
        jersey = get_name[2].get_text()
        player_stats = { 
            'player': name,
            'jersey': jersey,
            'minutes': data[0].get_text(),
            'points': data[1].get_text(),
            'field_goal_percentage': data[2].get_text(),
            'three_point_percentage': data[3].get_text(),
            'free_throw_percentage': data[4].get_text(),
            'rebounds': data[5].get_text(),
            'assists': data[6].get_text(),
            'turnovers': data[7].get_text(),
            'steals': data[8].get_text(),
            'blocks': data[9].get_text(),
            'o_rebounds': data[10].get_text(),
            'd_rebounds': data[11].get_text(),
            'fouls': data[12].get_text(),
            'plus_minus': data[13].get_text(),
            'Starter': True
        }
        players_data.append(player_stats)  # FIX: was players_data = player_stats

    for player in range(7, 17-dnp):
        data = stats[3].find_all("tr", class_="Table__TR Table__TR--sm Table__even")
        get_name = player_name[player].find_all("span")
        if not get_name:
            print(f"Warning: No span found for player {player} in home team data for {url}")
            continue
        if player >= len(data):          # ADDED bounds check
            continue
        data = data[player].find_all("td")
        if len(data) < 14:               # UPDATED bounds check
            continue
        name = get_name[0].get_text()
        jersey = get_name[2].get_text()
        player_stats = {
            'player': name,
            'jersey': jersey,
            'minutes': data[0].get_text(),
            'points': data[1].get_text(),
            'field_goal_percentage': data[2].get_text(),
            'three_point_percentage': data[3].get_text(),
            'free_throw_percentage': data[4].get_text(),
            'rebounds': data[5].get_text(),
            'assists': data[6].get_text(),
            'turnovers': data[7].get_text(),
            'steals': data[8].get_text(),
            'blocks': data[9].get_text(),
            'o_rebounds': data[10].get_text(),
            'd_rebounds': data[11].get_text(),
            'fouls': data[12].get_text(),
            'plus_minus': data[13].get_text(),
            'Starter': False
        }
        players_data.append(player_stats)  # FIX: was players_data = player_stats

    return players_data

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)  # launch once, reuse for all games


    # ADDED: loop through URLs
    for url in played_urls:
        url = format_url(url)
        game_count += 1

        # ADDED: initialize game data structure at the start of each loop iteration
        # [game_count] is the key for the current game, which will contain home and away team data
        final_data[f"game_{game_count}"] = {}

        try:
            # ADDED: open new page for each URL, but keep browser open to reuse for next URL
            page = browser.new_page()
            page.goto(url)

            # ADDED: print statement to indicate which URL is being processed, useful for tracking progress and debugging
            print(f"Processing {url}...")

            # ADDED: wait for table to load before parsing
            page.wait_for_selector("tbody.Table__TBODY", timeout=10000) 
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')

            # ADDED: call home and away team functions, store results in final_data under current game key
            final_data[f"game_{game_count}"]["home_team"] = hometeam(soup)
            final_data[f"game_{game_count}"]["away_team"] = awayteam(soup)


            print(f"Finished processing {url}.")

            # ADDED: close page after processing each URL to free up resources, but keep browser open for next URL
            page.close()

        except Exception as e:
            # ADDED: catch and log any exceptions that occur during processing of each URL, but continue with next URL
            print(f"Error processing {url}: {e}")
            page.close()

    browser.close()  # close once at the end

# ADDED: write final_data to JSON file after processing all URLs
with open('/home/dyl/Documents/Wizards/data/wizards_data_2026-05-03.json', 'w', encoding='utf-8') as f:
    json.dump(final_data, f, indent=4)