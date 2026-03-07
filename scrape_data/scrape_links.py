from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
from scrape_all_links import played_urls 
import json

def format_url(url):
    url = url[0:25] + "boxscore" + url[29:]
    return url

game_count = 0
final_data = {}

def dnp_count(soup):
    flex = soup.find_all("div", class_="flex")
    dnp = soup.find_all("td", class_="tc td BoxscoreItem__DNP Table__TD")
    return len(dnp)


def check(soup):
    flex = soup.find_all("div", class_="flex")
    player_name = flex[1].find_all("tr", class_="Table__TR Table__TR--sm Table__even")

    # ADD THIS to debug
    # for i, row in enumerate(player_name):
    #     spans = row.find_all("span")
    #     print(f"  row {i}: {[s.get_text() for s in spans]}")

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

    for url in played_urls:
        url = format_url(url)
        game_count += 1
        final_data[f"game_{game_count}"] = {}

        try:
            page = browser.new_page()
            page.goto(url)
            print(f"Processing {url}...")
            page.wait_for_selector("tbody.Table__TBODY", timeout=10000)  # wait for table, not fixed 5s
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')

            final_data[f"game_{game_count}"]["home_team"] = hometeam(soup)
            final_data[f"game_{game_count}"]["away_team"] = awayteam(soup)
            print(f"Finished processing {url}.")
            page.close()  # close page, not browser
        except Exception as e:
            print(f"Error processing {url}: {e}")
            page.close()

    browser.close()  # close once at the end

with open('wizards_data_2026-05-03.json', 'w', encoding='utf-8') as f:
    json.dump(final_data, f, indent=4)