from scrape_all_links import played_urls
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import json

final_data = {}
game_count = 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)  # launch once, reuse for all games
    # ADDED: loop through URLs
    for url in played_urls:
        game_count += 1

        # ADDED: initialize game data structure at the start of each loop iteration
        # [game_count] is the key for the current game, which will contain home and away team data
        final_data[f"game_{game_count}"] = {}

        try:
            # ADDED: open new page for each URL, but keep browser open to reuse for next URL
            page = browser.new_page()
            page.goto("https://www.espn.com/nba/game/_/gameId/401809939/wizards-bucks")

            # ADDED: print statement to indicate which URL is being processed, useful for tracking progress and debugging
            print(f"Processing {url}...")

            # ADDED: wait for table to load before parsing
            html = page.content()
            soup = BeautifulSoup(html, 'html.parser')
            

            date = soup.find("div", class_="mLASH Kiog YXOwE bmjsw").get_text()
            time = date[0:7]
            date = date[8:]

            print(f"{date} {time}")
            attendence = soup.find_all("div", class_="mLASH Kiog YXOwE bmjsw")[2].get_text().strip("Attendance: ")
            location = soup.find("div", class_="aCYDt nRFhJ mLASH VZTD UeCOM nkdHX LbeBv").get_text()
            refs = soup.find_all("div", class_="mLASH Kiog YXOwE bmjsw")[4]
            refs = refs.find_all("span", class_="LiUVm")

            refs_list = []

            for ref in refs:
                refs_list.append(ref.get_text())
                        
            final_data[f"game_{game_count}"]["game_date"] = date
            final_data[f"game_{game_count}"]["game_time"] = time
            final_data[f"game_{game_count}"]["game_location"] = location
            final_data[f"game_{game_count}"]["game_attandence"] = attendence
            final_data[f"game_{game_count}"]["game_refs"] = refs_list 

            
            print(f"Finished processing {url}.")

            # ADDED: close page after processing each URL to free up resources, but keep browser open for next URL
            page.close()

        except Exception as e:
            # ADDED: catch and log any exceptions that occur during processing of each URL, but continue with next URL
            print(f"Error processing {url}: {e}")
            page.close()

    browser.close()  # close once at the end

with open('/home/dyl/Documents/Wizards/data/wizards_game_data.json', 'w', encoding='utf-8') as f:
    json.dump(final_data, f, indent=4)
