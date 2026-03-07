from bs4 import BeautifulSoup

game_count = 0
# Octobber 22 was the first game 


# Read from file instead of requesting
with open('espn_data.html', 'r', encoding='utf-8') as f:
    html = f.read()


games_played = []
games_remaining = []

soup = BeautifulSoup(html, 'html.parser')
table_body = soup.find("tbody", class_="Table__TBODY")
anchors = table_body.find_all("a", class_="AnchorLink")
for anchor in anchors:

    #print("Anchor:", anchor)

    #print(anchor.get_text())

    text = anchor.get_text()
    link = anchor["href"]
    # if there is a dash in the visible text it means the game was played
    # and the anchor is showing the final score (e.g. "WAS 110 - NYK 102").
    if '-' in text:
        game_count += 1
        # print(f"Game {game_count}: {text}: {link}")
        games_played.append((text, link))
    # upcoming games show a time rather than a score
    elif "PM" in text or "AM" in text:
        # print(f"Remaining game: {text}: {link}")
        games_remaining.append((text, link))
    

# print(f"Games played: {game_count} (collected {len(games_played)} links)")
# print(f"Games remaining: {len(games_remaining)}")

# if you need just the URLs themselves you can extract them from the
# tuples.  for example:
played_urls = [l for (_, l) in games_played]
remaining_urls = [l for (_, l) in games_remaining]

print(played_urls)