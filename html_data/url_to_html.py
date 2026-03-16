from playwright.sync_api import sync_playwright

# List of all nba teams and their corresponding ESPN URLs
teams = ["boston-celtics", "brooklyn-nets", "new-york-knicks", "philadelphia-76ers", "toronto-raptors",
         "chicago-bulls", "cleveland-cavaliers", "detroit-pistons", "indiana-pacers", "milwaukee-bucks",
         "atlanta-hawks", "charlotte-hornets", "miami-heat", "orlando-magic", "washington-wizards",
         "denver-nuggets", "minnesota-timberwolves", "oklahoma-city-thunder", "portland-trail-blazers",
         "utah-jazz", "golden-state-warriors", "los-angeles-clippers", "los-angeles-lakers", "phoenix-suns", "sacramento-kings"]

team_abr = ["bos", "bkn", "nyk", "phi", "tor",
            "chi", "cle", "det", "ind", "mil",
            "atl", "cha", "mia", "orl", "wsh",
            "den", "min", "okc", "por", "uta", 
            "gsw", "lac", "lal", "phx", "sac"]


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    url = "https://www.espn.com/nba/team/schedule/_/name/wsh/washington-wizards"
    page.goto(url)
    page.wait_for_timeout(5000)  # Wait 5 seconds for page to load
    
    html = page.content()
    
    with open('espn_data.html', 'w', encoding='utf-8') as f:
        f.write(html)
    
    browser.close()
    print("Saved!")