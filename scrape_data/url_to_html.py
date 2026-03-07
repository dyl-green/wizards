from playwright.sync_api import sync_playwright

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