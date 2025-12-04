from playwright.sync_api import sync_playwright
from fake_useragent import UserAgent
import pandas as pd
import time

def scrape_investing(limit=200):

    ua = UserAgent()
    user_agent = ua.chrome

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1400, "height": 900}
        )
        page = context.new_page()

        url = "https://www.investing.com/stock-screener/technology"
        page.goto(url, timeout=60000)
        page.wait_for_selector("table", timeout=60000)

        # Accept cookies if available
        try:
            page.click("button:has-text('Accept')")
        except:
            pass

        # Scroll to load full table (first page = 50 rows)
        prev_height = 0
        while True:
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)

            new_height = page.evaluate("document.body.scrollHeight")
            if new_height == prev_height:
                break
            prev_height = new_height

            rows = page.query_selector_all("table tbody tr")
            if len(rows) >= limit:
                break

        # Extract data
        rows = page.query_selector_all("table tbody tr")
        data = []

        for row in rows[:limit]:
            # all non-sticky columns
            cols = row.query_selector_all("td")

            # sticky company column (th) contains the ticker link
            company_th = row.query_selector("th")
            ticker = ""
            if company_th:
                a_tag = company_th.query_selector("a")
                if a_tag:
                    ticker = a_tag.inner_text().strip()

            # Ensure we have enough data columns
            if len(cols) < 8:
                continue

            item = {
                "Ticker": ticker,
                "Name": cols[0].inner_text().strip(),
                "Exchange": cols[1].inner_text().strip(),
                "Sector": cols[2].inner_text().strip(),
                "Industry": cols[3].inner_text().strip(),
                "Market Cap": cols[4].inner_text().strip(),
                "P/E Ratio": cols[5].inner_text().strip(),
                "PEG Ratio": cols[6].inner_text().strip(),
                "Last Trade Price": cols[7].inner_text().strip(),
            }

            data.append(item)

        browser.close()

        df = pd.DataFrame(data)
        df.to_csv("raw_data.csv", index=False)
        print("success")

        return df


# -----------------------------
# Airflow wrapper (REQUIRED)
# -----------------------------
def scrape_task():
    scrape_investing(limit=200)

scrape_task()
