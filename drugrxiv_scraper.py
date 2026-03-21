import asyncio
import re
import random
from dateutil import parser
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright


# =========================================
# CONFIG
# =========================================

MAX_LOADS = 2  # (kept but no longer used for stopping)
MAX_LOAD_CLICKS = 30  # 🔥 Safety limit for load more


# =========================================
# DATE HELPERS
# =========================================

def normalize_date(raw):
    try:
        return parser.parse(raw).date().isoformat()
    except:
        return None


def in_range(date_str, start_date, end_date):
    try:
        d = parser.parse(date_str).date()
        s = parser.parse(start_date).date()
        e = parser.parse(end_date).date()
        return s <= d <= e
    except:
        return False


def extract_best_date(html):

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    for tag in soup.find_all("meta"):
        for attr in ["property", "name", "itemprop"]:
            if tag.get(attr) and "date" in tag.get(attr).lower():
                raw = tag.get("content")
                date = normalize_date(raw)
                if date:
                    return date

    for script in soup.find_all("script", type="application/ld+json"):
        if "datePublished" in script.text:
            match = re.search(r'"datePublished"\s*:\s*"([^"]+)"', script.text)
            if match:
                return normalize_date(match.group(1))

    return None


# =========================================
# FALLBACK ABSTRACT (UNCHANGED)
# =========================================

def extract_abstract_fallback(html):

    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    article = soup.find("article")
    if article:
        text = article.get_text(" ", strip=True)
        if len(text) > 300:
            return text[:4000]

    main = soup.find("main")
    if main:
        text = main.get_text(" ", strip=True)
        if len(text) > 300:
            return text[:4000]

    text = soup.get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text)

    return text[:4000] if len(text) > 300 else None


# =========================================
# COOKIE HANDLER (UNCHANGED)
# =========================================

async def handle_cookie_popup(page):
    try:
        await page.wait_for_selector("text=Accept all cookies", timeout=5000)
        await page.click("text=Accept all cookies")
        await asyncio.sleep(2)
    except:
        pass


# =========================================
# 🔥 BLOCKWISE LOAD MORE (UPDATED)
# =========================================

# =========================================
# 🔥 BLOCKWISE LOAD MORE (FIXED DATE SCOPE)
# =========================================

# =========================================
# 🔥 BLOCKWISE LOAD MORE (FIXED DATE SCOPE)
# =========================================

async def expand_results(page, start_date):

    start_date_obj = parser.parse(start_date).date()
    click_count = 0

    while True:

        # ✅ STEP 1: Get ALL visible links on current page
        links = await page.eval_on_selector_all(
            "a[href]",
            "elements => elements.map(e => e.href)"
        )

        # Filter only document links (real article pages)
        article_links = [l for l in links if "/document?" in l]

        if not article_links:
            print("No article links found on page. Stopping.")
            break

        print(f"Visible article count: {len(article_links)}")

        # ✅ STEP 2: Visit only the LAST visible article
        # (because results are sorted newest → oldest)
        last_link = article_links[-1]

        try:
            await page.goto(last_link)
            await asyncio.sleep(1)

            html = await page.content()
            last_date_str = extract_best_date(html)

            if not last_date_str:
                print("Could not extract date from last article. Stopping.")
                break

            last_date = parser.parse(last_date_str).date()

            print(f"Oldest visible article date: {last_date}")

        except:
            print("Error visiting last article. Stopping.")
            break

        # 🔥 STEP 3: Decide whether to load more
        if last_date < start_date_obj:
            print("Boundary reached. Stopping Load More.")
            break

        if click_count >= MAX_LOAD_CLICKS:
            print("Reached max load click safety limit.")
            break

        # Go back to search page
        await page.go_back()
        await asyncio.sleep(1)

        load_button = await page.query_selector("text=Load more results")

        if not load_button:
            print("Load More button not found. Stopping.")
            break

        print("Block valid. Clicking Load More once...")
        await load_button.click()
        await asyncio.sleep(2)

        click_count += 1


# =========================================
# COLLECT LINKS (UNCHANGED)
# =========================================
# =========================================
# COLLECT LINKS (UPDATED FOR SPA WAIT)
# =========================================

async def collect_raw_links(page, search_url, start_date):

    await page.goto(search_url)
    await page.wait_for_load_state("networkidle")

    # 🔥 NEW: wait for search results to render (SPA fix)
    try:
        await page.wait_for_selector("a[href*='document?vid=']", timeout=10000)
    except:
        pass

    await page.wait_for_timeout(2000)  # small buffer

    await handle_cookie_popup(page)
    await expand_results(page, start_date)

    links = await page.eval_on_selector_all(
        "a[href]",
        "elements => elements.map(e => e.href)"
    )

    return set(links)


# =========================================
# FILTER LINKS (UNCHANGED)
# =========================================

def filter_links(links):

    blocked_keywords = [
        "bookmark", "reviews", "#author", "#content", "#advanced", "#r",
        "altmetric", "facebook", "twitter", "linkedin", "youtube",
        "collection", "dashboard", "hosted-documents", "login",
        "/search#", "pdf", "rss", "alerts", "share", "wishlist",
        "signup", "search", "authors", "author", "feed", "download",
        "user", "related", "accounts", "cluster","hosted"
    ]

    return {
        link for link in links
        if not any(b in link.lower() for b in blocked_keywords)
    }


# =========================================
# FINAL ENTRY (UNCHANGED)
# =========================================
# UPDATED RUN SCRAPER (SPA-SAFE)
# =========================================

async def run_scraper(
    base_url,
    true_url,
    false_url,
    start_date,
    end_date,
    page=None
):

    browser_owner = False
    browser = None
    playwright_instance = None

    if page is None:
        playwright_instance = await async_playwright().start()
        browser = await playwright_instance.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        browser_owner = True
    else:
        context = page.context

    await page.goto(true_url)
    await page.wait_for_load_state("networkidle")
    await handle_cookie_popup(page)

    await page.wait_for_selector("a[href*='document?vid=']", timeout=10000)

    results = []
    seen_links = set()

    start_date_obj = parser.parse(start_date).date()
    end_date_obj = parser.parse(end_date).date()

    round_counter = 1

    while True:

        print(f"\n========== ROUND {round_counter} ==========")

        # Collect all visible article links
        current_links = await page.eval_on_selector_all(
            "a[href*='document?vid=']",
            "elements => elements.map(e => e.href)"
        )

        current_set = set(current_links)
        new_links = current_set - seen_links

        print(f"Total visible links: {len(current_links)}")
        print(f"New links this round: {len(new_links)}")

        if not new_links:
            load_button = await page.query_selector("text=Load more results")
            if not load_button:
                print("No Load More button. Finished.")
                break
            print("No new links but Load More exists. Clicking again...")
            await load_button.click()
            await page.wait_for_timeout(2500)
            round_counter += 1
            continue

        for link in new_links:

            seen_links.add(link)

            print(f"\nOpening article: {link}")

            try:
                article_page = await context.new_page()
                await article_page.goto(link)
                await article_page.wait_for_load_state("networkidle")

                html = await article_page.content()
                best_date_str = extract_best_date(html)

                if not best_date_str:
                    print("Date not found. Skipping.")
                    await article_page.close()
                    continue

                article_date = parser.parse(best_date_str).date()

                print(f"Article date: {article_date}")

                if article_date < start_date_obj:
                    print("Date boundary reached. Stopping.")
                    await article_page.close()
                    if browser_owner:
                        await browser.close()
                        await playwright_instance.stop()
                    return results

                if start_date_obj <= article_date <= end_date_obj:

                    abstract = extract_abstract_fallback(html)

                    print("✔ Added to results")

                    results.append({
                        "website": base_url,
                        "url": link,
                        "date": best_date_str,
                        "abstract": abstract
                    })
                else:
                    print("Outside date range. Skipped.")

                await article_page.close()

            except Exception as e:
                print("Error:", e)
                continue

        load_button = await page.query_selector("text=Load more results")

        if not load_button:
            print("Load More button not found. Finished.")
            break

        print("\nClicking Load More...")
        await load_button.click()
        await page.wait_for_timeout(2500)

        round_counter += 1

    if browser_owner:
        await browser.close()
        await playwright_instance.stop()

    return results
if __name__ == "__main__":

    base_url = "https://drugrepocentral.scienceopen.com/search"
    true_url = "https://drugrepocentral.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~3_'orderLowestFirst'~false_'query'~'gene'_'filters'~!('kind'~86_'not'~false_'offset'~1_'timeUnit'~5)*_'hideOthers'~false)"
    false_url = "https://drugrepocentral.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~3_'orderLowestFirst'~false_'query'~'3n4rt5'_'filters'~!('kind'~86_'not'~false_'offset'~1_'timeUnit'~5)*_'hideOthers'~false)"

    start_date = "2026-02-20"
    end_date = "2026-12-31"

    results = asyncio.run(
        run_scraper(
            base_url,
            true_url,
            false_url,
            start_date,
            end_date
        )
    )

    print("\nFINAL RESULTS:")
    for r in results:
        print(r)