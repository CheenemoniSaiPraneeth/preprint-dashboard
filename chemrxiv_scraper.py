import asyncio
import re
import random
from bs4 import BeautifulSoup
from dateutil import parser
from playwright.async_api import async_playwright


MAX_PAGES = 2


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


# =========================================
# DATE VALIDATION
# =========================================

def is_valid_article_by_date(html):

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    score = 0

    if re.search(r"Submitted\s+on\s+\d{1,2}\s+[A-Za-z]+\s+\d{4}", text):
        score += 4

    if re.search(r"Posted\s+[A-Za-z]+\s+\d{1,2},\s+\d{4}", text):
        score += 4

    for tag in soup.find_all("meta"):
        for attr in ["property", "name", "itemprop"]:
            if tag.get(attr) and "date" in tag.get(attr).lower():
                raw = tag.get("content")
                if normalize_date(raw):
                    score += 3
                    break

    if re.search(r"\bsearch results\b", text.lower()):
        return False

    return score >= 5


def extract_best_date(html):

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    submitted = re.search(
        r"Submitted\s+on\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text
    )
    if submitted:
        return normalize_date(submitted.group(1))

    posted = re.search(
        r"Posted\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", text
    )
    if posted:
        return normalize_date(posted.group(1))

    for tag in soup.find_all("meta"):
        for attr in ["property", "name", "itemprop"]:
            if tag.get(attr) and "date" in tag.get(attr).lower():
                raw = tag.get("content")
                date = normalize_date(raw)
                if date:
                    return date

    return None


# =========================================
# CLEAN ABSTRACT EXTRACTION
# =========================================

def extract_abstract(html):

    soup = BeautifulSoup(html, "html.parser")

    # 1️⃣ Try meta description first (ChemRxiv usually has it)
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        content = meta_desc["content"].strip()
        if len(content) > 100:
            return content[:4000]

    # 2️⃣ Look for section containing Abstract heading
    for header in soup.find_all(["h2", "h3", "strong"]):
        if "abstract" in header.get_text(strip=True).lower():
            next_block = header.find_next()
            if next_block:
                text = next_block.get_text(" ", strip=True)
                text = re.sub(r"\s+", " ", text)
                if len(text) > 100:
                    return text[:4000]

    # 3️⃣ Look for div with abstract-related class
    abstract_div = soup.find(
        "div",
        class_=re.compile("abstract", re.I)
    )

    if abstract_div:
        text = abstract_div.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        if len(text) > 100:
            return text[:4000]

    return None


# =========================================
# COOKIE HANDLER
# =========================================

async def handle_cookies(page):

    texts = [
        "Accept", "Accept All", "Accept all cookies",
        "I agree", "Agree", "Continue"
    ]

    for t in texts:
        try:
            btn = await page.query_selector(f"text={t}")
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(2)
                break
        except:
            continue


# =========================================
# CLOUDFLARE WAIT
# =========================================

import time

async def wait_for_clearance(page):

    start = time.time()

    while True:

        try:
            title = await page.title()
        except:
            await asyncio.sleep(2)
            continue

        # Cloudflare challenge page
        if "Just a moment" in title or "Checking your browser" in title:

            # If stuck more than 2 minutes → reload
            if time.time() - start > 120:
                print("⚠ Stuck on verification for 2 minutes. Reloading page...")
                start = time.time()
                try:
                    await page.reload()
                except:
                    pass

            await asyncio.sleep(3)
            continue

        break


# =========================================
# 🔥 STATIC STYLE PAGINATION
# =========================================

async def collect_all_links(page, start_url, start_date):

    await page.goto(start_url)
    await wait_for_clearance(page)
    await handle_cookies(page)

    collected = set()
    start_date_obj = parser.parse(start_date).date()

    blocked_keywords = [
        "search", "login", "pdf",
        "rss", "alerts", "share",
        "bookmark", "subject",
        "facet", "metrics", "add",
        "wishlist", "user", "accounts"
    ]

    page_number = 1

    while True:

        print(f"\n🔎 Scanning Page {page_number}")

        await page.wait_for_load_state("domcontentloaded")

        links = await page.eval_on_selector_all(
            "a[href]",
            "elements => elements.map(e => e.href)"
        )

        page_links = []

        for link in links:
            if any(b in link.lower() for b in blocked_keywords):
                continue
            page_links.append(link)

        older_count = 0

        for link in page_links:

            try:
                if "doi" not in link.lower():
                    continue
                await page.goto(link)
                await wait_for_clearance(page)
                await handle_cookies(page)

                html = await page.content()
                best_date = extract_best_date(html)

                if best_date:
                    article_date = parser.parse(best_date).date()
                    print(f"   Found article date: {article_date}")

                    if article_date < start_date_obj:
                        older_count += 1

                collected.add(link)

            except:
                continue

        print(f"   Older articles on this page: {older_count}")

        if older_count > 2:
            print("🛑 More than 2 older articles found. Stopping pagination.")
            break

        await page.goto(start_url)
        await page.wait_for_load_state("domcontentloaded")

        next_button = await page.query_selector(
            "a[rel='next'], "
            "a[aria-label*='Next'], "
            "a[title*='Next'], "
            "a:has-text('Next'), "
            "a:has-text('>'), "
            "a:has-text('›'), "
            "a:has-text('»')"
        )

        if not next_button:
            print("❌ No Next button found. Stopping.")
            break

        print("➡ Moving to next page...")
        await next_button.click()
        await page.wait_for_load_state("domcontentloaded")
        await wait_for_clearance(page)

        await asyncio.sleep(random.uniform(2, 3))

        page_number += 1

    return collected


# =========================================
# ROUTER ENTRY POINT
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
    context = None
    playwright_instance = None

    if page is None:

        playwright_instance = await async_playwright().start()

        context = await playwright_instance.chromium.launch_persistent_context(
            user_data_dir="chem_profile",
            headless=False,
            channel="chrome",
            args=["--disable-blink-features=AutomationControlled"]
        )

        page = context.pages[0] if context.pages else await context.new_page()

        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        browser_owner = True
    else:
        context = page.context

    await page.goto(base_url)
    await wait_for_clearance(page)
    await handle_cookies(page)

    A = await collect_all_links(page, true_url, start_date)
    B = await collect_all_links(page, false_url, start_date)

    candidate_links = A - B
    candidate_links = {
        l for l in candidate_links
        if "doi" in l.lower()
    }
    results = []

    for link in candidate_links:

        await asyncio.sleep(random.uniform(0.5, 1.0))

        try:
            await page.goto(link)
            await wait_for_clearance(page)
            await handle_cookies(page)

            html = await page.content()

            if not is_valid_article_by_date(html):
                continue

            best_date = extract_best_date(html)

            if best_date and in_range(best_date, start_date, end_date):

                abstract = extract_abstract(html)

                results.append({
                    "website": base_url,
                    "url": link,
                    "date": best_date,
                    "abstract": abstract
                })

        except:
            continue

    if browser_owner:
        await context.close()
        await playwright_instance.stop()

    return results
if __name__ == "__main__":

    base_url = "https://chemrxiv.org/"
    true_url = "https://chemrxiv.org/action/doSearch?AllField=gene&startPage=0&sortBy=EPubDate"
    false_url = "https://chemrxiv.org/action/doSearch?AllField=3nr5fg5&startPage=0&sortBy=EPubDate"

    start_date = "2026-02-06"
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