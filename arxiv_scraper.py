import asyncio
import re
import random
import time
from urllib.parse import urljoin
import aiohttp
from bs4 import BeautifulSoup
from dateutil import parser
import ssl
import certifi
ssl_context=ssl.create_default_context(cafile=certifi.where())
# =========================================
# CONFIG
# =========================================

MAX_PAGES = 2
CONCURRENT_REQUESTS = 6
MAX_BLOCK_TIME = 3000

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}


# =========================================
# DATE HELPERS
# =========================================

def normalize_date(raw):
    try:
        return parser.parse(raw).date().isoformat()
    except:
        return None


def in_range(date, start_date, end_date):
    return start_date <= date <= end_date


# =========================================
# LINK EXTRACTION
# =========================================

def extract_links(soup, base_url):
    links = set()
    for a in soup.find_all("a", href=True):
        links.add(urljoin(base_url, a["href"]))
    return links


def find_next_page(soup, current_url):
    if "/search" not in current_url:
        return None

    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True).lower()
        href = a["href"]

        if "next" in text and "/search" in href:
            return urljoin(current_url, href)

    return None


# =========================================
# ABSTRACT EXTRACTION (UNCHANGED)
# =========================================

def extract_abstract(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
        tag.decompose()

    bio_block = soup.find("div", class_=re.compile("abstract", re.I))
    if bio_block:
        text = bio_block.get_text(" ", strip=True)
        if len(text) > 100:
            return clean_abstract(text)

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            if "articleBody" in script.text:
                match = re.search(
                    r'"articleBody"\s*:\s*"(.+?)"\s*(,|\})',
                    script.text,
                    re.DOTALL
                )
                if match:
                    text = match.group(1)
                    text = re.sub(r"\s+", " ", text)
                    return clean_abstract(text)
        except:
            pass

    article = soup.find("article")
    if article:
        text = article.get_text(" ", strip=True)
        if len(text) > 300:
            return clean_abstract(text)

    main = soup.find("main")
    if main:
        text = main.get_text(" ", strip=True)
        if len(text) > 300:
            return clean_abstract(text)

    return None


def clean_abstract(text):

    text = re.sub(r"\s+", " ", text)

    stop_patterns = [
        r"\bNOTE:",
        r"\bCompeting Interest",
        r"\bConflict of Interest",
        r"\bCorresponding author",
        r"\bFunding",
        r"\bAuthor Contributions",
        r"\bData Availability",
        r"\bSupplementary",
        r"\bAcknowledgements?",
        r"\bEthics Statement",
        r"\bPatent",
    ]

    for pattern in stop_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            text = text[:match.start()]

    text = re.sub(r"\S+@\S+", "", text)
    text = re.sub(r"\bDepartment of .*? ,", "", text)

    return text[:4000]


# =========================================
# DATE EXTRACTION (UNCHANGED)
# =========================================

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
# SAFE GET WITH 429 DEBUG (UNCHANGED)
# =========================================

async def safe_get(session, url):

    print(f"\n🌐 Requesting: {url}")

    block_start = None

    while True:
        async with session.get(url) as r:

            if r.status == 429:

                if block_start is None:
                    block_start = time.time()

                elapsed = time.time() - block_start

                print("⚠ 429 detected. Waiting 180 seconds...")

                if elapsed > MAX_BLOCK_TIME:
                    print("❌ Blocked too long. Aborting:", url)
                    return None

                await asyncio.sleep(180)
                continue

            if r.status != 200:
                print(f"❌ Failed with status {r.status}")
                return None

            print("✅ Success")
            return await r.text(errors="ignore")


# =========================================
# 🔥 ONLY LOGIC CHANGE HERE
# =========================================

async def process_page(session, page_url, B, start_date, end_date, base_url):

    print("\n" + "="*80)
    print(f"📄 Processing page: {page_url}")

    results = []
    start_date_obj = parser.parse(start_date).date()
    valid_found = False

    html = await safe_get(session, page_url)
    if not html:
        return results, False

    soup = BeautifulSoup(html, "html.parser")
    A = extract_links(soup, page_url)

    print(f"🔎 A links found: {len(A)}")
    print(f"🚫 B links found: {len(B)}")

    candidate_links = A - B

    blocked_keywords = [
        "search", "authors", "author",
        "pdf", "login", "signup",
        "rss", "feed", "download",
        "share", "#","user","related","accounts","cluster","format"
    ]

    candidate_links = [
        link for link in candidate_links
        if not any(b in link.lower() for b in blocked_keywords)
    ]

    print(f"✅ After filtering: {len(candidate_links)}")

    for link in candidate_links:

        print(f"\n➡ Visiting article: {link}")

        article_html = await safe_get(session, link)
        if not article_html:
            continue

        best_date = extract_best_date(article_html)
        print(f"📅 Extracted date: {best_date}")

        if not best_date:
            continue

        article_date = parser.parse(best_date).date()

        if article_date < start_date_obj:
            print("⏩ Older than start_date.")
            continue

        if in_range(best_date, start_date, end_date):
            print("✅ In range. Collected.")
            valid_found = True

            abstract = extract_abstract(article_html)

            results.append({
                "website": base_url,
                "url": link,
                "date": best_date,
                "abstract": abstract
            })

    print(f"📊 Page summary: {len(results)} valid articles found")

    return results, valid_found


# =========================================
# PUBLIC ENTRY (UNCHANGED)
# =========================================

async def run_scraper(
    base_url,
    true_url,
    false_url,
    start_date,
    end_date
):

    async with aiohttp.ClientSession(headers=HEADERS,connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:

        current = true_url
        visited = set()
        all_results = []
        no_valid_counter = 0

        B = set()
        if false_url and false_url != true_url:
            html_false = await safe_get(session, false_url)
            if html_false:
                soup_false = BeautifulSoup(html_false, "html.parser")
                B = extract_links(soup_false, false_url)

        while current and current not in visited:

            print(f"\n🔁 Moving to page: {current}")

            page_results, valid_found = await process_page(
                session,
                current,
                B,
                start_date,
                end_date,
                base_url
            )

            all_results.extend(page_results)

            if not valid_found:
                no_valid_counter += 1
                print(f"⚠ No valid articles. Counter: {no_valid_counter}")
            else:
                no_valid_counter = 0

            if no_valid_counter >= 2:
                print("🛑 Stopping: 2 consecutive empty pages.")
                break

            visited.add(current)

            html = await safe_get(session, current)
            if not html:
                break

            soup = BeautifulSoup(html, "html.parser")
            current = find_next_page(soup, current)

            print(f"➡ Next page: {current}")

            if current:
                await asyncio.sleep(2)

        print("\n✅ Scraping finished.")
        print("📊 Total collected:", len(all_results))

        return all_results