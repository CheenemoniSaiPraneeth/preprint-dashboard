import asyncio
import json
import random
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta

from arxiv_scraper import run_scraper as arxiv_scraper
from chemrxiv_scraper import run_scraper as chemrxiv_scraper
from drugrxiv_scraper import run_scraper as drugrxiv_scraper


# =====================================
# LOAD CONFIG
# =====================================

with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)


# =====================================
# MODALITY KEYWORDS
# =====================================

MODALITY_KEYWORDS = {
    "bispecific_antibodies": {
        "primary_keywords": [
            "bispecific",
            "bsAb",
            "BiTE",
            "T-cell engager",
            "CrossMab"
        ],
        "secondary_keywords": [
            "DART",
            "dual specificity",
            "heterodimerization",
            "CD3 binding",
            "cell-cell bridging",
            "tumor targeting",
            "immune synapse",
            "T-cell redirection",
            "bispecific format",
            "dual targeting",
            "trispecific",
            "fragment-based bispecific",
            "half-life extension",
            "checkpoint engager",
            "CD19/CD3",
            "BCMA/CD3",
            "HER2/HER3",
            "2+1 bispecific",
            "asymmetric antibody",
            "knobs-into-holes",
            "Fc-containing bispecific",
            "IgG-like bispecific",
            "non-IgG bispecific",
            "redirected lysis",
            "tumor cell killing"
        ]
    },
    "monoclonal_antibodies": {
        "primary_keywords": [
            "monoclonal",
            "mAb",
            "therapeutic antibody",
            "humanized antibody",
            "fully human antibody"
        ],
        "secondary_keywords": [
            "Fc engineering",
            "Fab region",
            "ADCC",
            "CDC",
            "antigen binding",
            "neutralizing antibody",
            "antibody affinity",
            "epitope binding",
            "IgG1",
            "IgG4",
            "antibody-dependent cellular cytotoxicity",
            "complement activation",
            "receptor blockade",
            "biologic therapy",
            "immune effector function",
            "antibody optimization",
            "paratope",
            "epitope mapping",
            "biodistribution",
            "half-life",
            "FcRn",
            "antibody internalization",
            "payload delivery",
            "target engagement",
            "clinical antibody"
        ]
    },
    "molecular_glues": {
        "primary_keywords": [
            "molecular glue",
            "targeted protein degradation",
            "TPD",
            "ternary complex",
            "E3 ligase"
        ],
        "secondary_keywords": [
            "CRBN",
            "VHL",
            "ubiquitination",
            "induced proximity",
            "selective degradation",
            "degrader",
            "neosubstrate",
            "cereblon",
            "proteasomal degradation",
            "ligase recruitment",
            "protein homeostasis",
            "degradation tag",
            "substrate recognition",
            "degradation machinery",
            "E3 recruitment",
            "molecular recognition",
            "target degradation",
            "proximity-induced",
            "protein knockdown",
            "degron",
            "ubiquitin ligase",
            "degradation signal",
            "small molecule degrader",
            "chemical inducer",
            "ternary binding"
        ]
    },
    "gene_editing": {
        "primary_keywords": [
            "gene editing",
            "genome editing",
            "CRISPR",
            "CRISPR-Cas9",
            "guide RNA"
        ],
        "secondary_keywords": [
            "sgRNA",
            "base editing",
            "prime editing",
            "HDR",
            "off-target",
            "Cas9",
            "Cas12",
            "editing efficiency",
            "knock-in",
            "knock-out",
            "PAM sequence",
            "genetic correction",
            "nuclease",
            "DNA repair",
            "double-strand break",
            "homology-directed repair",
            "non-homologous end joining",
            "NHEJ",
            "editing specificity",
            "delivery vector",
            "ribonucleoprotein",
            "CRISPR screen",
            "genome engineering",
            "guide design",
            "precision editing"
        ]
    }
}


# =====================================
# DATE RANGE: TODAY TO LAST 7 DAYS
# =====================================

today = datetime.today().date()
start_date_obj = today - timedelta(days=7)

START_DATE = start_date_obj.strftime("%Y-%m-%d")
END_DATE = today.strftime("%Y-%m-%d")

OUTPUT_FILE = f"ranked_results_{END_DATE}.json"
ARCHIVE_DIR = "archive_data"


# =====================================
# MODALITY ROTATION
# =====================================

MODALITY_ROTATION = [
    "bispecific_antibodies",
    "monoclonal_antibodies",
    "molecular_glues",
    "gene_editing"
]


def get_today_modality():
    day_index = today.toordinal() % len(MODALITY_ROTATION)
    return MODALITY_ROTATION[day_index]


# =====================================
# HELPERS
# =====================================

def ensure_archive_dir():
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def normalize_server_name(website_name):
    """
    Convert website names/urls into safe json file names.
    Removes protocol, replaces invalid filename chars, and keeps only
    alphanumeric, underscore, hyphen, and dot.
    """
    if not website_name:
        return "unknown"

    website_name = str(website_name).strip().lower()

    website_name = re.sub(r"^https?://", "", website_name)
    website_name = website_name.replace("\\", "/")
    website_name = website_name.replace("/", "_")
    website_name = website_name.replace(" ", "_")

    website_name = re.sub(r"[^a-z0-9._-]", "", website_name)
    website_name = re.sub(r"_+", "_", website_name).strip("._-")

    return website_name if website_name else "unknown"


def get_archive_file_path(website_name):
    normalized = normalize_server_name(website_name)
    return os.path.join(ARCHIVE_DIR, f"{normalized}.json")


def load_json_file(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def archive_results_by_website(results):
    """
    Store results separately by website/server.
    Prevent duplicate insertion for the same:
    (url, modality, paper_date, date_scraped)
    """
    ensure_archive_dir()

    grouped_by_site = defaultdict(list)
    for item in results:
        website = item.get("website", "unknown")
        grouped_by_site[website].append(item)

    for website, items in grouped_by_site.items():
        archive_path = get_archive_file_path(website)
        existing_data = load_json_file(archive_path, [])

        existing_keys = {
            (
                record.get("url", ""),
                record.get("modality", ""),
                record.get("date", ""),
                record.get("date_scraped", "")
            )
            for record in existing_data
        }

        new_items = []
        for item in items:
            unique_key = (
                item.get("url", ""),
                item.get("modality", ""),
                item.get("date", ""),
                item.get("date_scraped", "")
            )
            if unique_key not in existing_keys:
                new_items.append(item)
                existing_keys.add(unique_key)

        if new_items:
            existing_data.extend(new_items)
            save_json_file(archive_path, existing_data)
            print(f"Archived {len(new_items)} new records to {archive_path}")
        else:
            print(f"No new records to archive for {website}")


# =====================================
# SCRAPER ROUTER
# =====================================

async def run_for_server(server, keyword):
    query = keyword

    search_url = server["search_url"].replace("{query}", query)
    false_query = query + "zzxxyy"
    false_url = server["search_url"].replace("{query}", false_query)

    print(f"\nRunning {server['name']} for keyword: {keyword}")
    print(f"Date range: {START_DATE} to {END_DATE}")

    if server["name"] in ["arXiv", "bioRxiv", "medRxiv"]:
        return await arxiv_scraper(
            server["url"],
            search_url,
            false_url,
            START_DATE,
            END_DATE
        )

    elif server["name"] == "ChemRxiv":
        return await chemrxiv_scraper(
            server["url"],
            search_url,
            false_url,
            START_DATE,
            END_DATE
        )

    elif server["name"] == "DrugRxiv":
        return await drugrxiv_scraper(
            server["url"],
            search_url,
            false_url,
            START_DATE,
            END_DATE
        )

    return []


# =====================================
# KEYWORD SCORING
# =====================================

def keyword_score(text, keywords):
    if not text:
        return 0

    text = text.lower()
    score = 0

    for k in keywords:
        score += text.count(k.lower())

    return score


def find_matched_keywords(text, keywords):
    if not text:
        return []

    text = text.lower()
    matched = []

    for k in keywords:
        if k.lower() in text:
            matched.append(k)

    return matched


# =====================================
# FINAL PIPELINE
# =====================================

async def run_pipeline():
    modality_results = {}

    today_modality = get_today_modality()
    print(f"\nToday's modality: {today_modality}")
    print(f"Scraping only one-week window: {START_DATE} to {END_DATE}")

    keyword_groups = MODALITY_KEYWORDS[today_modality]

    print(f"\n{'=' * 60}")
    print(f"Processing modality: {today_modality}")
    print(f"{'=' * 60}")

    primary_keywords = keyword_groups["primary_keywords"]
    secondary_keywords = keyword_groups["secondary_keywords"]

    all_results = []

    # search only with 5 primary keywords for today's modality only
    for keyword in primary_keywords:
        for server in CONFIG["preprint_servers"]:
            try:
                results = await run_for_server(server, keyword)

                for item in results:
                    item["modality"] = today_modality
                    item["matched_keyword"] = keyword

                all_results.extend(results)

            except Exception as e:
                print(f"Error for {server['name']} with keyword '{keyword}': {e}")

            wait_time = random.uniform(3, 6)
            print(f"Sleeping {wait_time:.2f}s\n")
            await asyncio.sleep(wait_time)

    print(f"\nTotal raw results for {today_modality}: {len(all_results)}")

    # =====================================
    # DEDUPLICATION PER WEBSITE + URL
    # =====================================

    grouped = defaultdict(list)

    for r in all_results:
        key = (r.get("website"), r.get("url"))
        grouped[key].append(r)

    final_results = []

    for (website, url), items in grouped.items():
        duplicate_score = len(items) - 1

        abstract = items[0].get("abstract", "")
        date = items[0].get("date", "")

        matched_keywords = sorted(
            list(set(item.get("matched_keyword", "") for item in items if item.get("matched_keyword")))
        )

        primary_keyword_hits = keyword_score(abstract, primary_keywords)
        primary_abstract_matched_keywords = find_matched_keywords(abstract, primary_keywords)

        secondary_keyword_hits = keyword_score(abstract, secondary_keywords)
        secondary_abstract_matched_keywords = find_matched_keywords(abstract, secondary_keywords)

        score = duplicate_score + primary_keyword_hits + secondary_keyword_hits

        final_results.append({
            "date_scraped": END_DATE,
            "date_window_start": START_DATE,
            "date_window_end": END_DATE,
            "modality": today_modality,
            "website": website,
            "url": url,
            "date": date,
            "abstract": abstract,
            "searched_keywords_found_duplicates": matched_keywords,
            "primary_abstract_matched_keywords": primary_abstract_matched_keywords,
            "secondary_abstract_matched_keywords": secondary_abstract_matched_keywords,
            "duplicate_count": duplicate_score,
            "primary_keyword_hits": primary_keyword_hits,
            "secondary_keyword_hits": secondary_keyword_hits,
            "score": score
        })

    final_results.sort(key=lambda x: x["score"], reverse=True)

    modality_results[today_modality] = final_results

    print(f"\n========== FINAL RANKED RESULTS FOR {today_modality} ==========")
    for r in final_results[:10]:
        print(r)

    # =====================================
    # SAVE TODAY'S RANKED FILE
    # =====================================

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(modality_results, f, indent=2, ensure_ascii=False)

    print(f"\nSaved ranked results to {OUTPUT_FILE}")

    # =====================================
    # SAVE SERVER-WISE ARCHIVE FILES
    # =====================================

    archive_results_by_website(final_results)

    return modality_results


# =====================================
# MAIN
# =====================================

if __name__ == "__main__":
    asyncio.run(run_pipeline())