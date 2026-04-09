"""
PrePrint Intelligence Dashboard — FastAPI Backend
Serves the React SPA + REST API + SQLite storage + APScheduler
"""

import asyncio
import json
import os
import re
import sqlite3
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

DB_PATH = "preprint.db"
STATIC_DIR = Path("static")
FRONTEND_FILE = STATIC_DIR / "index.html"

# Import scrapers only if they exist alongside this file
SCRAPERS_AVAILABLE = False
try:
    sys.path.insert(0, str(Path(__file__).parent))
    from arxiv_scraper import run_scraper as arxiv_scraper
    from chemrxiv_scraper import run_scraper as chemrxiv_scraper
    from drugrxiv_scraper import run_scraper as drugrxiv_scraper
    SCRAPERS_AVAILABLE = True
except ImportError:
    pass

MODALITY_ROTATION = [
    "bispecific_antibodies",
    "monoclonal_antibodies",
    "molecular_glues",
    "gene_editing",
]

MODALITY_LABELS = {
    "bispecific_antibodies": "Bispecific Antibodies",
    "monoclonal_antibodies": "Monoclonal Antibodies",
    "molecular_glues": "Molecular Glues",
    "gene_editing": "Gene Editing",
}

MODALITY_KEYWORDS = {
    "bispecific_antibodies": {
        "primary_keywords": ["bispecific","bsAb","BiTE","T-cell engager","CrossMab"],
        "secondary_keywords": ["DART","dual specificity","heterodimerization","CD3 binding","cell-cell bridging","tumor targeting","immune synapse","T-cell redirection","bispecific format","dual targeting","trispecific","fragment-based bispecific","half-life extension","checkpoint engager","CD19/CD3","BCMA/CD3","HER2/HER3","2+1 bispecific","asymmetric antibody","knobs-into-holes","Fc-containing bispecific","IgG-like bispecific","non-IgG bispecific","redirected lysis","tumor cell killing"],
    },
    "monoclonal_antibodies": {
        "primary_keywords": ["monoclonal","mAb","therapeutic antibody","humanized antibody","fully human antibody"],
        "secondary_keywords": ["Fc engineering","Fab region","ADCC","CDC","antigen binding","neutralizing antibody","antibody affinity","epitope binding","IgG1","IgG4","antibody-dependent cellular cytotoxicity","complement activation","receptor blockade","biologic therapy","immune effector function","antibody optimization","paratope","epitope mapping","biodistribution","half-life","FcRn","antibody internalization","payload delivery","target engagement","clinical antibody"],
    },
    "molecular_glues": {
        "primary_keywords": ["molecular glue","targeted protein degradation","TPD","ternary complex","E3 ligase"],
        "secondary_keywords": ["CRBN","VHL","ubiquitination","induced proximity","selective degradation","degrader","neosubstrate","cereblon","proteasomal degradation","ligase recruitment","protein homeostasis","degradation tag","substrate recognition","degradation machinery","E3 recruitment","molecular recognition","target degradation","proximity-induced","protein knockdown","degron","ubiquitin ligase","degradation signal","small molecule degrader","chemical inducer","ternary binding"],
    },
    "gene_editing": {
        "primary_keywords": ["gene editing","genome editing","CRISPR","CRISPR-Cas9","guide RNA"],
        "secondary_keywords": ["sgRNA","base editing","prime editing","HDR","off-target","Cas9","Cas12","editing efficiency","knock-in","knock-out","PAM sequence","genetic correction","nuclease","DNA repair","double-strand break","homology-directed repair","non-homologous end joining","NHEJ","editing specificity","delivery vector","ribonucleoprotein","CRISPR screen","genome engineering","guide design","precision editing"],
    },
}

PREPRINT_SERVERS = [
    {"name": "bioRxiv", "url": "https://www.biorxiv.org/", "search_url": "https://www.biorxiv.org/search/{query}%20numresults%3A10%20sort%3Apublication-date%20direction%3Adescending"},
    {"name": "medRxiv", "url": "https://www.medrxiv.org/", "search_url": "https://www.medrxiv.org/search/{query}%20numresults%3A10%20sort%3Apublication-date%20direction%3Adescending"},
    {"name": "ChemRxiv", "url": "https://chemrxiv.org/", "search_url": "https://chemrxiv.org/action/doSearch?AllField={query}&startPage=0&sortBy=EPubDate"},
    {"name": "DrugRxiv", "url": "https://drugrepocentral.scienceopen.com/search", "search_url": "https://drugrepocentral.scienceopen.com/search#('v'~4_'id'~''_'queryType'~1_'context'~null_'kind'~77_'order'~3_'orderLowestFirst'~false_'query'~'(query)'_'filters'~!('kind'~86_'not'~false_'offset'~1_'timeUnit'~5)*_'hideOthers'~false)"},
    {"name": "arXiv", "url": "https://arxiv.org/", "search_url": "https://arxiv.org/search/?searchtype=all&query={query}&abstracts=show&size=50&order=-announced_date_first"},
]

# ─────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_scraped TEXT NOT NULL,
            date_window_start TEXT,
            date_window_end TEXT,
            modality TEXT NOT NULL,
            website TEXT,
            url TEXT NOT NULL,
            date TEXT,
            abstract TEXT,
            searched_keywords_found_duplicates TEXT,
            primary_abstract_matched_keywords TEXT,
            secondary_abstract_matched_keywords TEXT,
            duplicate_count INTEGER DEFAULT 0,
            primary_keyword_hits INTEGER DEFAULT 0,
            secondary_keyword_hits INTEGER DEFAULT 0,
            score INTEGER DEFAULT 0,
            UNIQUE(url, modality, date_scraped)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_modality ON articles(modality)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_date_scraped ON articles(date_scraped)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_score ON articles(score DESC)")
    conn.commit()
    conn.close()
    print("✅ Database initialised")


def ingest_json_file(path: str):
    """Load a ranked_results_*.json file into SQLite."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    conn = get_db()
    inserted = 0
    skipped = 0

    for modality, articles in data.items():
        for a in articles:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO articles
                    (date_scraped, date_window_start, date_window_end, modality,
                     website, url, date, abstract,
                     searched_keywords_found_duplicates,
                     primary_abstract_matched_keywords,
                     secondary_abstract_matched_keywords,
                     duplicate_count, primary_keyword_hits,
                     secondary_keyword_hits, score)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    a.get("date_scraped", ""),
                    a.get("date_window_start", ""),
                    a.get("date_window_end", ""),
                    a.get("modality", modality),
                    a.get("website", ""),
                    a.get("url", ""),
                    a.get("date", ""),
                    a.get("abstract", ""),
                    json.dumps(a.get("searched_keywords_found_duplicates", [])),
                    json.dumps(a.get("primary_abstract_matched_keywords", [])),
                    json.dumps(a.get("secondary_abstract_matched_keywords", [])),
                    a.get("duplicate_count", 0),
                    a.get("primary_keyword_hits", 0),
                    a.get("secondary_keyword_hits", 0),
                    a.get("score", 0),
                ))
                inserted += 1
            except Exception:
                skipped += 1

    conn.commit()
    conn.close()
    print(f"✅ Ingested {inserted} articles from {path} (skipped {skipped})")


# ─────────────────────────────────────────────
# SCRAPER PIPELINE (mirrors main.py)
# ─────────────────────────────────────────────

def keyword_score(text, keywords):
    if not text:
        return 0
    text = text.lower()
    return sum(text.count(k.lower()) for k in keywords)


def find_matched_keywords(text, keywords):
    if not text:
        return []
    text = text.lower()
    return [k for k in keywords if k.lower() in text]


async def run_scraper_for_server(server, keyword, start_date, end_date):
    search_url = server["search_url"].replace("{query}", keyword)
    false_url = server["search_url"].replace("{query}", keyword + "zzxxyy")

    if server["name"] in ["arXiv", "bioRxiv", "medRxiv"]:
        return await arxiv_scraper(server["url"], search_url, false_url, start_date, end_date)
    elif server["name"] == "ChemRxiv":
        return await chemrxiv_scraper(server["url"], search_url, false_url, start_date, end_date)
    elif server["name"] == "DrugRxiv":
        return await drugrxiv_scraper(server["url"], search_url, false_url, start_date, end_date)
    return []


async def run_daily_pipeline():
    if not SCRAPERS_AVAILABLE:
        print("⚠ Scrapers not available — skipping pipeline run")
        return

    today = datetime.today().date()
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = today.strftime("%Y-%m-%d")

    day_index = today.toordinal() % len(MODALITY_ROTATION)
    modality = MODALITY_ROTATION[day_index]
    kw_groups = MODALITY_KEYWORDS[modality]
    primary_kws = kw_groups["primary_keywords"]
    secondary_kws = kw_groups["secondary_keywords"]

    print(f"\n🚀 Daily pipeline | modality={modality} | {start_date} → {end_date}")

    all_raw = []
    for kw in primary_kws:
        for server in PREPRINT_SERVERS:
            try:
                results = await run_scraper_for_server(server, kw, start_date, end_date)
                for r in results:
                    r["modality"] = modality
                    r["matched_keyword"] = kw
                all_raw.extend(results)
            except Exception as e:
                print(f"  ⚠ {server['name']} / {kw}: {e}")
            await asyncio.sleep(3)

    from collections import defaultdict
    grouped = defaultdict(list)
    for r in all_raw:
        grouped[(r.get("website"), r.get("url"))].append(r)

    conn = get_db()
    inserted = 0
    for (website, url), items in grouped.items():
        dup = len(items) - 1
        abstract = items[0].get("abstract", "")
        date = items[0].get("date", "")
        matched_kws = sorted(set(i.get("matched_keyword", "") for i in items if i.get("matched_keyword")))
        p_hits = keyword_score(abstract, primary_kws)
        s_hits = keyword_score(abstract, secondary_kws)
        score = dup + p_hits + s_hits

        try:
            conn.execute("""
                INSERT OR IGNORE INTO articles
                (date_scraped, date_window_start, date_window_end, modality,
                 website, url, date, abstract,
                 searched_keywords_found_duplicates,
                 primary_abstract_matched_keywords,
                 secondary_abstract_matched_keywords,
                 duplicate_count, primary_keyword_hits,
                 secondary_keyword_hits, score)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                end_date, start_date, end_date, modality,
                website, url, date, abstract,
                json.dumps(matched_kws),
                json.dumps(find_matched_keywords(abstract, primary_kws)),
                json.dumps(find_matched_keywords(abstract, secondary_kws)),
                dup, p_hits, s_hits, score,
            ))
            inserted += 1
        except Exception:
            pass

    conn.commit()
    conn.close()
    print(f"✅ Pipeline complete — {inserted} new articles stored")


# ─────────────────────────────────────────────
# STARTUP / SHUTDOWN
# ─────────────────────────────────────────────

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()

    # Auto-import any ranked_results_*.json found in working dir
    for p in Path(".").glob("ranked_results_*.json"):
        print(f"📂 Auto-importing {p}")
        ingest_json_file(str(p))

    # Schedule daily scrape at 08:00
    scheduler.add_job(
        run_daily_pipeline,
        CronTrigger(hour=8, minute=0),
        id="daily_scrape",
        replace_existing=True,
    )
    scheduler.start()
    print("⏰ Scheduler started — daily scrape at 08:00")
    yield
    scheduler.shutdown()


# ─────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────

app = FastAPI(title="PrePrint Intelligence", lifespan=lifespan)


# Serve static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ─────────────────────────────────────────────
# REST API
# ─────────────────────────────────────────────

def row_to_dict(row):
    d = dict(row)
    for field in ["searched_keywords_found_duplicates",
                  "primary_abstract_matched_keywords",
                  "secondary_abstract_matched_keywords"]:
        try:
            d[field] = json.loads(d[field]) if d[field] else []
        except Exception:
            d[field] = []
    return d


@app.get("/api/modalities")
def get_modalities():
    """Return summary stats for all 4 modalities."""
    conn = get_db()
    result = []
    for key in MODALITY_ROTATION:
        # Latest scrape date for this modality
        row = conn.execute(
            "SELECT MAX(date_scraped) as last_scraped, COUNT(*) as total FROM articles WHERE modality=? AND date_scraped >= date('now', '-7 days')",
            (key,)
        ).fetchone()
        result.append({
            "key": key,
            "label": MODALITY_LABELS[key],
            "last_scraped": row["last_scraped"],
            "total_articles": row["total"],
        })
    conn.close()
    return result


@app.get("/api/modalities/{modality}/articles")
def get_articles(
    modality: str,
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
):
    """Return articles for a modality, ordered by score desc."""
    if modality not in MODALITY_ROTATION:
        raise HTTPException(404, "Unknown modality")

    conn = get_db()

    filters = ["modality = ?"]
    params: list = [modality]
    if not date_from and not date_to:
        cutoff = (datetime.today().date() - timedelta(days=7)).isoformat()
        filters.append("date_scraped >= ?")
        params.append(cutoff)
    if date_from:
        filters.append("date >= ?")
        params.append(date_from)
    if date_to:
        filters.append("date <= ?")
        params.append(date_to)

    where = " AND ".join(filters)

    total = conn.execute(f"SELECT COUNT(*) FROM articles WHERE {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT * FROM articles WHERE {where} ORDER BY score DESC, date DESC LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchall()

    conn.close()
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "articles": [row_to_dict(r) for r in rows],
    }


@app.get("/api/modalities/{modality}/archive")
def get_archive(modality: str):
    """Return articles from the last 7 days for a modality."""
    if modality not in MODALITY_ROTATION:
        raise HTTPException(404, "Unknown modality")

    cutoff = (datetime.today().date() - timedelta(days=7)).isoformat()
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM articles WHERE modality=? AND date >= ? ORDER BY score DESC, date DESC",
        (modality, cutoff),
    ).fetchall()
    conn.close()

    return {
        "modality": modality,
        "label": MODALITY_LABELS[modality],
        "from_date": cutoff,
        "articles": [row_to_dict(r) for r in rows],
    }


@app.get("/api/articles/{article_id}")
def get_article(article_id: int):
    """Return a single article by ID."""
    conn = get_db()
    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    conn.close()
    if not row:
        raise HTTPException(404, "Article not found")
    return row_to_dict(row)


@app.get("/api/stats")
def get_stats():
    """Overall DB stats."""
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    by_modality = conn.execute(
        "SELECT modality, COUNT(*) as n FROM articles GROUP BY modality"
    ).fetchall()
    latest = conn.execute("SELECT MAX(date_scraped) FROM articles").fetchone()[0]
    conn.close()
    return {
        "total_articles": total,
        "latest_scrape": latest,
        "by_modality": {r["modality"]: r["n"] for r in by_modality},
    }


@app.post("/api/admin/trigger-scrape")
async def trigger_scrape():
    """Manually trigger today's scrape pipeline."""
    asyncio.create_task(run_daily_pipeline())
    return {"status": "started", "message": "Pipeline triggered in background"}


@app.post("/api/admin/ingest")
def ingest_file(path: str):
    """Manually ingest a JSON results file."""
    if not os.path.exists(path):
        raise HTTPException(400, f"File not found: {path}")
    ingest_json_file(path)
    return {"status": "ok", "path": path}


# ─────────────────────────────────────────────
# SPA CATCH-ALL
# ─────────────────────────────────────────────

@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if FRONTEND_FILE.exists():
        return FileResponse(str(FRONTEND_FILE))
    return HTMLResponse("<h1>Frontend not built yet</h1>", status_code=503)
