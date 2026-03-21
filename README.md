# PrePrint Intelligence Dashboard

A team dashboard for monitoring biotech preprint papers across 4 drug modalities.

## Project Structure

```
preprint_dashboard/
├── server.py           ← FastAPI backend + scheduler + DB
├── requirements.txt
├── render.yaml         ← Render.com deployment config
├── static/
│   └── index.html      ← React SPA (no build step needed)
├── arxiv_scraper.py    ← copy from your scrapers
├── chemrxiv_scraper.py ← copy from your scrapers
├── drugrxiv_scraper.py ← copy from your scrapers
└── config.json         ← copy from your config
```

## Local Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Install playwright browsers (for ChemRxiv + DrugRxiv)
playwright install chromium

# 3. Copy your scrapers into this folder
cp /path/to/arxiv_scraper.py .
cp /path/to/chemrxiv_scraper.py .
cp /path/to/drugrxiv_scraper.py .

# 4. Import your existing JSON results (one-time)
# Place your ranked_results_*.json file in this folder — it auto-imports on startup

# 5. Start the server
uvicorn server:app --reload --port 8000

# 6. Open http://localhost:8000
```

## Deploying to Render.com (FREE)

1. Push this folder to a GitHub repo
2. Go to https://render.com → New Web Service
3. Connect your GitHub repo
4. Render auto-detects `render.yaml` and sets up everything
5. Your app will be live at `https://your-app.onrender.com`

> **Important:** On Render's free tier, the server sleeps after inactivity.
> Upgrade to the $7/month Starter plan for always-on + persistent disk.

## How the Scheduler Works

- The daily scraper runs at **08:00 UTC** every day
- It scrapes today's modality (rotates every 4 days: bispecific → monoclonal → molecular glues → gene editing)
- Results go straight into SQLite
- The API serves the latest data immediately

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/modalities` | All 4 modalities with article counts |
| `GET /api/modalities/{key}/articles` | Ranked articles for a modality |
| `GET /api/modalities/{key}/archive` | Last 7 days of articles |
| `GET /api/articles/{id}` | Single article detail |
| `GET /api/stats` | Global stats |
| `POST /api/admin/trigger-scrape` | Manually trigger today's scrape |
| `POST /api/admin/ingest?path=...` | Import a JSON results file |

## Manual Ingest

To import a JSON file after deployment:
```bash
curl -X POST "https://your-app.onrender.com/api/admin/ingest?path=/data/ranked_results_2026-03-21.json"
```
