# Marketplace Supplier Sync Engine

A FastAPI service that reconciles a supplier's CSV catalog against live prices and stock from marketplace seller accounts. It flags pricing and inventory discrepancies before they cost money, and shows them on a web dashboard and in a nightly email report.

## What it does

1. **Imports the supplier catalog.** Downloads a semicolon-delimited CSV from the supplier, or accepts an upload. Each import is stored as a timestamped batch, so price and stock history builds up over time.
2. **Imports the local warehouse.** Loads an Excel stock sheet, so stock you hold yourself counts toward availability.
3. **Syncs the marketplace.** Pulls every price and FBS stock level from one or more marketplace seller accounts.
4. **Compares and flags.** Joins the three sources on `offer_id` and classifies each product.

| Status | Condition (first match wins) |
|---|---|
| `CRITICAL LOSS` | marketplace price ≤ supplier dealer price × 1.10 |
| `GHOST STOCK` | supplier stock is 0 but the marketplace shows stock, and no local warehouse stock covers it |
| `MISSING STOCK` | supplier or local stock is available but marketplace stock is 0 |
| `LOW MARGIN` | marketplace price < supplier dealer price × 1.30 |

Anything else is considered safe and is not stored.

## Architecture

```
 Supplier CSV ──┐
                ├──► SQLite (sync_engine.db) ──► Dashboard (Jinja2 + Tailwind)
 Warehouse XLSX ┤         ▲                 ├► Excel / CSV export
                │         │                 └► Nightly HTML email (SMTP)
 Marketplace API┘   APScheduler jobs
 (per store)        18:00 import · 19:00 sync · 20:00 report (Europe/Istanbul)
```

- **`main.py`**: the FastAPI app: routes, import pipeline, marketplace API client, discrepancy engine and email builder.
- **`database.py`**: SQLite schema and connection handling. `init_db()` runs at startup and applies additive migrations.
- **`templates/`**: server-rendered dashboard pages.
- **`test_marketplace_ids.py`**: diagnostic script that prints raw API responses for a few offer IDs.

### offer_id mapping

Supplier rows are mapped to marketplace `offer_id`s by brand-specific rules in `make_offer_id()` (for example, a brand prefix plus the trailing digits of the EAN). Every comparison is normalized with `.strip().upper()`.

## Setup

Requires Python 3.10+.

```bash
git clone https://github.com/josephyast/marketplace-supplier-sync-engine.git
cd marketplace-supplier-sync-engine
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env    # then fill in real values
uvicorn main:app --reload
```

The app is served under `BASE_PATH` (default `/sync`): http://127.0.0.1:8000/sync/

### Configuration (`.env`)

All configuration is read from environment variables. `.env.example` lists them with placeholder values:

| Variable | Purpose |
|---|---|
| `BASE_PATH` | URL prefix the app is mounted under |
| `SOURCE_URL` | Supplier CSV catalog URL |
| `MARKETPLACE_API_URL` | Marketplace seller API base URL |
| `MARKETPLACE_API_KEY` / `MARKETPLACE_CLIENT_ID` | Store A credentials |
| `MARKETPLACE_API_KEY2` / `MARKETPLACE_CLIENT_ID2` | Store B credentials |
| `SMTP_SERVER`, `SMTP_PORT`, `SENDER_EMAIL`, `SENDER_PASSWORD`, `RECEIVER_EMAIL` | Daily email report |

A store with missing credentials is skipped. Never commit `.env`; it is gitignored.

## Main endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Dashboard: catalog, diff and marketplace discrepancies |
| GET | `/product/{product_id}` | Product detail, price/stock history and per-store status |
| GET | `/marketplace/all-products` | Every listing seen on the marketplace |
| POST | `/import` | Upload a supplier CSV |
| POST | `/warehouse/import` | Upload a local warehouse Excel |
| POST | `/fetch-now` | Pull the supplier CSV now |
| POST | `/marketplace/sync-now` | Run the marketplace sync in the background |
| POST | `/sync-all` | Import, then sync, in the background |
| GET | `/export/catalog.xlsx` | Export the current catalog |
| GET | `/marketplace/export/{fmt}` | Export discrepancies |

## License

[MIT](LICENSE)
