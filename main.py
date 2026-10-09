from fastapi import FastAPI, UploadFile, File, Request, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from typing import Optional
from dotenv import load_dotenv
import pandas as pd
import sqlite3
import io
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import httpx
import logging
from urllib.parse import urlencode
import traceback

load_dotenv()

from database import init_db, get_conn, DB_PATH

BASE_PATH = os.getenv("BASE_PATH", "/sync")

SOURCE_URL     = os.getenv("SOURCE_URL", "https://api.supplier-example.com/v1/catalog.csv")
MARKETPLACE_API_KEY   = os.getenv("MARKETPLACE_API_KEY", "")
MARKETPLACE_CLIENT_ID = os.getenv("MARKETPLACE_CLIENT_ID", "")
MARKETPLACE_API_KEY2   = os.getenv("MARKETPLACE_API_KEY2", "")
MARKETPLACE_CLIENT_ID2 = os.getenv("MARKETPLACE_CLIENT_ID2", "")

SMTP_SERVER       = os.getenv("SMTP_SERVER", "")
SMTP_PORT         = int(os.getenv("SMTP_PORT", "465"))
SENDER_EMAIL      = os.getenv("SENDER_EMAIL", "")
SENDER_PASSWORD   = os.getenv("SENDER_PASSWORD", "")
RECEIVER_EMAIL    = os.getenv("RECEIVER_EMAIL", "")

MARKETPLACE_ACCOUNTS = [
    ("Store A",      MARKETPLACE_API_KEY,  MARKETPLACE_CLIENT_ID),
    ("Store B",  MARKETPLACE_API_KEY2, MARKETPLACE_CLIENT_ID2),
]

# Remote CSV column → DB column
COLUMN_MAP = {
    "product_id":              "product_id",
    "name":                    "title",
    "available_stock":         "stock",
    "price":                   "price",
    "dealer_price":            "dealer_price",
    "discharge_date":          "discharge_date",
    "availability":            "availability",
    "product_url":             "product_url",
    "brand":                   "brand",
    "ean":                     "ean",
    "main_image_url":          "main_image_url",
    "minimum_units_per_order": "minimum_units_per_order",
    "on_sale":                 "on_sale",
    "brand_hierarchy":         "brand_hierarchy",
}

app = FastAPI(root_path=BASE_PATH)
templates = Jinja2Templates(directory="templates")
templates.env.globals["base_path"] = BASE_PATH


def page_url(request: Request, path: str = "/", **overrides) -> str:
    """Rebuild the current URL with only the named parameters changed.

    Pagination, sorting and export links all start from the *whole* incoming
    query string instead of a hand-listed subset. Listing parameters by hand is
    what made links silently drop filters: the catalog's links only carried
    catalog parameters, so paging the catalog reset every marketplace_* filter, and any
    filter added later would have to be remembered in each link.

    An override of None or "" removes that parameter, which is how the Clear
    links drop one module's filters without touching the other's.
    """
    params = dict(request.query_params)
    for key, value in overrides.items():
        if value is None or value == "":
            params.pop(key, None)
        else:
            params[key] = str(value)
    query = urlencode(params)
    return f"{BASE_PATH}{path}?{query}" if query else f"{BASE_PATH}{path}"


templates.env.globals["page_url"] = page_url
scheduler = AsyncIOScheduler()


# ── Diff Engine ──────────────────────────────────────────────────────────────

# batch_diff kinds, in the order the dashboard renders them.
DIFF_KINDS = ("NEW", "OUT_OF_STOCK", "PRICE_UP", "PRICE_DOWN")


def _diff_batch_pair(conn) -> tuple:
    """The batch to report on, and the batch it is compared against.

    Comparison is day-to-day rather than batch-to-batch: a manual re-import in the
    afternoon must still diff against yesterday, not against the morning's run.
    Within a day the last batch wins, so the newest known state of each day is used.
    Returns (None, None) when there are fewer than two days of history.
    """
    days = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT date(fetched_at) FROM supplier_products ORDER BY 1 DESC LIMIT 2"
        ).fetchall()
    ]
    if len(days) < 2:
        return None, None
    last_of = lambda d: conn.execute(
        "SELECT MAX(fetched_at) FROM supplier_products WHERE date(fetched_at) = ?", (d,)
    ).fetchone()[0]
    return last_of(days[0]), last_of(days[1])


# One INSERT per kind rather than a single CASE expression: the four categories
# overlap by design, and a CASE would silently keep only the first match.
_DIFF_INSERTS = {
    "NEW": """
        INSERT OR REPLACE INTO batch_diff
               (batch_at, prev_at, kind, product_id, title, stock_new, price_new)
        SELECT :new, :prev, 'NEW', n.product_id, n.title, n.stock, n.price
        FROM supplier_products n
        LEFT JOIN supplier_products o
               ON o.product_id = n.product_id AND o.fetched_at = :prev
        WHERE n.fetched_at = :new AND o.product_id IS NULL
    """,
    "OUT_OF_STOCK": """
        INSERT OR REPLACE INTO batch_diff
               (batch_at, prev_at, kind, product_id, title, stock_old, stock_new)
        SELECT :new, :prev, 'OUT_OF_STOCK', n.product_id, n.title, o.stock, n.stock
        FROM supplier_products n
        JOIN supplier_products o
          ON o.product_id = n.product_id AND o.fetched_at = :prev
        WHERE n.fetched_at = :new AND o.stock > 0 AND n.stock = 0
    """,
    "PRICE_UP": """
        INSERT OR REPLACE INTO batch_diff
               (batch_at, prev_at, kind, product_id, title, price_old, price_new)
        SELECT :new, :prev, 'PRICE_UP', n.product_id, n.title, o.price, n.price
        FROM supplier_products n
        JOIN supplier_products o
          ON o.product_id = n.product_id AND o.fetched_at = :prev
        WHERE n.fetched_at = :new
          AND o.price IS NOT NULL AND n.price IS NOT NULL AND n.price > o.price
    """,
    "PRICE_DOWN": """
        INSERT OR REPLACE INTO batch_diff
               (batch_at, prev_at, kind, product_id, title, price_old, price_new)
        SELECT :new, :prev, 'PRICE_DOWN', n.product_id, n.title, o.price, n.price
        FROM supplier_products n
        JOIN supplier_products o
          ON o.product_id = n.product_id AND o.fetched_at = :prev
        WHERE n.fetched_at = :new
          AND o.price IS NOT NULL AND n.price IS NOT NULL AND n.price < o.price
    """,
}


def rebuild_batch_diff(new_at: Optional[str] = None, prev_at: Optional[str] = None) -> int:
    """Compute one batch's differences and store them in batch_diff.

    Runs once per import. Both sides of every join hit idx_sp_diff as a covering
    index, so the table itself is only read for the title column.
    """
    with get_conn() as conn:
        if new_at is None:
            new_at, prev_at = _diff_batch_pair(conn)
        if not new_at or not prev_at:
            return 0

        conn.execute("DELETE FROM batch_diff WHERE batch_at = ?", (new_at,))
        total = 0
        for kind in DIFF_KINDS:
            total += conn.execute(
                _DIFF_INSERTS[kind], {"new": new_at, "prev": prev_at}
            ).rowcount

    logging.info("[batch_diff] %s vs %s: %d satır", new_at, prev_at, total)
    return total


def backfill_batch_diff() -> int:
    """Populate batch_diff for every day pair already in supplier_products."""
    with get_conn() as conn:
        days = [
            r[0] for r in conn.execute(
                "SELECT DISTINCT date(fetched_at) FROM supplier_products ORDER BY 1"
            ).fetchall()
        ]
        pairs = []
        for prev_day, new_day in zip(days, days[1:]):
            last_of = lambda d: conn.execute(
                "SELECT MAX(fetched_at) FROM supplier_products WHERE date(fetched_at) = ?", (d,)
            ).fetchone()[0]
            pairs.append((last_of(new_day), last_of(prev_day)))

    return sum(rebuild_batch_diff(new_at, prev_at) for new_at, prev_at in pairs)


def compute_diff():
    """The four dashboard lists, read straight out of batch_diff.

    Previously this pulled every row of supplier_products into pandas on each
    request; the work now happens once per import in rebuild_batch_diff().
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT kind, product_id, title, stock_old, stock_new, price_old, price_new
            FROM batch_diff
            WHERE batch_at = (SELECT MAX(batch_at) FROM batch_diff)
            """
        ).fetchall()

    buckets: dict = {k: [] for k in DIFF_KINDS}
    for r in rows:
        buckets[r["kind"]].append(r)

    new_items = [
        {"product_id": r["product_id"], "title": r["title"],
         "stock": r["stock_new"], "price": r["price_new"]}
        for r in buckets["NEW"]
    ]
    out_of_stock = [
        {"product_id": r["product_id"], "title": r["title"], "stock_old": r["stock_old"]}
        for r in buckets["OUT_OF_STOCK"]
    ]
    price_moved = {
        kind: [
            {"product_id": r["product_id"], "title": r["title"],
             "price_old": r["price_old"], "price_new": r["price_new"]}
            for r in buckets[kind]
        ]
        for kind in ("PRICE_UP", "PRICE_DOWN")
    }

    return new_items, out_of_stock, price_moved["PRICE_UP"], price_moved["PRICE_DOWN"]


def compute_marketplace_alerts():
    # marketplace_products is only populated by the Marketplace CSV upload, so read it first
    # and bail out before pulling the whole catalog into pandas for nothing.
    with get_conn() as conn:
        marketplace = pd.read_sql_query("SELECT product_id, stock, status FROM marketplace_products", conn)
        if marketplace.empty:
            return [], []

        supplier = pd.read_sql_query("SELECT product_id, stock FROM supplier_current", conn)

    if supplier.empty:
        return [], []

    merged = supplier.merge(marketplace, on="product_id", suffixes=("_sup", "_marketplace"))

    # Active on Marketplace but out of stock in supplier CSV
    alert1 = merged[
        (merged["status"] == "active") & (merged["stock_sup"] == 0)
    ][["product_id", "stock_sup", "stock_marketplace", "status"]]

    # In stock in supplier CSV but out of stock on Marketplace
    alert2 = merged[
        (merged["stock_sup"] > 0) & (merged["stock_marketplace"] == 0)
    ][["product_id", "stock_sup", "stock_marketplace", "status"]]

    return alert1.to_dict("records"), alert2.to_dict("records")


# ── CSV Import ───────────────────────────────────────────────────────────────

def import_csv_data(content: bytes):
    raw = pd.read_csv(io.BytesIO(content), sep=";", encoding="utf-8-sig", low_memory=False)

    missing = set(COLUMN_MAP.keys()) - set(raw.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    df = raw[list(COLUMN_MAP.keys())].rename(columns=COLUMN_MAP)

    df["product_id"]              = df["product_id"].astype(str)
    df["stock"]                   = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["price"]                   = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["dealer_price"]            = pd.to_numeric(df["dealer_price"], errors="coerce").fillna(0.0)
    df["minimum_units_per_order"] = pd.to_numeric(df["minimum_units_per_order"], errors="coerce").fillna(0).astype(int)
    for col in ("title", "discharge_date", "availability", "product_url",
                "brand", "ean", "main_image_url", "on_sale", "brand_hierarchy"):
        df[col] = df[col].fillna("").astype(str)

    df["fetched_at"] = datetime.now().isoformat(timespec="seconds")
    df["offer_id"]   = df.apply(lambda r: make_offer_id(r.to_dict()) or "", axis=1)

    with get_conn() as conn:
        df.to_sql("supplier_products", conn, if_exists="append", index=False)
        refresh_supplier_current(conn, df["fetched_at"].iloc[0])

    rebuild_batch_diff()

    return len(df)


def import_local_warehouse_excel(content: bytes) -> int:
    """Parse local warehouse Excel and refresh the local_warehouse table.

    Layout (1-indexed): header on row 4, data from row 5.
      Column C  → offer_id
      Column M  → local stock quantity
    offer_id stored uppercased+trimmed to match Marketplace/supplier offer_id keys.
    """
    # header=3 → row 4 (0-indexed) is the header, data starts row 5
    raw = pd.read_excel(io.BytesIO(content), header=3, engine="openpyxl")

    if raw.shape[1] <= 12:
        raise ValueError(
            f"Excel must have at least 13 columns (through column M); found {raw.shape[1]}"
        )

    offer_ids = raw.iloc[:, 2].astype(str).str.strip().str.upper()          # column C
    stocks    = pd.to_numeric(raw.iloc[:, 12], errors="coerce").fillna(0).astype(int)  # column M

    df = pd.DataFrame({"offer_id": offer_ids, "local_stock_quantity": stocks})
    df = df[df["offer_id"].ne("") & df["offer_id"].ne("NAN") & df["offer_id"].ne("NONE")]

    if df.empty:
        with get_conn() as conn:
            conn.execute("DELETE FROM local_warehouse")
        return 0

    # Collapse duplicate offer_ids by summing stock
    df = df.groupby("offer_id", as_index=False)["local_stock_quantity"].sum()
    df["updated_at"] = datetime.now().isoformat(timespec="seconds")

    with get_conn() as conn:
        conn.execute("DELETE FROM local_warehouse")
        df.to_sql("local_warehouse", conn, if_exists="append", index=False)

    return len(df)


async def fetch_and_import() -> dict:
    async with httpx.AsyncClient(timeout=60) as client:
        resp = await client.get(SOURCE_URL)
        resp.raise_for_status()

    df = pd.read_csv(
        io.BytesIO(resp.content),
        sep=";",
        encoding="utf-8-sig",
        low_memory=False,
        usecols=list(COLUMN_MAP.keys()),
    )

    df = df.rename(columns=COLUMN_MAP)

    df["product_id"]              = df["product_id"].astype(str)
    df["stock"]                   = pd.to_numeric(df["stock"], errors="coerce").fillna(0).astype(int)
    df["price"]                   = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
    df["dealer_price"]            = pd.to_numeric(df["dealer_price"], errors="coerce").fillna(0.0)
    df["minimum_units_per_order"] = pd.to_numeric(df["minimum_units_per_order"], errors="coerce").fillna(0).astype(int)
    for col in ("title", "discharge_date", "availability", "product_url",
                "brand", "ean", "main_image_url", "on_sale", "brand_hierarchy"):
        df[col] = df[col].fillna("").astype(str)

    df["fetched_at"] = datetime.now().isoformat(timespec="seconds")
    df["offer_id"]   = df.apply(lambda r: make_offer_id(r.to_dict()) or "", axis=1)

    with get_conn() as conn:
        df.to_sql("supplier_products", conn, if_exists="append", index=False)
        refresh_supplier_current(conn, df["fetched_at"].iloc[0])

    rebuild_batch_diff()

    return {"imported": len(df), "fetched_at": df["fetched_at"].iloc[0]}


async def scheduled_import():
    await fetch_and_import()


SUPPLIER_CURRENT_COLS = (
    "product_id", "offer_id", "title", "brand", "ean", "stock", "price",
    "dealer_price", "main_image_url", "product_url", "availability", "fetched_at",
)


def refresh_supplier_current(conn, batch_at: Optional[str] = None) -> int:
    """Rebuild supplier_current from one batch of supplier_products.

    Deliberately a replace rather than an accumulating upsert: a product that
    drops out of the supplier feed must drop out of the catalog too, which is
    what the old `WHERE fetched_at = (SELECT MAX(...))` filter did. Runs inside
    the caller's transaction, so readers never observe an empty table.
    """
    if batch_at is None:
        row = conn.execute("SELECT MAX(fetched_at) FROM supplier_products").fetchone()
        batch_at = row[0] if row else None
    if not batch_at:
        return 0

    cols = ", ".join(SUPPLIER_CURRENT_COLS)
    conn.execute("DELETE FROM supplier_current")
    conn.execute(
        f"""
        INSERT INTO supplier_current ({cols})
        SELECT {cols} FROM supplier_products
        WHERE fetched_at = ?
        GROUP BY product_id
        """,
        (batch_at,),
    )
    return conn.execute("SELECT COUNT(*) FROM supplier_current").fetchone()[0]


def _migrate_normalize_offer_ids():
    """One-time backfill: uppercase offer_ids written before normalization."""
    with get_conn() as conn:
        n = conn.execute(
            "UPDATE supplier_products SET offer_id = TRIM(UPPER(offer_id)) "
            "WHERE offer_id IS NOT NULL AND offer_id <> TRIM(UPPER(offer_id))"
        ).rowcount
    if n:
        logging.info("[migration] normalized offer_id on %d rows", n)


def _migrate_supplier_current():
    """Populate the hot table on first run after it is introduced."""
    with get_conn() as conn:
        if conn.execute("SELECT COUNT(*) FROM supplier_current").fetchone()[0]:
            return
        n = refresh_supplier_current(conn)
    logging.info("[migration] supplier_current seeded with %d rows", n)


def _migrate_offer_ids():
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT id, brand, ean, product_id FROM supplier_products WHERE offer_id IS NULL OR offer_id = ''"
        ).fetchall()
    if not rows:
        return
    with get_conn() as conn:
        for row in rows:
            oid = make_offer_id(dict(row)) or ""
            conn.execute("UPDATE supplier_products SET offer_id = ? WHERE id = ?", (oid, row["id"]))
    logging.info("[migration] filled offer_id for %d rows", len(rows))


# ── Routes ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    init_db()
    _migrate_offer_ids()
    _migrate_normalize_offer_ids()
    _migrate_supplier_current()
    scheduler.add_job(scheduled_import, "cron", hour=18, minute=0, timezone="Europe/Istanbul")
    scheduler.add_job(sync_and_compare_marketplace, "cron", hour=19, minute=0, timezone="Europe/Istanbul")
    def _scheduled_email():
        try:
            send_daily_report_email()
        except Exception as e:
            logging.error("[email] Scheduled send failed: %s", e)

    scheduler.add_job(_scheduled_email, "cron", hour=20, minute=0, timezone="Europe/Istanbul")
    logging.info("[scheduler] Scheduled daily Marketplace sync task successfully for 19:00.")
    logging.info("[scheduler] Scheduled daily email report successfully for 20:00.")
    scheduler.start()


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


CATALOG_PAGE_SIZE = 50
ALLOWED_SORT = {"product_id", "title", "brand", "stock", "dealer_price"}


def _int(v: Optional[str]) -> Optional[int]:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _float(v: Optional[str]) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def count_latest_batch() -> int:
    with get_conn() as conn:
        return conn.execute("SELECT COUNT(*) FROM supplier_current").fetchone()[0]


def get_last_sync() -> Optional[str]:
    with get_conn() as conn:
        # Every supplier_current row carries the same batch timestamp.
        row = conn.execute("SELECT fetched_at FROM supplier_current LIMIT 1").fetchone()
    return row[0] if row and row[0] else None


def catalog_where(
    q: str = "",
    brand: str = "",
    stock_min: Optional[int] = None,
    stock_max: Optional[int] = None,
    dealer_price_min: Optional[float] = None,
    dealer_price_max: Optional[float] = None,
    not_listed: bool = False,
) -> tuple:
    """Build the catalog WHERE clause shared by the table and the Excel export.

    Both read supplier_current aliased as `c`, so the exported rows are exactly
    the rows the filters produced on screen — there is no second copy of this
    logic to drift out of step.
    """
    conditions: list = []
    params: list = []

    if q:
        conditions.append("(title LIKE ? OR brand LIKE ? OR ean LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like]
    if brand:
        conditions.append("brand LIKE ?")
        params.append(f"%{brand}%")
    if stock_min is not None:
        conditions.append("stock >= ?")
        params.append(stock_min)
    if stock_max is not None:
        conditions.append("stock <= ?")
        params.append(stock_max)
    if dealer_price_min is not None:
        conditions.append("dealer_price >= ?")
        params.append(dealer_price_min)
    if dealer_price_max is not None:
        conditions.append("dealer_price <= ?")
        params.append(dealer_price_max)
    if not_listed:
        # Both sides are stored normalized, so this is a plain indexed equality
        # test. NOT EXISTS rather than NOT IN: NOT IN returns nothing at all if
        # the subquery ever yields a NULL.
        conditions.append(
            "NOT EXISTS (SELECT 1 FROM marketplace_master_catalog m WHERE m.offer_id = c.offer_id)"
        )

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    return where, params


def get_latest_catalog(
    page: int = 1,
    q: str = "",
    brand: str = "",
    stock_min: Optional[int] = None,
    stock_max: Optional[int] = None,
    dealer_price_min: Optional[float] = None,
    dealer_price_max: Optional[float] = None,
    sort_by: str = "product_id",
    order: str = "asc",
    not_listed: bool = False,
) -> tuple:
    sort_col = sort_by if sort_by in ALLOWED_SORT else "product_id"
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    offset = (page - 1) * CATALOG_PAGE_SIZE

    where, params = catalog_where(
        q=q, brand=brand,
        stock_min=stock_min, stock_max=stock_max,
        dealer_price_min=dealer_price_min, dealer_price_max=dealer_price_max,
        not_listed=not_listed,
    )

    with get_conn() as conn:
        total = conn.execute(
            f"SELECT COUNT(*) FROM supplier_current c {where}", params
        ).fetchone()[0]

        df = pd.read_sql_query(
            f"""
            SELECT product_id, offer_id, title, brand, ean, stock, dealer_price,
                   main_image_url, product_url
            FROM supplier_current c {where}
            ORDER BY {sort_col} {sort_dir}
            LIMIT ? OFFSET ?
            """,
            conn,
            params=params + [CATALOG_PAGE_SIZE, offset],
        )

    if df.empty:
        return [], total

    df["dealer_price"] = df["dealer_price"].fillna(0.0)
    return df.to_dict("records"), total


def get_product_history(product_id: str) -> tuple:
    with get_conn() as conn:
        df = pd.read_sql_query(
            """
            SELECT product_id, offer_id, title, brand, ean, stock, price, dealer_price,
                   main_image_url, product_url, availability, fetched_at
            FROM supplier_products
            WHERE product_id = ? OR offer_id = ?
            ORDER BY fetched_at DESC
            """,
            conn,
            params=(product_id, product_id),
        )

    if df.empty:
        return None, [], {}

    df["fetched_at"] = pd.to_datetime(df["fetched_at"])
    df["price"]        = df["price"].fillna(0.0)
    df["dealer_price"] = df["dealer_price"].fillna(0.0)

    latest = df.iloc[0].to_dict()
    latest["fetched_at"] = str(latest["fetched_at"])
    now = df["fetched_at"].max()

    def snapshot_at(days: int):
        target = now - pd.Timedelta(days=days)
        past = df[df["fetched_at"] <= target]
        if past.empty:
            return None
        r = past.iloc[0]
        return {"stock": int(r["stock"]), "price": float(r["price"]), "date": str(r["fetched_at"].date())}

    def delta(current, snap, key):
        if snap is None:
            return None
        return {"diff": round(current - snap[key], 4), "past": snap[key], "date": snap["date"]}

    cur_stock = int(latest["stock"])
    cur_price = float(latest["price"])
    w, m, y = snapshot_at(7), snapshot_at(30), snapshot_at(365)

    analytics = {
        "stock_week":  delta(cur_stock, w, "stock"),
        "stock_month": delta(cur_stock, m, "stock"),
        "stock_year":  delta(cur_stock, y, "stock"),
        "price_week":  delta(cur_price, w, "price"),
        "price_month": delta(cur_price, m, "price"),
        "price_year":  delta(cur_price, y, "price"),
    }

    history = df[["fetched_at", "stock", "price", "dealer_price", "availability"]].copy()
    history["fetched_at"] = history["fetched_at"].astype(str)
    return latest, history.to_dict("records"), analytics


def get_marketplace_store_status(offer_id: str) -> list:
    """Returns per-store presence + Marketplace price/stock from marketplace_master_catalog."""
    if not offer_id:
        return [{"name": name, "found": False, "marketplace_price": 0.0, "marketplace_stock": 0}
                for name, _, _ in MARKETPLACE_ACCOUNTS]
    normalized = offer_id.strip().upper()
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT store_name, marketplace_price, marketplace_stock
            FROM marketplace_master_catalog
            WHERE offer_id = ?
            """,
            (normalized,),
        ).fetchall()
    telemetry = {
        r["store_name"]: {
            "marketplace_price": float(r["marketplace_price"]) if r["marketplace_price"] is not None else 0.0,
            "marketplace_stock": int(r["marketplace_stock"]) if r["marketplace_stock"] is not None else 0,
        }
        for r in rows
    }
    return [
        {
            "name": name,
            "found": name in telemetry,
            "marketplace_price": telemetry[name]["marketplace_price"] if name in telemetry else 0.0,
            "marketplace_stock": telemetry[name]["marketplace_stock"] if name in telemetry else 0,
        }
        for name, _, _ in MARKETPLACE_ACCOUNTS
    ]


# ── Marketplace API Client ──────────────────────────────────────────────────────────

MARKETPLACE_API_URL   = os.getenv("MARKETPLACE_API_URL", "https://api.marketplace-example.com/v1").rstrip("/")
MARKETPLACE_PRICE_URL = f"{MARKETPLACE_API_URL}/v5/product/info/prices"
MARKETPLACE_STOCK_URL = f"{MARKETPLACE_API_URL}/v2/product/info/stocks-by-warehouse/fbs"


def make_offer_id(row: dict) -> Optional[str]:
    """Build the offer_id used to join supplier rows against Marketplace data.

    The result is normalized (trimmed + uppercased) so every consumer can compare
    it with a plain equality test. Marketplace and the warehouse import already store
    their keys this way; normalizing here is what lets the queries drop the
    TRIM(UPPER(...)) wrappers that used to defeat the indexes.
    """
    brand = str(row.get("brand", "")).strip().lower()
    ean   = str(row.get("ean",   "")).strip()
    if brand == "banpresto" and len(ean) >= 6:
        return ("BAN" + ean[-6:]).strip().upper()
    if brand == "funko" and len(ean) >= 6:
        return ("FU" + ean[-6:-1]).strip().upper()
    pid = str(row.get("product_id", "")).strip().upper()
    return pid or None


async def get_marketplace_prices(api_key: str, client_id: str) -> list:
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    items: list = []
    cursor = ""
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            body = {"filter": {}, "limit": 1000, "cursor": cursor}
            try:
                resp = await client.post(MARKETPLACE_PRICE_URL, json=body, headers=headers)
                resp.raise_for_status()
            except Exception as e:
                logging.error("[marketplace_prices] page error (cursor=%s): %s", cursor, e)
                break
            data = resp.json()
            batch = data.get("items", [])
            items.extend(batch)
            print(f"[DEBUG] get_marketplace_prices page: got {len(batch)} items, total so far {len(items)}")
            next_cursor = data.get("cursor", "")
            if not batch or not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor
    return items


async def get_marketplace_stocks(api_key: str, client_id: str, offer_ids: list) -> list:
    headers = {"Client-Id": client_id, "Api-Key": api_key, "Content-Type": "application/json"}
    rows: list = []
    async with httpx.AsyncClient(timeout=60) as client:
        if not offer_ids:
            cursor = ""
            while True:
                body = {"limit": 1000}
                if cursor:
                    body["cursor"] = cursor
                try:
                    resp = await client.post(MARKETPLACE_STOCK_URL, json=body, headers=headers)
                    resp.raise_for_status()
                    data = resp.json()
                    rows.extend(data.get("products", []))
                    if data.get("has_next") and data.get("cursor"):
                        cursor = data["cursor"]
                    else:
                        break
                except Exception as e:
                    logging.error("[marketplace_stocks] fetch-all error: %s", e)
                    break
        else:
            for i in range(0, len(offer_ids), 1000):
                chunk = offer_ids[i : i + 1000]
                cursor = ""
                while True:
                    body = {"offer_id": chunk, "limit": 1000}
                    if cursor:
                        body["cursor"] = cursor
                    try:
                        resp = await client.post(MARKETPLACE_STOCK_URL, json=body, headers=headers)
                        resp.raise_for_status()
                        data = resp.json()
                        rows.extend(data.get("products", []))
                        if data.get("has_next") and data.get("cursor"):
                            cursor = data["cursor"]
                        else:
                            break
                    except Exception as e:
                        logging.error("[marketplace_stocks] chunk %d error: %s", i // 1000, e)
                        break
    return rows


async def sync_and_compare_marketplace() -> dict:
    active = [(name, k, c) for name, k, c in MARKETPLACE_ACCOUNTS if k and c]
    if not active:
        logging.error("[marketplace_sync] No credentials configured in .env")
        return {"synced_at": None, "discrepancies": 0}

    with get_conn() as conn:
        supplier_df = pd.read_sql_query(
            """
            SELECT product_id, offer_id, title, main_image_url, stock, dealer_price
            FROM supplier_current
            """,
            conn,
        )

    # Local warehouse stock, keyed by normalized offer_id (0 if absent)
    with get_conn() as conn:
        lw_rows = conn.execute(
            "SELECT offer_id, local_stock_quantity FROM local_warehouse"
        ).fetchall()
    local_map: dict = {
        str(r["offer_id"]).strip().upper(): int(r["local_stock_quantity"] or 0)
        for r in lw_rows
    }
    print(f"[DEBUG] Local warehouse offer_ids loaded: {len(local_map)}")

    # Build offer_map keyed by normalized (stripped+uppercased) offer_id
    offer_map: dict = {}
    for row in supplier_df.to_dict("records"):
        oid = str(row.get("offer_id", "")).strip().upper()
        if oid:
            offer_map[oid] = row

    generated_offer_ids = list(offer_map.keys())
    print(f"[DEBUG] Total products in local DB with generated offer_id: {len(generated_offer_ids)}")
    if generated_offer_ids:
        print(f"[DEBUG] Sample offer_ids (first 5): {generated_offer_ids[:5]}")

    synced_at = datetime.now().isoformat(timespec="seconds")
    results: list = []

    # Clear master catalog once before iterating stores
    with get_conn() as conn:
        conn.execute("DELETE FROM marketplace_master_catalog")

    for account_name, api_key, client_id in active:
        print(f"[DEBUG] === Starting sync for store: {account_name} ===")

        try:
            price_items = await get_marketplace_prices(api_key, client_id)
        except Exception as e:
            logging.error("[marketplace_sync] %s: price fetch failed: %s", account_name, e)
            print(f"[DEBUG] {account_name}: price fetch FAILED — {e}")
            continue

        marketplace_prices: dict = {}
        for item in price_items:
            oid = str(item.get("offer_id", "")).strip().upper()
            if not oid:
                continue
            raw = item.get("price", {}).get("price", "0") or "0"
            try:
                marketplace_prices[oid] = float(raw)
            except (ValueError, TypeError):
                marketplace_prices[oid] = 0.0

        try:
            stock_rows = await get_marketplace_stocks(api_key, client_id, generated_offer_ids)
        except Exception as e:
            logging.error("[marketplace_sync] %s: stock fetch failed: %s", account_name, e)
            print(f"[DEBUG] {account_name}: stock fetch FAILED — {e}")
            stock_rows = []

        marketplace_stocks: dict = {}
        for row in stock_rows:
            oid = str(row.get("offer_id", "")).strip().upper()
            if not oid:
                continue
            qty = int(row.get("free_stock", 0) or 0)
            marketplace_stocks[oid] = marketplace_stocks.get(oid, 0) + qty

        print(f"[DEBUG] Store {account_name} — Received {len(marketplace_prices)} prices and {len(marketplace_stocks)} stocks from Marketplace API")
        if marketplace_prices:
            sample_keys = list(marketplace_prices.keys())[:5]
            print(f"[DEBUG] Sample Marketplace price offer_ids: {sample_keys}")
        if marketplace_stocks:
            sample_stock_keys = list(marketplace_stocks.keys())[:5]
            print(f"[DEBUG] Sample Marketplace stock offer_ids: {sample_stock_keys}")

        # Populate marketplace_master_catalog with every offer_id seen on this store
        # Keys in marketplace_prices/marketplace_stocks are already stripped+uppercased
        all_marketplace_ids = set(marketplace_prices.keys()) | set(marketplace_stocks.keys())
        if all_marketplace_ids:
            with get_conn() as conn:
                conn.executemany(
                    "INSERT INTO marketplace_master_catalog (offer_id, store_name, marketplace_price, marketplace_stock) VALUES (?, ?, ?, ?)",
                    [
                        (oid, account_name, marketplace_prices.get(oid, 0.0), marketplace_stocks.get(oid, 0))
                        for oid in all_marketplace_ids
                    ],
                )
        print(f"[DEBUG] {account_name}: inserted {len(all_marketplace_ids)} offer_ids into marketplace_master_catalog")

        # Check overlap between local offer_ids and Marketplace offer_ids
        local_set  = set(generated_offer_ids)
        price_set  = set(marketplace_prices.keys())
        stock_set  = set(marketplace_stocks.keys())
        overlap_p  = local_set & price_set
        overlap_s  = local_set & stock_set
        print(f"[DEBUG] Overlap — local∩prices: {len(overlap_p)}, local∩stocks: {len(overlap_s)}")

        store_count = 0
        for gen_oid, db_row in offer_map.items():
            try:
                if gen_oid not in marketplace_prices and gen_oid not in marketplace_stocks:
                    continue  # product not listed on Marketplace for this store

                marketplace_price  = marketplace_prices.get(gen_oid, 0.0)
                marketplace_stock  = marketplace_stocks.get(gen_oid, 0)
                sup_stock   = int(db_row["stock"] or 0)
                sup_price   = float(db_row["dealer_price"] or 0)
                local_stock = local_map.get(gen_oid, 0)  # 0 if not in warehouse Excel

                # Priority: CRITICAL LOSS → GHOST STOCK → MISSING STOCK → LOW MARGIN → skip
                if marketplace_price <= sup_price * 1.10:
                    status = "CRITICAL LOSS"
                elif sup_stock == 0 and marketplace_stock > 0:
                    # Rule A: supplier empty but Marketplace still selling = ghost stock,
                    # UNLESS we can fulfill from our own local warehouse.
                    if local_stock > 0:
                        continue  # SAFE — sell from local warehouse
                    status = "GHOST STOCK"
                elif (sup_stock > 0 or local_stock > 0) and marketplace_stock == 0:
                    # Rule B: we have stock (supplier or local) but Marketplace is closed/zero.
                    status = "MISSING STOCK"
                elif marketplace_price < sup_price * 1.30:
                    status = "LOW MARGIN"
                else:
                    continue  # SAFE — no discrepancy
                results.append({
                    "store_name":            account_name,
                    "product_id":            gen_oid,
                    "title":                 db_row.get("title", ""),
                    "main_image_url":        db_row.get("main_image_url", ""),
                    "supplier_stock":        sup_stock,
                    "marketplace_stock":            marketplace_stock,
                    "local_stock":           local_stock,
                    "supplier_dealer_price": sup_price,
                    "marketplace_price":            marketplace_price,
                    "status":                status,
                    "synced_at":             synced_at,
                })
                store_count += 1
            except Exception:
                print(f"[DEBUG] Comparison crashed for offer_id={gen_oid}:", traceback.format_exc())

        print(f"[DEBUG] {account_name}: {store_count} discrepancies added to results list")

    print(f"[DEBUG] Total discrepancies across all stores: {len(results)}")

    with get_conn() as conn:
        conn.execute("DELETE FROM marketplace_sync_results")
        if results:
            df_results = pd.DataFrame(results)
            print(f"[DEBUG] Inserting DataFrame with shape {df_results.shape} into marketplace_sync_results")
            df_results.to_sql("marketplace_sync_results", conn, if_exists="append", index=False)
        # get_conn() context manager commits on exit — verified in database.py
        row_count = conn.execute("SELECT COUNT(*) FROM marketplace_sync_results").fetchone()[0]
        print(f"[DEBUG] Rows in marketplace_sync_results after commit: {row_count}")

    return {"synced_at": synced_at, "discrepancies": len(results)}


MARKETPLACE_SYNC_PAGE_SIZE = 50
MARKETPLACE_SYNC_ALLOWED_SORT = {
    "product_id", "title", "store_name",
    "supplier_stock", "marketplace_stock",
    "supplier_dealer_price", "marketplace_price",
}


_STATUS_PRIORITY = (
    "CASE WHEN status='CRITICAL LOSS' THEN 0 "
    "WHEN status='GHOST STOCK' THEN 1 "
    "WHEN status='MISSING STOCK' THEN 2 "
    "WHEN status='LOW MARGIN' THEN 3 ELSE 4 END"
)


def get_marketplace_sync_results(
    page: int = 1,
    per_page: int = MARKETPLACE_SYNC_PAGE_SIZE,
    q: str = "",
    store: str = "",
    status: str = "",
    sort_by: str = "status",
    order: str = "asc",
) -> tuple:
    sort_col = sort_by if sort_by in MARKETPLACE_SYNC_ALLOWED_SORT else "status"
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    offset   = (page - 1) * per_page

    conditions: list = []
    params:     list = []
    if q:
        conditions.append("(product_id LIKE ? OR title LIKE ?)")
        like = f"%{q}%"
        params += [like, like]
    if store:
        conditions.append("store_name = ?")
        params.append(store)
    if status:
        conditions.append("status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    order_expr = (
        f"{_STATUS_PRIORITY} {sort_dir}"
        if sort_col == "status"
        else f"{sort_col} {sort_dir}"
    )

    with get_conn() as conn:
        unfiltered_total = conn.execute("SELECT COUNT(*) FROM marketplace_sync_results").fetchone()[0]
        filtered_total   = conn.execute(f"SELECT COUNT(*) FROM marketplace_sync_results {where}", params).fetchone()[0]
        last             = conn.execute("SELECT MAX(synced_at) FROM marketplace_sync_results").fetchone()[0]
        # Grand-total counts per status (always unfiltered — for metric cards)
        count_rows = conn.execute(
            "SELECT status, COUNT(*) as cnt FROM marketplace_sync_results GROUP BY status"
        ).fetchall()
        status_counts = {r["status"]: r["cnt"] for r in count_rows}
        df = pd.read_sql_query(
            f"""
            SELECT * FROM marketplace_sync_results {where}
            ORDER BY {order_expr}
            LIMIT ? OFFSET ?
            """,
            conn,
            params=params + [per_page, offset],
        )

    if df.empty:
        return [], filtered_total, unfiltered_total, status_counts, last
    df["supplier_dealer_price"] = df["supplier_dealer_price"].fillna(0.0)
    df["marketplace_price"]            = df["marketplace_price"].fillna(0.0)
    return df.to_dict("records"), filtered_total, unfiltered_total, status_counts, last


MARKETPLACE_ALL_PAGE_SIZE = 50
# title sort excluded — not in marketplace_master_catalog; sorting after join defeats pagination
MARKETPLACE_ALL_ALLOWED_SORT = {"offer_id", "marketplace_price", "marketplace_stock", "store_name"}


def get_marketplace_all_products(
    page: int = 1,
    per_page: int = MARKETPLACE_ALL_PAGE_SIZE,
    q: str = "",
    sort_by: str = "offer_id",
    order: str = "asc",
) -> tuple:
    sort_col = sort_by if sort_by in MARKETPLACE_ALL_ALLOWED_SORT else "offer_id"
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    offset   = (page - 1) * per_page

    # Filter only on marketplace_master_catalog — no join needed for count or pagination
    where_conditions: list = []
    where_params:     list = []
    if q:
        where_conditions.append("offer_id LIKE ?")
        where_params.append(f"%{q}%")
    where = ("WHERE " + " AND ".join(where_conditions)) if where_conditions else ""

    with get_conn() as conn:
        # Fast count — single table, no join
        total = conn.execute(
            f"SELECT COUNT(*) FROM marketplace_master_catalog {where}", where_params
        ).fetchone()[0]

        if total == 0:
            return [], 0

        # Paginate marketplace_master_catalog first; join supplier only for these N rows
        omc_rows = conn.execute(
            f"""
            SELECT offer_id, store_name, marketplace_price, marketplace_stock
            FROM marketplace_master_catalog {where}
            ORDER BY {sort_col} {sort_dir}
            LIMIT ? OFFSET ?
            """,
            where_params + [per_page, offset],
        ).fetchall()

        if not omc_rows:
            return [], total

        # Fetch titles for exactly these offer_ids — no full-table scan
        offer_ids = [r["offer_id"] for r in omc_rows]
        placeholders = ",".join("?" * len(offer_ids))
        title_rows = conn.execute(
            f"""
            SELECT offer_id, title, main_image_url
            FROM supplier_current
            WHERE offer_id IN ({placeholders})
            GROUP BY offer_id
            """,
            offer_ids,
        ).fetchall()

    title_map = {r["offer_id"].strip().upper(): dict(r) for r in title_rows}

    results = []
    for r in omc_rows:
        meta = title_map.get(r["offer_id"].strip().upper(), {})
        results.append({
            "offer_id":       r["offer_id"],
            "store_name":     r["store_name"],
            "marketplace_price":     float(r["marketplace_price"] or 0),
            "marketplace_stock":     int(r["marketplace_stock"] or 0),
            "title":          meta.get("title") or "",
            "main_image_url": meta.get("main_image_url") or "",
        })
    return results, total


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    # catalog params
    page: int = 1,
    q: str = "",
    brand: str = "",
    stock_min: Optional[str] = None,
    stock_max: Optional[str] = None,
    dealer_price_min: Optional[str] = None,
    dealer_price_max: Optional[str] = None,
    sort_by: str = "product_id",
    order: str = "asc",
    not_listed: bool = False,
    # marketplace sync params
    marketplace_page: int = 1,
    marketplace_q: str = "",
    marketplace_store: str = "",
    marketplace_status: str = "",
    marketplace_sort_by: str = "status",
    marketplace_order: str = "asc",
    marketplace_per_page: int = MARKETPLACE_SYNC_PAGE_SIZE,
):
    smin  = _int(stock_min)
    smax  = _int(stock_max)
    dpmin = _float(dealer_price_min)
    dpmax = _float(dealer_price_max)

    new_items, out_of_stock, price_up, price_down = compute_diff()
    catalog, catalog_total = get_latest_catalog(
        page=page, q=q, brand=brand,
        stock_min=smin, stock_max=smax,
        dealer_price_min=dpmin, dealer_price_max=dpmax,
        sort_by=sort_by, order=order,
        not_listed=not_listed,
    )
    catalog_batch_total = count_latest_batch()
    last_sync = get_last_sync()
    marketplace_sync_rows, marketplace_sync_total, marketplace_sync_batch_total, marketplace_sync_counts, marketplace_sync_last = get_marketplace_sync_results(
        page=marketplace_page, per_page=marketplace_per_page,
        q=marketplace_q, store=marketplace_store, status=marketplace_status,
        sort_by=marketplace_sort_by, order=marketplace_order,
    )

    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={
            "last_sync": last_sync,
            "new_items": new_items,
            "out_of_stock": out_of_stock,
            "price_up": price_up,
            "price_down": price_down,
            "catalog": catalog,
            "catalog_total": catalog_total,
            "catalog_batch_total": catalog_batch_total,
            "catalog_page": page,
            "catalog_q": q,
            "catalog_brand": brand,
            "catalog_stock_min": smin,
            "catalog_stock_max": smax,
            "catalog_dealer_price_min": dpmin,
            "catalog_dealer_price_max": dpmax,
            "catalog_sort_by": sort_by,
            "catalog_order": order,
            "catalog_page_size": CATALOG_PAGE_SIZE,
            "catalog_not_listed": not_listed,
            "marketplace_sync_rows": marketplace_sync_rows,
            "marketplace_sync_total": marketplace_sync_total,
            "marketplace_sync_batch_total": marketplace_sync_batch_total,
            "marketplace_sync_counts": marketplace_sync_counts,
            "marketplace_sync_last": marketplace_sync_last,
            "marketplace_page": marketplace_page,
            "marketplace_per_page": marketplace_per_page,
            "marketplace_q": marketplace_q,
            "marketplace_store": marketplace_store,
            "marketplace_status": marketplace_status,
            "marketplace_sort_by": marketplace_sort_by,
            "marketplace_order": marketplace_order,
        },
    )


@app.get("/product/{product_id}", response_class=HTMLResponse)
async def product_detail(request: Request, product_id: str):
    latest, history, analytics = get_product_history(product_id)
    if latest is None:
        return templates.TemplateResponse(
            request=request,
            name="product_not_found.html",
            context={"product_id": product_id},
            status_code=404,
        )
    offer_id = str(latest.get("offer_id", "") or "").strip().upper()
    marketplace_store_status = get_marketplace_store_status(offer_id)

    # Explicit per-store variables for unambiguous template rendering
    store_map = {s["name"]: s for s in marketplace_store_status}
    store_a = store_map.get("Store A",     {"found": False, "marketplace_price": 0.0, "marketplace_stock": 0})
    store_b    = store_map.get("Store B", {"found": False, "marketplace_price": 0.0, "marketplace_stock": 0})

    return templates.TemplateResponse(
        request=request,
        name="product.html",
        context={
            "product": latest,
            "history": history,
            "analytics": analytics,
            "marketplace_store_status": marketplace_store_status,
            "store_a_found": store_a["found"],
            "store_a_price": store_a["marketplace_price"],
            "store_a_stock": store_a["marketplace_stock"],
            "store_b_found":    store_b["found"],
            "store_b_price":    store_b["marketplace_price"],
            "store_b_stock":    store_b["marketplace_stock"],
        },
    )


@app.get("/marketplace/all-products", response_class=HTMLResponse)
async def marketplace_all_products_page(
    request: Request,
    page: int = 1,
    q: str = "",
    sort_by: str = "offer_id",
    order: str = "asc",
    per_page: int = MARKETPLACE_ALL_PAGE_SIZE,
):
    rows, total = get_marketplace_all_products(
        page=page, per_page=per_page, q=q, sort_by=sort_by, order=order
    )
    total_pages = max(1, (total + per_page - 1) // per_page)
    return templates.TemplateResponse(
        request=request,
        name="marketplace_all_products.html",
        context={
            "rows": rows,
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": total_pages,
            "q": q,
            "sort_by": sort_by,
            "order": order,
        },
    )


@app.post("/import")
async def import_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files accepted")
    content = await file.read()
    try:
        count = import_csv_data(content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return {"imported": count}


@app.post("/warehouse/import")
async def import_warehouse(file: UploadFile = File(...)):
    """Upload local warehouse Excel (header row 4; offer_id col C, stock col M)."""
    if not file.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Only Excel files (.xlsx/.xls) accepted")
    content = await file.read()
    try:
        count = import_local_warehouse_excel(content)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Excel parse failed: {e}")
    return {"imported": count}


@app.post("/fetch-now")
async def fetch_now():
    try:
        result = await fetch_and_import()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Download failed: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    return result


@app.post("/marketplace/sync-now")
async def marketplace_sync_now(background_tasks: BackgroundTasks):
    background_tasks.add_task(sync_and_compare_marketplace)
    return {
        "status": "sync_started",
        "synced": 0,
        "message": "Marketplace sync started in the background. Refresh in ~30 seconds.",
    }


async def _fetch_and_sync_all():
    try:
        result = await fetch_and_import()
        logging.info("[sync_all] CSV import done: %d rows", result.get("imported", 0))
    except Exception as e:
        logging.error("[sync_all] CSV fetch failed: %s", e)
    try:
        await sync_and_compare_marketplace()
    except Exception as e:
        logging.error("[sync_all] Marketplace sync failed: %s", e)


@app.post("/sync-all")
async def sync_all(background_tasks: BackgroundTasks):
    background_tasks.add_task(_fetch_and_sync_all)
    return {
        "status": "sync_started",
        "message": "Fetching supplier CSV then syncing Marketplace in background. Refresh in ~70 seconds.",
    }


# Column -> Excel header for the catalog export, in sheet order.
CATALOG_EXPORT_COLUMNS = {
    "product_id":     "Product ID",
    "offer_id":       "Offer ID",
    "title":          "Title",
    "brand":          "Brand",
    "ean":            "EAN",
    "stock":          "Stock",
    "price":          "Price",
    "dealer_price":   "Dealer Price",
    "availability":   "Availability",
    "product_url":    "Product URL",
    "main_image_url": "Image URL",
}


@app.get("/export/catalog.xlsx")
async def export_catalog(
    q: str = "",
    brand: str = "",
    stock_min: Optional[str] = None,
    stock_max: Optional[str] = None,
    dealer_price_min: Optional[str] = None,
    dealer_price_max: Optional[str] = None,
    sort_by: str = "product_id",
    order: str = "asc",
    not_listed: bool = False,
):
    """Every row matching the current catalog filters, as an Excel download.

    The workbook is built in a BytesIO buffer and streamed straight to the
    client: nothing is ever written to the filesystem, so there is no temp file
    to clean up and no way for one request to leave anything behind. Takes the
    same parameters as the catalog view and reuses its WHERE clause, so the file
    matches what the filters showed — every page of it, not just the current one.
    """
    where, params = catalog_where(
        q=q, brand=brand,
        stock_min=_int(stock_min), stock_max=_int(stock_max),
        dealer_price_min=_float(dealer_price_min),
        dealer_price_max=_float(dealer_price_max),
        not_listed=not_listed,
    )
    sort_col = sort_by if sort_by in ALLOWED_SORT else "product_id"
    sort_dir = "ASC" if order.lower() == "asc" else "DESC"
    cols = ", ".join(CATALOG_EXPORT_COLUMNS)

    with get_conn() as conn:
        df = pd.read_sql_query(
            f"SELECT {cols} FROM supplier_current c {where} ORDER BY {sort_col} {sort_dir}",
            conn,
            params=params,
        )

    df = df.rename(columns=CATALOG_EXPORT_COLUMNS)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="SupplierStok")
    buf.seek(0)

    filename = f"supplierstok_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/marketplace/export/{fmt}")
async def marketplace_export(fmt: str):
    allowed = {"xlsx", "csv", "json"}
    if fmt not in allowed:
        raise HTTPException(400, detail=f"Format must be one of: {', '.join(sorted(allowed))}")

    with get_conn() as conn:
        df = pd.read_sql_query(
            f"SELECT store_name, product_id, title, supplier_stock, marketplace_stock, local_stock, "
            f"supplier_dealer_price, marketplace_price, status, synced_at "
            f"FROM marketplace_sync_results ORDER BY {_STATUS_PRIORITY}",
            conn,
        )

    date_str = datetime.now().strftime("%Y%m%d")

    if fmt == "csv":
        buf = io.BytesIO(df.to_csv(index=False).encode("utf-8-sig"))
        return StreamingResponse(buf, media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename=marketplace_report_{date_str}.csv"})

    if fmt == "json":
        buf = io.BytesIO(df.to_json(orient="records", force_ascii=False, indent=2).encode("utf-8"))
        return StreamingResponse(buf, media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=marketplace_report_{date_str}.json"})

    # xlsx
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Discrepancies")
    buf.seek(0)
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=Marketplace_Discrepancy_Report_{date_str}.xlsx"},
    )


def _build_email_html(
    critical: pd.DataFrame,
    ghost: pd.DataFrame,
    missing: pd.DataFrame,
) -> str:
    date_str = datetime.now().strftime("%d %B %Y")

    def _table_html(sub: pd.DataFrame) -> str:
        if sub.empty:
            return "<p style='color:#94a3b8;font-size:13px;margin:8px 0 0'>Kayıt bulunamadı.</p>"
        hdr = "".join(
            f"<th style='text-align:left;padding:7px 12px;border-bottom:2px solid #e2e8f0;"
            f"font-size:11px;text-transform:uppercase;color:#64748b;white-space:nowrap'>{c}</th>"
            for c in sub.columns
        )
        body_rows = ""
        for _, r in sub.iterrows():
            cells = "".join(
                f"<td style='padding:6px 12px;border-bottom:1px solid #f1f5f9;"
                f"font-size:13px;white-space:nowrap'>{r[c]}</td>"
                for c in sub.columns
            )
            body_rows += f"<tr>{cells}</tr>"
        return (
            f"<div style='overflow-x:auto'>"
            f"<table style='width:100%;border-collapse:collapse'>"
            f"<thead><tr style='background:#f8fafc'>{hdr}</tr></thead>"
            f"<tbody>{body_rows}</tbody></table></div>"
        )

    n_critical    = len(critical)
    n_ghost       = len(ghost)
    n_missing     = len(missing)
    total_action  = n_critical + n_ghost + n_missing

    summary_html = f"""
    <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:16px 20px;margin-bottom:24px'>
      <h3 style='margin:0 0 12px;font-size:13px;color:#475569;text-transform:uppercase;letter-spacing:.05em'>
        Özet Rapor &mdash; {date_str}
      </h3>
      <table style='border-collapse:separate;border-spacing:12px 0'><tr>
        <td style='background:#fef2f2;border:1px solid #fecaca;border-radius:6px;padding:12px 20px;min-width:150px;vertical-align:top'>
          <div style='font-size:28px;font-weight:700;color:#dc2626;line-height:1'>{n_critical}</div>
          <div style='font-size:12px;color:#991b1b;margin-top:4px'>🚨 Kritik Fiyat Uyarısı</div>
          <div style='font-size:11px;color:#b91c1c;margin-top:2px'>adet ürün</div>
        </td>
        <td style='background:#faf5ff;border:1px solid #e9d5ff;border-radius:6px;padding:12px 20px;min-width:150px;vertical-align:top'>
          <div style='font-size:28px;font-weight:700;color:#7c3aed;line-height:1'>{n_ghost}</div>
          <div style='font-size:12px;color:#5b21b6;margin-top:4px'>⛔ Stok Bitti &mdash; Marketplace&apos;da Kapat</div>
          <div style='font-size:11px;color:#6d28d9;margin-top:2px'>adet ürün</div>
        </td>
        <td style='background:#eff6ff;border:1px solid #bfdbfe;border-radius:6px;padding:12px 20px;min-width:150px;vertical-align:top'>
          <div style='font-size:28px;font-weight:700;color:#2563eb;line-height:1'>{n_missing}</div>
          <div style='font-size:12px;color:#1d4ed8;margin-top:4px'>📦 Stok Yenilendi &mdash; Stok Aç</div>
          <div style='font-size:11px;color:#1e40af;margin-top:2px'>adet ürün</div>
        </td>
      </tr></table>
      <p style='margin:12px 0 0;font-size:12px;color:#94a3b8'>
        Toplam <strong style='color:#475569'>{total_action}</strong> adet ürün acil aksiyon gerektiriyor.
      </p>
    </div>"""

    sections = []

    if n_critical > 0:
        sub = critical[["product_id", "title", "store_name", "supplier_dealer_price", "marketplace_price"]].copy()
        sub.columns = ["Offer ID", "Başlık", "Mağaza", "Alış Fiyatı (€)", "Marketplace Fiyatı (₽)"]
        sections.append(
            f"<div style='margin-bottom:28px'>"
            f"<div style='background:#fef2f2;border-left:4px solid #dc2626;padding:10px 16px;"
            f"border-radius:0 6px 6px 0;margin-bottom:12px'>"
            f"<h3 style='color:#dc2626;margin:0;font-size:15px;font-weight:700'>"
            f"🚨 KRİTİK FİYAT UYARISI &mdash; {n_critical} adet ürün</h3>"
            f"<p style='color:#b91c1c;font-size:12px;margin:4px 0 0'>"
            f"Zarar ediyorsun, fiyatı acil yükselt!</p></div>"
            f"{_table_html(sub)}</div>"
        )

    if n_ghost > 0:
        sub = ghost[["product_id", "title", "store_name", "supplier_stock", "local_stock", "marketplace_stock"]].copy()
        sub.columns = ["Offer ID", "Başlık", "Mağaza", "Toptancı Stok", "Depo Stok", "Marketplace Stok"]
        sections.append(
            f"<div style='margin-bottom:28px'>"
            f"<div style='background:#faf5ff;border-left:4px solid #7c3aed;padding:10px 16px;"
            f"border-radius:0 6px 6px 0;margin-bottom:12px'>"
            f"<h3 style='color:#7c3aed;margin:0;font-size:15px;font-weight:700'>"
            f"⛔ STOK BİTTİ &mdash; MARKETPLACE'TE KAPAT &mdash; {n_ghost} adet ürün</h3>"
            f"<p style='color:#5b21b6;font-size:12px;margin:4px 0 0'>"
            f"Ürün stoğu bitti, ceza yememek için Marketplace'da hemen kapat!</p></div>"
            f"{_table_html(sub)}</div>"
        )

    if n_missing > 0:
        sub = missing[["product_id", "title", "store_name", "supplier_stock", "local_stock", "marketplace_stock"]].copy()
        sub.columns = ["Offer ID", "Başlık", "Mağaza", "Toptancı Stok", "Depo Stok", "Marketplace Stok"]
        sections.append(
            f"<div style='margin-bottom:28px'>"
            f"<div style='background:#eff6ff;border-left:4px solid #2563eb;padding:10px 16px;"
            f"border-radius:0 6px 6px 0;margin-bottom:12px'>"
            f"<h3 style='color:#2563eb;margin:0;font-size:15px;font-weight:700'>"
            f"📦 STOK YENİLENDİ &mdash; STOK AÇ &mdash; {n_missing} adet ürün</h3>"
            f"<p style='color:#1d4ed8;font-size:12px;margin:4px 0 0'>"
            f"Ürün stoğu yenilendi, satış kaçırmamak için Marketplace'da stoğu aç!</p></div>"
            f"{_table_html(sub)}</div>"
        )

    body = "\n".join(sections) if sections else (
        "<p style='color:#94a3b8;text-align:center;padding:32px 0'>"
        "Bugün aksiyon gerektiren bir ürün bulunamadı. 🎉</p>"
    )

    return f"""<!DOCTYPE html>
<html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="font-family:Arial,Helvetica,sans-serif;max-width:840px;margin:0 auto;padding:20px;color:#1e293b;background:#f1f5f9">
  <div style="background:#4f46e5;color:white;padding:18px 24px;border-radius:10px 10px 0 0">
    <h2 style="margin:0;font-size:19px;font-weight:700">SupplierCSV Monster &mdash; Günlük Marketplace Raporu</h2>
    <p style="margin:4px 0 0;opacity:.8;font-size:13px">{date_str} &mdash; {total_action} ürün acil aksiyon bekliyor</p>
  </div>
  <div style="background:#ffffff;padding:24px;border:1px solid #e2e8f0;border-top:none;border-radius:0 0 10px 10px">
    {summary_html}
    {body}
    <hr style="border:none;border-top:1px solid #e2e8f0;margin:24px 0">
    <p style="color:#94a3b8;font-size:12px;margin:0">
      Aksiyon gerektiren ürünlerin detayları Excel dosyasına eklenmiştir.
      Oluşturulma: {datetime.now().strftime('%H:%M')} Istanbul saati.
    </p>
  </div>
</body></html>"""


def send_daily_report_email():
    if not all([SMTP_SERVER, SENDER_EMAIL, SENDER_PASSWORD, RECEIVER_EMAIL]):
        logging.warning("[email] SMTP credentials missing in .env — skipping daily report")
        return

    with get_conn() as conn:
        df = pd.read_sql_query(
            f"SELECT store_name, product_id, title, supplier_stock, marketplace_stock, local_stock, "
            f"supplier_dealer_price, marketplace_price, status, synced_at "
            f"FROM marketplace_sync_results ORDER BY {_STATUS_PRIORITY}",
            conn,
        )

    if df.empty:
        logging.info("[email] marketplace_sync_results is empty — no email sent")
        return

    critical = df[df["status"] == "CRITICAL LOSS"].copy()
    ghost    = df[df["status"] == "GHOST STOCK"].copy()
    missing  = df[df["status"] == "MISSING STOCK"].copy()

    html_body = _build_email_html(critical, ghost, missing)

    # Action-only Excel: CRITICAL LOSS + GHOST STOCK + MISSING STOCK rows only
    action_label_map = {
        "CRITICAL LOSS": "KRİTİK FİYAT UYARISI",
        "GHOST STOCK":   "STOK BİTTİ (GHOST STOCK)",
        "MISSING STOCK": "STOK YENİLENDİ (MISSING STOCK)",
    }
    action_df = pd.concat([critical, ghost, missing], ignore_index=True)
    if not action_df.empty:
        action_df.insert(0, "Uyuşmazlık Tipi / Aksiyon",
                         action_df["status"].map(action_label_map))

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if not action_df.empty:
            action_df.to_excel(writer, index=False, sheet_name="Eylem Gerektiren")
        else:
            pd.DataFrame({"Mesaj": ["Bugün aksiyon gerektiren ürün yok."]}).to_excel(
                writer, index=False, sheet_name="Eylem Gerektiren"
            )
    xlsx_bytes = buf.getvalue()

    msg = MIMEMultipart()
    msg["From"]    = SENDER_EMAIL
    msg["To"]      = RECEIVER_EMAIL
    msg["Subject"] = (
        f"[SupplierCSV] Günlük Marketplace Raporu — {datetime.now().strftime('%d.%m.%Y')} — "
        f"{len(critical)} Kritik / {len(ghost)} Stok Bitti / {len(missing)} Stok Aç"
    )
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(xlsx_bytes)
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        'attachment; filename="Marketplace_Eylem_Gerektiren_Stok_Raporu.xlsx"',
    )
    msg.attach(part)

    with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
    logging.info(
        "[email] Daily report sent to %s — %d critical / %d ghost / %d missing",
        RECEIVER_EMAIL, len(critical), len(ghost), len(missing),
    )


@app.post("/product/{product_id}/update-offer-id")
async def update_offer_id_route(product_id: str, request: Request):
    data = await request.json()
    new_oid = str(data.get("offer_id", "")).strip()
    with get_conn() as conn:
        conn.execute(
            "UPDATE supplier_products SET offer_id = ? WHERE product_id = ?",
            (new_oid, product_id),
        )
    return {"ok": True, "offer_id": new_oid}


@app.post("/marketplace/sync")
async def sync_marketplace(file: UploadFile = File(...)):
    """Upload Marketplace product CSV (product_id, stock, status) to refresh marketplace_products table."""
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files accepted")

    content = await file.read()
    df = pd.read_csv(io.BytesIO(content))

    required = {"product_id", "stock", "status"}
    if not required.issubset(df.columns):
        raise HTTPException(status_code=422, detail=f"CSV missing columns: {required - set(df.columns)}")

    df = df[["product_id", "stock", "status"]]

    with get_conn() as conn:
        conn.execute("DELETE FROM marketplace_products")
        df.to_sql("marketplace_products", conn, if_exists="append", index=False)

    return {"synced": len(df)}
