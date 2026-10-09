import sqlite3
from contextlib import contextmanager

DB_PATH = "monster.db"


def _init_pragmas():
    """One-time, file-level settings. Both persist in the database file itself,
    but a freshly created file starts as journal_mode=delete / auto_vacuum=NONE,
    so they have to be set on first run. Uses its own connection because
    journal_mode cannot be changed inside a transaction.
    """
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        # auto_vacuum must come first: it only takes effect on a database with no
        # pages yet, and setting journal_mode writes the header. If it still did
        # not stick (an existing file), one VACUUM converts it — a no-op cost on a
        # fresh database, and skipped entirely once the mode is already set.
        conn.execute("PRAGMA auto_vacuum = INCREMENTAL")
        if conn.execute("PRAGMA auto_vacuum").fetchone()[0] != 2:
            conn.execute("VACUUM")
        conn.execute("PRAGMA journal_mode = WAL")
    finally:
        conn.close()


def init_db():
    _init_pragmas()
    with get_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supplier_products (
                id                     INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id             TEXT NOT NULL,
                title                  TEXT,
                stock                  INTEGER,
                price                  REAL,
                dealer_price           REAL,
                discharge_date         TEXT,
                availability           TEXT,
                product_url            TEXT,
                brand                  TEXT,
                ean                    TEXT,
                main_image_url         TEXT,
                minimum_units_per_order INTEGER,
                on_sale                TEXT,
                brand_hierarchy        TEXT,
                fetched_at             TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS marketplace_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id TEXT NOT NULL UNIQUE,
                stock INTEGER,
                status TEXT
            )
        """)
        # Migrate: rename account → store_name if old schema present
        cols = [r[1] for r in conn.execute("PRAGMA table_info(marketplace_sync_results)").fetchall()]
        if cols and "account" in cols:
            conn.execute("DROP TABLE IF EXISTS marketplace_sync_results")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS marketplace_sync_results (
                id                    INTEGER PRIMARY KEY AUTOINCREMENT,
                store_name            TEXT NOT NULL,
                product_id            TEXT NOT NULL,
                title                 TEXT,
                main_image_url        TEXT,
                supplier_stock        INTEGER,
                marketplace_stock            INTEGER,
                local_stock           INTEGER DEFAULT 0,
                supplier_dealer_price REAL,
                marketplace_price            REAL,
                status                TEXT,
                synced_at             TEXT NOT NULL
            )
        """)
        # Migrate: add local_stock column if table existed without it
        osr_cols = [r[1] for r in conn.execute("PRAGMA table_info(marketplace_sync_results)").fetchall()]
        if osr_cols and "local_stock" not in osr_cols:
            conn.execute("ALTER TABLE marketplace_sync_results ADD COLUMN local_stock INTEGER DEFAULT 0")
        # Migrate: add offer_id column if missing
        sp_cols = [r[1] for r in conn.execute("PRAGMA table_info(supplier_products)").fetchall()]
        if "offer_id" not in sp_cols:
            conn.execute("ALTER TABLE supplier_products ADD COLUMN offer_id TEXT DEFAULT ''")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS marketplace_master_catalog (
                offer_id   TEXT NOT NULL,
                store_name TEXT NOT NULL,
                marketplace_price REAL    DEFAULT 0.0,
                marketplace_stock INTEGER DEFAULT 0
            )
        """)
        # Migrate: add price/stock columns if table existed without them
        omc_cols = [r[1] for r in conn.execute("PRAGMA table_info(marketplace_master_catalog)").fetchall()]
        if "marketplace_price" not in omc_cols:
            conn.execute("ALTER TABLE marketplace_master_catalog ADD COLUMN marketplace_price REAL DEFAULT 0.0")
        if "marketplace_stock" not in omc_cols:
            conn.execute("ALTER TABLE marketplace_master_catalog ADD COLUMN marketplace_stock INTEGER DEFAULT 0")

        # Local warehouse stock — uploaded via Excel (offer_id → local stock qty)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS local_warehouse (
                offer_id             TEXT PRIMARY KEY,
                local_stock_quantity INTEGER DEFAULT 0,
                updated_at           TEXT
            )
        """)

        # Hot table: exactly one row per product, rebuilt from the newest batch on
        # every import. Every "what does the catalog look like right now" query
        # reads this instead of filtering the full append-only history, so their
        # cost stays flat as supplier_products grows.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS supplier_current (
                product_id     TEXT PRIMARY KEY,
                offer_id       TEXT,
                title          TEXT,
                brand          TEXT,
                ean            TEXT,
                stock          INTEGER,
                price          REAL,
                dealer_price   REAL,
                main_image_url TEXT,
                product_url    TEXT,
                availability   TEXT,
                fetched_at     TEXT NOT NULL
            )
        """)
        # offer_id is stored already normalized, so these serve equality joins
        # against marketplace_master_catalog without a TRIM(UPPER(...)) wrapper.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cur_offer ON supplier_current(offer_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cur_brand ON supplier_current(brand)")

        # Precomputed batch-to-batch differences, written once per import instead
        # of recomputed on every dashboard request. A product can legitimately land
        # in more than one kind (an item can go out of stock AND drop in price),
        # so kind is part of the key rather than a single classification.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS batch_diff (
                batch_at   TEXT NOT NULL,
                prev_at    TEXT,
                kind       TEXT NOT NULL,   -- NEW | OUT_OF_STOCK | PRICE_UP | PRICE_DOWN
                product_id TEXT NOT NULL,
                title      TEXT,
                stock_old  INTEGER,
                stock_new  INTEGER,
                price_old  REAL,
                price_new  REAL,
                PRIMARY KEY (batch_at, kind, product_id)
            )
        """)

        conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_product_id ON supplier_products(product_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_fetched_at ON supplier_products(fetched_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_omc_offer_id ON marketplace_master_catalog(offer_id)")
        # Covering index for batch-to-batch comparison: serves both sides of the
        # diff join without touching the table. Its (fetched_at, product_id)
        # prefix also drives the "latest batch, ordered by product_id" catalog query.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sp_diff "
            "ON supplier_products(fetched_at, product_id, stock, price)"
        )
        # offer_id is the join key against Marketplace data but had no index.
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sp_offer_id ON supplier_products(offer_id)")
        # SQLite never refreshes planner statistics on its own.
        conn.execute("ANALYZE")


@contextmanager
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    # journal_mode=WAL is stored in the file itself; these three are per-connection
    # and have to be re-applied every time. WAL is what keeps the nightly import
    # from blocking dashboard reads.
    conn.execute("PRAGMA synchronous = NORMAL")   # safe under WAL, markedly faster
    conn.execute("PRAGMA busy_timeout = 5000")    # wait on a lock instead of raising
    conn.execute("PRAGMA cache_size = -64000")    # 64 MB page cache (negative = KiB)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
