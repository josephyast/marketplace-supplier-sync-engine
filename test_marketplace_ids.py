import os
import json
import httpx
from dotenv import load_dotenv

load_dotenv()

STORES = [
    ("Store A",     os.getenv("MARKETPLACE_API_KEY",  ""), os.getenv("MARKETPLACE_CLIENT_ID",  "")),
    ("Store B", os.getenv("MARKETPLACE_API_KEY2", ""), os.getenv("MARKETPLACE_CLIENT_ID2", "")),
]

API_URL   = os.getenv("MARKETPLACE_API_URL", "https://api.marketplace-example.com/v1").rstrip("/")
PRICE_URL = f"{API_URL}/v5/product/info/prices"
STOCK_URL = f"{API_URL}/v2/product/info/stocks-by-warehouse/fbs"

TEST_IDS = ["FU75654", "FU83716", "FU90136", "FU30855", "FU86776"]


def run():
    for store_name, api_key, client_id in STORES:
        if not api_key or not client_id:
            print(f"\n[{store_name}] SKIP — credentials missing in .env")
            continue

        headers = {
            "Client-Id": client_id,
            "Api-Key": api_key,
            "Content-Type": "application/json",
        }

        print(f"\n{'='*60}")
        print(f"STORE: {store_name}  (client_id={client_id[:6]}...)")
        print(f"{'='*60}")

        # ── Prices ───────────────────────────────────────────────
        print("\n--- POST /v5/product/info/prices ---")
        price_body = {"filter": {"offer_id": TEST_IDS}, "limit": 100}
        try:
            r = httpx.post(PRICE_URL, json=price_body, headers=headers, timeout=30)
            print(f"Status: {r.status_code}")
            print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"ERROR: {e}")

        # ── Stocks ───────────────────────────────────────────────
        print("\n--- POST /v2/product/info/stocks-by-warehouse/fbs ---")
        stock_body = {"offer_id": TEST_IDS, "limit": 100}
        try:
            r = httpx.post(STOCK_URL, json=stock_body, headers=headers, timeout=30)
            print(f"Status: {r.status_code}")
            print(json.dumps(r.json(), ensure_ascii=False, indent=2))
        except Exception as e:
            print(f"ERROR: {e}")


if __name__ == "__main__":
    run()
