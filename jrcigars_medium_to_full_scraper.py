import sys
import time
import logging
from datetime import datetime

import pyodbc
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

BASE_URL = "https://www.jrcigars.com"

# Pagination pattern:
# Page 1: ?sz=60
# Page 2: ?start=60&sz=60
# Page 3: ?start=120&sz=60
START_URL = "https://www.jrcigars.com/cigars-by-strength/medium-to-full-bodied-cigars/?sz=60"

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

LOG_FILE = "jrcigars_medium_to_full_errors.log"
RETAILER_NAME = "JR Cigars"

# ---------------------------------------------------------
# Logging
# ---------------------------------------------------------

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def safe_float(v):
    try:
        if v is None:
            return None
        return float(str(v).replace(",", "").strip())
    except Exception:
        return None

def safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def print_progress(current, total, prefix="Processing"):
    if total <= 0:
        return
    bar_len = 30
    filled = int(bar_len * current / total)
    bar = "#" * filled + "-" * (bar_len - filled)
    pct = round(100 * current / total, 1)
    sys.stdout.write(f"\r{prefix}: |{bar}| {pct}% ({current}/{total})")
    sys.stdout.flush()
    if current == total:
        sys.stdout.write("\n")

# ---------------------------------------------------------
# SQL insert
# ---------------------------------------------------------

def save_record_to_sql(conn, record):
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO Cigar_Pricing_Rating.dbo.cigar_data (
                Retailer_Name,
                Cigar_Name,
                Brand,
                Size_Length,
                Size_Ring_Gauge,
                Wrapper_Color,
                Wrapper_Leaf,
                Wrapper_Country_of_Origin,
                Filler,
                Binder,
                Country_of_Origin,
                Price_Per_Stick,
                Price_Per_Bundle,
                Bundle_Quantity,
                Price_Per_Box,
                Box_Quantity,
                Stock_Status,
                Rating,
                Review_Count,
                URL,
                Scrape_Date,
                Strength,
                Profile,
                Shape,
                Website_Notes
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["retailer_name"],
                record["cigar_name"],
                record["brand"],
                record["size_length"],
                record["size_ring_gauge"],
                record["wrapper_color"],
                record["wrapper_leaf"],
                record["wrapper_origin"],
                record["filler"],
                record["binder"],
                record["country_of_origin"],
                record["price_per_stick"],
                record["price_per_bundle"],
                record["bundle_qty"],
                record["price_per_box"],
                record["box_qty"],
                record["stock_status"],
                record["rating"],
                record["review_count"],
                record["url"],
                record["scrape_date"],
                record["strength"],
                record["profile"],
                record["shape"],
                record["website_notes"],
            ),
        )
        conn.commit()
    except Exception as e:
        logging.error(f"SQL error for {record.get('url')}: {e}")
        print(f"SQL ERROR for {record.get('url')}: {e}")

# ---------------------------------------------------------
# Listing page scraper (pagination-based)
# ---------------------------------------------------------

def scrape_listing_page(page, url):
    try:
        page.goto(url, timeout=30000)
        page.wait_for_selector("a.product-tile-link", timeout=15000)

        items = page.evaluate(
            """
            () => {
                const tiles = Array.from(document.querySelectorAll("a.product-tile-link"));
                return tiles.map(tile => {
                    const url = tile.getAttribute("href") || null;

                    const brandEl = tile.querySelector(".item-desc1");
                    const nameEl  = tile.querySelector(".item-desc2");
                    const sizeEl  = tile.querySelector(".item-detail--size span");

                    const brand = brandEl ? brandEl.textContent.trim() : null;
                    const name  = nameEl ? nameEl.textContent.trim() : null;
                    const size  = sizeEl ? sizeEl.textContent.trim() : null;

                    let price = null;
                    const priceEl = tile.parentElement?.querySelector(".item-price, .price, .pricing");
                    if (priceEl) {
                        const match = priceEl.textContent
                            .replace(/[,\\s]/g, "")
                            .match(/\\$?([0-9]+(\\.[0-9]+)?)/);
                        if (match) price = parseFloat(match[1]);
                    }

                    return { url, brand, name, size, price };
                });
            }
            """
        )

        return items

    except Exception as e:
        print(f"!! Error scraping listing page {url}: {e}")
        logging.error(f"Error scraping listing page {url}: {e}")
        return []

# ---------------------------------------------------------
# Detail page scraper
# ---------------------------------------------------------

def scrape_detail_page(page, url):
    data = {
        "description": None,
        "length": None,
        "ring": None,
        "shape": None,
        "wrapper_type": None,
        "binder": None,
        "filler": None,
        "origin": None,
        "strength": None,
        "wrapper_shade": None,
    }

    try:
        page.goto(url, timeout=30000)

        try:
            page.wait_for_selector("div.cigar-details", timeout=15000)
        except Exception:
            pass

        data["description"] = page.evaluate(
            """
            () => {
                const el = document.querySelector("div.js-read-more-content.item-description");
                return el ? el.textContent.trim() : null;
            }
            """
        )

        extra_fields = page.evaluate(
            """
            () => {
                const root = document.querySelector("div.cigar-details");
                if (!root) return [];
                const labels = root.querySelectorAll("label.control-label");
                const results = [];
                labels.forEach(label => {
                    const labelText = label.textContent.trim();
                    const valueContainer = label.parentElement?.parentElement?.querySelector(".form-control-static");
                    let value = null;
                    if (valueContainer) {
                        const strong = valueContainer.querySelector("strong");
                        value = strong ? strong.textContent.trim() : valueContainer.textContent.trim();
                    }
                    results.push({ label: labelText, value });
                });
                return results;
            }
            """
        )

        for field in extra_fields:
            label = (field.get("label") or "").upper()
            value = safe_str(field.get("value"))

            if not label or not value:
                continue

            if "LENGTH" in label:
                data["length"] = value
            elif "RING" in label:
                data["ring"] = value
            elif "SHAPE" in label:
                data["shape"] = value
            elif "WRAPPER TYPE" in label:
                data["wrapper_type"] = value
            elif "BINDER" in label:
                data["binder"] = value
            elif "FILLER" in label:
                data["filler"] = value
            elif "ORIGIN" in label:
                data["origin"] = value
            elif "STRENGTH" in label:
                data["strength"] = value
            elif "WRAPPER SHADE" in label:
                data["wrapper_shade"] = value

        return data

    except Exception as e:
        print(f"  !! Error scraping detail page {url}: {e}")
        logging.error(f"Error scraping detail page {url}: {e}")
        return data

# ---------------------------------------------------------
# Main scraper
# ---------------------------------------------------------

def run_scraper():
    conn = pyodbc.connect(SQL_CONN_STR)
    scrape_date = datetime.now().date().isoformat()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        all_items = []
        start = 0
        page_size = 60

        while True:
            if start == 0:
                url = START_URL
            else:
                url = (
                    "https://www.jrcigars.com/cigars-by-strength/"
                    "medium-to-full-bodied-cigars/"
                    f"?start={start}&sz={page_size}"
                )

            print(f"\n=== JR Medium-to-Full Listing start={start} ===")
            items = scrape_listing_page(page, url)
            print(f"Found {len(items)} items at start={start}")

            if not items:
                print("No more items found. Pagination complete.")
                break

            all_items.extend(items)

            if len(items) < page_size:
                break

            start += page_size
            time.sleep(1)

        print(f"\nTotal items collected: {len(all_items)}")

        for idx, item in enumerate(all_items, start=1):
            print_progress(idx, len(all_items), prefix="Scraping details")

            product_url = item.get("url")
            if not product_url:
                continue

            if product_url.startswith("/"):
                product_url = BASE_URL + product_url

            detail = scrape_detail_page(page, product_url)

            size_text = safe_str(item.get("size"))
            length_from_listing = None
            ring_from_listing = None

            if size_text and "×" in size_text:
                parts = [s.strip() for s in size_text.split("×", 1)]
                if len(parts) == 2:
                    length_from_listing, ring_from_listing = parts

            record = {
                "retailer_name": RETAILER_NAME,
                "cigar_name": safe_str(item.get("name")),
                "brand": safe_str(item.get("brand")),
                "size_length": safe_float(detail["length"]) or safe_float(length_from_listing),
                "size_ring_gauge": safe_float(detail["ring"]) or safe_float(ring_from_listing),
                "wrapper_color": safe_str(detail["wrapper_shade"]),
                "wrapper_leaf": safe_str(detail["wrapper_type"]),
                "wrapper_origin": None,
                "filler": safe_str(detail["filler"]),
                "binder": safe_str(detail["binder"]),
                "country_of_origin": safe_str(detail["origin"]),
                "price_per_stick": safe_float(item.get("price")),
                "price_per_bundle": None,
                "bundle_qty": None,
                "price_per_box": None,
                "box_qty": None,
                "stock_status": "Unknown",
                "rating": None,
                "review_count": None,
                "url": product_url,
                "scrape_date": scrape_date,
                "strength": safe_str(detail["strength"]),
                "profile": None,
                "shape": safe_str(detail["shape"]),
                "website_notes": safe_str(detail["description"]),
            }

            save_record_to_sql(conn, record)

        browser.close()
    conn.close()
    print("\nJR Cigars medium-to-full scraping completed.")

if __name__ == "__main__":
    run_scraper()
