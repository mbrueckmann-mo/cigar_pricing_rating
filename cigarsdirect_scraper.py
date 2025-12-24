import requests
import time
import logging
from datetime import datetime
import pyodbc
import re

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

BASE_URL = "https://www.cigarsdirect.com"
COLLECTION = "cigars"

JSON_URL = f"{BASE_URL}/collections/{COLLECTION}/products.json"

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

LOG_FILE = "cigarsdirect_json_errors.log"
RETAILER_NAME = "CigarsDirect"

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
    except:
        return None

def safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def parse_variant_title(title):
    """
    Extract shape, length, ring gauge from variant title.
    Option 2: Shape + numeric size.
    Examples:
        "Robusto 5 x 50"
        "Toro / 6 x 52"
        "Box of 20 / 5 x 50"
        "Single 6x50"
    """
    if not title:
        return None, None, None

    t = title.lower().replace("×", "x")

    # Extract numeric size
    size_match = re.search(r"(\d+)\s*x\s*(\d+)", t)
    length = ring = None
    if size_match:
        length = safe_float(size_match.group(1))
        ring = safe_float(size_match.group(2))

    # Extract shape (text before size)
    shape = None
    if size_match:
        shape_part = t[:size_match.start()].strip()
        shape_part = re.sub(r"[/\-]", " ", shape_part).strip()
        shape = shape_part.title() if shape_part else None

    return shape, length, ring

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
# Main scraper
# ---------------------------------------------------------

def run_scraper():
    conn = pyodbc.connect(SQL_CONN_STR)
    scrape_date = datetime.now().date().isoformat()

    page = 1
    total_inserted = 0

    print("\n=== Starting CigarsDirect JSON Scraper ===")

    while True:
        url = f"{JSON_URL}?limit=250&page={page}"
        print(f"\nFetching page {page}: {url}")

        try:
            r = requests.get(url, timeout=20)
            data = r.json()
        except Exception as e:
            print(f"!! Error fetching page {page}: {e}")
            logging.error(f"Error fetching page {page}: {e}")
            break

        products = data.get("products", [])
        if not products:
            print(f"No products returned on page {page}. Ending.")
            break

        print(f"Found {len(products)} products on page {page}")

        for product in products:
            brand = safe_str(product.get("vendor"))
            name = safe_str(product.get("title"))
            description = safe_str(product.get("body_html"))
            handle = product.get("handle")
            product_url = f"{BASE_URL}/products/{handle}"

            for variant in product.get("variants", []):
                variant_title = safe_str(variant.get("title"))
                price = safe_float(variant.get("price"))
                sku = safe_str(variant.get("sku"))

                shape, length, ring = parse_variant_title(variant_title)

                record = {
                    "retailer_name": RETAILER_NAME,
                    "cigar_name": name,
                    "brand": brand,
                    "size_length": length,
                    "size_ring_gauge": ring,
                    "wrapper_color": None,
                    "wrapper_leaf": None,
                    "wrapper_origin": None,
                    "filler": None,
                    "binder": None,
                    "country_of_origin": None,
                    "price_per_stick": price,
                    "price_per_bundle": None,
                    "bundle_qty": None,
                    "price_per_box": None,
                    "box_qty": None,
                    "stock_status": "Unknown",
                    "rating": None,
                    "review_count": None,
                    "url": product_url,
                    "scrape_date": scrape_date,
                    "strength": None,
                    "profile": None,
                    "shape": shape,
                    "website_notes": description,
                }

                save_record_to_sql(conn, record)
                total_inserted += 1

        page += 1
        time.sleep(0.5)

    conn.close()
    print(f"\nCigarsDirect JSON scraping completed. Total rows inserted: {total_inserted}")

if __name__ == "__main__":
    run_scraper()
