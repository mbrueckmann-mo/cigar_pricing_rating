import re
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pyodbc

print("RUNNING Holt's Cigar Full Strength Scraper")

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

BASE_URL = "https://www.holts.com"

STRENGTH_URLS = {
    "Mild": f"{BASE_URL}/cigars/mild-cigars.html?p=1&limit=all",
    "Medium": f"{BASE_URL}/cigars/medium-cigars.html?p=1&limit=all",
    "Strong": f"{BASE_URL}/cigars/strong-cigars.html?p=1&limit=all",
    "Full": f"{BASE_URL}/cigars/full-strength-cigars.html?p=1&limit=all",
}

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

LOG_FILE = "holts_cigar_errors.log"
RETAILER_NAME = "Holt's Cigar"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None

def fetch_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def parse_price_range(price_text):
    """
    '$8.53 - $10.89' or '$8.53'
    -> float(lower bound)
    """
    if not price_text:
        return None
    nums = re.findall(r"\d+\.\d+", price_text.replace(",", ""))
    if not nums:
        return None
    return safe_float(nums[0])

def parse_size_from_name(name_text):
    """
    'Gordo - 6 x 60' -> shape='Gordo', length=6, ring=60
    """
    if not name_text:
        return None, None, None

    text = " ".join(name_text.split())
    parts = text.split("-")
    shape = parts[0].strip() if parts else None
    size_part = parts[1].strip() if len(parts) > 1 else ""

    m = re.search(r"(\d+(\.\d+)?)\s*[xX]\s*(\d+(\.\d+)?)", size_part)
    length = ring = None
    if m:
        length = safe_float(m.group(1))
        ring = safe_float(m.group(3))
    return shape, length, ring

def parse_rating_from_width(style_text):
    """
    style="width:83.636%" -> rating approx on 0-5 scale
    """
    if not style_text:
        return None
    m = re.search(r"width\s*:\s*([\d\.]+)%", style_text)
    if not m:
        return None
    pct = safe_float(m.group(1))
    if pct is None:
        return None
    rating = 5 * (pct / 100.0)
    return round(rating, 1)

def parse_review_count(text):
    """
    '11 Reviews' -> 11
    """
    if not text:
        return 0
    m = re.search(r"(\d+)", text)
    return int(m.group(1)) if m else 0

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
# PDP Parsing
# ---------------------------------------------------------

def parse_pdp(url):
    """
    Parse a Holt's PDP for:
    - brand, title
    - strength, country, wrapper, shapes
    - size (from name-wrapper)
    - description
    """
    try:
        soup = fetch_soup(url)
    except Exception as e:
        logging.error(f"PDP fetch error for {url}: {e}")
        print(f"PDP FETCH ERROR: {url} -> {e}")
        return {
            "brand": None,
            "title": None,
            "strength": None,
            "country": None,
            "wrapper_leaf": None,
            "shapes_list": None,
            "shape": None,
            "size_length": None,
            "size_ring": None,
            "description": None,
        }

    # Title
    title_tag = soup.select_one("h1")
    title = title_tag.get_text(strip=True) if title_tag else None

    # Brand heuristic from title
    brand = None
    if title:
        parts = title.split()
        brand = " ".join(parts[:2]) if len(parts) >= 2 else title

    strength = None
    country = None
    wrapper_leaf = None
    shapes_list = None
    shape = None
    size_length = None
    size_ring = None

    details_div = soup.select_one(".pdp-cigar-details")
    if details_div:
        strength_value = details_div.select_one(".strength-o-meter .value")
        if strength_value:
            strength = strength_value.get_text(strip=True)

        for li in details_div.select("ul li"):
            label_span = li.select_one("span.label")
            if not label_span:
                continue
            label_text = label_span.get_text(strip=True)
            value_span = label_span.select_one("span.value")
            value_text = value_span.get_text(strip=True) if value_span else None

            if "Country" in label_text:
                country = value_text
            elif "Wrapper" in label_text:
                wrapper_leaf = value_text

        sizes_div = details_div.select_one(".sizes")
        if sizes_div:
            text = sizes_div.get_text(" ", strip=True)
            text = re.sub(r"^shapes:\s*", "", text, flags=re.IGNORECASE)
            shapes_list = text if text else None

    name_wrapper = soup.select_one(".name-wrapper .name")
    if name_wrapper:
        name_text = name_wrapper.get_text(" ", strip=True)
        shape_from_name, length, ring = parse_size_from_name(name_text)
        shape = shape_from_name or shape
        size_length = length
        size_ring = ring

    desc_div = soup.select_one(".std.product-description")
    description = desc_div.get_text(" ", strip=True) if desc_div else None

    return {
        "brand": brand,
        "title": title,
        "strength": strength,
        "country": country,
        "wrapper_leaf": wrapper_leaf,
        "shapes_list": shapes_list,
        "shape": shape,
        "size_length": size_length,
        "size_ring": size_ring,
        "description": description,
    }

# ---------------------------------------------------------
# Category Scraper
# ---------------------------------------------------------

def scrape_strength_category(conn, strength_name, url):
    print(f"\n=== Scraping Holt's: {strength_name} ===")
    soup = fetch_soup(url)

    product_cards = soup.select("li.item")
    print(f"Found {len(product_cards)} products for {strength_name}")

    scrape_date = datetime.now().date().isoformat()
    total_inserted = 0

    for card in product_cards:
        try:
            link_tag = card.select_one("a.product-image")
            href = link_tag["href"].strip() if link_tag and link_tag.has_attr("href") else None

            name_tag = card.select_one("h2.product-name a")
            grid_name = name_tag.get_text(strip=True) if name_tag else None

            price_box = card.select_one(".price-box")
            price_text = price_box.get_text(" ", strip=True) if price_box else None
            price_per_stick = parse_price_range(price_text)

            rating_div = card.select_one(".bf-rating")
            rating_style = rating_div.get("style") if rating_div else None
            rating = parse_rating_from_width(rating_style)

            review_span = card.select_one(".search-ngrid-review .total")
            review_count = parse_review_count(
                review_span.get_text(strip=True) if review_span else ""
            )

            pdp = parse_pdp(href) if href else {
                "brand": None,
                "title": None,
                "strength": None,
                "country": None,
                "wrapper_leaf": None,
                "shapes_list": None,
                "shape": None,
                "size_length": None,
                "size_ring": None,
                "description": None,
            }

            notes_parts = []
            if pdp.get("description"):
                notes_parts.append(pdp["description"])
            if pdp.get("shapes_list"):
                notes_parts.append(f"Shapes: {pdp['shapes_list']}")
            website_notes = "\n\n".join(notes_parts) if notes_parts else None

            cigar_name = pdp["title"] or grid_name
            brand = pdp["brand"]

            record = {
                "retailer_name": RETAILER_NAME,
                "cigar_name": cigar_name,
                "brand": brand,
                "size_length": pdp["size_length"],
                "size_ring_gauge": pdp["size_ring"],
                "wrapper_color": None,       # not explicitly exposed
                "wrapper_leaf": pdp["wrapper_leaf"],
                "wrapper_origin": None,
                "filler": None,              # not exposed in provided HTML
                "binder": None,              # not exposed in provided HTML
                "country_of_origin": pdp["country"],
                "price_per_stick": price_per_stick,
                "price_per_bundle": None,
                "bundle_qty": None,
                "price_per_box": None,       # can be added later from PDP
                "box_qty": None,
                "stock_status": None,        # can be added later from PDP
                "rating": rating,
                "review_count": review_count,
                "url": href,
                "scrape_date": scrape_date,
                "strength": pdp["strength"] or strength_name,
                "profile": None,
                "shape": pdp["shape"],
                "website_notes": website_notes,
            }

            save_record_to_sql(conn, record)
            total_inserted += 1

        except Exception as e:
            logging.error(f"Error scraping Holt's product card: {e}")
            print(f"SCRAPE ERROR (Holt's card): {e}")

    print(f"Inserted {total_inserted} rows for Holt's {strength_name}")
    return total_inserted

# ---------------------------------------------------------
# Runner
# ---------------------------------------------------------

def run_scraper():
    conn = pyodbc.connect(SQL_CONN_STR)
    grand_total = 0

    for strength, url in STRENGTH_URLS.items():
        grand_total += scrape_strength_category(conn, strength, url)

    conn.close()
    print(f"\n=== Holt's Cigar Scraping Complete. Total rows inserted: {grand_total} ===")

if __name__ == "__main__":
    run_scraper()
