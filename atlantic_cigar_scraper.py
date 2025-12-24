import re
import logging
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pyodbc

print("RUNNING Atlantic Cigar Full Strength Scraper")

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

BASE_URL = "https://atlanticcigar.com"

STRENGTH_URLS = {
    "Mild": "https://atlanticcigar.com/cigars/all-cigars/?_bc_fsnf=1&Strength=Mild",
    "Mild-Medium": "https://atlanticcigar.com/cigars/all-cigars/?_bc_fsnf=1&Strength=Mild-Medium",
    "Medium": "https://atlanticcigar.com/cigars/all-cigars/?_bc_fsnf=1&Strength=Medium",
    "Medium-Full": "https://atlanticcigar.com/cigars/all-cigars/?_bc_fsnf=1&Strength=Medium-Full",
    "Full": "https://atlanticcigar.com/cigars/all-cigars/?_bc_fsnf=1&Strength=Full",
}

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

LOG_FILE = "atlantic_cigar_errors.log"
RETAILER_NAME = "Atlantic Cigar"

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
    except:
        return None

def parse_size(size_text):
    if not size_text:
        return None, None
    m = re.search(r"(\d+(\.\d+)?)\s*[xX]\s*(\d+(\.\d+)?)", size_text)
    if not m:
        return None, None
    return safe_float(m.group(1)), safe_float(m.group(3))

def parse_price_range(price_text):
    if not price_text:
        return None
    nums = re.findall(r"\d+\.\d+", price_text.replace(",", ""))
    if not nums:
        return None
    return safe_float(nums[0])

def money_to_float(text):
    if not text:
        return None
    nums = re.findall(r"\d+\.\d+", text.replace(",", ""))
    if not nums:
        return None
    return safe_float(nums[0])

def fetch_soup(url):
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

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
    try:
        soup = fetch_soup(url)
    except Exception as e:
        logging.error(f"PDP fetch error for {url}: {e}")
        print(f"PDP FETCH ERROR: {url} -> {e}")
        return {k: None for k in [
            "brand","title","rating","review_count","country","shape","strength",
            "size_length","size_ring","color","binder","filler","wrapper_leaf",
            "price_box","stock_status","description"
        ]}

    # Brand
    brand_tag = soup.select_one("header.product-header a.product-brand")
    brand = brand_tag.get_text(strip=True) if brand_tag else None

    # Title
    title_tag = soup.select_one("header.product-header h1.product-title")
    title = title_tag.get_text(strip=True) if title_tag else None

    # Rating
    filled_stars = soup.select("header.product-header .rating-star.star-full")
    rating = len(filled_stars) if filled_stars else 0

    # Review Count
    review_text_tag = soup.select_one("header.product-header .ratings-count-text")
    review_count = 0
    if review_text_tag:
        text = review_text_tag.get_text(strip=True)
        if "Be the first" not in text:
            m = re.search(r"(\d+)", text)
            if m:
                review_count = int(m.group(1))

    # Description
    desc_div = soup.select_one(".description-section .tab-product-description")
    description_text = " ".join(desc_div.stripped_strings) if desc_div else None

    # Details
    details = {
        "country": None,
        "shape": None,
        "strength": None,
        "size_length": None,
        "size_ring": None,
        "color": None,
        "binder": None,
        "filler": None,
        "wrapper_leaf": None,
    }

    detail_container = soup.select_one(".product-detail-container")
    if detail_container:
        for div in detail_container.select(".product-detail.product-detail-custom-field"):
            key_span = div.select_one(".product-detail-key")
            val_span = div.select_one("[data-product-custom-field]")
            if not key_span or not val_span:
                continue

            label = key_span.get_text(strip=True)
            value = val_span.get_text(strip=True)

            if label == "Country of Origin":
                details["country"] = value
            elif label == "Shape":
                details["shape"] = value
            elif label == "Strength":
                details["strength"] = value
            elif label == "Size":
                length, ring = parse_size(value)
                details["size_length"] = length
                details["size_ring"] = ring
            elif label == "Color":
                details["color"] = value
            elif label == "Binder / Filler":
                parts = [p.strip() for p in value.split("/", 1)]
                details["binder"] = parts[0] if len(parts) >= 1 else None
                details["filler"] = parts[1] if len(parts) >= 2 else None
            elif label == "Wrapper":
                details["wrapper_leaf"] = value

    # Price (box)
    price_value_span = soup.select_one(".product-detail-container .price .price-value")
    price_box = money_to_float(price_value_span.get_text(strip=True)) if price_value_span else None

    # Stock Status
    stock_status = "Unknown"
    stock_div = soup.select_one(".product-detail-container .product-detail-stock-level")
    if stock_div:
        key_span = stock_div.select_one(".product-detail-key")
        if key_span:
            key_text = key_span.get_text(strip=True)
            if "In Stock" in key_text:
                stock_status = "In Stock"
            else:
                stock_status = key_text

    return {
        "brand": brand,
        "title": title,
        "rating": rating,
        "review_count": review_count,
        "country": details["country"],
        "shape": details["shape"],
        "strength": details["strength"],
        "size_length": details["size_length"],
        "size_ring": details["size_ring"],
        "color": details["color"],
        "binder": details["binder"],
        "filler": details["filler"],
        "wrapper_leaf": details["wrapper_leaf"],
        "price_box": price_box,
        "stock_status": stock_status,
        "description": description_text,
    }

# ---------------------------------------------------------
# Category Scraper (with pagination)
# ---------------------------------------------------------

def scrape_strength_category(conn, strength_name, base_url):
    print(f"\n=== Scraping Atlantic Cigar: {strength_name} ===")

    page = 1
    total_inserted = 0
    scrape_date = datetime.now().date().isoformat()

    while True:
        url = f"{base_url}&page={page}"
        soup = fetch_soup(url)

        product_cards = soup.select("article.product-item.product-item-grid")
        if not product_cards:
            break

        print(f"Page {page}: {len(product_cards)} products")

        for card in product_cards:
            try:
                grid_brand = card.get("data-product-brand") or None
                name_tag = card.select_one("h3.product-item-title a")
                grid_name = name_tag.get_text(strip=True) if name_tag else None

                url_tag = card.select_one("a.product-item-image")
                href = url_tag["href"].strip() if url_tag and url_tag.has_attr("href") else None
                full_url = BASE_URL + href if href.startswith("/") else href

                price_span = card.select_one(".product-item-price .price-without-tax")
                price_text = price_span.get_text(strip=True) if price_span else None
                price_per_stick = parse_price_range(price_text)

                summary_div = card.select_one(".product-item-summary")
                summary_text = summary_div.get_text(strip=True) if summary_div else None

                pdp = parse_pdp(full_url)

                website_notes_parts = []
                if summary_text:
                    website_notes_parts.append(summary_text)
                if pdp.get("description"):
                    website_notes_parts.append(pdp["description"])
                website_notes = "\n\n".join(website_notes_parts) if website_notes_parts else None

                cigar_name = pdp["title"] or grid_name
                brand = pdp["brand"] or grid_brand

                record = {
                    "retailer_name": RETAILER_NAME,
                    "cigar_name": cigar_name,
                    "brand": brand,
                    "size_length": pdp["size_length"],
                    "size_ring_gauge": pdp["size_ring"],
                    "wrapper_color": pdp["color"],
                    "wrapper_leaf": pdp["wrapper_leaf"],
                    "wrapper_origin": None,
                    "filler": pdp["filler"],
                    "binder": pdp["binder"],
                    "country_of_origin": pdp["country"],
                    "price_per_stick": price_per_stick,
                    "price_per_bundle": None,
                    "bundle_qty": None,
                    "price_per_box": pdp["price_box"],
                    "box_qty": None,
                    "stock_status": pdp["stock_status"],
                    "rating": pdp["rating"],
                    "review_count": pdp["review_count"],
                    "url": full_url,
                    "scrape_date": scrape_date,
                    "strength": pdp["strength"] or strength_name,
                    "profile": None,
                    "shape": pdp["shape"],
                    "website_notes": website_notes,
                }

                save_record_to_sql(conn, record)
                total_inserted += 1

            except Exception as e:
                logging.error(f"Error scraping product card: {e}")
                print(f"SCRAPE ERROR: {e}")

        page += 1

    print(f"Inserted {total_inserted} rows for {strength_name}")
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
    print(f"\n=== Atlantic Cigar Scraping Complete. Total rows inserted: {grand_total} ===")

if __name__ == "__main__":
    run_scraper()
