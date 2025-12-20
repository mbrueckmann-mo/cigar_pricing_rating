import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc
import time
import re

BASE_URL = "https://cigarsdaily.com"
START_URL = "https://cigarsdaily.com/country-of-origin/dominican-republic/"

# ---------------------------------------------------------
# ✅ FIXED SQL CONNECTION — UPDATE THIS LINE ONLY
# ---------------------------------------------------------

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=YOUR_SERVER_NAME_HERE;"   # <-- UPDATE THIS
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

def get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, timeout=15, headers=HEADERS)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")

def safe_decimal(value):
    try:
        return float(value) if value is not None else None
    except:
        return None

def safe_int(value):
    try:
        return int(value) if value is not None else None
    except:
        return None

def safe_str(value):
    if not value:
        return None
    s = str(value).strip()
    return s if s else None

def parse_size_from_name(name):
    """
    Extract size from product name like:
    'Robusto (5.5×54)' or 'Robusto (5.5x54)'
    """
    match = re.search(r"\(([\d\.]+)\s*[x×]\s*(\d+)\)", name)
    if not match:
        return None, None
    return safe_decimal(match.group(1)), safe_int(match.group(2))

# ---------------------------------------------------------
# Detail page scraper
# ---------------------------------------------------------

def scrape_detail_page(url):
    soup = get_soup(url)

    # Product title
    title_el = soup.select_one("h1.product-title")
    product_title = safe_str(title_el.get_text(strip=True)) if title_el else None

    # Breadcrumb brand
    breadcrumb = soup.select("nav.woocommerce-breadcrumb a")
    breadcrumb_brand = safe_str(breadcrumb[-1].get_text(strip=True)) if len(breadcrumb) >= 3 else None

    # MSRP range
    msrp_min = msrp_max = None
    msrp_el = soup.select_one(".woocommerce_msrp_price")
    if msrp_el:
        prices = re.findall(r"[\d\.]+", msrp_el.get_text())
        if len(prices) >= 2:
            msrp_min, msrp_max = safe_decimal(prices[0]), safe_decimal(prices[1])

    # Customer rating
    rating_el = soup.select_one(".woocommerce-product-rating strong.rating")
    rating = safe_decimal(rating_el.get_text(strip=True)) if rating_el else None

    # Review count
    review_el = soup.select_one(".woocommerce-review-link .count")
    review_count = safe_int(review_el.get_text(strip=True)) if review_el else None

    # Price range
    price_min = price_max = None
    price_el = soup.select_one(".price-wrapper .price")
    if price_el:
        prices = re.findall(r"[\d\.]+", price_el.get_text())
        if len(prices) >= 2:
            price_min, price_max = safe_decimal(prices[0]), safe_decimal(prices[1])

    # Short description
    desc_el = soup.select_one(".product-short-description p")
    website_notes = safe_str(desc_el.get_text(strip=True)) if desc_el else None

    # Attributes table
    attrs = {}
    for row in soup.select("table.woocommerce-product-attributes.shop_attributes tr"):
        th = row.select_one("th.woocommerce-product-attributes-item__label")
        td = row.select_one("td.woocommerce-product-attributes-item__value")
        if th and td:
            attrs[th.get_text(strip=True).lower()] = td.get_text(strip=True)

    # Length / Ring Gauge
    size_length = size_ring_gauge = None

    if "length in inches" in attrs:
        m = re.search(r"[\d\.]+", attrs["length in inches"])
        if m:
            size_length = safe_decimal(m.group(0))

    if "ring gauge" in attrs:
        m = re.search(r"\d+", attrs["ring gauge"])
        if m:
            size_ring_gauge = safe_int(m.group(0))

    # Wrapper / Binder / Filler / Strength / Country / Brand
    wrapper = safe_str(attrs.get("wrapper"))
    binder = safe_str(attrs.get("binder"))
    filler = safe_str(attrs.get("filler"))
    strength = safe_str(attrs.get("strength"))
    country = safe_str(attrs.get("country of origin"))
    cigar_brand_attr = safe_str(attrs.get("cigar brand"))

    # Split wrapper
    wrapper_country = wrapper_leaf = None
    if wrapper:
        parts = wrapper.split()
        wrapper_country = parts[0]
        wrapper_leaf = " ".join(parts[1:]) if len(parts) > 1 else None

    return {
        "product_title": product_title,
        "breadcrumb_brand": breadcrumb_brand,
        "cigar_brand_attr": cigar_brand_attr,
        "msrp_min": msrp_min,
        "msrp_max": msrp_max,
        "rating": rating,
        "review_count": review_count,
        "price_min": price_min,
        "price_max": price_max,
        "website_notes": website_notes,
        "size_length": size_length,
        "size_ring_gauge": size_ring_gauge,
        "wrapper_country": wrapper_country,
        "wrapper_leaf": wrapper_leaf,
        "binder": binder,
        "filler": filler,
        "country": country,
        "strength": strength,
        "shape": None,
    }

# ---------------------------------------------------------
# Listing page scraper
# ---------------------------------------------------------

def scrape_listing_page(url):
    soup = get_soup(url)

    products = soup.select("div.product-small.box, li.product")
    print("Products found:", len(products))

    results = []

    for p in products:
        link = p.select_one("a")
        if not link:
            continue

        product_url = link.get("href")
        name = safe_str(link.get("title")) or safe_str(link.get("alt")) or safe_str(link.get_text(strip=True))

        if not name:
            continue

        size_length, size_ring_gauge = parse_size_from_name(name)
        brand = name.split()[0] if name else None

        results.append({
            "name": name,
            "url": product_url,
            "brand": brand,
            "size_length": size_length,
            "size_ring_gauge": size_ring_gauge,
        })

    return results

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
                None,
                record["wrapper_leaf"],
                record["wrapper_country"],
                record["filler"],
                record["binder"],
                record["country"],
                record["price_min"],
                None,
                None,
                record["price_max"],
                None,
                None,
                record["rating"],
                record["review_count"],
                record["url"],
                record["scrape_date"],
                record["strength"],
                None,
                record["shape"],
                record["website_notes"],
            ),
        )
        conn.commit()

    except Exception as e:
        print("SQL ERROR:", e)
        print("Record:", record)

# ---------------------------------------------------------
# Main scraper
# ---------------------------------------------------------

def main():
    print("Connecting to SQL Server...")

    conn = pyodbc.connect(SQL_CONN_STR)

    retailer_name = "Cigars Daily"
    scrape_date = datetime.now().date().isoformat()

    listing = scrape_listing_page(START_URL)

    for item in listing:
        try:
            detail = scrape_detail_page(item["url"])
            time.sleep(1)

            cigar_name = detail["product_title"] or item["name"]

            brand = (
                detail["cigar_brand_attr"]
                or detail["breadcrumb_brand"]
                or item["brand"]
            )

            size_length = detail["size_length"] if detail["size_length"] is not None else item["size_length"]
            size_ring_gauge = detail["size_ring_gauge"] if detail["size_ring_gauge"] is not None else item["size_ring_gauge"]

            record = {
                "retailer_name": retailer_name,
                "cigar_name": cigar_name,
                "brand": brand,
                "size_length": size_length,
                "size_ring_gauge": size_ring_gauge,
                "wrapper_country": detail["wrapper_country"],
                "wrapper_leaf": detail["wrapper_leaf"],
                "filler": detail["filler"],
                "binder": detail["binder"],
                "country": detail["country"],
                "price_min": detail["price_min"],
                "price_max": detail["price_max"],
                "rating": detail["rating"],
                "review_count": detail["review_count"],
                "url": item["url"],
                "scrape_date": scrape_date,
                "strength": detail["strength"],
                "shape": detail["shape"],
                "website_notes": detail["website_notes"],
            }

            save_record_to_sql(conn, record)

        except Exception as e:
            print(f"!! Error scraping {item['url']}: {e}")

    conn.close()
    print("Cigars Daily scraping completed.")

if __name__ == "__main__":
    main()
