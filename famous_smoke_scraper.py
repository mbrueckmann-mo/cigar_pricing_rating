import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc
import time


BASE_URL = "https://www.famous-smoke.com"
LISTING_URL = f"{BASE_URL}/cigars"

SQL_CONN_STR = (
    "Driver={SQL Server};"
    "Server=VOCBook15;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)


# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

def get_soup(url: str) -> BeautifulSoup:
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def safe_decimal(value):
    """Convert to float or return None."""
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def safe_int(value):
    """Convert to int or return None."""
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def safe_str(value):
    """Convert to stripped string or return None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_quantity_packaging(text):
    """
    Extract quantity from strings like:
    - 'Pack of 8'
    - 'Bundle of 10'
    - 'Box of 20'
    - 'Pack of 100'
    """
    if not text:
        return None
    parts = text.split()
    for p in parts:
        if p.isdigit():
            return int(p)
    return None


def parse_rating(card: BeautifulSoup):
    """Count filled stars for rating and extract review count."""
    filled_stars = len(card.select(".sv-product-review-star--filled"))
    rating = safe_decimal(filled_stars)

    review_el = card.select_one(".sv-product-review-small__text")
    review_count = None
    if review_el:
        text = review_el.get_text(strip=True)
        first = text.split()[0]
        review_count = safe_int(first)

    return rating, review_count


def parse_single_price_from_card(card: BeautifulSoup):
    """Extract single-unit price from the listing card."""
    price_el = card.select_one("[data-price-type='finalPrice']")
    if not price_el:
        return None
    return safe_decimal(price_el.get("data-price-amount"))


def parse_size(size_text: str):
    """
    Parse size like '4 1/4 x 32' -> (4.25, 32).
    Handles simple fractions 1/4, 1/2, 3/4.
    """
    if not size_text or "x" not in size_text:
        return None, None

    size_text = size_text.strip()
    try:
        length_part, ring_part = [p.strip() for p in size_text.split("x", 1)]
    except ValueError:
        return None, None

    def to_float_length(part: str):
        parts = part.split()
        if len(parts) == 1:
            return safe_decimal(parts[0])
        if len(parts) == 2:
            whole, frac = parts
            frac_map = {"1/4": 0.25, "1/2": 0.5, "3/4": 0.75}
            base = safe_decimal(whole)
            return (base or 0) + frac_map.get(frac, 0.0)
        return None

    length = to_float_length(length_part)
    ring = safe_int(ring_part)

    return length, ring


# ---------------------------------------------------------
# Smart wrapper parsing
# ---------------------------------------------------------

WRAPPER_COLORS = [
    "natural", "maduro", "oscuro", "claro", "double claro",
    "ems", "sun grown", "sungrown", "colorado", "colorado claro",
    "colorado maduro"
]

WRAPPER_ORIGINS = [
    "usa", "united states", "honduras", "nicaragua", "dominican",
    "dominican republic", "mexico", "ecuador", "brazil", "cameroon",
    "panama", "costa rica"
]


def smart_parse_wrapper(combined: str):
    """
    Smart parse a combined wrapper string like:
    'Connecticut Broadleaf Maduro (USA)'
    into (color, leaf, origin).
    """
    if not combined:
        return None, None, None

    text = combined.lower()

    color = None
    for c in sorted(WRAPPER_COLORS, key=len, reverse=True):
        if c in text:
            color = c.title()
            break

    origin = None
    for o in sorted(WRAPPER_ORIGINS, key=len, reverse=True):
        if o in text:
            if o in ("usa", "united states"):
                origin = "USA"
            elif o == "dominican":
                origin = "Dominican Republic"
            else:
                origin = o.title()
            break

    leaf_candidate = combined
    if color:
        leaf_candidate = leaf_candidate.replace(color, "", 1)
    if origin:
        leaf_candidate = leaf_candidate.replace(origin, "", 1)
    leaf_candidate = leaf_candidate.replace("()", "").strip(" -,")

    leaf = safe_str(leaf_candidate)

    return color, leaf, origin


# ---------------------------------------------------------
# Product detail scraper
# ---------------------------------------------------------

def scrape_product_details(product_url: str) -> dict:
    """Scrape the product detail page for full specifications."""
    soup = get_soup(product_url)

    specs_table = soup.select_one("table#product-attribute-specs-table")

    def get_spec(label: str):
        if not specs_table:
            return None
        cell = specs_table.select_one(f"td[data-th='{label}']")
        return safe_str(cell.get_text(strip=True)) if cell else None

    brand = get_spec("Brand")
    country = get_spec("Country of Origin")

    wrapper_color = get_spec("Wrapper Color")
    wrapper_leaf = get_spec("Wrapper Leaf")
    wrapper_origin = get_spec("Wrapper Origin")

    wrapper_combined = get_spec("Wrapper")

    if wrapper_combined:
        c_color, c_leaf, c_origin = smart_parse_wrapper(wrapper_combined)
        if wrapper_color is None and c_color:
            wrapper_color = c_color
        if wrapper_leaf is None and c_leaf:
            wrapper_leaf = c_leaf
        if wrapper_origin is None and c_origin:
            wrapper_origin = c_origin

    strength = get_spec("Strength")
    shape = get_spec("Cigar Shape")
    quantity_packaging = get_spec("Quantity per Packaging")
    size_text = get_spec("Cigar Size")

    filler = get_spec("Filler")
    binder = get_spec("Binder")
    profile = get_spec("Profile")

    size_length, size_ring_gauge = parse_size(size_text) if size_text else (None, None)

    qty_number = parse_quantity_packaging(quantity_packaging)

    bundle_qty = qty_number if quantity_packaging and "bundle" in quantity_packaging.lower() else None
    box_qty = qty_number if quantity_packaging and "box" in quantity_packaging.lower() else None
    # Packs are ignored for bundle/box quantity; they stay NULL

    stock_status = "Unknown"
    if soup.select_one("button.action.tocart"):
        stock_status = "In Stock"
    if soup.find(string=lambda t: isinstance(t, str) and "Out of Stock" in t):
        stock_status = "Out of Stock"

    price_single = None
    price_el = soup.select_one("[data-price-type='finalPrice']")
    if price_el:
        price_single = safe_decimal(price_el.get("data-price-amount"))

    price_bundle = None
    price_box = None

    return {
        "brand": brand,
        "country_of_origin": country,
        "wrapper_color": wrapper_color,
        "wrapper_leaf": wrapper_leaf,
        "wrapper_origin": wrapper_origin,
        "strength": strength,
        "shape": shape,
        "quantity_packaging": quantity_packaging,
        "size_length": size_length,
        "size_ring_gauge": size_ring_gauge,
        "filler": filler,
        "binder": binder,
        "profile": profile,
        "stock_status": stock_status,
        "price_single": price_single,
        "price_bundle": price_bundle,
        "bundle_qty": bundle_qty,
        "price_box": price_box,
        "box_qty": box_qty,
    }


# ---------------------------------------------------------
# Listing page scraper
# ---------------------------------------------------------

def scrape_listing_page(page_url: str):
    """Scrape a listing page for product names, URLs, ratings, and prices."""
    soup = get_soup(page_url)

    cards = soup.select("div.product-item-info")
    results = []

    for card in cards:
        name_el = card.select_one("strong.product-item-name a.product-item-link")
        if not name_el:
            continue

        href = name_el.get("href")
        if not href:
            continue

        url = href if href.startswith("http") else BASE_URL + href
        cigar_name = safe_str(name_el.get_text(strip=True))

        rating, review_count = parse_rating(card)
        price_single_card = parse_single_price_from_card(card)

        results.append({
            "cigar_name": cigar_name,
            "url": url,
            "rating": rating,
            "review_count": review_count,
            "price_single_card": price_single_card,
        })

    next_link = soup.select_one("a.action.next")
    next_page_url = None
    if next_link and next_link.get("href"):
        href = next_link["href"]
        next_page_url = href if href.startswith("http") else BASE_URL + href

    return results, next_page_url


# ---------------------------------------------------------
# SQL insert
# ---------------------------------------------------------

def save_record_to_sql(conn, record: dict):
    cursor = conn.cursor()

    # String fields → str or None
    for key in [
        "retailer_name", "cigar_name", "brand", "wrapper_color", "wrapper_leaf",
        "wrapper_origin", "filler", "binder", "country_of_origin", "stock_status",
        "url", "strength", "profile", "shape"
    ]:
        record[key] = safe_str(record.get(key))

    # Numeric fields → float/int or None
    record["size_length"] = safe_decimal(record.get("size_length"))
    record["size_ring_gauge"] = safe_int(record.get("size_ring_gauge"))
    record["price_single"] = safe_decimal(record.get("price_single"))
    record["price_bundle"] = safe_decimal(record.get("price_bundle"))
    record["bundle_qty"] = safe_int(record.get("bundle_qty"))
    record["price_box"] = safe_decimal(record.get("price_box"))
    record["box_qty"] = safe_int(record.get("box_qty"))
    record["rating"] = safe_decimal(record.get("rating"))
    record["review_count"] = safe_int(record.get("review_count"))

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
            Shape
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            record["price_single"],
            record["price_bundle"],
            record["bundle_qty"],
            record["price_box"],
            record["box_qty"],
            record["stock_status"],
            record["rating"],
            record["review_count"],
            record["url"],
            record["scrape_date"],
            record["strength"],
            record["profile"],
            record["shape"],
        ),
    )
    conn.commit()


# ---------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------

def main():
    conn = pyodbc.connect(SQL_CONN_STR)

    retailer_name = "Famous Smoke"
    scrape_date = datetime.now().date().isoformat()  # store as 'YYYY-MM-DD' string

    page_url = LISTING_URL
    page_num = 1

    while page_url:
        print(f"Scraping listing page {page_num}: {page_url}")
        listing_records, next_page_url = scrape_listing_page(page_url)

        for item in listing_records:
            try:
                print(f"  -> Scraping product: {item['cigar_name']}")
                detail = scrape_product_details(item["url"])
                time.sleep(1.0)

                record = {
                    "retailer_name": retailer_name,
                    "cigar_name": item["cigar_name"],
                    "brand": detail["brand"],
                    "size_length": detail["size_length"],
                    "size_ring_gauge": detail["size_ring_gauge"],
                    "wrapper_color": detail["wrapper_color"],
                    "wrapper_leaf": detail["wrapper_leaf"],
                    "wrapper_origin": detail["wrapper_origin"],
                    "filler": detail["filler"],
                    "binder": detail["binder"],
                    "country_of_origin": detail["country_of_origin"],
                    "price_single": detail["price_single"] or item["price_single_card"],
                    "price_bundle": detail["price_bundle"],
                    "bundle_qty": detail["bundle_qty"],
                    "price_box": detail["price_box"],
                    "box_qty": detail["box_qty"],
                    "stock_status": detail["stock_status"],
                    "rating": item["rating"],
                    "review_count": item["review_count"],
                    "url": item["url"],
                    "scrape_date": scrape_date,
                    "strength": detail["strength"],
                    "profile": detail["profile"],
                    "shape": detail["shape"],
                }

                save_record_to_sql(conn, record)

            except Exception as e:
                print(f"    !! Error scraping {item['url']}: {e}")

        page_url = next_page_url
        page_num += 1
        if page_url:
            time.sleep(2.0)

    conn.close()
    print("Scraping completed.")


if __name__ == "__main__":
    main()
