import re
import time
import logging
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pyodbc

print("RUNNING Gotham Cigars Scraper (Requests + BS4, variant-style using spec table)")

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

BASE_URL = "https://www.gothamcigars.com"

STRENGTH_URLS = {
    "Mellow": "https://www.gothamcigars.com/mellow-cigars/?sort=bestselling&limit=100&mode=6",
    "Mellow-to-Medium": "https://www.gothamcigars.com/mellow-to-medium-cigars/?sort=bestselling&limit=100&mode=6",
    "Medium": "https://www.gothamcigars.com/medium-cigars/?sort=bestselling&limit=100&mode=6",
    "Medium-to-Full": "https://www.gothamcigars.com/medium-to-full-cigars/?sort=bestselling&limit=100&mode=6",
    "Full": "https://www.gothamcigars.com/full-bodied-cigars/?sort=bestselling&limit=100&mode=6",
}

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

RETAILER_NAME = "Gotham Cigars"
LOG_FILE = "gotham_cigars_errors.log"

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
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REQUEST_SLEEP = 1.0


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def safe_float(v):
    try:
        return float(v)
    except Exception:
        return None


def fetch_soup(url, params=None):
    time.sleep(REQUEST_SLEEP)
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
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
# Category parsing
# ---------------------------------------------------------

def iterate_category_pages(strength_name, base_url):
    page = 1
    while True:
        params = {"page": page}
        try:
            soup = fetch_soup(base_url, params=params)
        except Exception as e:
            logging.error(f"Gotham category fetch error {base_url} p={page}: {e}")
            print(f"CATEGORY FETCH ERROR: {base_url} p={page} -> {e}")
            break

        cards = soup.select(".card, .product") or soup.select(".productGrid .product")
        if not cards:
            if page == 1:
                print(f"No products found for {strength_name} page 1 – check URL.")
            break

        print(f"{strength_name}: page {page}, found {len(cards)} products")
        for card in cards:
            yield card

        page += 1


def parse_category_card(card):
    link = card.select_one("a.card-figure, a.card-title, a.product-title, a")
    href = link["href"].strip() if link and link.has_attr("href") else None
    full_url = urljoin(BASE_URL, href) if href else None

    name_el = card.select_one(".card-title, .product-title, .card-figure img, a")
    name = None
    if name_el:
        if name_el.name == "img":
            name = name_el.get("alt") or name_el.get("title")
        else:
            name = name_el.get_text(strip=True)

    price_text = None
    price_el = card.select_one(".price, .card-text .price, [data-product-price]")
    if price_el:
        price_text = price_el.get_text(" ", strip=True)

    low_price = None
    high_price = None
    if price_text:
        prices = re.findall(r"\$\s*([\d,]+\.\d{2})", price_text)
        if prices:
            low_price = safe_float(prices[0].replace(",", ""))
            if len(prices) > 1:
                high_price = safe_float(prices[-1].replace(",", ""))

    rating = None
    rating_el = card.select_one("[data-test-info-type='productRating'], .rating--small")
    if rating_el:
        m = re.search(r"([\d\.]+)", rating_el.get_text(strip=True))
        if m:
            rating = safe_float(m.group(1))

    review_count = 0
    review_el = card.select_one(".productReview-link, .rating--small span")
    if review_el:
        m = re.search(r"(\d+)", review_el.get_text(strip=True).replace(",", ""))
        if m:
            review_count = int(m.group(1))

    return {
        "url": full_url,
        "name": name,
        "low_price": low_price,
        "high_price": high_price,
        "rating": rating,
        "review_count": review_count,
    }


# ---------------------------------------------------------
# PDP parsing
# ---------------------------------------------------------

def parse_pdp_description_and_notes(soup):
    texts = []

    main_desc = soup.select_one(
        ".productView-description-tabContent[data-emthemesmodez-mobile-collapse-content]"
    )
    if main_desc:
        texts.append(main_desc.get_text(" ", strip=True))

    tasting_tab = soup.select_one("#tab-warranty .productView-description-tabContent")
    if tasting_tab:
        texts.append(tasting_tab.get_text(" ", strip=True))

    combined = " ".join(t for t in texts if t)
    return combined or None


def parse_specs_table(soup):
    result = {
        "size_length": None,
        "ring_gauge": None,
        "shape": None,
        "box_qty": None,
        "bundle_qty": None,
        "wrapper_leaf": None,
        "binder": None,
        "filler": None,
        "profile": None,
        "country_of_origin": None,
    }

    table = soup.select_one(".productView-description-tabContent table")
    if not table:
        return result

    rows = table.select("tr")
    if len(rows) < 2:
        return result

    val_tds = rows[1].select("td")
    if len(val_tds) < 7:
        return result

    size_text = val_tds[0].get_text(" ", strip=True)
    qty_text = val_tds[1].get_text(" ", strip=True)
    wrapper_text = val_tds[2].get_text(" ", strip=True)
    binder_text = val_tds[3].get_text(" ", strip=True)
    filler_text = val_tds[4].get_text(" ", strip=True)
    strength_td = val_tds[5]
    origin_text = val_tds[6].get_text(" ", strip=True)

    if size_text:
        m_len = re.search(r"(\d+\s*\d*/?\d*)", size_text)
        if m_len:
            frac = m_len.group(1).replace(" ", "")
            if "/" in frac:
                parts = frac.split("/")
                try:
                    num = float(parts[0])
                    den = float(parts[1])
                    result["size_length"] = num / den
                except Exception:
                    pass
            else:
                result["size_length"] = safe_float(frac)

        m_rg = re.search(r"[xX]\s*(\d+)", size_text)
        if m_rg:
            result["ring_gauge"] = int(m_rg.group(1))

        m_shape = re.search(r"\(([^)]+)\)", size_text)
        if m_shape:
            result["shape"] = m_shape.group(1).strip()

    if qty_text:
        m_two = re.search(r"(\d+)\s+\w+\s+of\s+(\d+)", qty_text, re.IGNORECASE)
        if m_two:
            outer = int(m_two.group(1))
            inner = int(m_two.group(2))
            result["box_qty"] = outer
            result["bundle_qty"] = inner
        else:
            m_one = re.search(r"(\d+)", qty_text)
            if m_one:
                result["box_qty"] = int(m_one.group(1))

    if wrapper_text:
        result["wrapper_leaf"] = wrapper_text.strip()
    if binder_text:
        result["binder"] = binder_text.strip()
    if filler_text:
        result["filler"] = filler_text.strip()

    strength_text = strength_td.get_text(" ", strip=True)
    if not strength_text:
        img = strength_td.select_one("img")
        if img:
            strength_text = (img.get("title") or img.get("alt") or "").strip()

    if strength_text:
        if "Mellow" in strength_text and "Medium" in strength_text:
            result["profile"] = "Mellow-to-Medium"
        elif "Mellow" in strength_text:
            result["profile"] = "Mellow"
        elif "Full" in strength_text and "Medium" in strength_text:
            result["profile"] = "Medium-to-Full"
        elif "Full" in strength_text:
            result["profile"] = "Full"
        elif "Medium" in strength_text:
            result["profile"] = "Medium"
        else:
            result["profile"] = strength_text

    if origin_text:
        result["country_of_origin"] = origin_text.strip()

    return result


def parse_pdp(full_url):
    try:
        soup = fetch_soup(full_url)
    except Exception as e:
        logging.error(f"Gotham PDP fetch error for {full_url}: {e}")
        print(f"PDP FETCH ERROR: {full_url} -> {e}")
        return None

    title_el = soup.select_one(".productView-title, h1")
    cigar_name = title_el.get_text(strip=True) if title_el else None

    brand = None
    brand_el = soup.select_one(".productView-brand a, .productView-brand")
    if brand_el:
        brand = brand_el.get_text(strip=True)
    elif cigar_name:
        brand = cigar_name.split()[0]

    website_notes = parse_pdp_description_and_notes(soup)
    specs = parse_specs_table(soup)

    stock_status = "In Stock"
    out_el = soup.find(string=re.compile("Out of stock", re.IGNORECASE))
    if out_el:
        stock_status = "Out of Stock"

    return {
        "cigar_name": cigar_name,
        "brand": brand,
        "website_notes": website_notes,
        "size_length": specs["size_length"],
        "ring_gauge": specs["ring_gauge"],
        "shape": specs["shape"],
        "box_qty": specs["box_qty"],
        "bundle_qty": specs["bundle_qty"],
        "wrapper_leaf": specs["wrapper_leaf"],
        "binder": specs["binder"],
        "filler": specs["filler"],
        "profile": specs["profile"],
        "country_of_origin": specs["country_of_origin"],
        "stock_status": stock_status,
    }


# ---------------------------------------------------------
# Strength scraper
# ---------------------------------------------------------

def scrape_strength(conn, strength_name, base_url):
    print(f"\n=== Scraping Gotham Strength: {strength_name} ===")
    total_records = 0
    scrape_date = datetime.now().date().isoformat()

    for card in iterate_category_pages(strength_name, base_url):
        try:
            cat_info = parse_category_card(card)
            full_url = cat_info["url"]
            if not full_url:
                continue

            pdp = parse_pdp(full_url)
            if not pdp:
                continue

            price = cat_info["low_price"]

            record = {
                "retailer_name": RETAILER_NAME,
                "cigar_name": pdp["cigar_name"] or cat_info["name"],
                "brand": pdp["brand"],
                "size_length": pdp["size_length"],
                "size_ring_gauge": pdp["ring_gauge"],
                "wrapper_color": None,
                "wrapper_leaf": pdp["wrapper_leaf"],
                "wrapper_origin": None,
                "filler": pdp["filler"],
                "binder": pdp["binder"],
                "country_of_origin": pdp["country_of_origin"],
                "price_per_stick": None,
                "price_per_bundle": None,
                "bundle_qty": pdp["bundle_qty"],
                "price_per_box": price,
                "box_qty": pdp["box_qty"],
                "stock_status": pdp["stock_status"],
                "rating": cat_info["rating"],
                "review_count": cat_info["review_count"],
                "url": full_url,
                "scrape_date": scrape_date,
                "strength": strength_name,
                "profile": pdp["profile"] or strength_name,
                "shape": pdp["shape"],
                "website_notes": pdp["website_notes"],
            }

            save_record_to_sql(conn, record)
            total_records += 1

        except Exception as e:
            logging.error(f"Error scraping Gotham product for {strength_name}: {e}")
            print(f"SCRAPE ERROR (Gotham {strength_name}): {e}")

    print(f"Inserted {total_records} rows for Gotham {strength_name}")
    return total_records


# ---------------------------------------------------------
# Runner
# ---------------------------------------------------------

def run_scraper():
    conn = pyodbc.connect(SQL_CONN_STR)
    grand_total = 0

    for strength, url in STRENGTH_URLS.items():
        grand_total += scrape_strength(conn, strength, url)

    conn.close()
    print(f"\n=== Gotham Scraping Complete. Total rows inserted: {grand_total} ===")


if __name__ == "__main__":
    run_scraper()
