import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc
import time

BASE_URL = "https://www.cigaraficionado.com"
SEARCH_URL = (
    "https://www.cigaraficionado.com/ratings/search?"
    "q=&page={page}&score%5B0%5D=95+TO+100&score%5B1%5D=90+TO+94"
)

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
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def safe_int(value):
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def safe_str(value):
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def parse_fractional_length(text):
    """
    Convert lengths like '5 1/8"' into decimal inches.
    """
    if not text:
        return None

    text = text.replace('"', "").strip()
    parts = text.split()

    if len(parts) == 1:
        return safe_decimal(parts[0])

    if len(parts) == 2:
        whole = safe_decimal(parts[0])
        frac = parts[1]
        frac_map = {
            "1/4": 0.25, "1/2": 0.5, "3/4": 0.75,
            "1/8": 0.125, "3/8": 0.375, "5/8": 0.625, "7/8": 0.875,
        }
        return (whole or 0) + frac_map.get(frac, 0.0)

    return None


def parse_wrapper_text(text):
    """
    Cigar Aficionado typically formats wrapper as 'Country Leaf', e.g.:
    'Nicaragua Habano', 'Ecuador Sumatra', etc.

    Returns (wrapper_country, wrapper_leaf).
    """
    if not text:
        return None, None

    parts = text.split()
    if len(parts) == 1:
        return parts[0], None
    if len(parts) >= 2:
        country = parts[0]
        leaf = " ".join(parts[1:])
        return country, leaf

    return None, None

# ---------------------------------------------------------
# Detail page scraper
# ---------------------------------------------------------

def scrape_detail_page(url):
    soup = get_soup(url)

    # LENGTH: find the column whose title is 'LENGTH'
    size_length = None
    for col in soup.select(".attributes-item"):
        title_el = col.select_one(".attributes-item_title")
        if not title_el:
            continue
        if title_el.get_text(strip=True).upper() == "LENGTH":
            length_label = col.select_one(".attributes-item_label strong")
            if length_label:
                size_length = parse_fractional_length(length_label.get_text(strip=True))
            break

    # GAUGE
    gauge_el = soup.select_one(".attributes-item_gauge")
    size_ring_gauge = safe_int(gauge_el.get_text(strip=True)) if gauge_el else None

    # STRENGTH: find the column whose title is 'STRENGTH'
    strength = None
    for col in soup.select(".attributes-item"):
        title_el = col.select_one(".attributes-item_title")
        if not title_el:
            continue
        if title_el.get_text(strip=True).upper() == "STRENGTH":
            label_el = col.select_one(".attributes-item_label strong")
            if label_el:
                strength = safe_str(label_el.get_text(strip=True))
            break

    # SHAPE (Size:)
    shape_el = soup.find("strong", string=lambda t: t and t.strip().startswith("Size"))
    shape = None
    if shape_el:
        shape = safe_str(
            shape_el.parent.get_text(strip=True).replace("Size:", "").strip()
        )

    # FILLER
    filler_el = soup.find("strong", string=lambda t: t and t.strip().startswith("Filler"))
    filler = None
    if filler_el:
        filler = safe_str(
            filler_el.parent.get_text(strip=True).replace("Filler:", "").strip()
        )

    # BINDER
    binder_el = soup.find("strong", string=lambda t: t and t.strip().startswith("Binder"))
    binder = None
    if binder_el:
        binder = safe_str(
            binder_el.parent.get_text(strip=True).replace("Binder:", "").strip()
        )

    # WRAPPER (flexible label match)
    wrapper = None
    for strong in soup.select("strong"):
        label = strong.get_text(strip=True).replace(":", "").lower()
        if label == "wrapper":
            wrapper = safe_str(
                strong.parent.get_text(strip=True)
                .replace("Wrapper:", "")
                .replace("Wrapper", "")
                .strip()
            )
            break

    wrapper_country, wrapper_leaf = parse_wrapper_text(wrapper)

    # COUNTRY
    country_el = soup.find("strong", string=lambda t: t and t.strip().startswith("Country"))
    country = None
    if country_el:
        country = safe_str(
            country_el.parent.get_text(strip=True).replace("Country:", "").strip()
        )

    # PRICE
    price_el = soup.find("strong", string=lambda t: t and t.strip().startswith("Price"))
    price = None
    if price_el:
        raw = price_el.parent.get_text(strip=True).replace("Price:", "").strip()
        raw = raw.replace("£", "").replace("$", "")
        price = safe_decimal(raw)

    # TASTING NOTE → Website_Notes
    note_el = soup.select_one(".cigar-detail_tastingnote p")
    website_notes = safe_str(note_el.get_text(strip=True)) if note_el else None

    return {
        "size_length": size_length,
        "size_ring_gauge": size_ring_gauge,
        "strength": strength,
        "shape": shape,
        "filler": filler,
        "binder": binder,
        "wrapper_country": wrapper_country,
        "wrapper_leaf": wrapper_leaf,
        "country": country,
        "price": price,
        "website_notes": website_notes,
    }

# ---------------------------------------------------------
# Listing page scraper
# ---------------------------------------------------------

def parse_listing_block(div):
    name_el = div.select_one("h4 a")
    cigar_name = safe_str(name_el.get_text(strip=True)) if name_el else None
    url = BASE_URL + name_el.get("href") if name_el else None

    score_el = div.select_one(".score-number")
    rating = safe_decimal(score_el.get_text(strip=True)) if score_el else None

    brand = cigar_name.split()[0] if cigar_name else None

    return {
        "cigar_name": cigar_name,
        "url": url,
        "rating": rating,
        "brand": brand,
    }

# ---------------------------------------------------------
# SQL insert
# ---------------------------------------------------------

def save_record_to_sql(conn, record):
    cursor = conn.cursor()

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
            None,  # Wrapper_Color not available from CA
            record["wrapper_leaf"],
            record["wrapper_country"],
            record["filler"],
            record["binder"],
            record["country"],
            record["price"],
            None,  # Price_Per_Bundle
            None,  # Bundle_Quantity
            None,  # Price_Per_Box
            None,  # Box_Quantity
            None,  # Stock_Status (CA is not a retailer)
            record["rating"],
            None,  # Review_Count
            record["url"],
            record["scrape_date"],
            record["strength"],
            None,  # Profile
            record["shape"],
            record["website_notes"],
        ),
    )
    conn.commit()

# ---------------------------------------------------------
# Main scraper
# ---------------------------------------------------------

def main():
    conn = pyodbc.connect(SQL_CONN_STR)

    retailer_name = "Cigar Aficionado"
    scrape_date = datetime.now().date().isoformat()

    page = 1
    while True:
        url = SEARCH_URL.format(page=page)
        print(f"Scraping page {page}: {url}")

        soup = get_soup(url)
        cigar_divs = soup.select("div.row[id^='cigar-']")

        if not cigar_divs:
            print("No more cigars found.")
            break

        for div in cigar_divs:
            try:
                listing = parse_listing_block(div)
                if not listing["url"]:
                    continue

                detail = scrape_detail_page(listing["url"])

                record = {
                    "retailer_name": retailer_name,
                    "cigar_name": listing["cigar_name"],
                    "brand": listing["brand"],
                    "size_length": detail["size_length"],
                    "size_ring_gauge": detail["size_ring_gauge"],
                    "wrapper_leaf": detail["wrapper_leaf"],
                    "wrapper_country": detail["wrapper_country"],
                    "filler": detail["filler"],
                    "binder": detail["binder"],
                    "country": detail["country"],
                    "price": detail["price"],
                    "rating": listing["rating"],
                    "url": listing["url"],
                    "scrape_date": scrape_date,
                    "strength": detail["strength"],
                    "shape": detail["shape"],
                    "website_notes": detail["website_notes"],
                }

                save_record_to_sql(conn, record)

            except Exception as e:
                print(f"    !! Error scraping cigar: {e}")

        page += 1
        time.sleep(2)

    conn.close()
    print("Cigar Aficionado scraping completed.")


if __name__ == "__main__":
    main()
