import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pyodbc
import time
import re

BASE_URL = "https://www.cigarbid.com"

# Example: search results or category page; update as needed.
START_URL = f"{BASE_URL}/shop/cigars"

# ---------------------------------------------------------
# SQL connection (default instance on your machine)
# ---------------------------------------------------------

SQL_CONN_STR = (
    "Driver={ODBC Driver 17 for SQL Server};"
    "Server=localhost;"
    "Database=Cigar_Pricing_Rating;"
    "Trusted_Connection=yes;"
)

# ---------------------------------------------------------
# HTTP / parsing helpers
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
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None

def parse_dimensions(dim_text: str):
    """
    Parse dimensions like (4.0"x52) -> length, ring_gauge
    """
    if not dim_text:
        return None, None

    # Remove parens, quotes, spaces
    cleaned = dim_text.strip("()").replace('"', "").replace(" ", "")
    if "x" not in cleaned:
        return None, None

    length_part, ring_part = cleaned.split("x", 1)
    length = safe_decimal(length_part)
    ring = safe_int(ring_part)
    return length, ring

def map_strength_from_class(class_list):
    """
    CigarBid uses classes like swatch-medium-full, swatch-full, etc.
    Map them to a human-readable strength string.
    """
    if not class_list:
        return None

    for c in class_list:
        if c.startswith("swatch-"):
            key = c.replace("swatch-", "").replace("-", " ").title()
            return key  # e.g. "Medium Full"
    return None

# ---------------------------------------------------------
# Detail page scraper (elements 2, 3, 4)
# ---------------------------------------------------------

def scrape_detail_page(url: str) -> dict:
    soup = get_soup(url)

    # --- Lot heading (element two) ---
    heading = soup.select_one("div.lot-heading")
    lot_id = None
    title_name = None
    title_shape = None
    dimensions = None
    pack_type = None

    if heading:
        lot_id_el = heading.select_one(".lot-id")
        if lot_id_el:
            # "Lot #5597961" -> 5597961
            m = re.search(r"#(\d+)", lot_id_el.get_text())
            if m:
                lot_id = m.group(1)

        title_span = heading.select_one(".lot-title span.title")
        if title_span:
            name_el = title_span.select_one(".title-name")
            shape_el = title_span.select_one(".title-shape")
            dim_el = title_span.select_one(".dimensions")
            pack_el = title_span.select_one(".title-pack")

            title_name = safe_str(name_el.get_text(strip=True)) if name_el else None
            title_shape = safe_str(shape_el.get_text(strip=True).strip("()")) if shape_el else None
            dimensions = safe_str(dim_el.get_text(strip=True)) if dim_el else None
            pack_type = safe_str(pack_el.get_text(strip=True)) if pack_el else None

    size_length, size_ring = parse_dimensions(dimensions) if dimensions else (None, None)

    # --- Description (element three) ---
    description_text = None
    desc_panel = soup.select_one("div.list-group-panel[data-panelid='Description']")
    if desc_panel:
        body = desc_panel.select_one(".list-group-body .p")
        if body:
            paragraphs = [p.get_text(" ", strip=True) for p in body.select("p")]
            description_text = safe_str(" ".join(paragraphs)) if paragraphs else None

    # --- Lot details (element four) ---
    status = None
    starting_bid = None
    msrp = None
    units_available = None
    opens_str = None
    closes_str = None

    lot_table = soup.select_one("table.lot-details")
    if lot_table:
        for row in lot_table.select("tr"):
            tds = row.select("td")
            if len(tds) < 2:
                continue

            label = tds[0].get_text(strip=True)
            value_cell = tds[1]

            if label == "Status":
                status = safe_str(value_cell.get_text(strip=True))

            elif label == "Starting Bid":
                # e.g. "$1.00"
                text = value_cell.get_text(strip=True).replace("$", "")
                starting_bid = safe_decimal(text)

            elif label.startswith("MSRP"):
                text = tds[-1].get_text(strip=True).replace("$", "")
                msrp = safe_decimal(text)

            elif label == "Units Available":
                units_text = value_cell.get_text(strip=True)
                units_available = safe_int(units_text)

            elif label == "Opens":
                opens_el = value_cell.select_one("time")
                opens_str = opens_el.get("datetime") if opens_el else None

            elif label == "Closes":
                closes_el = value_cell.select_one("time")
                closes_str = closes_el.get("datetime") if closes_el else None

    # Build a compact note about auction metadata to tuck into Website_Notes
    meta_bits = []
    if starting_bid is not None:
        meta_bits.append(f"Starting Bid: ${starting_bid:.2f}")
    if msrp is not None:
        meta_bits.append(f"MSRP: ${msrp:.2f}")
    if units_available is not None:
        meta_bits.append(f"Units Available: {units_available}")
    if status:
        meta_bits.append(f"Lot Status: {status}")
    if lot_id:
        meta_bits.append(f"Lot ID: {lot_id}")
    if opens_str:
        meta_bits.append(f"Opens: {opens_str}")
    if closes_str:
        meta_bits.append(f"Closes: {closes_str}")

    meta_text = " | ".join(meta_bits) if meta_bits else None

    if description_text and meta_text:
        website_notes = f"{description_text}\n\n{meta_text}"
    elif description_text:
        website_notes = description_text
    else:
        website_notes = meta_text

    return {
        "lot_id": lot_id,
        "title_name": title_name,
        "title_shape": title_shape,
        "dimensions": dimensions,
        "pack_type": pack_type,
        "size_length": size_length,
        "size_ring_gauge": size_ring,
        "status": status,
        "starting_bid": starting_bid,
        "msrp": msrp,
        "units_available": units_available,
        "opens": opens_str,
        "closes": closes_str,
        "website_notes": website_notes,
    }

# ---------------------------------------------------------
# Listing page scraper (element one)
# ---------------------------------------------------------

def scrape_listing_page(url: str):
    soup = get_soup(url)

    cards = soup.select("div.search-res.search-res-auction")
    results = []

    for card in cards:
        # URL and title pieces
        title_link = card.select_one("a.title")
        if not title_link or not title_link.get("href"):
            continue

        href = title_link["href"]
        product_url = href if href.startswith("http") else BASE_URL + href

        name_el = title_link.select_one(".title-name")
        shape_el = title_link.select_one(".title-shape")
        dim_el = title_link.select_one(".dimensions")
        pack_el = title_link.select_one(".title-pack")

        cigar_name = safe_str(name_el.get_text(strip=True)) if name_el else None
        shape = safe_str(shape_el.get_text(strip=True).strip("()")) if shape_el else None
        dimensions = safe_str(dim_el.get_text(strip=True)) if dim_el else None
        pack_type = safe_str(pack_el.get_text(strip=True)) if pack_el else None

        size_length, size_ring = parse_dimensions(dimensions) if dimensions else (None, None)

        # Characteristics table: Profile/Strength, Shape, Wrapper, Origin
        wrapper = None
        origin = None
        strength = None

        char_rows = card.select("table.characteristics tr")
        for row in char_rows:
            tds = row.select("td")
            if len(tds) < 2:
                continue

            label = tds[0].get_text(strip=True)
            value_td = tds[1]

            if label == "Profile":
                strength_span = value_td.select_one(".strength .swatch")
                if strength_span:
                    strength = map_strength_from_class(strength_span.get("class", []))

            elif label == "Wrapper":
                wrap_span = value_td.select_one("span span")
                wrapper = safe_str(wrap_span.get_text(strip=True)) if wrap_span else None

            elif label == "Origin":
                origin_span = value_td.select_one("span span")
                origin = safe_str(origin_span.get_text(strip=True)) if origin_span else None

        # Units available + Bid to Win + time left
        info = card.select_one(".search-res-info")
        units_available = None
        bid_to_win = None
        closes_text = None

        if info:
            units_span = info.select_one(".lot-qty .lot-units")
            if units_span:
                units_available = safe_int(units_span.get_text(strip=True))

            bid_amt = info.select_one(".lot-btw .price-amount")
            if bid_amt:
                bid_text = "".join(bid_amt.stripped_strings)
                # e.g. "$5.50"
                bid_text = bid_text.replace("$", "")
                bid_to_win = safe_decimal(bid_text)

            closes_time = info.select_one(".lot-closes time")
            if closes_time:
                closes_text = closes_time.get("datetime") or closes_time.get_text(strip=True)

        # Lot ID from data-lot
        lot_id = card.get("data-lot")

        results.append({
            "lot_id": lot_id,
            "cigar_name": cigar_name,
            "shape": shape,
            "dimensions": dimensions,
            "size_length": size_length,
            "size_ring_gauge": size_ring,
            "pack_type": pack_type,
            "wrapper": wrapper,
            "origin": origin,
            "strength": strength,
            "units_available": units_available,
            "bid_to_win": bid_to_win,
            "closes_text": closes_text,
            "url": product_url,
        })

    # Pagination (placeholder – you can adapt if needed)
    next_page_url = None
    next_link = soup.select_one("a[rel='next'], a.pagination-next")
    if next_link and next_link.get("href"):
        nhref = next_link["href"]
        next_page_url = nhref if nhref.startswith("http") else BASE_URL + nhref

    return results, next_page_url

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
        print("SQL ERROR:", e)
        print("Record:", record)

# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    conn = pyodbc.connect(SQL_CONN_STR)

    retailer_name = "CigarBid"
    scrape_date = datetime.now().date().isoformat()

    page_url = START_URL
    page_num = 1

    while page_url:
        print(f"Scraping listing page {page_num}: {page_url}")
        listing_items, next_page_url = scrape_listing_page(page_url)

        for item in listing_items:
            try:
                detail = scrape_detail_page(item["url"])
                time.sleep(1.0)

                cigar_name = detail["title_name"] or item["cigar_name"]

                # For now, treat the full name as Brand; you can refine later
                brand = cigar_name

                size_length = detail["size_length"] or item["size_length"]
                size_ring = detail["size_ring_gauge"] or item["size_ring_gauge"]

                price_per_stick = item["bid_to_win"] or detail["starting_bid"]

                record = {
                    "retailer_name": retailer_name,
                    "cigar_name": cigar_name,
                    "brand": brand,
                    "size_length": size_length,
                    "size_ring_gauge": size_ring,
                    "wrapper_color": item["wrapper"],
                    "wrapper_leaf": None,
                    "wrapper_origin": item["origin"],
                    "filler": None,
                    "binder": None,
                    "country_of_origin": item["origin"],
                    "price_per_stick": price_per_stick,
                    "price_per_bundle": None,
                    "bundle_qty": None,
                    "price_per_box": None,
                    "box_qty": detail["units_available"] or item["units_available"],
                    "stock_status": detail["status"] or "Open",
                    "rating": None,
                    "review_count": None,
                    "url": item["url"],
                    "scrape_date": scrape_date,
                    "strength": item["strength"],
                    "profile": None,
                    "shape": item["shape"] or detail["title_shape"],
                    "website_notes": detail["website_notes"],
                }

                save_record_to_sql(conn, record)

            except Exception as e:
                print(f"!! Error scraping {item['url']}: {e}")

        page_url = next_page_url
        page_num += 1
        if page_url:
            time.sleep(2.0)

    conn.close()
    print("CigarBid scraping completed.")

if __name__ == "__main__":
    main()
