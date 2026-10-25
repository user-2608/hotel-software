"""Scan a range of AxisRooms bookingEngineId values and write hotel + contact details to CSV.

For every id in --start..--end it loads
    https://app.axisrooms.com/beV2/home1.html?bookingEngineId=<id>
and, if the engine has a property, records one CSV row per property: names, ids,
website, contact page, address, phones, emails and (unless --no-search) rooms available / lowest price for the search dates.

Resumable: ids already checked are listed in <out>.checked and skipped on re-run.

Usage:
    python axisrooms.py --start 4380 --end 4400
    python axisrooms.py --start 1 --end 6000 --workers 8 --out out/axisrooms_hotels.csv
"""
import argparse
import csv
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from urllib.parse import urljoin

import requests

# ---------------------------------------------------------------------------
# AxisRooms booking-engine helpers (previously imported from a separate
# `axisrooms` module; inlined here so this file is self-contained).
# ---------------------------------------------------------------------------

BASE = "https://app.axisrooms.com/beV2"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_PHONE_RE = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")


def _get(session, url, **kw):
    """GET with sane defaults."""
    kw.setdefault("timeout", 30)
    return session.get(url, headers=HEADERS, **kw)


def list_properties_from_html(html):
    """Extract the properties configured on a booking engine home page.

    Returns a list of {"product_id": str, "name": str} dicts (empty if none).
    Looks for the product links the engine embeds in home1.html, e.g.
        home1.html?bookingEngineId=..&productId=123 ... Hotel Name
    falling back to any embedded JSON product records.
    """
    props, seen = [], set()
    for m in re.finditer(
        r"productId=(\d+)[^>]*>([^<]{2,120})<", html, flags=re.I
    ):
        pid, name = m.group(1), m.group(2).strip()
        if pid not in seen:
            seen.add(pid)
            props.append({"product_id": pid, "name": name})
    if props:
        return props
    # Fallback: {"productId":"123","hotelName":"..."} style JSON blobs (any key order).
    import json
    for m in re.finditer(r"\{[^{}]*\}", html):
        try:
            obj = json.loads(m.group(0))
        except ValueError:
            continue
        pid = next((str(v) for k, v in obj.items() if k.lower().replace("_", "") == "productid"), None)
        name = next((str(v) for k, v in obj.items()
                     if k.lower().replace("_", "") in ("hotelname", "propertyname", "name")), None)
        if pid and name and pid not in seen:
            seen.add(pid)
            props.append({"product_id": pid, "name": name.strip()})
    return props


def find_website_url(html):
    """Return the hotel's own web site URL mentioned on the engine page, if any."""
    for m in re.finditer(
        r'href=["\'](https?://[^"\']+)["\']', html, flags=re.I
    ):
        u = m.group(1)
        if "axisrooms.com" not in u.lower():
            return u
    return None


def find_contact_url(home_html, website=None):
    """Best-effort link to a contact page (on the engine page or the hotel site)."""
    for m in re.finditer(
        r'href=["\']([^"\']*[Cc]ontact[^"\']*)["\']', home_html
    ):
        return urljoin(BASE + "/", m.group(1))
    if website:
        try:
            page = _get(requests.Session(), website).text
            for m in re.finditer(
                r'href=["\']([^"\']*[Cc]ontact[^"\']*)["\']', page
            ):
                return urljoin(website, m.group(1))
        except requests.RequestException:
            pass
    return None


def scrape_contact(session, contact_url):
    """Fetch a contact page and pull out address / phones / emails."""
    text = _get(session, contact_url).text
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = re.sub(r"\s+", " ", plain)
    emails = sorted(set(_EMAIL_RE.findall(plain)))
    phones = []
    for m in _PHONE_RE.findall(plain):
        p = m.strip(" .-")
        if p not in phones and len(re.sub(r"\D", "", p)) >= 8:
            phones.append(p)
    addr = ""
    mm = re.search(
        r'\baddress\b[:\s]*((?:[\w&,./#-]+ ){5,25}(?:Road|Street|Lane|Marg|Nagar|Colony|Area|District|Bengaluru|Bangalore|India)|'
        r'[\w&,./#-]+(?: [\w&,./#-]+)* \d{6})',
        plain, flags=re.I,
    )
    if mm:
        addr = mm.group(1).strip(" ,:")
    return {"address": addr, "phones": phones, "emails": emails}


def search(session, engine_id, product_id, checkin, checkout, adults, children):
    """Hit the availability endpoint and return the raw HTML."""
    params = {
        "bookingEngineId": engine_id,
        "productId": product_id,
        "checkIn": checkin.isoformat(),
        "checkOut": checkout.isoformat(),
        "adults": adults,
        "children": children,
    }
    return _get(session, f"{BASE}/searchResult.html", params=params).text


def parse_results(html):
    """Parse a search-result page into {"hotel_name", "rooms": [...]}."""
    name_m = re.search(r"<title>\s*([^<|]{2,120})", html)
    rooms = []
    for m in re.finditer(
        r'(?:"room_?[nN]ame"|data-room)\s*[:=]\s*"([^"]{2,120})".{0,400}?'
        r'(?:₹|INR|Rs\.?|currency)\s*([\d,.]+)',
        html,
        flags=re.S | re.I,
    ):
        price = float(m.group(2).replace(",", ""))
        rooms.append({"name": m.group(1).strip(), "from_price_per_night": price})
    return {"hotel_name": name_m.group(1).strip() if name_m else "", "rooms": rooms}

FIELDS = [
    "booking_engine_id", "product_id", "hotel_name", "website", "contact_url",
    "address", "phones", "emails", "rooms_available", "min_price_per_night", "booking_url", "error",
]


RETRIES = 3          # attempts per request for transient network / HTTP errors
RETRY_WAIT = 2.0     # base back-off in seconds (grows exponentially + jitter)


def _fetch_with_retry(fn):
    """Call ``fn`` retrying transient failures with exponential back-off."""
    last = None
    for attempt in range(RETRIES):
        try:
            resp = fn()
            resp.raise_for_status()
            return resp
        except (requests.ConnectionError, requests.Timeout) as e:
            last = e
        except requests.HTTPError as e:
            # Retry only server-side / rate-limit responses.
            code = e.response.status_code if e.response is not None else 0
            if code >= 500 or code == 429:
                last = e
            else:
                raise
        time.sleep(RETRY_WAIT * (2 ** attempt) + random.uniform(0, 0.5))
    raise last


def scrape_engine(engine_id, checkin, nights, do_search, delay=0.0):
    """Return a list of CSV row dicts (empty if the engine id has no property)."""
    if delay:
        time.sleep(random.uniform(delay * 0.5, delay))  # spread load across workers
    session = requests.Session()
    session.headers.update(HEADERS)
    home = _fetch_with_retry(
        lambda: session.get(f"{BASE}/home1.html", params={"bookingEngineId": engine_id}, timeout=30)
    ).text
    props = list_properties_from_html(home)
    if not props:
        return []

    website = find_website_url(home)
    contact, contact_error = None, ""
    contact_url = find_contact_url(home, website)
    try:
        if contact_url:
            contact = scrape_contact(session, contact_url)
    except Exception as e:  # the hotel's own site can be down / block us; keep the row anyway
        contact_error = f"contact: {type(e).__name__}"

    rows = []
    for prop in props:
        row = {
            "booking_engine_id": engine_id,
            "product_id": prop["product_id"],
            "hotel_name": prop["name"],
            "website": website or "",
            "contact_url": contact_url or "",
            "address": (contact or {}).get("address") or "",
            "phones": " | ".join((contact or {}).get("phones", [])),
            "emails": " | ".join((contact or {}).get("emails", [])),
            "rooms_available": "",
            "min_price_per_night": "",
            "booking_url": f"{BASE}/home1.html?bookingEngineId={engine_id}",
            "error": contact_error,
        }
        if do_search:
            try:
                html = _fetch_with_retry(
                    lambda p=prop: search(
                        session, engine_id, p["product_id"],
                        checkin, checkin + timedelta(days=nights), 2, 0,
                    )
                ).text
                parsed = parse_results(html)
                prices = [r["from_price_per_night"] for r in parsed["rooms"] if r["from_price_per_night"]]
                row["hotel_name"] = parsed["hotel_name"] or row["hotel_name"]
                row["rooms_available"] = len(parsed["rooms"])
                row["min_price_per_night"] = min(prices) if prices else ""
            except Exception as e:
                row["error"] = (row["error"] + f" search: {type(e).__name__}").strip()
        rows.append(row)
    return rows


def main():
    p = argparse.ArgumentParser(description="Scrape AxisRooms booking engines by id range into a CSV.")
    p.add_argument("--start", type=int, required=True, help="first bookingEngineId")
    p.add_argument("--end", type=int, required=True, help="last bookingEngineId (inclusive)")
    p.add_argument("--out", type=Path, default=Path("out/axisrooms_hotels.csv"))
    p.add_argument("--workers", type=int, default=6)
    p.add_argument("--checkin", type=date.fromisoformat, default=date.today() + timedelta(days=7))
    p.add_argument("--nights", type=int, default=1)
    p.add_argument("--delay", type=float, default=0.0,
                   help="average per-id delay in seconds to throttle requests (0 = off)")
    p.add_argument("--no-search", action="store_true", help="skip the availability search (no rooms/price columns)")
    a = p.parse_args()

    if a.start > a.end:
        p.error("--start must be <= --end")
    if a.nights < 1:
        p.error("--nights must be >= 1")
    if a.workers < 1:
        p.error("--workers must be >= 1")

    a.out.parent.mkdir(parents=True, exist_ok=True)
    checked_file = a.out.with_suffix(a.out.suffix + ".checked")
    checked = set(checked_file.read_text().split()) if checked_file.exists() else set()
    todo = [i for i in range(a.start, a.end + 1) if str(i) not in checked]
    print(f"{len(todo)} ids to check ({len(checked)} already done)")

    new_file = not a.out.exists()
    lock = threading.Lock()
    found = 0
    failed = 0
    with a.out.open("a", newline="", encoding="utf-8-sig") as f, checked_file.open("a") as cf, \
            ThreadPoolExecutor(a.workers) as pool:
        w = csv.DictWriter(f, FIELDS)
        if new_file:
            w.writeheader()
        futures = {pool.submit(scrape_engine, i, a.checkin, a.nights, not a.no_search, a.delay): i
                   for i in todo}
        try:
            for n, fut in enumerate(as_completed(futures), 1):
                i = futures[fut]
                try:
                    rows = fut.result()
                except Exception as e:
                    # Network failure: don't mark as checked so a re-run retries it.
                    failed += 1
                    print(f"[{i}] failed: {type(e).__name__}: {e}")
                    continue
                with lock:
                    for r in rows:
                        w.writerow(r)
                        print(f"[{i}] {r['hotel_name']} | {r['phones']} | {r['emails']}")
                    found += len(rows)
                    f.flush()
                    cf.write(f"{i}\n")
                    cf.flush()
                if n % 100 == 0:
                    eta = (len(todo) - n) / max(n, 1)  # rough remaining batches
                    print(f"-- {n}/{len(todo)} checked, {found} hotels found, {failed} failed (~{eta:.0f} left)")
        except KeyboardInterrupt:
            print("\nInterrupted - cancelling pending jobs; re-run to resume.")
            for fut in futures:
                fut.cancel()
            pool.shutdown(wait=False, cancel_futures=True)
    print(f"Done. {found} hotel rows written to {a.out}" + (f" ({failed} ids failed, will retry on next run)" if failed else ""))


if __name__ == "__main__":
    main()
