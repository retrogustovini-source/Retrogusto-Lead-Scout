import subprocess
import sys

# Auto-install dependencies at startup
subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx==0.27.0", "openpyxl==3.1.2", "-q"])

import os
import re
import asyncio
import logging
from datetime import datetime, date
import httpx
import openpyxl
from openpyxl import load_workbook
from copy import copy
import io

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- CONFIG ---
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
XLSX_PATH = os.environ.get("XLSX_PATH", "leads.xlsx")

# Search queries for Prague wine/food venues
SEARCH_QUERIES = [
    "wine bar Praha",
    "vinoteka Praha",
    "enoteca Praha",
    "ristorante italiano Praha",
    "bistro vino Praha",
    "vinarna Praha",
]

# Venue types to include (from Google Places types)
INCLUDE_TYPES = {"restaurant", "bar", "food", "cafe", "night_club", "establishment"}

# Keywords in name that suggest wine focus
WINE_KEYWORDS = [
    "wine", "vino", "vinoteka", "vinarna", "enoteca", "wein", "vin ",
    "vinice", "sommelier", "cava", "cellar", "sklep", "degustace",
    "bacchus", "bacchante", "vinny", "vinné", "enolog"
]

# Keywords that suggest NOT a good fit
EXCLUDE_KEYWORDS = [
    "supermarket", "tesco", "albert", "billa", "lidl", "penny",
    "kaufland", "spar", "hotel chain", "fast food", "mcdonald", "kfc",
    "subway", "burger", "kebab", "sushi conveyor"
]


def normalize_name(name: str) -> str:
    """Lowercase and strip for comparison."""
    return re.sub(r'\s+', ' ', name.lower().strip())


def load_existing_leads(xlsx_path: str) -> set:
    """Load existing lead names from the xlsx file."""
    existing = set()
    if not os.path.exists(xlsx_path):
        log.warning(f"XLSX not found at {xlsx_path}, will create fresh.")
        return existing
    wb = load_workbook(xlsx_path, read_only=True)
    ws = wb["Leads"]
    first_row = True
    for row in ws.iter_rows(values_only=True):
        if first_row:
            first_row = False
            continue
        if row[0]:
            existing.add(normalize_name(str(row[0])))
    wb.close()
    return existing


def is_wine_relevant(name: str, types: list) -> bool:
    """Check if venue is likely wine-relevant."""
    name_lower = name.lower()
    for kw in WINE_KEYWORDS:
        if kw in name_lower:
            return True
    for t in types:
        if t in ("bar", "night_club"):
            return True
    return False


def has_exclude_keyword(name: str) -> bool:
    name_lower = name.lower()
    for kw in EXCLUDE_KEYWORDS:
        if kw in name_lower:
            return True
    return False


async def search_places(client: httpx.AsyncClient, query: str) -> list:
    """Search places using Google Places API (New) Text Search."""
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.displayName,places.formattedAddress,"
            "places.nationalPhoneNumber,places.websiteUri,"
            "places.types,places.rating,places.userRatingCount,"
            "places.priceLevel,places.id"
        )
    }
    payload = {
        "textQuery": query,
        "languageCode": "cs",
        "maxResultCount": 20,
        "locationBias": {
            "circle": {
                "center": {"latitude": 50.0755, "longitude": 14.4378},
                "radius": 15000.0
            }
        }
    }
    try:
        resp = await client.post(url, headers=headers, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("places", [])
    except Exception as e:
        log.error(f"Error searching '{query}': {e}")
        return []


def extract_zone(address: str) -> str:
    """Try to extract Praha zone from address."""
    if not address:
        return "praga"
    address_lower = address.lower()
    # Look for Praha X pattern
    match = re.search(r'praha\s*(\d+)', address_lower)
    if match:
        return f"praga {match.group(1)}"
    # Look for district names
    districts = [
        "vinohrady", "žižkov", "smíchov", "dejvice", "holešovice",
        "nusle", "vršovice", "karlín", "florenc", "josefov", "malá strana",
        "hradčany", "nové město", "staré město", "letná", "bubeneč",
        "strašnice", "michle", "pankrác", "anděl", "andel", "nusle"
    ]
    for d in districts:
        if d in address_lower:
            return d
    return "praga"


def guess_type(types: list, name: str) -> str:
    """Guess venue type from Google types and name."""
    name_lower = name.lower()
    if any(w in name_lower for w in ["vinoteka", "vinarna", "enoteca", "wine bar", "vinný bar"]):
        return "Wine Bar"
    if any(w in name_lower for w in ["enoteca", "vinoteka", "wine shop"]):
        return "Enoteca"
    if "restaurant" in types:
        return "Ristorante"
    if "bar" in types:
        return "Wine Bar"
    if "cafe" in types:
        return "Ristorante"
    return "Ristorante"


def guess_language(address: str) -> str:
    """Default to CZ for Prague."""
    return "CZ"


def append_leads_to_xlsx(xlsx_path: str, new_leads: list) -> str:
    """Append new leads to xlsx and return path of new file."""
    today_str = date.today().strftime("%Y-%m-%d")
    output_path = f"leads_updated_{today_str}.xlsx"

    if os.path.exists(xlsx_path):
        wb = load_workbook(xlsx_path)
    else:
        wb = openpyxl.Workbook()
        wb.active.title = "Leads"
        ws = wb["Leads"]
        headers = [
            "Lead Name", "Type", "Language", "Zone", "Contact Channels",
            "Contact Quality", "Situation", "Status", "Priority",
            "Date Added", "Last Contact Date", "Next Follow-up Date",
            "Follow-up Strategy", "Last Email Sent (text)", "Last Message / Notes",
            "Next Action", "Probability (%)", "email", "telefono"
        ]
        ws.append(headers)

    ws = wb["Leads"]

    # Find last row
    last_row = ws.max_row

    today_excel = (date.today() - date(1899, 12, 30)).days  # Excel serial date

    for lead in new_leads:
        row = [
            lead["name"],           # Lead Name
            lead["type"],           # Type
            lead["language"],       # Language
            lead["zone"],           # Zone
            "Email",                # Contact Channels
            "freddo",               # Contact Quality
            "cold mail",            # Situation
            "Waiting",              # Status
            "Media",                # Priority
            today_excel,            # Date Added
            None,                   # Last Contact Date
            None,                   # Next Follow-up Date
            None,                   # Follow-up Strategy
            None,                   # Last Email Sent
            f"Auto-aggiunto da Lead Scout. Sito: {lead.get('website', '')}. Rating: {lead.get('rating', '')} ({lead.get('reviews', '')} recensioni)",
            "Email follow-up",      # Next Action
            0.2,                    # Probability
            lead.get("email", ""),  # email
            lead.get("phone", ""),  # telefono
        ]
        ws.append(row)

    wb.save(output_path)
    log.info(f"Saved updated xlsx to {output_path}")
    return output_path


async def send_telegram_message(client: httpx.AsyncClient, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    resp = await client.post(url, json=payload, timeout=15)
    resp.raise_for_status()


async def send_telegram_file(client: httpx.AsyncClient, file_path: str, caption: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": (os.path.basename(file_path), f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        data = {"chat_id": TELEGRAM_CHAT_ID, "caption": caption}
        resp = await client.post(url, data=data, files=files, timeout=30)
        resp.raise_for_status()


async def main():
    log.info("🍷 Retrogusto Lead Scout avviato")

    existing_leads = load_existing_leads(XLSX_PATH)
    log.info(f"Lead esistenti nel CRM: {len(existing_leads)}")

    new_leads = []
    seen_place_ids = set()

    async with httpx.AsyncClient() as client:
        for query in SEARCH_QUERIES:
            log.info(f"Ricerca: {query}")
            places = await search_places(client, query)
            log.info(f"  → {len(places)} risultati")

            for place in places:
                place_id = place.get("id", "")
                if place_id in seen_place_ids:
                    continue
                seen_place_ids.add(place_id)

                name = place.get("displayName", {}).get("text", "")
                if not name:
                    continue

                # Skip excluded keywords
                if has_exclude_keyword(name):
                    continue

                # Check not already in CRM
                if normalize_name(name) in existing_leads:
                    log.info(f"  → già nel CRM: {name}")
                    continue

                types = place.get("types", [])

                # Check if wine-relevant
                if not is_wine_relevant(name, types):
                    # Still include restaurants but mark as lower priority
                    if "restaurant" not in types and "bar" not in types:
                        continue

                address = place.get("formattedAddress", "")
                phone = place.get("nationalPhoneNumber", "")
                website = place.get("websiteUri", "")
                rating = place.get("rating", "")
                reviews = place.get("userRatingCount", "")

                lead = {
                    "name": name,
                    "type": guess_type(types, name),
                    "language": guess_language(address),
                    "zone": extract_zone(address),
                    "phone": phone,
                    "website": website,
                    "email": "",
                    "rating": rating,
                    "reviews": reviews,
                    "address": address,
                }
                new_leads.append(lead)
                log.info(f"  ✅ Nuovo lead: {name} ({lead['type']}, {lead['zone']})")

            await asyncio.sleep(1)  # be polite with the API

    log.info(f"Nuovi lead trovati: {len(new_leads)}")

    async with httpx.AsyncClient() as client:
        if not new_leads:
            await send_telegram_message(
                client,
                "🍷 <b>Retrogusto Lead Scout</b>\n\nNessun nuovo lead trovato questa settimana."
            )
            return

        # Build summary message
        today_str = date.today().strftime("%d/%m/%Y")
        lines = [f"🍷 <b>Retrogusto Lead Scout — {today_str}</b>"]
        lines.append(f"Trovati <b>{len(new_leads)} nuovi lead</b> questa settimana:\n")

        for i, lead in enumerate(new_leads, 1):
            line = f"{i}. <b>{lead['name']}</b> — {lead['type']}, {lead['zone']}"
            if lead.get("phone"):
                line += f"\n    📞 {lead['phone']}"
            if lead.get("website"):
                line += f"\n    🌐 {lead['website']}"
            if lead.get("rating"):
                line += f"\n    ⭐ {lead['rating']} ({lead.get('reviews', '?')} recensioni)"
            lines.append(line)

        lines.append("\n📎 File xlsx aggiornato allegato.")
        message = "\n".join(lines)

        # Save xlsx
        output_path = append_leads_to_xlsx(XLSX_PATH, new_leads)

        # Send message
        await send_telegram_message(client, message)

        # Send file
        await send_telegram_file(
            client,
            output_path,
            f"CRM aggiornato — {len(new_leads)} nuovi lead — {today_str}"
        )

    log.info("✅ Completato")


if __name__ == "__main__":
    asyncio.run(main())
