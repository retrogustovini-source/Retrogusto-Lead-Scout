import subprocess
import sys

# Auto-install dependencies at startup
subprocess.check_call([sys.executable, "-m", "pip", "install", "httpx==0.27.0", "openpyxl==3.1.2", "-q"])

import os
import re
import csv
import asyncio
import logging
from datetime import datetime, date
import httpx
import openpyxl
from openpyxl import load_workbook

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# --- CONFIG ---
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]
XLSX_PATH = os.environ.get("XLSX_PATH", "leads.xlsx")

SEARCH_QUERIES = [
    "wine bar Praha",
    "vinoteka Praha",
    "enoteca Praha",
    "ristorante italiano Praha",
    "bistro vino Praha",
    "vinarna Praha",
]

WINE_KEYWORDS = [
    "wine", "vino", "vinoteka", "vinarna", "enoteca", "wein", "vin ",
    "vinice", "sommelier", "cava", "cellar", "sklep", "degustace",
    "bacchus", "vinny", "vinné", "enolog"
]

EXCLUDE_KEYWORDS = [
    "supermarket", "tesco", "albert", "billa", "lidl", "penny",
    "kaufland", "spar", "fast food", "mcdonald", "kfc", "subway",
    "burger", "kebab", "karaoke"
]


def normalize_name(name: str) -> str:
    return re.sub(r'\s+', ' ', name.lower().strip())


def load_existing_leads(xlsx_path: str) -> set:
    existing = set()
    if not os.path.exists(xlsx_path):
        log.warning(f"XLSX not found at {xlsx_path}")
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
    url = "https://places.googleapis.com/v1/places:searchText"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": (
            "places.displayName,places.formattedAddress,"
            "places.nationalPhoneNumber,places.websiteUri,"
            "places.types,places.rating,places.userRatingCount,places.id"
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
        return resp.json().get("places", [])
    except Exception as e:
        log.error(f"Error searching '{query}': {e}")
        return []


def extract_zone(address: str) -> str:
    if not address:
        return "praga"
    address_lower = address.lower()
    match = re.search(r'praha\s*(\d+)', address_lower)
    if match:
        return f"praga {match.group(1)}"
    districts = [
        "vinohrady", "žižkov", "smíchov", "dejvice", "holešovice",
        "nusle", "vršovice", "karlín", "florenc", "josefov",
        "letná", "bubeneč", "andel", "anděl", "pankrác"
    ]
    for d in districts:
        if d in address_lower:
            return d
    return "praga"


def guess_type(types: list, name: str) -> str:
    name_lower = name.lower()
    if any(w in name_lower for w in ["vinoteka", "vinarna", "enoteca", "wine bar", "vinný bar"]):
        return "Wine Bar"
    if any(w in name_lower for w in ["enoteca", "vinoteka", "wine shop"]):
        return "Enoteca"
    if "restaurant" in types:
        return "Ristorante"
    if "bar" in types:
        return "Wine Bar"
    return "Ristorante"


def save_leads_to_csv(new_leads: list) -> str:
    today_str = date.today().strftime("%Y-%m-%d")
    output_path = f"nuovi_lead_{today_str}.csv"
    today_display = date.today().strftime("%d/%m/%Y")

    headers = [
        "Lead Name", "Type", "Language", "Zone", "Contact Channels",
        "Contact Quality", "Situation", "Status", "Priority",
        "Date Added", "Next Action", "Probability (%)",
        "email", "telefono", "website", "rating", "recensioni"
    ]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for lead in new_leads:
            writer.writerow([
                lead["name"],
                lead["type"],
                "CZ",
                lead["zone"],
                "Email",
                "freddo",
                "cold mail",
                "Waiting",
                "Media",
                today_display,
                "Email follow-up",
                0.2,
                lead.get("email", ""),
                lead.get("phone", ""),
                lead.get("website", ""),
                lead.get("rating", ""),
                lead.get("reviews", ""),
            ])

    log.info(f"Saved CSV to {output_path}")
    return output_path


async def send_telegram_message(client: httpx.AsyncClient, text: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    resp = await client.post(url, json=payload, timeout=15)
    resp.raise_for_status()


async def send_telegram_file(client: httpx.AsyncClient, file_path: str, caption: str):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendDocument"
    with open(file_path, "rb") as f:
        files = {"document": (os.path.basename(file_path), f, "text/csv")}
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
                if has_exclude_keyword(name):
                    continue
                if normalize_name(name) in existing_leads:
                    log.info(f"  → già nel CRM: {name}")
                    continue

                types = place.get("types", [])
                if not is_wine_relevant(name, types):
                    if "restaurant" not in types and "bar" not in types:
                        continue

                lead = {
                    "name": name,
                    "type": guess_type(types, name),
                    "zone": extract_zone(place.get("formattedAddress", "")),
                    "phone": place.get("nationalPhoneNumber", ""),
                    "website": place.get("websiteUri", ""),
                    "email": "",
                    "rating": place.get("rating", ""),
                    "reviews": place.get("userRatingCount", ""),
                }
                new_leads.append(lead)
                log.info(f"  ✅ Nuovo lead: {name} ({lead['type']}, {lead['zone']})")

            await asyncio.sleep(1)

    log.info(f"Nuovi lead trovati: {len(new_leads)}")

    async with httpx.AsyncClient() as client:
        if not new_leads:
            await send_telegram_message(
                client,
                "🍷 <b>Retrogusto Lead Scout</b>\n\nNessun nuovo lead trovato questa settimana."
            )
            return

        today_str = date.today().strftime("%d/%m/%Y")

        # Build lead lines
        lead_lines = []
        for i, lead in enumerate(new_leads, 1):
            line = f"{i}. <b>{lead['name']}</b> — {lead['type']}, {lead['zone']}"
            if lead.get("phone"):
                line += f"\n    📞 {lead['phone']}"
            if lead.get("website"):
                line += f"\n    🌐 {lead['website']}"
            if lead.get("rating"):
                line += f"\n    ⭐ {lead['rating']} ({lead.get('reviews', '?')} rec.)"
            lead_lines.append(line)

        # Split into chunks under 4000 chars
        header = f"🍷 <b>Retrogusto Lead Scout — {today_str}</b>\nTrovati <b>{len(new_leads)} nuovi lead</b>:\n"
        chunks = []
        current = header
        for line in lead_lines:
            if len(current) + len(line) + 2 > 4000:
                chunks.append(current)
                current = line + "\n"
            else:
                current += line + "\n"
        if current.strip():
            chunks.append(current)

        # Save CSV
        csv_path = save_leads_to_csv(new_leads)

        # Send messages
        for idx, chunk in enumerate(chunks):
            if idx == len(chunks) - 1:
                chunk += "\n\n📎 CSV con i nuovi lead allegato."
            await send_telegram_message(client, chunk)
            await asyncio.sleep(0.5)

        # Send CSV
        await send_telegram_file(
            client,
            csv_path,
            f"Nuovi lead Retrogusto — {len(new_leads)} — {today_str}"
        )

    log.info("✅ Completato")


if __name__ == "__main__":
    asyncio.run(main())
