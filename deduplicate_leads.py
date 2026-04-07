#!/usr/bin/env python3
"""
Close CRM Lead Deduplication Script
====================================
Fetches all leads from Close CRM and identifies likely duplicates based on:
  - Exact email match       (confidence: 1.00)
  - Exact phone match       (confidence: 0.95)
  - Fuzzy name match        (confidence: rapidfuzz score)

Output: duplicate_report.csv + duplicate_report.json

Usage:
  CLOSE_API_KEY=your_key python3 -u deduplicate_leads.py

Runtime: ~5-8 minutes for 63k leads (0.5s throttle, ~630 API calls).
"""

import csv
import json
import os
import re
import time
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone

import requests
from rapidfuzz import fuzz

# ── Config ────────────────────────────────────────────────────────────────────

API_KEY = os.environ["CLOSE_API_KEY"]
FUZZY_NAME_THRESHOLD = 85   # 0–100; pairs at or above this score are flagged
OUTPUT_CSV  = "duplicate_report.csv"
OUTPUT_JSON = "duplicate_report.json"

# Fields to fetch per lead — keep small to reduce response size and API time
LEAD_FIELDS = "id,display_name,contacts,primary_email,primary_phone,date_created,status_label"

# ── HTTP Session ──────────────────────────────────────────────────────────────

session = requests.Session()
session.auth = (API_KEY, "")
session.headers.update({"Content-Type": "application/json"})


def close_get(endpoint, params=None):
    time.sleep(0.5)
    url = f"https://api.close.com/api/v1/{endpoint}"
    for attempt in range(5):
        resp = session.get(url, params=params or {}, timeout=60)
        if resp.status_code == 429:
            wait = float(resp.headers.get("Retry-After", 5))
            print(f"  Rate limited. Waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ── Lead Fetching ─────────────────────────────────────────────────────────────

def fetch_all_leads():
    """
    Paginate ALL leads with minimal fields.
    ~630 API calls for 63k leads; ~5 min at 0.5s throttle.
    """
    leads = []
    skip  = 0
    limit = 100
    page  = 0

    print("Fetching all leads (this takes ~5-8 minutes)...", flush=True)
    while True:
        page += 1
        data  = close_get("lead/", {"_fields": LEAD_FIELDS, "_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        leads.extend(batch)
        print(f"  Page {page}: +{len(batch)} leads  (running total: {len(leads)})", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    print(f"Done. Total leads fetched: {len(leads)}\n", flush=True)
    return leads


# ── Normalisation Helpers ─────────────────────────────────────────────────────

def normalize_email(email: str) -> str | None:
    return email.strip().lower() if email else None


def normalize_phone(phone: str) -> str | None:
    """Strip non-digits; collapse 11-digit US numbers (1XXXXXXXXXX → XXXXXXXXXX)."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", phone)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    return digits if len(digits) >= 7 else None


def normalize_name(name: str) -> str:
    """Lowercase, NFD-normalise, strip punctuation, collapse whitespace."""
    if not name:
        return ""
    name = unicodedata.normalize("NFKD", name)
    name = name.lower()
    name = re.sub(r"[^\w\s]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def get_lead_emails(lead: dict) -> set[str]:
    emails = set()
    if lead.get("primary_email") and lead["primary_email"].get("email"):
        emails.add(normalize_email(lead["primary_email"]["email"]))
    for contact in lead.get("contacts", []):
        for e in contact.get("emails", []):
            if e.get("email"):
                emails.add(normalize_email(e["email"]))
    return emails - {None}


def get_lead_phones(lead: dict) -> set[str]:
    phones = set()
    if lead.get("primary_phone") and lead["primary_phone"].get("phone"):
        phones.add(normalize_phone(lead["primary_phone"]["phone"]))
    for contact in lead.get("contacts", []):
        for p in contact.get("phones", []):
            if p.get("phone"):
                phones.add(normalize_phone(p["phone"]))
    return phones - {None}


# ── Duplicate Detection ───────────────────────────────────────────────────────

def find_email_duplicates(leads: list[dict]) -> dict:
    """Return pairs of lead IDs sharing an exact email address."""
    index = defaultdict(list)
    for lead in leads:
        for email in get_lead_emails(lead):
            index[email].append(lead["id"])

    pairs = {}
    for email, ids in index.items():
        if len(ids) < 2:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                key = tuple(sorted([ids[i], ids[j]]))
                pairs[key] = {"match_type": "email_exact", "match_value": email, "confidence": 1.0}
    return pairs


def find_phone_duplicates(leads: list[dict]) -> dict:
    """Return pairs of lead IDs sharing an exact normalised phone number."""
    index = defaultdict(list)
    for lead in leads:
        for phone in get_lead_phones(lead):
            index[phone].append(lead["id"])

    pairs = {}
    for phone, ids in index.items():
        if len(ids) < 2:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                key = tuple(sorted([ids[i], ids[j]]))
                if key not in pairs or pairs[key]["confidence"] < 0.95:
                    pairs[key] = {"match_type": "phone_exact", "match_value": phone, "confidence": 0.95}
    return pairs


def find_name_duplicates(leads: list[dict], threshold: int = FUZZY_NAME_THRESHOLD) -> dict:
    """
    Fuzzy name matching with first-word blocking to avoid O(n²) comparisons.

    Leads are grouped by the first word of their normalised display_name.
    Fuzzy comparison only happens within each group, making this practical
    at scale while still catching the most common naming variants.
    """
    blocks: dict[str, list] = defaultdict(list)
    for lead in leads:
        name = normalize_name(lead.get("display_name", ""))
        if not name:
            continue
        words = name.split()
        first_word = words[0] if words else ""
        if len(first_word) < 3:   # skip tokens like "a", "mr", "dr"
            continue
        blocks[first_word].append((lead["id"], name))

    pairs = {}
    compared = 0
    for block_leads in blocks.values():
        if len(block_leads) < 2:
            continue
        for i in range(len(block_leads)):
            for j in range(i + 1, len(block_leads)):
                id_a, name_a = block_leads[i]
                id_b, name_b = block_leads[j]
                compared += 1
                score = fuzz.token_sort_ratio(name_a, name_b)
                if score >= threshold:
                    key = tuple(sorted([id_a, id_b]))
                    conf = round(score / 100, 3)
                    if key not in pairs or pairs[key]["confidence"] < conf:
                        pairs[key] = {
                            "match_type": "name_fuzzy",
                            "match_value": f'"{name_a}" ≈ "{name_b}"',
                            "confidence": conf,
                        }

    print(f"  Name fuzzy: {compared:,} comparisons across {sum(1 for b in blocks.values() if len(b) >= 2)} blocks",
          flush=True)
    return pairs


# ── Report ────────────────────────────────────────────────────────────────────

def build_report(leads, email_pairs, phone_pairs, name_pairs) -> list[dict]:
    lead_map = {lead["id"]: lead for lead in leads}

    # Merge all pair dicts; for any key seen multiple times keep the highest-confidence entry
    all_pairs: dict = {}
    for source in [name_pairs, phone_pairs, email_pairs]:   # ascending confidence order
        for key, match in source.items():
            if key not in all_pairs or all_pairs[key]["confidence"] < match["confidence"]:
                all_pairs[key] = match

    rows = []
    for (id_a, id_b), match in all_pairs.items():
        a = lead_map.get(id_a, {})
        b = lead_map.get(id_b, {})
        rows.append({
            "lead_id_1":      id_a,
            "lead_name_1":    a.get("display_name", ""),
            "lead_status_1":  a.get("status_label", ""),
            "lead_created_1": (a.get("date_created") or "")[:10],
            "lead_emails_1":  "|".join(sorted(get_lead_emails(a))),
            "lead_phones_1":  "|".join(sorted(get_lead_phones(a))),
            "lead_id_2":      id_b,
            "lead_name_2":    b.get("display_name", ""),
            "lead_status_2":  b.get("status_label", ""),
            "lead_created_2": (b.get("date_created") or "")[:10],
            "lead_emails_2":  "|".join(sorted(get_lead_emails(b))),
            "lead_phones_2":  "|".join(sorted(get_lead_phones(b))),
            "match_type":     match["match_type"],
            "match_value":    match["match_value"],
            "confidence":     match["confidence"],
        })

    rows.sort(key=lambda r: r["confidence"], reverse=True)
    return rows


def write_csv(rows: list[dict], path: str) -> None:
    if not rows:
        print("No duplicate pairs found.", flush=True)
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"CSV report written → {path}  ({len(rows)} pairs)", flush=True)


def write_json(rows: list[dict], path: str) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_pairs":  len(rows),
        "pairs":        rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"JSON report written → {path}", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    leads = fetch_all_leads()

    print("Building duplicate indexes...", flush=True)

    email_pairs = find_email_duplicates(leads)
    print(f"  Email exact:  {len(email_pairs):,} pairs", flush=True)

    phone_pairs = find_phone_duplicates(leads)
    print(f"  Phone exact:  {len(phone_pairs):,} pairs", flush=True)

    print("  Running fuzzy name matching...", flush=True)
    name_pairs = find_name_duplicates(leads)
    print(f"  Name fuzzy:   {len(name_pairs):,} pairs  (threshold={FUZZY_NAME_THRESHOLD})", flush=True)

    rows = build_report(leads, email_pairs, phone_pairs, name_pairs)
    print(f"\nTotal unique duplicate pairs: {len(rows):,}", flush=True)

    write_csv(rows, OUTPUT_CSV)
    write_json(rows, OUTPUT_JSON)


if __name__ == "__main__":
    main()
