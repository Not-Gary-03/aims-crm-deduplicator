#!/usr/bin/env python3
"""
Close CRM Lead Deduplication Script
====================================
Fetches all leads from Close CRM and identifies likely duplicates based on:
  - Exact email match       (confidence: 1.00)
  - Exact phone match       (confidence: 0.95)
  - Fuzzy name match        (disabled by default — too many false positives)

Output: duplicate_report.csv + duplicate_report.json

Usage:
  CLOSE_API_KEY=your_key python deduplicate_leads.py

Debug (fast — limits leads fetched):
  CLOSE_API_KEY=your_key DEBUG_LEAD_LIMIT=500 python deduplicate_leads.py

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

# Set to True to re-enable fuzzy name matching (produces many false positives — use with caution)
ENABLE_FUZZY_NAME    = False
FUZZY_NAME_THRESHOLD = 85   # 0–100; only relevant when ENABLE_FUZZY_NAME = True

# Debug: set to a positive integer to cap leads fetched (e.g. 500). 0 = fetch all.
DEBUG_LEAD_LIMIT = int(os.environ.get("DEBUG_LEAD_LIMIT", 10000))

OUTPUT_CSV  = "duplicate_report.csv"
OUTPUT_JSON = "duplicate_report.json"

# Fields to fetch per lead — keep small to reduce response size and API time
LEAD_FIELDS    = "id,display_name,contacts,primary_email,primary_phone,date_created,status_label"
MEETING_FIELDS = "id,lead_id,contact_id,assigned_to,date_start,date_end,status,title"

MEETING_DUPE_CSV  = "meeting_dupe_report.csv"
MEETING_DUPE_JSON = "meeting_dupe_report.json"

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

    Set DEBUG_LEAD_LIMIT to a positive integer to cap the fetch for fast test runs.
    """
    leads = []
    skip  = 0
    limit = 100
    page  = 0

    if DEBUG_LEAD_LIMIT:
        print(f"DEBUG: fetching first {DEBUG_LEAD_LIMIT} leads only.", flush=True)
    else:
        print("Fetching all leads (this takes ~5-8 minutes)...", flush=True)

    while True:
        page += 1
        # In debug mode shrink the page size to exactly what's needed so we don't over-fetch
        page_limit = min(limit, DEBUG_LEAD_LIMIT - len(leads)) if DEBUG_LEAD_LIMIT else limit
        data  = close_get("lead/", {"_fields": LEAD_FIELDS, "_skip": skip, "_limit": page_limit})
        batch = data.get("data", [])
        leads.extend(batch)
        print(f"  Page {page}: +{len(batch)} leads  (running total: {len(leads)})", flush=True)

        if not data.get("has_more"):
            break
        if DEBUG_LEAD_LIMIT and len(leads) >= DEBUG_LEAD_LIMIT:
            break
        skip += limit

    print(f"Done. Total leads fetched: {len(leads)}\n", flush=True)
    return leads


def fetch_all_meetings():
    """
    Paginate ALL meetings from the activity/meeting endpoint.
    ~120 API calls for ~12k meetings; ~1 min at 0.5s throttle.

    NOTE: Close silently ignores date filter params on this endpoint.
    All date filtering must happen in Python after fetching.
    """
    meetings = []
    skip  = 0
    limit = 100
    page  = 0

    print("Fetching all meetings...", flush=True)
    while True:
        page += 1
        data  = close_get("activity/meeting/", {"_fields": MEETING_FIELDS, "_skip": skip, "_limit": limit})
        batch = data.get("data", [])
        meetings.extend(batch)
        print(f"  Page {page}: +{len(batch)} meetings  (running total: {len(meetings)})", flush=True)

        if not data.get("has_more"):
            break
        skip += limit

    print(f"Done. Total meetings fetched: {len(meetings)}\n", flush=True)
    return meetings


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

    # Collect ALL signals that fired for each pair (a pair can match on email + phone + name)
    # Structure: key -> {"email": match, "phone": match, "name": match}
    all_signals: dict[tuple, dict] = {}
    for signal_key, source in [("email", email_pairs), ("phone", phone_pairs), ("name", name_pairs)]:
        for key, match in source.items():
            all_signals.setdefault(key, {})[signal_key] = match

    rows = []
    for (id_a, id_b), signals in all_signals.items():
        a = lead_map.get(id_a, {})
        b = lead_map.get(id_b, {})

        # First column: which signals matched, in priority order
        matched = "+".join(k for k in ("email", "phone", "name") if k in signals)

        # Best match is used for the detail columns (email > phone > name)
        best = signals.get("email") or signals.get("phone") or signals.get("name")

        rows.append({
            "matched_signals": matched,
            "lead_id_1":       id_a,
            "lead_name_1":     a.get("display_name", ""),
            "lead_status_1":   a.get("status_label", ""),
            "lead_created_1":  (a.get("date_created") or "")[:10],
            "lead_emails_1":   "|".join(sorted(get_lead_emails(a))),
            "lead_phones_1":   "|".join(sorted(get_lead_phones(a))),
            "lead_id_2":       id_b,
            "lead_name_2":     b.get("display_name", ""),
            "lead_status_2":   b.get("status_label", ""),
            "lead_created_2":  (b.get("date_created") or "")[:10],
            "lead_emails_2":   "|".join(sorted(get_lead_emails(b))),
            "lead_phones_2":   "|".join(sorted(get_lead_phones(b))),
            "best_match_type":  best["match_type"],
            "best_match_value": best["match_value"],
            "confidence":       best["confidence"],
        })

    # Sort: multi-signal matches first, then by confidence descending
    rows.sort(key=lambda r: (r["matched_signals"].count("+"), r["confidence"]), reverse=True)
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


# ── Meeting Duplicate Detection ──────────────────────────────────────────────

def find_duplicate_meetings(leads: list[dict], meetings: list[dict]) -> list[dict]:
    """
    Detect two categories of duplicate meetings:

    1. same_lead   — one lead has 2+ meetings scheduled (double-booked under the same record)
    2. cross_lead  — two different leads share an email or phone AND each has a meeting
                     (same person booked under two separate lead records)

    Returns a flat list of duplicate-pair dicts ready for CSV/JSON output.
    """
    # Build lead lookup
    lead_map = {lead["id"]: lead for lead in leads}

    # Build email → lead_ids and phone → lead_ids indexes from lead contact data
    email_to_leads: dict[str, list[str]] = defaultdict(list)
    phone_to_leads: dict[str, list[str]] = defaultdict(list)
    for lead in leads:
        for email in get_lead_emails(lead):
            email_to_leads[email].append(lead["id"])
        for phone in get_lead_phones(lead):
            phone_to_leads[phone].append(lead["id"])

    # Index: lead_id → list of meetings on that lead
    lead_to_meetings: dict[str, list[dict]] = defaultdict(list)
    for meeting in meetings:
        lead_id = meeting.get("lead_id")
        if lead_id:
            lead_to_meetings[lead_id].append(meeting)

    rows = []

    # ── Category 1: same lead, multiple meetings ──────────────────────────────
    for lead_id, mtgs in lead_to_meetings.items():
        if len(mtgs) < 2:
            continue
        lead = lead_map.get(lead_id, {})
        # Produce a row for every pair of meetings on this lead
        for i in range(len(mtgs)):
            for j in range(i + 1, len(mtgs)):
                a, b = mtgs[i], mtgs[j]
                rows.append({
                    "duplicate_type":   "same_lead",
                    "shared_signal":    f"lead_id:{lead_id}",
                    "lead_id_1":        lead_id,
                    "lead_name_1":      lead.get("display_name", ""),
                    "meeting_id_1":     a.get("id", ""),
                    "meeting_date_1":   (a.get("date_start") or "")[:10],
                    "meeting_status_1": a.get("status", ""),
                    "meeting_title_1":  a.get("title", ""),
                    "lead_id_2":        lead_id,
                    "lead_name_2":      lead.get("display_name", ""),
                    "meeting_id_2":     b.get("id", ""),
                    "meeting_date_2":   (b.get("date_start") or "")[:10],
                    "meeting_status_2": b.get("status", ""),
                    "meeting_title_2":  b.get("title", ""),
                })

    # ── Category 2: different leads sharing contact info, each with meetings ──
    seen_cross: set[tuple] = set()

    def _check_cross(shared_signal: str, lead_ids: list[str]) -> None:
        """For a group of leads sharing a signal, pair up any that have meetings."""
        with_meetings = [lid for lid in lead_ids if lead_to_meetings.get(lid)]
        for i in range(len(with_meetings)):
            for j in range(i + 1, len(with_meetings)):
                id_a, id_b = with_meetings[i], with_meetings[j]
                if id_a == id_b:
                    continue
                key = tuple(sorted([id_a, id_b]))
                if key in seen_cross:
                    continue
                seen_cross.add(key)
                lead_a = lead_map.get(id_a, {})
                lead_b = lead_map.get(id_b, {})
                # Include all meeting pairs across the two leads
                for ma in lead_to_meetings[id_a]:
                    for mb in lead_to_meetings[id_b]:
                        rows.append({
                            "duplicate_type":   "cross_lead",
                            "shared_signal":    shared_signal,
                            "lead_id_1":        id_a,
                            "lead_name_1":      lead_a.get("display_name", ""),
                            "meeting_id_1":     ma.get("id", ""),
                            "meeting_date_1":   (ma.get("date_start") or "")[:10],
                            "meeting_status_1": ma.get("status", ""),
                            "meeting_title_1":  ma.get("title", ""),
                            "lead_id_2":        id_b,
                            "lead_name_2":      lead_b.get("display_name", ""),
                            "meeting_id_2":     mb.get("id", ""),
                            "meeting_date_2":   (mb.get("date_start") or "")[:10],
                            "meeting_status_2": mb.get("status", ""),
                            "meeting_title_2":  mb.get("title", ""),
                        })

    for email, lead_ids in email_to_leads.items():
        if len(set(lead_ids)) > 1:
            _check_cross(f"email:{email}", lead_ids)

    for phone, lead_ids in phone_to_leads.items():
        if len(set(lead_ids)) > 1:
            _check_cross(f"phone:{phone}", lead_ids)

    same  = sum(1 for r in rows if r["duplicate_type"] == "same_lead")
    cross = sum(1 for r in rows if r["duplicate_type"] == "cross_lead")
    print(f"  Meeting duplicates — same_lead: {same:,}  cross_lead: {cross:,}", flush=True)
    return rows


def write_meeting_csv(rows: list[dict], path: str) -> None:
    if not rows:
        print("No duplicate meetings found.", flush=True)
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"Meeting CSV written → {path}  ({len(rows)} pairs)", flush=True)


def write_meeting_json(rows: list[dict], path: str) -> None:
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_pairs":  len(rows),
        "pairs":        rows,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
    print(f"Meeting JSON written → {path}", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    leads = fetch_all_leads()

    print("Building duplicate indexes...", flush=True)

    email_pairs = find_email_duplicates(leads)
    print(f"  Email exact:  {len(email_pairs):,} pairs", flush=True)

    phone_pairs = find_phone_duplicates(leads)
    print(f"  Phone exact:  {len(phone_pairs):,} pairs", flush=True)

    if ENABLE_FUZZY_NAME:
        print("  Running fuzzy name matching...", flush=True)
        name_pairs = find_name_duplicates(leads)
        print(f"  Name fuzzy:   {len(name_pairs):,} pairs  (threshold={FUZZY_NAME_THRESHOLD})", flush=True)
    else:
        name_pairs = {}
        print("  Name fuzzy:   skipped (ENABLE_FUZZY_NAME=False)", flush=True)

    rows = build_report(leads, email_pairs, phone_pairs, name_pairs)
    print(f"\nTotal unique duplicate pairs: {len(rows):,}", flush=True)

    write_csv(rows, OUTPUT_CSV)
    write_json(rows, OUTPUT_JSON)

    # ── Meeting duplicate detection ───────────────────────────────────────────
    print("\n── Meeting duplicates ───────────────────────────────────", flush=True)
    meetings = fetch_all_meetings()
    meeting_rows = find_duplicate_meetings(leads, meetings)
    write_meeting_csv(meeting_rows, MEETING_DUPE_CSV)
    write_meeting_json(meeting_rows, MEETING_DUPE_JSON)


if __name__ == "__main__":
    main()
