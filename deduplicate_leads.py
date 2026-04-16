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

# Minimum fuzzy name score (0–100) for conditions that require a name match.
FUZZY_NAME_THRESHOLD = 85

# Debug: set to a positive integer to cap leads fetched (e.g. 500). 0 = fetch all.
DEBUG_LEAD_LIMIT = int(os.environ.get("DEBUG_LEAD_LIMIT", 0))

# ── Exclude List ──────────────────────────────────────────────────────────────
# Values listed here are never used as duplicate signals.
#
# email   — substring match (case-insensitive). Use a domain like "@example.com"
#            to exclude every address at that domain, or a full address for one.
# phone   — substring match against normalised digits (e.g. "5550000000").
# name    — substring match (case-insensitive) against the lead's display_name.
# website — substring match (case-insensitive) against any URL on the lead or
#            its contacts. Excludes the ENTIRE lead from all duplicate detection.
#            e.g. "aimarketingservices.com" blocks all leads with that domain.
#
EXCLUDE_LIST = {
    "email": [
        # "@aimarketingservices.com",
        "@wescalecreators.com",
        "@socialprofitmedia.com",
        "@leveraged-creator.com",
        "q@quentininniss.com"
    ],
    "phone": [
        # "5550000000",
    ],
    "name": [
        # "Test Lead",
    ],
    "website": [
        # "aimarketingservices.com",
        "http://www.quentininniss.com",
        "http://www.wescalecreators.com"
    ],
}

OUTPUT_CSV  = "duplicate_report.csv"
OUTPUT_JSON = "duplicate_report.json"

# Fields to fetch per lead — keep small to reduce response size and API time
LEAD_FIELDS    = "id,display_name,contacts,primary_email,primary_phone,date_created,status_label,url"
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


# ── Exclude List Helper ───────────────────────────────────────────────────────

def is_excluded(value: str, field: str) -> bool:
    """
    Return True if value should be ignored based on EXCLUDE_LIST.
    All fields use substring matching (case-insensitive).
    """
    patterns = EXCLUDE_LIST.get(field, [])
    if not patterns:
        return False
    value_lower = value.lower()
    return any(p.lower() in value_lower for p in patterns)


def get_lead_websites(lead: dict) -> set[str]:
    """Collect all URLs from the lead itself and its contacts."""
    urls = set()
    if lead.get("url"):
        urls.add(lead["url"].strip().lower())
    for contact in lead.get("contacts", []):
        if contact.get("url"):
            urls.add(contact["url"].strip().lower())
    return urls - {None, ""}


def filter_website_excluded_leads(leads: list[dict]) -> list[dict]:
    """
    Remove any lead whose URL matches a website exclude pattern.
    Website exclusion applies to the whole lead — not just one signal.
    """
    patterns = EXCLUDE_LIST.get("website", [])
    if not patterns:
        return leads
    kept, removed = [], 0
    for lead in leads:
        websites = get_lead_websites(lead)
        if any(is_excluded(url, "website") for url in websites):
            removed += 1
        else:
            kept.append(lead)
    if removed:
        print(f"  Website exclude list: removed {removed:,} leads", flush=True)
    return kept


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
    return {e for e in emails if e and not is_excluded(e, "email")}


def get_lead_phones(lead: dict) -> set[str]:
    phones = set()
    if lead.get("primary_phone") and lead["primary_phone"].get("phone"):
        phones.add(normalize_phone(lead["primary_phone"]["phone"]))
    for contact in lead.get("contacts", []):
        for p in contact.get("phones", []):
            if p.get("phone"):
                phones.add(normalize_phone(p["phone"]))
    return {p for p in phones if p and not is_excluded(p, "phone")}


# ── Duplicate Detection ───────────────────────────────────────────────────────

def find_duplicate_leads(leads: list[dict]) -> dict:
    """
    Flags a pair of leads as duplicates if ANY of these conditions are met:
      1. Same email  + fuzzy name match  (confidence: avg of 1.0 and name score)
      2. Same phone  + fuzzy name match  (confidence: avg of 0.95 and name score)
      3. Same email  + same phone        (confidence: 1.0 — two hard signals)

    Name matching only runs on candidate pairs that already share an email or
    phone, so there is no O(n²) name comparison across the full lead set.
    """
    lead_map = {lead["id"]: lead for lead in leads}

    # Build email / phone indexes: value → [lead_ids]
    email_index: dict[str, list[str]] = defaultdict(list)
    phone_index: dict[str, list[str]] = defaultdict(list)
    for lead in leads:
        for email in get_lead_emails(lead):
            email_index[email].append(lead["id"])
        for phone in get_lead_phones(lead):
            phone_index[phone].append(lead["id"])

    # Build candidate pair sets: which pairs share an email / phone
    def _pairs_from_index(index: dict) -> dict[tuple, set]:
        out: dict[tuple, set] = defaultdict(set)
        for val, ids in index.items():
            unique = list(dict.fromkeys(ids))
            if len(unique) < 2:
                continue
            for i in range(len(unique)):
                for j in range(i + 1, len(unique)):
                    out[tuple(sorted([unique[i], unique[j]]))].add(val)
        return out

    email_pairs = _pairs_from_index(email_index)
    phone_pairs = _pairs_from_index(phone_index)

    def _name_score(id_a: str, id_b: str) -> float:
        a = normalize_name(lead_map.get(id_a, {}).get("display_name", ""))
        b = normalize_name(lead_map.get(id_b, {}).get("display_name", ""))
        if not a or not b:
            return 0.0
        return fuzz.token_sort_ratio(a, b) / 100

    results: dict[tuple, dict] = {}

    # Condition 3: same email AND same phone (strongest signal — no name needed)
    for key in email_pairs:
        if key in phone_pairs:
            shared_email = next(iter(email_pairs[key]))
            shared_phone = next(iter(phone_pairs[key]))
            results[key] = {
                "matched_signals": "email+phone",
                "match_value":     f"{shared_email} / {shared_phone}",
                "confidence":      1.0,
            }

    threshold = FUZZY_NAME_THRESHOLD / 100

    # Condition 1: same email + fuzzy name match
    for key, emails in email_pairs.items():
        if key in results:
            continue
        id_a, id_b = key
        score = _name_score(id_a, id_b)
        if score >= threshold:
            shared_email = next(iter(emails))
            results[key] = {
                "matched_signals": "email+name",
                "match_value":     f"{shared_email} (name score: {score:.0%})",
                "confidence":      round((1.0 + score) / 2, 3),
            }

    # Condition 2: same phone + fuzzy name match
    for key, phones in phone_pairs.items():
        if key in results:
            continue
        id_a, id_b = key
        score = _name_score(id_a, id_b)
        if score >= threshold:
            shared_phone = next(iter(phones))
            results[key] = {
                "matched_signals": "phone+name",
                "match_value":     f"{shared_phone} (name score: {score:.0%})",
                "confidence":      round((0.95 + score) / 2, 3),
            }

    counts = {"email+phone": 0, "email+name": 0, "phone+name": 0}
    for r in results.values():
        counts[r["matched_signals"]] += 1
    print(f"  email+phone: {counts['email+phone']:,} pairs", flush=True)
    print(f"  email+name:  {counts['email+name']:,} pairs  (name threshold={FUZZY_NAME_THRESHOLD})", flush=True)
    print(f"  phone+name:  {counts['phone+name']:,} pairs  (name threshold={FUZZY_NAME_THRESHOLD})", flush=True)
    return results


# ── Report ────────────────────────────────────────────────────────────────────

def build_report(leads: list[dict], pairs: dict) -> list[dict]:
    lead_map = {lead["id"]: lead for lead in leads}

    rows = []
    for (id_a, id_b), match in pairs.items():
        a = lead_map.get(id_a, {})
        b = lead_map.get(id_b, {})
        rows.append({
            "matched_signals": match["matched_signals"],
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
            "match_value":     match["match_value"],
            "confidence":      match["confidence"],
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

    # Index: lead_id → list of meetings on that lead (upcoming only)
    lead_to_meetings: dict[str, list[dict]] = defaultdict(list)
    statuses_seen: dict[str, int] = defaultdict(int)
    for meeting in meetings:
        status = meeting.get("status") or "none"
        statuses_seen[status] += 1
        if status != "upcoming":
            continue
        lead_id = meeting.get("lead_id")
        if lead_id and lead_id in lead_map:
            lead_to_meetings[lead_id].append(meeting)
    print(f"  Meeting statuses found: { {k: v for k, v in sorted(statuses_seen.items())} }", flush=True)
    print(f"  Kept {sum(len(v) for v in lead_to_meetings.values()):,} upcoming meetings", flush=True)

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

    leads = filter_website_excluded_leads(leads)

    pairs = find_duplicate_leads(leads)

    rows = build_report(leads, pairs)
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
