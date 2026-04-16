#!/usr/bin/env python3
"""
Close CRM Field Explorer
=========================
Standalone script for inspecting the raw data structure of leads and meetings.
Nothing is written back to Close. No connection to deduplicate_leads.py.

Usage:
    CLOSE_API_KEY=your_key python fetch_fields.py

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
QUICK CONFIG  ← change these two lines to switch modes
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import contextlib
import csv
import json
import os
import re
import time
from collections import defaultdict

import requests

# ── Quick Config ──────────────────────────────────────────────────────────────

LEAD_LIMIT = 200
# How many leads to fetch. Set to 0 to fetch ALL leads (slow — 63k leads ~8 min).

OUTPUT_MODE = "pretty"
# Pick one:
#   "pretty"   — print the first fetched lead as formatted JSON. Best for seeing
#                every field name and its raw value on one real record.
#   "fields"   — scan all fetched leads and print every unique top-level field
#                name found, with its type and a sample value.
#   "summary"  — richer table: field | types seen | % of leads that have it | sample value
#   "csv"      — write all leads to leads_export.csv (top-level fields only)

ALSO_SHOW_MEETING = True
# If True, fetch one meeting record and display it the same way as the leads,
# so you can compare lead fields vs meeting fields side by side.

INCLUDE_LEAD_MEETINGS = False
# If True, the "pretty" mode fetches and prints all meetings for each lead.
# Set to False to display only the lead record itself.

SEARCH = "Quentin"
# Search for a specific lead instead of paginating from the top.
# Set to a lead ID  → exact lookup:    SEARCH = "lead_abc123"
# Set to a name     → name search:     SEARCH = "John Smith"
# When SEARCH is set, LEAD_LIMIT is ignored.

# ── API Setup ─────────────────────────────────────────────────────────────────

API_KEY = os.environ["CLOSE_API_KEY"]

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
            print(f"  Rate limited — waiting {wait}s...", flush=True)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()


# ── Fetching ──────────────────────────────────────────────────────────────────

def fetch_leads(limit: int) -> list[dict]:
    """
    Fetch leads with NO _fields filter so the full raw object is returned.
    limit=0 fetches all leads.
    """
    leads = []
    skip      = 0
    page_size = 100
    page      = 0

    label = f"first {limit}" if limit else "ALL"
    print(f"Fetching {label} leads (full objects, no field filter)...", flush=True)

    while True:
        page += 1
        fetch_n = min(page_size, limit - len(leads)) if limit else page_size
        data    = close_get("lead/", {"_skip": skip, "_limit": fetch_n})
        batch   = data.get("data", [])
        leads.extend(batch)
        print(f"  Page {page}: +{len(batch)}  (total: {len(leads)})", flush=True)

        if not data.get("has_more"):
            break
        if limit and len(leads) >= limit:
            break
        skip += page_size

    print(f"Done. {len(leads)} leads fetched.\n", flush=True)
    return leads


def fetch_lead_by_id(lead_id: str) -> list[dict]:
    """Fetch a single lead directly by its ID (e.g. 'lead_abc123')."""
    print(f"Fetching lead by ID: {lead_id}...", flush=True)
    data = close_get(f"lead/{lead_id}")
    print(f"  Found: {data.get('display_name', '(no name)')}\n", flush=True)
    return [data]


def fetch_leads_by_name(name: str) -> list[dict]:
    """
    Search for leads whose name contains the given string.
    Close's list endpoint accepts a 'query' parameter that searches across
    lead name, contact name, email, and phone. Results are returned in
    relevance order. All matches are returned (up to 100 — enough for a name search).
    """
    print(f"Searching for leads matching: '{name}'...", flush=True)
    data  = close_get("lead/", {"query": name, "_limit": 100})
    leads = data.get("data", [])
    print(f"  {len(leads)} result(s) found.\n", flush=True)
    if not leads:
        print(f"  No leads matched '{name}'.")
    return leads


def fetch_one_meeting() -> dict | None:
    """Fetch a single meeting record with no field filter."""
    print("Fetching one meeting record...", flush=True)
    data = close_get("activity/meeting/", {"_limit": 1})
    meetings = data.get("data", [])
    if not meetings:
        print("  No meetings found.\n", flush=True)
        return None
    print(f"  Got meeting id: {meetings[0].get('id')}\n", flush=True)
    return meetings[0]


# ── Output Modes ─────────────────────────────────────────────────────────────

def fetch_all_meetings_for_lead(lead_id: str) -> list[dict]:
    """Paginate all meetings belonging to a single lead."""
    meetings = []
    skip = 0
    while True:
        data  = close_get("activity/meeting/", {"lead_id": lead_id, "_skip": skip, "_limit": 100})
        batch = data.get("data", [])
        meetings.extend(batch)
        if not data.get("has_more"):
            break
        skip += 100
    return meetings


def show_pretty(records: list[dict], label: str) -> None:
    """
    Print each record as formatted JSON.
    When displaying leads, also fetches and prints all meetings for each lead.
    """
    if not records:
        print(f"No {label} records to display.")
        return

    for i, record in enumerate(records):
        print(f"\n{'━'*60}")
        print(f"  {label.upper()} {i + 1} of {len(records)}")
        print(f"{'━'*60}\n")
        print(json.dumps(record, indent=2, ensure_ascii=False, default=str))

        if label == "lead" and INCLUDE_LEAD_MEETINGS:
            lead_id = record.get("id")
            print(f"\n  ── Meetings for {record.get('display_name', lead_id)} ──", flush=True)
            meetings = fetch_all_meetings_for_lead(lead_id)
            if not meetings:
                print("  (no meetings found)")
            else:
                print(f"  {len(meetings)} meeting(s):\n")
                for j, mtg in enumerate(meetings):
                    print(f"  [{j + 1}] {json.dumps(mtg, indent=6, ensure_ascii=False, default=str)}")


def show_fields(records: list[dict], label: str) -> None:
    """
    List every unique top-level field found across all records,
    with its Python type and a sample value from the first record that has it.
    """
    field_samples: dict[str, tuple] = {}   # field_name -> (type_str, sample_value)
    for record in records:
        for key, val in record.items():
            if key not in field_samples:
                field_samples[key] = (type(val).__name__, val)

    print(f"\n{'━'*60}")
    print(f"  {label.upper()} — unique field names ({len(field_samples)} found across {len(records)} records)")
    print(f"{'━'*60}\n")
    print(f"  {'FIELD':<35} {'TYPE':<12} SAMPLE VALUE")
    print(f"  {'-'*35} {'-'*12} {'-'*30}")
    for field, (type_str, sample) in sorted(field_samples.items()):
        sample_str = repr(sample)
        if len(sample_str) > 60:
            sample_str = sample_str[:57] + "..."
        print(f"  {field:<35} {type_str:<12} {sample_str}")


def show_summary(records: list[dict], label: str) -> None:
    """
    Richer table: field | types seen | % populated | sample value.
    Useful for understanding data quality — which fields are actually filled in.
    """
    field_types:   dict[str, set]   = defaultdict(set)
    field_count:   dict[str, int]   = defaultdict(int)
    field_samples: dict[str, object] = {}
    total = len(records)

    for record in records:
        for key, val in record.items():
            field_types[key].add(type(val).__name__)
            if val not in (None, "", [], {}):
                field_count[key] += 1
                if key not in field_samples:
                    field_samples[key] = val

    print(f"\n{'━'*60}")
    print(f"  {label.upper()} — field summary ({len(field_types)} fields, {total} records)")
    print(f"{'━'*60}\n")
    print(f"  {'FIELD':<35} {'TYPES':<18} {'% SET':<8} SAMPLE VALUE")
    print(f"  {'-'*35} {'-'*18} {'-'*8} {'-'*30}")

    for field in sorted(field_types.keys()):
        types_str  = "|".join(sorted(field_types[field]))
        pct        = f"{100 * field_count[field] / total:.0f}%" if total else "n/a"
        sample     = repr(field_samples.get(field, ""))
        if len(sample) > 50:
            sample = sample[:47] + "..."
        print(f"  {field:<35} {types_str:<18} {pct:<8} {sample}")


def show_csv(records: list[dict], label: str, filename: str = "leads_export.csv") -> None:
    """
    Write all records to a CSV. Nested objects (lists/dicts) are JSON-encoded
    into a single cell so the file stays flat and openable in Excel.
    """
    if not records:
        print(f"No {label} records to write.")
        return

    # Collect all keys in order of first appearance
    all_keys: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record.keys():
            if key not in seen:
                all_keys.append(key)
                seen.add(key)

    def flatten(val) -> str:
        if isinstance(val, (dict, list)):
            return json.dumps(val, ensure_ascii=False)
        return str(val) if val is not None else ""

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({k: flatten(record.get(k)) for k in all_keys})

    print(f"\n  CSV written → {filename}  ({len(records)} rows, {len(all_keys)} columns)")


# ── Dispatch ──────────────────────────────────────────────────────────────────

MODES = {
    "pretty":  show_pretty,
    "fields":  show_fields,
    "summary": show_summary,
    "csv":     show_csv,
}


def run(records: list[dict], label: str) -> None:
    fn = MODES.get(OUTPUT_MODE)
    if fn is None:
        print(f"Unknown OUTPUT_MODE '{OUTPUT_MODE}'. Choose from: {list(MODES.keys())}")
        return
    if OUTPUT_MODE == "csv":
        filename = f"{label.lower().replace(' ', '_')}_export.csv"
        fn(records, label, filename)
    else:
        fn(records, label)


# ── Main ──────────────────────────────────────────────────────────────────────

def make_output_filename() -> str:
    """Build the output filename from SEARCH, or 'leads' if SEARCH is not set."""
    label = SEARCH if SEARCH else "leads"
    safe  = re.sub(r"[^\w\-]", "_", label)   # replace spaces / special chars with _
    return f"{safe}-fields.txt"


def main():
    if SEARCH:
        if SEARCH.startswith("lead_"):
            leads = fetch_lead_by_id(SEARCH)
        else:
            leads = fetch_leads_by_name(SEARCH)
    else:
        leads = fetch_leads(LEAD_LIMIT)

    out_path = make_output_filename()
    with open(out_path, "w", encoding="utf-8") as f:
        with contextlib.redirect_stdout(f):
            run(leads, "lead")
            if ALSO_SHOW_MEETING:
                meeting = fetch_one_meeting()
                if meeting:
                    run([meeting], "meeting")

    print(f"Output written → {out_path}", flush=True)


if __name__ == "__main__":
    main()
