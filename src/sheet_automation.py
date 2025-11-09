#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mail_automation_pipeline.py

Phase 1 — Gmail -> local CSV (overwrite daily, chunked by Gmail pagination)
Phase 2 — CSV -> Google Sheets:
  - Source book has zone tabs: EAST, WEST, CENTRAL
  - For each CSV row: match Address in source zone tab (chunked column scans)
  - On match: ALWAYS delete the matched source row
              IF type == "Sold To 3rd Party", append to target zone tab:
                <ZONE> - Sold_3rd_Party
                Columns pushed:
                  Link, Address, State, Opening Bid, Est. Market Value, Auction Start Date,
                  Final Bid (from CSV), Surplus Amount (= Final Bid - Opening Bid; currency-safe)

Usage:
  python mail_automation_pipeline.py collect
  python mail_automation_pipeline.py upload
  python mail_automation_pipeline.py both
"""

import os
import re
import csv
import sys
import time
import base64
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, List, Tuple, Set, DefaultDict
from collections import defaultdict
import random

# ---------------------- CONFIG ----------------------
SERVICE_ACCOUNT_FILE = Path("secrets/sheet_credentials.json")
IMPERSONATE_AS       = "leads@nsyteagents.com"

# Local CSV (overwrite daily)
CSV_DIR   = (Path(__file__).resolve().parent).parent / "mail_automation_data"
CSV_FINAL = CSV_DIR / "auctions_processed.csv"
CSV_TMP   = CSV_DIR / "auctions_processed.tmp"

# Gmail query: unread from sender
GMAIL_QUERY     = 'is:unread in:inbox from:noreply@auction.com'
LIST_PAGE_SIZE  = 200  # Gmail list page size (chunk)

# Google Sheets
# SOURCE spreadsheet: contains zone tabs "EAST", "WEST", "CENTRAL"
SOURCE_SPREADSHEET_ID = "1pkTmWR5rr2TFK3MNEO1mdindCk9erk4RQXItVPaEEjA"

# TARGET spreadsheet: contains tabs named "<ZONE> - Sold_3rd_Party"
TARGET_SPREADSHEET_ID = "1kPNWYy7wlyQfEtwLemxpgLGhDAsq6l_2BPMoJCCxVqA"

# Target/header schema (for target tabs)
TARGET_HEADER = [
    "Link", "Address", "State", "Opening Bid", "Est. Market Value", "Auction Start Date",
    "Final Bid", "Surplus Amount"
]

# Batch sizes
SHEETS_BATCH_SIZE   = 50     # append/insert batch size
SOURCE_CHUNK_SIZE   = 500    # chunk size for scanning source sheet address column
CSV_BATCH_SIZE      = 500    # rows per CSV processing batch

# Retry config
MAX_ATTEMPTS = 6
BASE_DELAY   = 1.0  # seconds
JITTER       = 0.5  # seconds

# ----------------------------------------------------

# --------------- LIB IMPORTS (Google) ---------------
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import gspread
from gspread.exceptions import APIError
# ----------------------------------------------------


# ================== ZONE MAPPING ====================
WEST    = {"CA", "AZ", "NV", "WA", "OR", "UT", "ID", "CO"}
CENTRAL = {"TX", "OK", "LA", "MS", "OH", "MI", "MN"}
EAST    = {"FL", "GA", "NC", "VA", "TN", "AL"}

def zone_for_state(state: Optional[str]) -> str:
    if not state:
        return ""
    s = state.upper()
    if s in WEST: return "WEST"
    if s in CENTRAL: return "CENTRAL"
    if s in EAST: return "EAST"
    return ""
# ====================================================


# --------------- SUBJECT & BODY PARSERS -------------
# Property Removed
SUBJ_REMOVED_RE = re.compile(
    r'^\s*Property\s+Removed:\s*(.*?)\s*has\s+been\s+removed', re.IGNORECASE
)

# Transaction Update — SOLD TO 3RD PARTY (strict match)
SUBJ_UPDATE_SOLD_3RD_RE = re.compile(
    r'^\s*Transaction\s+Update:\s*(.*?)\s*-\s*Sold\s+To\s+3rd\s+Party\b.*$', re.IGNORECASE
)

# Transaction Update — SOLD TO BENEFICIARY (explicitly treat as Removed)
SUBJ_UPDATE_SOLD_BENEF_RE = re.compile(
    r'^\s*Transaction\s+Update:\s*(.*?)\s*-\s*Sold\s+To\s+Beneficiary\b.*$', re.IGNORECASE
)

# Fallback: any Transaction Update — treat as Removed
SUBJ_UPDATE_ANY_RE = re.compile(
    r'^\s*Transaction\s+Update:\s*(.*?)(?:\s*-\s*.*)?\s*$', re.IGNORECASE
)

# 2-letter state finder
STATE_RE = re.compile(
    r'(?:(?<=^)|(?<=[\s,\.]))'
    r'(CA|AZ|NV|WA|OR|UT|ID|CO|'
    r'TX|OK|LA|MS|OH|MI|MN|'
    r'FL|GA|NC|VA|TN|AL)'
    r'(?=$|[\s,\.])',
    re.IGNORECASE
)

# Body anchor for final bid
AMOUNT_ANCHOR_RE = re.compile(
    r'was\s+sold\s+at\s+auction\s+today\s+for\s*(?:USD|US\$)?\s*\$?\s*'
    r'([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)\.?',
    re.IGNORECASE
)

def extract_state(address: str) -> Optional[str]:
    mm = STATE_RE.search(address or "")
    return mm.group(1).upper() if mm else None

def parse_subject(subject: str) -> Optional[Dict[str, str]]:
    """
    Returns dict with keys: type, address, state
      - type in {"Removed", "Sold To 3rd Party"}
    """
    s = subject.strip()

    m = SUBJ_REMOVED_RE.match(s)
    if m:
        address = " ".join(m.group(1).split())
        state = extract_state(address)
        return {"type": "Removed", "address": address, "state": state or ""}

    m = SUBJ_UPDATE_SOLD_3RD_RE.match(s)
    if m:
        address = " ".join(m.group(1).split())
        state = extract_state(address)
        return {"type": "Sold To 3rd Party", "address": address, "state": state or ""}

    m = SUBJ_UPDATE_SOLD_BENEF_RE.match(s)
    if m:
        address = " ".join(m.group(1).split())
        state = extract_state(address)
        return {"type": "Removed", "address": address, "state": state or ""}

    m = SUBJ_UPDATE_ANY_RE.match(s)
    if m:
        address = " ".join((m.group(1) or "").split())
        state = extract_state(address)
        return {"type": "Removed", "address": address, "state": state or ""}

    return None

def extract_final_bid_from_body_text(text: str) -> str:
    if not text:
        return ""
    m = AMOUNT_ANCHOR_RE.search(text)
    if not m:
        return ""
    amount = m.group(1)
    return f"${amount}"


# ------------------ RETRY HELPERS -------------------
def _sleep_with_jitter(delay: float):
    time.sleep(delay + random.uniform(0, JITTER))

def _should_retry_exception(ex: Exception) -> bool:
    txt = str(ex)
    retryable_markers = [
        "[429]", "[500]", "[502]", "[503]", "[504]",
        "Rate Limit Exceeded", "quota", "Quota", "timeout",
        "Connection aborted", "Remote end closed", "reset by peer",
    ]
    return any(mark in txt for mark in retryable_markers)

def retry(func, *args, **kwargs):
    """
    Generic retry wrapper for gspread/googleapiclient calls.
    Retries on APIError/HttpError/requests transport errors based on message.
    """
    attempt = 1
    delay = BASE_DELAY
    while True:
        try:
            res = func(*args, **kwargs)
            return res
        except (APIError, HttpError, Exception) as ex:
            if attempt >= MAX_ATTEMPTS or not _should_retry_exception(ex):
                # Log and bubble up or return None to continue
                print(f"[retry] giving up after {attempt} attempts: {ex}")
                raise
            print(f"[retry] attempt {attempt} failed: {ex} — retrying in {delay:.1f}s")
            _sleep_with_jitter(delay)
            delay = min(delay * 2, 16.0)
            attempt += 1

def retry_or_none(func, *args, **kwargs):
    """
    Same as retry() but returns None on final failure instead of raising.
    Useful when we want to skip a single failed call and continue.
    """
    attempt = 1
    delay = BASE_DELAY
    while True:
        try:
            res = func(*args, **kwargs)
            return res
        except (APIError, HttpError, Exception) as ex:
            if attempt >= MAX_ATTEMPTS or not _should_retry_exception(ex):
                print(f"[retry] non-fatal give up after {attempt} attempts: {ex}")
                return None
            print(f"[retry] attempt {attempt} failed: {ex} — retrying in {delay:.1f}s")
            _sleep_with_jitter(delay)
            delay = min(delay * 2, 16.0)
            attempt += 1
# ---------------------------------------------------


# ------------------ GMAIL HELPERS ------------------
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

def gmail_client():
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE), scopes=GMAIL_SCOPES
    ).with_subject(IMPERSONATE_AS)
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def iter_unread_pages(gmail, query: str, page_size: int = 200) -> Iterable[List[str]]:
    token = None
    page = 0
    print(f"[collector] Querying Gmail with: '{query}', page size={page_size}")
    while True:
        resp = retry_or_none(
            gmail.users().messages().list(
                userId="me",
                q=query,
                maxResults=page_size,
                pageToken=token,
                fields="nextPageToken,messages/id"
            ).execute
        )
        if resp is None:
            print("[collector] Gmail list failed, stopping pagination early.")
            break
        ids = [m["id"] for m in (resp.get("messages", []) or [])]
        page += 1
        print(f"[collector] Page {page}: {len(ids)} message id(s) retrieved")
        if ids:
            yield ids
        token = resp.get("nextPageToken")
        if not token:
            print(f"[collector] No more pages. Total pages={page}")
            break

def fetch_metadata(gmail, msg_id: str) -> Optional[Dict[str, Any]]:
    return retry_or_none(
        gmail.users().messages().get(
            userId="me", id=msg_id, format="metadata",
            metadataHeaders=["Subject", "From", "Date"],
            fields="id,payload/headers"
        ).execute
    )

def fetch_body_text(gmail, msg_id: str) -> str:
    msg = retry_or_none(
        gmail.users().messages().get(
            userId="me", id=msg_id, format="full", fields="payload"
        ).execute
    )
    if not msg:
        return ""
    return extract_plaintext(msg.get("payload", {}))

def extract_plaintext(payload: Dict[str, Any]) -> str:
    text = ""
    def walk(part):
        nonlocal text
        if text:
            return
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            data = part["body"]["data"]
            padded = data + "=" * (-len(data) % 4)
            try:
                text_local = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
            except Exception:
                text_local = ""
            text = text_local
            return
        for p in part.get("parts", []) or []:
            walk(p)
    walk(payload)
    return text

def mark_read(gmail, msg_id: str):
    _ = retry_or_none(
        gmail.users().messages().modify(
            userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
        ).execute
    )
# ---------------------------------------------------


# ------------------- SHEETS HELPERS ----------------
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def gs_client():
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE), scopes=SHEETS_SCOPES
    )
    return gspread.authorize(creds)

def get_or_create_worksheet(sh: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = retry(sh.add_worksheet, title=title, rows=20000, cols=40)
        print(f"[uploader] Created missing worksheet: '{title}'")
        return ws

def ensure_target_header(ws: gspread.Worksheet):
    cur = retry_or_none(ws.get_values, "A1:H1")
    time.sleep(1)
    header = cur[0] if cur else []
    if header != TARGET_HEADER:
        retry_or_none(ws.update, "A1:H1", [TARGET_HEADER])
        time.sleep(1)
        print(f"[uploader] Ensured target header on '{ws.title}'")

def col_to_a1(col_idx_1based: int) -> str:
    s = ""
    n = col_idx_1based
    while n:
        n, rem = divmod(n - 1, 26)
        s = chr(65 + rem) + s
    return s
# ---------------------------------------------------


# ------------------- CSV HELPERS -------------------
CSV_COLUMNS = ["address", "zone", "type", "final_bid"]

def atomic_csv_writer_open():
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    if CSV_TMP.exists():
        try:
            CSV_TMP.unlink()
            print("[collector] Removed stale temp CSV")
        except OSError:
            pass
    f = open(CSV_TMP, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
    w.writeheader()
    print(f"[collector] Opened temp CSV for writing: {CSV_TMP}")
    return f, w

def atomic_csv_commit(fh):
    fh.flush()
    os.fsync(fh.fileno())
    fh.close()
    os.replace(CSV_TMP, CSV_FINAL)
    print(f"[collector] Atomic commit complete: {CSV_FINAL}")
# ---------------------------------------------------


# ------------- ADDRESS NORMALIZATION ---------------

ALNUM_SPACE_RE = re.compile(r'[^a-z0-9 ]+', re.IGNORECASE)
MULTISPACE_RE  = re.compile(r'\s+')

def normalize_address_basic(addr: str) -> str:
    """
    Lowercase, strip, remove punctuation/symbols, keep only letters/numbers/spaces,
    and collapse multiple spaces to one.
    """
    if not addr:
        return ""
    s = addr.lower().strip()
    s = ALNUM_SPACE_RE.sub('', s)        # keep [a-z0-9 ] only
    s = MULTISPACE_RE.sub(' ', s).strip()
    return s



# ---------------- MONEY PARSING ---------------------
MONEY_SANITIZE_RE = re.compile(r'[^0-9.]')

def money_to_int_or_none(value: str) -> Optional[int]:
    if not value:
        return None
    s = MONEY_SANITIZE_RE.sub('', str(value))
    if not s or not any(ch.isdigit() for ch in s):
        return None
    if '.' in s:
        s = s.split('.', 1)[0]
    try:
        return int(s)
    except ValueError:
        return None
# ---------------------------------------------------


# ===================== PHASE 1 ======================
def collect_to_csv() -> None:
    gmail = gmail_client()

    written = 0
    soft_fail = 0
    processed_msgs = 0
    sold_msgs = 0
    removed_msgs = 0
    skipped_msgs = 0

    print("[collector] Starting email collection (unread only)…")
    fh, writer = atomic_csv_writer_open()

    try:
        page_no = 0
        for id_list in iter_unread_pages(gmail, GMAIL_QUERY, LIST_PAGE_SIZE):
            page_no += 1
            print(f"[collector] Processing page {page_no}: {len(id_list)} message(s)")
            for msg_id in id_list:
                try:
                    meta = fetch_metadata(gmail, msg_id)
                    if not meta:
                        soft_fail += 1
                        continue
                    headers = {h["name"].lower(): h["value"] for h in meta.get("payload", {}).get("headers", [])}
                    subject = headers.get("subject", "").strip()

                    sl = subject.lower()
                    if not (sl.startswith("transaction update:") or sl.startswith("property removed:")):
                        mark_read(gmail, msg_id)
                        skipped_msgs += 1
                        continue

                    parsed = parse_subject(subject)
                    if not parsed:
                        mark_read(gmail, msg_id)
                        skipped_msgs += 1
                        continue

                    zone = zone_for_state(parsed.get("state"))
                    final_bid = ""
                    if parsed["type"] == "Sold To 3rd Party":
                        body_text = fetch_body_text(gmail, msg_id)
                        final_bid = extract_final_bid_from_body_text(body_text)
                        sold_msgs += 1
                    else:
                        removed_msgs += 1

                    writer.writerow({
                        "address":   parsed["address"],
                        "zone":      zone,
                        "type":      parsed["type"],
                        "final_bid": final_bid
                    })
                    written += 1
                    processed_msgs += 1

                    print(f"[collector] #{processed_msgs} | {parsed['type']} | zone={zone or '-'} | address='{parsed['address']}' | final_bid='{final_bid or '-'}'")

                    mark_read(gmail, msg_id)

                except Exception as ex:
                    soft_fail += 1
                    print(f"[collector] Exception while processing a message: {ex} | soft_fail={soft_fail}")

        atomic_csv_commit(fh)

    except Exception as ex:
        try:
            fh.close()
        except Exception:
            pass
        print(f"[collector] FATAL: {ex}")
        # Do not re-raise to avoid hard crash
        return

    print(f"[collector] Completed. CSV rows written={written} | sold={sold_msgs} | removed={removed_msgs} | skipped={skipped_msgs} | soft_failures={soft_fail}")
# ===================================================


def _parse_sheet_date(value: str) -> Optional[dt.date]:
    """
    Parse dates in the guaranteed format: 'Nov 8, 2025'.
    Returns a date or None if empty/whitespace.
    """
    if not value or not str(value).strip():
        return None
    s = str(value).strip()
    try:
        # Strict format: abbreviated English month, no leading zero on day
        return dt.datetime.strptime(s, "%b %d, %Y").date()
    except ValueError:
        # If someone accidentally types a leading-zero day or extra spaces,
        # try a couple of tiny fallbacks (still strict to this style).
        for fmt in ("%b %e, %Y", "%b %d,%Y", "%b  %d, %Y"):
            try:
                return dt.datetime.strptime(s, fmt).date()
            except ValueError:
                continue
    return None


def clean_source_sheets(src_sh: gspread.Spreadsheet, zones: list) -> None:
    """
    Pre-phase cleanup for SOURCE sheets (EAST/WEST/CENTRAL):
      - Remove duplicate rows based on Link column (col A). Keep first occurrence.
      - Remove rows where 'Auction Start Date' is at least 15 days in the past.
    Reads in SOURCE_CHUNK_SIZE chunks; deletions bottom→up with 1s delay.
    """

    for z in zones:
        try:
            ws = get_or_create_worksheet(src_sh, z)
        except Exception as ex:
            print(f"[cleaner] Skipping zone '{z}' (cannot open/create worksheet): {ex}")
            continue

        # Detect headers
        header_vals = retry_or_none(ws.get_values, "1:1")
        time.sleep(1)
        header = header_vals[0] if header_vals else []
        hdr_lower = [h.strip().lower() for h in header]

        # Find "Auction Start Date" column
        try:
            auction_idx_0 = hdr_lower.index("auction start date")
            auction_col_letter = col_to_a1(auction_idx_0 + 1)
        except ValueError:
            auction_idx_0 = None
            auction_col_letter = None
            print(f"[cleaner] Warning: 'Auction Start Date' not found in '{z}'. Age filter skipped.")

        # Always use column A (Link) for de-dup
        link_col_letter = "A"

        seen_links: Set[str] = set()
        to_delete: Set[int] = set()

        start = 2  # skip header
        try:
            max_row = ws.row_count or 10000
        except Exception:
            max_row = 10000
        chunk_no = 0

        while start <= max_row:
            end = min(start + SOURCE_CHUNK_SIZE - 1, max_row)
            chunk_no += 1

            # Only load Link + Auction Start Date
            ranges = [f"{link_col_letter}{start}:{link_col_letter}{end}"]
            if auction_col_letter:
                ranges.append(f"{auction_col_letter}{start}:{auction_col_letter}{end}")

            data = retry_or_none(ws.batch_get, ranges)
            time.sleep(1)
            if data is None:
                print(f"[cleaner] [{z}] chunk {chunk_no} {start}-{end}: batch_get failed; skipping.")
                start = end + 1
                continue

            links = data[0] if len(data) > 0 else []
            auction_dates = data[1] if len(data) > 1 else []

            rows_in_chunk = max(len(links), len(auction_dates))
            if rows_in_chunk == 0:
                if end > 5000:
                    print(f"[cleaner] [{z}] chunk {chunk_no} {start}-{end}: empty; early tail exit.")
                    break
                start = end + 1
                continue

            print(f"[cleaner] [{z}] chunk {chunk_no} {start}-{end}: scanning {rows_in_chunk} row(s)")

            for i in range(rows_in_chunk):
                rownum = start + i
                link = (links[i][0].strip() if i < len(links) and links[i] else "")
                auction_val = (
                    auction_dates[i][0].strip()
                    if auction_col_letter and i < len(auction_dates) and auction_dates[i]
                    else ""
                )

                # skip if row empty
                if not link and not auction_val:
                    continue

                # 1) Remove duplicates by Link
                if link:
                    if link in seen_links:
                        to_delete.add(rownum)
                        continue
                    seen_links.add(link)

                # 2) Remove if Auction Start Date ≥15 days old
                if auction_col_letter and auction_val:
                    d = _parse_sheet_date(auction_val)
                    if d and (dt.date.today() - d).days >= 15:
                        to_delete.add(rownum)

            start = end + 1

        # Perform deletions bottom→up
        if to_delete:
            dels = sorted(to_delete, reverse=True)
            print(f"[cleaner] [{z}] deleting {len(dels)} row(s) bottom→up with 1s delay…")
            for r in dels:
                ok = retry_or_none(ws.delete_rows, r)
                time.sleep(1.0)
                if ok is None:
                    print(f"[cleaner]   Delete failed for row {r} in '{z}' — skipped.")

        print(f"[cleaner] [{z}] done. removed={len(to_delete)} duplicate/expired row(s).")


# ===================== PHASE 2 ======================
def upload_csv_to_sheet() -> None:
    """
    Phase 2 (chunked, low-reads, resilient):
      - Per ZONE: read sheet in 500-row chunks (A:F + Address column)
      - Match CSV (in 500 batches) against current chunk using exact digit-token subset + state guard
      - Stage inserts (row 2) for Sold To 3rd Party, queue deletions for all matches
      - Insert staged rows (top) in batches with sleeps/retries
      - Delete queued rows bottom→up with sleeps/retries; skip failures, continue
    """
    if not CSV_FINAL.exists():
        print(f"[uploader] CSV not found at {CSV_FINAL}; nothing to upload.")
        return

    try:
        client = gs_client()
    except Exception as ex:
        print(f"[uploader] Could not authorize Sheets client: {ex}")
        return

    print("[uploader] Connecting to spreadsheets…")
    try:
        src_sh = retry(client.open_by_key, SOURCE_SPREADSHEET_ID)
        tgt_sh = retry(client.open_by_key, TARGET_SPREADSHEET_ID)
    except Exception as ex:
        print(f"[uploader] Failed opening spreadsheets: {ex}")
        return

    zones = ["EAST", "WEST", "CENTRAL"]

    # --- run cleanup before processing ---
    clean_source_sheets(src_sh, zones)

    # Prepare target tabs
    tgt_ws: Dict[str, gspread.Worksheet] = {}
    for z in zones:
        try:
            tab_name = f"{z} - Sold_3rd_Party"
            ws = get_or_create_worksheet(tgt_sh, tab_name)
            ensure_target_header(ws)
            tgt_ws[z] = ws
        except Exception as ex:
            print(f"[uploader] Failed preparing target tab '{z} - Sold_3rd_Party': {ex}")
            # If target sheet not available, we still can delete source matches
            continue
    print(f"[uploader] Target tabs ready: {', '.join([w.title for w in tgt_ws.values()])}")

    total_processed = 0
    total_matched   = 0
    total_appended  = 0
    total_deleted   = 0

    for z in zones:
        try:
            ws = get_or_create_worksheet(src_sh, z)
        except Exception as ex:
            print(f"[uploader] Skipping zone '{z}' (cannot open/create worksheet): {ex}")
            continue

        # Detect Address column index from header (default to B if missing)
        header_vals = retry_or_none(ws.get_values, "1:1")
        time.sleep(1)
        header = header_vals[0] if header_vals else []
        try:
            addr_idx_0 = [h.strip().lower() for h in header].index("address")
        except ValueError:
            addr_idx_0 = 1
            print(f"[uploader] Warning: 'Address' header not found in '{z}'. Assuming column B.")
        addr_col_1b = addr_idx_0 + 1
        addr_col_letter = col_to_a1(addr_col_1b)

        to_delete: Set[int] = set()
        target_rows_buffer: List[List[str]] = []

        start = 2  # skip header
        try:
            max_row = ws.row_count or 10000
        except Exception:
            max_row = 10000
        chunk_no = 0

        while start <= max_row:
            end = min(start + SOURCE_CHUNK_SIZE - 1, max_row)
            chunk_no += 1

            # Read A:F and Address col for this chunk with retry
            ranges = [f"A{start}:F{end}", f"{addr_col_letter}{start}:{addr_col_letter}{end}"]
            chunk_data = retry_or_none(ws.batch_get, ranges)
            time.sleep(1)
            if chunk_data is None:
                print(f"[uploader] [{z}] chunk {chunk_no} {start}-{end}: batch_get failed; skipping this chunk.")
                start = end + 1
                continue

            rows_af = chunk_data[0] if len(chunk_data) > 0 else []
            rows_addr = chunk_data[1] if len(chunk_data) > 1 else []

            # Build per-row data for this chunk
            # Build per-row data for this chunk (store normalized address)
            # New shape: Dict[int, Tuple[List[str], str, str]]
            chunk_map: Dict[int, Tuple[List[str], str, str]] = {}

            for i in range(0, max(len(rows_af), len(rows_addr))):
                sheet_row = start + i
                af = rows_af[i] if i < len(rows_af) else []
                addr_txt = (rows_addr[i][0] if i < len(rows_addr) and rows_addr[i] else "").strip()
                if not addr_txt and not any(af):
                    continue
                while len(af) < 6:
                    af.append("")
                addr_norm = normalize_address_basic(addr_txt)
                chunk_map[sheet_row] = (af, addr_txt, addr_norm)


            if not chunk_map:
                if end > 5000:
                    print(f"[uploader] [{z}] chunk {chunk_no} {start}-{end}: empty; early tail exit.")
                    break
                else:
                    print(f"[uploader] [{z}] chunk {chunk_no} {start}-{end}: empty; continue.")
                    start = end + 1
                    continue

            print(f"[uploader] [{z}] chunk {chunk_no} {start}-{end}: loaded {len(chunk_map)} row(s)")

            # For this sheet chunk, stream the CSV in batches and try to match
            try:
                with open(CSV_FINAL, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    csv_batch: List[Dict[str, str]] = []
                    for csv_row in reader:
                        csv_batch.append(csv_row)
                        if len(csv_batch) >= CSV_BATCH_SIZE:
                            _match_csv_batch_against_chunk(
                                z, csv_batch, chunk_map, to_delete, target_rows_buffer
                            )
                            total_processed += len(csv_batch)
                            csv_batch.clear()
                    if csv_batch:
                        _match_csv_batch_against_chunk(
                            z, csv_batch, chunk_map, to_delete, target_rows_buffer
                        )
                        total_processed += len(csv_batch)
                        csv_batch.clear()
            except Exception as ex:
                print(f"[uploader] CSV read/match error in zone '{z}', chunk {chunk_no}: {ex}")

            start = end + 1

        # Insert staged target rows at top (row 2)
        if target_rows_buffer and z in tgt_ws:
            for i in range(0, len(target_rows_buffer), SHEETS_BATCH_SIZE):
                part = target_rows_buffer[i:i+SHEETS_BATCH_SIZE]
                ok = retry_or_none(tgt_ws[z].insert_rows, part, row=2, value_input_option="USER_ENTERED")
                time.sleep(1)
                if ok is None:
                    print(f"[uploader] [{z}] insert_rows batch failed; skipped {len(part)} rows.")
                else:
                    total_appended += len(part)
            print(f"[uploader] [{z}] inserted {total_appended} row(s) at top of '{tgt_ws[z].title}'")

        # Delete queued rows (descending) with retry and 1s delay
        if to_delete:
            dels = sorted(to_delete, reverse=True)
            print(f"[uploader] [{z}] deleting {len(dels)} row(s) bottom→up with 1s delay…")
            for r in dels:
                ok = retry_or_none(ws.delete_rows, r)
                time.sleep(1.0)
                if ok is None:
                    print(f"[uploader]   Delete failed for row {r} in '{z}' — skipped.")
                    continue
                total_deleted += 1

        zone_matches = len(to_delete)
        zone_appends = len(target_rows_buffer)
        print(f"[uploader] [{z}] summary: matched={zone_matches} | appended={zone_appends}")

        total_matched += zone_matches

    print(f"[uploader] DONE — processed={total_processed}, matched={total_matched}, appended={total_appended}, deleted={total_deleted}")
# ===================================================


# --- Match CSV batch against one sheet chunk (resilient) ---
def _match_csv_batch_against_chunk(
    zone: str,
    csv_batch: List[Dict[str, str]],
    chunk_map: Dict[int, Tuple[List[str], str, Set[str]]],
    to_delete: Set[int],
    target_rows_buffer: List[List[str]],
) -> None:
    for row in csv_batch:
        try:
            address   = (row.get("address") or "").strip()
            typ       = (row.get("type") or "").strip()
            final_bid = (row.get("final_bid") or "").strip()

            state = extract_state(address) or ""
            z = zone_for_state(state)
            if z != zone:
                continue

            csv_norm = normalize_address_basic(address)
            if not csv_norm:
                continue

            matched_rownum: Optional[int] = None
            matched_af: Optional[List[str]] = None

            for rnum, (af, addr_txt, sheet_norm) in list(chunk_map.items()):
                # Optional but helpful: state guard
                if state and state.upper() not in (addr_txt.upper() if addr_txt else ""):
                    continue
                # New: normalized substring containment
                if csv_norm and sheet_norm and (csv_norm in sheet_norm):
                    matched_rownum = rnum
                    matched_af = af
                    del chunk_map[rnum]  # avoid matching the same sheet row twice
                    break


            if not matched_rownum:
                continue

            to_delete.add(matched_rownum)

            if typ.lower() == "sold to 3rd party":
                af = matched_af or [""] * 6
                while len(af) < 6:
                    af.append("")
                link, addr_src, state_src, opening_bid, est_mv, auction_start_date = af[:6]

                fb_int = money_to_int_or_none(final_bid)
                ob_int = money_to_int_or_none(opening_bid)

                # Skip if either is missing
                if fb_int is None or ob_int is None:
                    continue

                surplus = fb_int - ob_int

                # NEW RULE — Only append if surplus >= 100
                if surplus < 100:
                    continue

                surplus_str = str(surplus)

                # Append because surplus is valid
                target_rows_buffer.append([
                    link, addr_src, state_src, opening_bid, est_mv, auction_start_date,
                    final_bid, surplus_str
                ])

        except Exception as ex:
            # Skip this CSV row on error, continue with next
            print(f"[uploader] match error on address '{row.get('address','')[:80]}…': {ex}")
# --------------------------------------------------------------------------------




# ====================== MAIN =======================
def main():
    choice = (sys.argv[1].strip().lower() if len(sys.argv) > 1 else "both")

    if choice == "collect":
        collect_to_csv()
    elif choice == "upload":
        upload_csv_to_sheet()
    elif choice == "both":
        collect_to_csv()
        upload_csv_to_sheet()
    else:
        print("Usage:")
        print("  python mail_automation_pipeline.py collect")
        print("  python mail_automation_pipeline.py upload")
        print("  python mail_automation_pipeline.py both")

if __name__ == "__main__":
    main()
# ===================================================
