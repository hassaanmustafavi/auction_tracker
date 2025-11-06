#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mail_automation_pipeline.py

- collect_to_csv(): Part 1 — Gmail -> local CSV (overwrite daily)
- upload_csv_to_sheet(): Part 2 — CSV -> Google Sheet (append rows)

Assumptions:
- Google Workspace + Service Account with Domain-Wide Delegation (no OAuth popups)
- Gmail API + Sheets API enabled in the same GCP project
- Spreadsheet shared with the service account as Editor

Usage:
  python mail_automation_pipeline.py collect   # run Part 1 only
  python mail_automation_pipeline.py upload    # run Part 2 only
  python mail_automation_pipeline.py both      # run Part 1 then Part 2
"""

import os
import re
import csv
import sys
import time
import base64
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Optional, Iterable, List

# ---------------------- CONFIG ----------------------
SERVICE_ACCOUNT_FILE = Path.cwd() / "secrets" / "sheet_credentials.json" # your SA key JSON
IMPERSONATE_AS       = "leads@nsyteagents.com"  # Workspace mailbox to read

# Single local CSV (overwrite daily) — path is parallel to this script's parent folder
CSV_DIR   = (Path(__file__).resolve().parent).parent / "mail_automation_data"
CSV_FINAL = CSV_DIR / "auctions_processed.csv"
CSV_TMP   = CSV_DIR / "auctions_processed.tmp"

# Gmail query: unread & from noreply@auction.com
GMAIL_QUERY     = 'is:unread in:inbox from:noreply@auction.com'
LIST_PAGE_SIZE  = 200  # Gmail list page size

# Google Sheet target
SPREADSHEET_ID  = "1pkTmWR5rr2TFK3MNEO1mdindCk9erk4RQXItVPaEEjA"
SHEET_NAME      = "TEST"   # sheet tab name

# Your sheet header + extra "Final Bid" column at the end
SHEET_HEADER = [
    "Link", "Address", "State", "Opening Bid", "Est. Market Value",
    "Auction Start Date", "Auction Start Time", "Status",
    "Completed", "Added Date", "Final Bid", "Surplus Amount"
]

# Batch size for appending rows to the sheet
SHEETS_BATCH_SIZE = 50

# ----------------------------------------------------

# --------------- LIB IMPORTS (Google) ---------------
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
import gspread
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
# Subject examples:
# Property Removed: 816 Bahia Lane, Bessemer, AL 35023 has been removed at this time, check out alternatives.
SUBJ_REMOVED_RE = re.compile(
    r'^\s*Property\s+Removed:\s*(.*?)\s*has\s+been\s+removed', re.IGNORECASE
)

# Transaction Update: 107 Vaughan Memorial Dr, Selma, AL 36701 - Sold To 3rd Party.
# NEW RULE:
#   - For Transaction Update, treat as SOLD only when the subject contains " - Sold"
#   - Extract the address strictly between ":" and "- Sold"
SUBJ_UPDATE_SOLD_RE = re.compile(
    r'^\s*Transaction\s+Update:\s*(.*?)\s*-\s*Sold\b.*$', re.IGNORECASE
)

# Fallback to capture address for any Transaction Update (non-sold treated as Removed)
SUBJ_UPDATE_ANY_RE = re.compile(
    r'^\s*Transaction\s+Update:\s*(.*?)(?:\s*-\s*.*)?\s*$', re.IGNORECASE
)

# Standalone 2-letter state near comma/space boundaries to avoid false matches
STATE_RE = re.compile(
    r'(?:,\s*|\s+)\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|IA|ID|IL|IN|KS|KY|LA|MA|MD|ME|MI|MN|MO|MS|MT|NC|ND|NE|NH|NJ|NM|NV|NY|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VA|VT|WA|WI|WV|WY)\b(?:\s*,|\s+)'
)

# In “Sold To 3rd Party” bodies, you said the anchored sentence is:
# “… was sold at auction today for $426,100.”
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

    # Property Removed
    m = SUBJ_REMOVED_RE.match(s)
    if m:
        address = " ".join(m.group(1).split())
        state = extract_state(address)
        return {"type": "Removed", "address": address, "state": state or ""}

    # Transaction Update — SOLD (strictly between ":" and "- Sold")
    m = SUBJ_UPDATE_SOLD_RE.match(s)
    if m:
        address = " ".join(m.group(1).split())
        state = extract_state(address)
        return {"type": "Sold To 3rd Party", "address": address, "state": state or ""}

    # Transaction Update — NOT SOLD → treat as Removed, still capture address
    m = SUBJ_UPDATE_ANY_RE.match(s)
    if m:
        address = " ".join((m.group(1) or "").split())
        state = extract_state(address)
        return {"type": "Removed", "address": address, "state": state or ""}

    return None

def extract_final_bid_from_body_text(text: str) -> str:
    """
    Only used for Sold To 3rd Party emails.
    Looks for the anchored phrase … "was sold at auction today for $X".
    """
    if not text:
        return ""
    m = AMOUNT_ANCHOR_RE.search(text)
    if not m:
        return ""
    amount = m.group(1)  # keep commas / decimals
    return f"${amount}"
# ---------------------------------------------------


# ------------------ GMAIL HELPERS ------------------
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]

def gmail_client():
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE), scopes=GMAIL_SCOPES
    ).with_subject(IMPERSONATE_AS)
    return build("gmail", "v1", credentials=creds, cache_discovery=False)

def iter_unread_ids(gmail, query: str, page_size: int = 200) -> Iterable[str]:
    token = None
    while True:
        resp = gmail.users().messages().list(
            userId="me",
            q=query,
            maxResults=page_size,
            pageToken=token,
            fields="nextPageToken,messages/id"
        ).execute()
        for m in resp.get("messages", []) or []:
            yield m["id"]
        token = resp.get("nextPageToken")
        if not token:
            break

def fetch_metadata(gmail, msg_id: str) -> Dict[str, Any]:
    # metadata with only the headers we care about
    return gmail.users().messages().get(
        userId="me", id=msg_id, format="metadata",
        metadataHeaders=["Subject", "From", "Date"],
        fields="id,payload/headers"
    ).execute()

def fetch_body_text(gmail, msg_id: str) -> str:
    # fetch full only when we need to parse amount for 3rd-party
    msg = gmail.users().messages().get(
        userId="me", id=msg_id, format="full",
        fields="payload"
    ).execute()
    return extract_plaintext(msg.get("payload", {}))

def extract_plaintext(payload: Dict[str, Any]) -> str:
    text = ""
    def walk(part):
        nonlocal text
        if text:
            return
        if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
            data = part["body"]["data"]
            # urlsafe base64 decode with padding tolerance
            padded = data + "=" * (-len(data) % 4)
            text = base64.urlsafe_b64decode(padded).decode("utf-8", errors="ignore")
            return
        for p in part.get("parts", []) or []:
            walk(p)
    walk(payload)
    return text

def mark_read(gmail, msg_id: str):
    gmail.users().messages().modify(
        userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
    ).execute()
# ---------------------------------------------------


# ------------------- SHEETS HELPERS ----------------
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

def worksheet():
    creds = service_account.Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE), scopes=SHEETS_SCOPES
    )
    client = gspread.authorize(creds)
    sh = client.open_by_key(SPREADSHEET_ID)
    try:
        ws = sh.worksheet(SHEET_NAME)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_NAME, rows=20000, cols=40)

    # Ensure header
    cur = ws.get_values("A1:K1")
    if (cur[0] if cur else []) != SHEET_HEADER:
        ws.update("A1:K1", [SHEET_HEADER])
    return ws

def build_sheet_row(address: str, zone: str, typ: str, final_bid: str) -> List[str]:
    """
    Map the CSV fields to the sheet header order:
    ["Link","Address","State","Opening Bid","Est. Market Value",
     "Auction Start Date","Auction Start Time","Status",
     "Completed","Added Date","Final Bid"]
    - State is derived from address again here (so you see it in the sheet).
    - Completed is binary (1).
    - Added Date is UTC ISO.
    """
    state = extract_state(address) or ""
    now_iso = dt.datetime.utcnow().isoformat() + "Z"
    status = typ  # keep plain ('Removed' or 'Sold To 3rd Party')
    return [
        "",               # Link
        address,          # Address
        state,            # State (extracted here)
        "",               # Opening Bid
        "",               # Est. Market Value
        "",               # Auction Start Date
        "",               # Auction Start Time
        status,           # Status
        1,                # Completed (binary)
        now_iso,          # Added Date
        final_bid or ""   # Final Bid
    ]
# ---------------------------------------------------


# ------------------- CSV HELPERS -------------------
CSV_COLUMNS = ["address", "zone", "type", "final_bid"]

def atomic_csv_writer_open():
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    # cleanup stale temp
    if CSV_TMP.exists():
        try: CSV_TMP.unlink()
        except OSError: pass
    f = open(CSV_TMP, "w", newline="", encoding="utf-8")
    w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
    w.writeheader()
    return f, w

def atomic_csv_commit(fh):
    fh.flush()
    os.fsync(fh.fileno())
    fh.close()
    os.replace(CSV_TMP, CSV_FINAL)
# ---------------------------------------------------


# ===================== PART 1 ======================
def collect_to_csv() -> None:
    """
    Part 1:
      - Scan unread emails from noreply@auction.com
      - Subject must start with 'Transaction Update:' or 'Property Removed:'
      - For Transaction Update: SOLD only when subject contains ' - Sold' (address between ':' and '- Sold')
      - For SOLD, fetch body and extract anchored price
      - Write a SINGLE local CSV (overwrite daily): address, zone, type, final_bid
      - Mark each processed email as READ immediately
    """
    gmail = gmail_client()

    written = 0
    soft_fail = 0

    fh, writer = atomic_csv_writer_open()

    try:
        for msg_id in iter_unread_ids(gmail, GMAIL_QUERY, LIST_PAGE_SIZE):
            try:
                meta = fetch_metadata(gmail, msg_id)
                headers = {h["name"].lower(): h["value"] for h in meta.get("payload", {}).get("headers", [])}
                subject = headers.get("subject", "").strip()

                # gate: must start with our prefixes
                sl = subject.lower()
                if not (sl.startswith("transaction update:") or sl.startswith("property removed:")):
                    # Not our target — mark read so we don't loop on next run
                    mark_read(gmail, msg_id)
                    continue

                parsed = parse_subject(subject)
                if not parsed:
                    # Subject matched prefix but parse failed — mark read, skip
                    mark_read(gmail, msg_id)
                    continue

                # Zone from parsed state
                zone = zone_for_state(parsed.get("state"))

                # Amount only for "Sold To 3rd Party"
                final_bid = ""
                if parsed["type"] == "Sold To 3rd Party":
                    body_text = fetch_body_text(gmail, msg_id)
                    final_bid = extract_final_bid_from_body_text(body_text)

                # Write CSV row
                writer.writerow({
                    "address":   parsed["address"],
                    "zone":      zone,
                    "type":      parsed["type"],
                    "final_bid": final_bid
                })
                written += 1

                # Mark as read immediately (UID not needed elsewhere)
                mark_read(gmail, msg_id)

            except HttpError as e:
                if e.resp.status in (429, 500, 502, 503, 504):
                    # brief backoff; skip this message (soft failure)
                    time.sleep(2)
                    soft_fail += 1
                else:
                    soft_fail += 1
            except Exception:
                soft_fail += 1

        # Commit final CSV atomically
        atomic_csv_commit(fh)

    except Exception:
        try: fh.close()
        except Exception: pass
        raise

    print(f"[collector] CSV written: {CSV_FINAL} | rows: {written} | soft-failures: {soft_fail}")
# ===================================================


# ===================== PART 2 ======================
def upload_csv_to_sheet() -> None:
    """
    Part 2:
      - Read the single local CSV (address, zone, type, final_bid)
      - Append rows to the Google Sheet (SHEET_HEADER order)
      - Completed = 1 (binary); Added Date = UTC ISO
      - Small batches to respect quotas
    """
    if not CSV_FINAL.exists():
        print(f"[uploader] CSV not found at {CSV_FINAL}; nothing to upload.")
        return

    ws = worksheet()

    batch: List[List[str]] = []
    total = 0

    with open(CSV_FINAL, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out = build_sheet_row(
                address=row.get("address", "").strip(),
                zone=row.get("zone", "").strip(),
                typ=row.get("type", "").strip(),
                final_bid=row.get("final_bid", "").strip()
            )
            batch.append(out)

            if len(batch) >= SHEETS_BATCH_SIZE:
                ws.append_rows(batch, value_input_option="USER_ENTERED")
                total += len(batch)
                batch.clear()

    if batch:
        ws.append_rows(batch, value_input_option="USER_ENTERED")
        total += len(batch)

    print(f"[uploader] Pushed {total} rows to sheet '{SHEET_NAME}' in spreadsheet {SPREADSHEET_ID}.")
# ===================================================


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
