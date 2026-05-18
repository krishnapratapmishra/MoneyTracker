#!/usr/bin/env python3
"""
Sync vision cards: metadata from CSV, images from current DB.
For cards missing from DB, inserts with CSV metadata (image will be partial).
"""

import csv
import base64
import sqlite3
import sys
import os
from pathlib import Path

CSV_PATH = r"C:\Users\DevOps\Downloads\vison.csv"
DB_PATH  = Path(__file__).parent / "money_tracker.db"


def fix_base64_image(raw: str) -> str:
    """Fix Excel-truncated base64: trim to 4-char boundary, pad, re-encode."""
    raw = raw.strip()
    if not raw:
        return ""
    prefix, b64 = (raw.split(",", 1) if "," in raw
                   else ("data:image/jpeg;base64", raw))
    prefix += ","
    b64 = b64.replace("\n", "").replace("\r", "").replace(" ", "")
    b64 = b64[:(len(b64) // 4) * 4]           # trim to 4-char boundary
    b64 += "=" * ((4 - len(b64) % 4) % 4)     # add padding
    try:
        raw_bytes = base64.b64decode(b64, validate=False)
        if len(raw_bytes) < 10:
            return ""
        return prefix + base64.b64encode(raw_bytes).decode("ascii")
    except Exception:
        return ""


def sync():
    if not os.path.exists(CSV_PATH):
        print("ERROR: CSV not found at", CSV_PATH)
        sys.exit(1)

    # --- Read CSV ---
    csv_rows = []
    with open(CSV_PATH, encoding="cp1252") as f:
        for r in csv.reader(f):
            if r:
                while len(r) < 8:
                    r.append("")
                csv_rows.append(r)
    print(f"\n  CSV rows  : {len(csv_rows)}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")

    # --- Load existing DB cards (id → photo_data) ---
    db_cards = {
        r["id"]: dict(r)
        for r in conn.execute("SELECT * FROM um_vision_cards").fetchall()
    }
    print(f"  DB cards  : {len(db_cards)}")
    print()

    inserted = updated = 0

    for i, r in enumerate(csv_rows):
        card_id     = r[0].strip()
        magnet      = r[1].strip()
        title       = r[2].strip()
        description = r[3].strip()
        raw_photo   = r[4].strip()
        created_at  = r[5].strip() or None
        updated_at  = r[6].strip() or None
        vision_type = r[7].strip() or "general"

        print(f"  [{i+1:02}/{len(csv_rows)}] {magnet:15} | {title}")

        if card_id in db_cards:
            # Keep full image from DB — only update metadata from CSV
            existing_photo = db_cards[card_id]["photo_data"] or ""
            img_len = len(existing_photo)
            print(f"          Image : from DB ({img_len:,} chars) OK")

            conn.execute("""
                UPDATE um_vision_cards
                SET magnet=?, title=?, description=?,
                    photo_data=?,
                    updated_at=COALESCE(?, updated_at),
                    vision_type=?
                WHERE id=?
            """, (magnet, title, description,
                  existing_photo,
                  updated_at, vision_type, card_id))
            updated += 1

        else:
            # Card not in DB — use CSV image (fix truncation as best-effort)
            print(f"          Image : not in DB — using CSV image (may be partial)")
            photo_data = fix_base64_image(raw_photo) if raw_photo else ""
            img_len = len(photo_data)
            print(f"          Image : fixed to {img_len:,} chars")

            conn.execute("""
                INSERT INTO um_vision_cards
                  (id, magnet, title, description, photo_data,
                   created_at, updated_at, vision_type)
                VALUES (?,?,?,?,?,
                        COALESCE(?, datetime('now')),
                        COALESCE(?, datetime('now')), ?)
            """, (card_id, magnet, title, description, photo_data,
                  created_at, updated_at, vision_type))
            inserted += 1

    conn.commit()

    # --- Final verification ---
    print()
    print("  -- Final state ---------------------------------------------")
    final = conn.execute(
        "SELECT magnet, title, length(photo_data) as img_len "
        "FROM um_vision_cards ORDER BY magnet, title"
    ).fetchall()
    ok = bad = 0
    for row in final:
        img_len = row["img_len"] or 0
        status = "OK" if img_len > 50000 else ("PARTIAL" if img_len > 0 else "NO IMAGE")
        if status == "OK":
            ok += 1
        else:
            bad += 1
        print(f"  {row['magnet']:15} | {row['title']:35} | {img_len:>9,} chars | {status}")

    conn.close()
    print()
    print(f"  Inserted : {inserted}  Updated : {updated}")
    print(f"  Full images : {ok}   Partial/missing : {bad}")
    print()
    print("  Reload the Vision Board page in the app to see your cards.")
    print()


if __name__ == "__main__":
    sync()
