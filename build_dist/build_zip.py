#!/usr/bin/env python3
"""
Build script — creates UniverseMagnet.zip ready to share.
Run this from the MoneyTracker project root:
    python build_dist/build_zip.py
"""

import os
import shutil
import sqlite3
import zipfile
from pathlib import Path

# Paths
SCRIPT_DIR  = Path(__file__).parent.resolve()
PROJECT_DIR = SCRIPT_DIR.parent                  # MoneyTracker/
OUTPUT_ZIP  = PROJECT_DIR / "UniverseMagnet.zip"
STAGE_DIR   = PROJECT_DIR / "_dist_stage"        # temp staging area

# Files/dirs to copy into source/
SOURCE_INCLUDES = [
    "app.py",
    "requirements.txt",
    "money_tracker.db",
    "templates",
    "static",
]

# Patterns to exclude inside source/
EXCLUDE_PATTERNS = {
    "__pycache__", ".venv", ".venv-win", ".venv-mac", ".venv-linux",
    ".setup_done", "launcher.log", "money_tracker.db-shm",
    "money_tracker.db-wal", ".gitignore",
    "SampleFiles", "import_excel.py", "start.bat",
    "MoneyTracker.xlsx", "UniverseMagnetPlayBook.xlsx",
    "Tempcookies.txt", "Tempmt_cookies.txt",
    "PROJECT_DOCUMENT.md", "SOP.md",
    "README_START_HERE.txt",
}


def should_exclude(path: Path) -> bool:
    for part in path.parts:
        if part in EXCLUDE_PATTERNS:
            return True
        if part.endswith(".pyc"):
            return True
    return False


def copy_tree(src: Path, dst: Path):
    """Recursively copy src into dst, honouring exclusions."""
    dst.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if should_exclude(item):
            continue
        target = dst / item.name
        if item.is_dir():
            copy_tree(item, target)
        else:
            shutil.copy2(item, target)


def _sanitise_db(db_path: Path):
    """
    Prepare the staged DB for distribution:
      - Reset app_settings to clean defaults (admin / Universe, no onboarding flags)
      - um_vision_cards_default is frozen in the DB and included as-is — never touched here
    """
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("""
            DELETE FROM app_settings
            WHERE key IN ('app_password','user_name',
                          'onboarding_complete','onboarding_wizard_done')
        """)
        conn.execute("INSERT INTO app_settings (key,value) VALUES ('app_password','Universe')")
        conn.execute("INSERT INTO app_settings (key,value) VALUES ('user_name','admin')")
        conn.commit()

        rows = conn.execute(
            "SELECT key, value FROM app_settings "
            "WHERE key IN ('app_password','user_name')"
        ).fetchall()
        print("  [DB ] app_settings defaults:")
        for k, v in rows:
            print("         {} = {}".format(k, "*" * len(v) if k == "app_password" else v))

        # Verify InvestMapping (Asset01–Asset24) is intact — never wiped by build
        im_count = conn.execute("SELECT COUNT(*) FROM InvestMapping").fetchone()[0]
        print("  [DB ] InvestMapping preserved: {} rows (Asset01–Asset{:02d})".format(
            im_count, im_count))

        # Report frozen default table (read-only, never modified by build)
        count = conn.execute("SELECT COUNT(*) FROM um_vision_cards_default").fetchone()[0]
        total_mb = conn.execute(
            "SELECT SUM(LENGTH(photo_data)) FROM um_vision_cards_default"
        ).fetchone()[0] or 0
        print("  [DB ] um_vision_cards_default (frozen): {} cards  ({:.1f} MB)".format(
            count, total_mb / 1_048_576))

        # Checkpoint WAL — clean self-contained .db file, no -wal/-shm needed
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.commit()
    finally:
        conn.close()


def build():
    print("\n  Building UniverseMagnet.zip...\n")

    # Clean staging dir
    if STAGE_DIR.exists():
        shutil.rmtree(STAGE_DIR)

    root = STAGE_DIR / "UniverseMagnet"
    src  = root / "source"
    src.mkdir(parents=True)

    # --- Root level files ---
    shutil.copy2(SCRIPT_DIR / "launch.py",      root / "launch.py")
    shutil.copy2(SCRIPT_DIR / "launch.command",  root / "launch.command")
    shutil.copy2(SCRIPT_DIR / "README.md",       root / "README.md")

    # --- Source folder ---
    for item_name in SOURCE_INCLUDES:
        item = PROJECT_DIR / item_name
        if not item.exists():
            print("  [SKIP] Not found: {}".format(item))
            continue
        if item.is_dir():
            copy_tree(item, src / item_name)
            print("  [DIR ] source/{}/".format(item_name))
        else:
            shutil.copy2(item, src / item_name)
            print("  [FILE] source/{}".format(item_name))

    # --- Sanitise the staged DB: reset app_settings to clean defaults ---
    staged_db = src / "money_tracker.db"
    if staged_db.exists():
        _sanitise_db(staged_db)

    # --- Create zip ---
    if OUTPUT_ZIP.exists():
        OUTPUT_ZIP.unlink()

    with zipfile.ZipFile(OUTPUT_ZIP, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for file in sorted(STAGE_DIR.rglob("*")):
            if file.is_file():
                arcname = file.relative_to(STAGE_DIR)
                zf.write(file, arcname)

    # Cleanup staging
    shutil.rmtree(STAGE_DIR)

    size_mb = OUTPUT_ZIP.stat().st_size / (1024 * 1024)
    print("\n  [DONE] Created: {}  ({:.1f} MB)\n".format(OUTPUT_ZIP.name, size_mb))
    print("  Share this zip file. Recipient extracts it and double-clicks:")
    print("    macOS   -> launch.command")
    print("    Windows -> launch.py\n")


if __name__ == "__main__":
    build()
