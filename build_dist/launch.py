# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Universe Magnet - Smart Launcher
Works on Windows and macOS. Double-click to start the app.
"""

import os
import sys
import json
import time
import platform
import hashlib
import subprocess
import webbrowser
import socket
import shutil
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
APP_NAME  = "Universe Magnet"
APP_PORT  = 9876
APP_ENTRY = "app.py"
MIN_PYTHON = (3, 8)

_OS      = platform.system()   # 'Windows' | 'Darwin' | 'Linux'
_OS_SLUG = {"Windows": "win", "Darwin": "mac", "Linux": "linux"}.get(_OS, "other")

# Root = folder containing this launch.py
ROOT_DIR   = Path(__file__).parent.resolve()
# Source = where app.py lives (always the 'source' subfolder)
SOURCE_DIR = ROOT_DIR / "source"

VENV_DIR   = ROOT_DIR / ".venv-{}".format(_OS_SLUG)
SETUP_FILE = ROOT_DIR / ".setup_done"
REQ_FILE   = SOURCE_DIR / "requirements.txt"

# App URL — use localhost (no hosts-file tricks needed for end users)
APP_URL = "http://localhost:{}".format(APP_PORT)


# ---------------------------------------------------------------------------
# Console helpers
# ---------------------------------------------------------------------------

def _banner(msg):
    width = max(56, len(msg) + 4)
    bar   = "=" * width
    print("\n+" + bar + "+")
    print("|  {:<{}}  |".format(msg, width - 2))
    print("+" + bar + "+")

def _line(icon, msg):
    print("  {}  {}".format(icon, msg))

def _section(title):
    print("\n  -- {} {}".format(title, "-" * max(0, 42 - len(title))))

def _pause(msg="Press Enter to continue..."):
    try:
        input("\n  " + msg)
    except (EOFError, KeyboardInterrupt):
        pass

def _req_hash():
    if not REQ_FILE.exists():
        return ""
    return hashlib.md5(REQ_FILE.read_bytes()).hexdigest()

def _read_setup():
    try:
        return json.loads(SETUP_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def _write_setup(data):
    SETUP_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

def _venv_python():
    if _OS == "Windows":
        return VENV_DIR / "Scripts" / "python.exe"
    return VENV_DIR / "bin" / "python"

def _venv_pip():
    if _OS == "Windows":
        return VENV_DIR / "Scripts" / "pip.exe"
    return VENV_DIR / "bin" / "pip"

def _port_open(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# ---------------------------------------------------------------------------
# Step 1: Python version check
# ---------------------------------------------------------------------------

def check_python():
    ver = sys.version_info
    if ver >= MIN_PYTHON:
        _line("[OK]", "Python {}.{}.{} found".format(ver.major, ver.minor, ver.micro))
        return

    _line("[!!]", "Python {}.{} is too old (need {}.{}+)".format(
        ver.major, ver.minor, MIN_PYTHON[0], MIN_PYTHON[1]))
    print()
    if _OS == "Darwin":
        print("  Please install Python 3 from: https://www.python.org/downloads/")
        print()
        print("  Steps:")
        print("  1. Visit the link above and click 'Download Python 3.x.x'")
        print("  2. Open the downloaded .pkg file and follow the installer")
        print("  3. Once installed, double-click 'launch.command' again")
    else:
        print("  Please download Python from: https://www.python.org/downloads/")
        print("  During install — CHECK 'Add Python to PATH'")
    print()
    _pause("Press Enter to open the download page...")
    webbrowser.open("https://www.python.org/downloads/")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Step 2: pip
# ---------------------------------------------------------------------------

def ensure_pip():
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "--version"],
            check=True, capture_output=True
        )
    except subprocess.CalledProcessError:
        _line("[..]", "pip not found — installing via ensurepip...")
        subprocess.run([sys.executable, "-m", "ensurepip", "--upgrade"],
                       check=True, capture_output=True)
        _line("[OK]", "pip installed")


def upgrade_pip(py):
    """Upgrade pip inside the venv — outdated pip causes slow/hung installs on Windows."""
    _line("[..]", "Upgrading pip (prevents slow installs)...")
    subprocess.run(
        [str(py), "-m", "pip", "install", "--upgrade", "pip",
         "-q", "--disable-pip-version-check"],
        capture_output=True
    )
    _line("[OK]", "pip is up to date")


# ---------------------------------------------------------------------------
# Step 3: Virtual environment
# ---------------------------------------------------------------------------

def setup_venv():
    py = _venv_python()
    venv_ok = py.exists()
    if venv_ok:
        try:
            subprocess.run([str(py), "--version"], check=True, capture_output=True)
        except Exception:
            venv_ok = False
            _line("[..]", "Environment appears broken — recreating...")
            shutil.rmtree(VENV_DIR, ignore_errors=True)

    if not venv_ok:
        _line("[..]", "Creating isolated Python environment (first-time only)...")
        subprocess.run(
            [sys.executable, "-m", "venv", str(VENV_DIR)],
            check=True, capture_output=True
        )
        _line("[OK]", "Environment created")

    return py


def _parse_requirements():
    """Return list of (raw_line, display_name) from requirements.txt."""
    pkgs = []
    for raw in REQ_FILE.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        # display name = everything before any version specifier
        import re
        name = re.split(r"[><=!;\[]", line)[0].strip()
        pkgs.append((line, name))
    return pkgs


def _progress_bar(done, total, width=28):
    """Return a string like  [=========>          ] 3/5"""
    filled = int(width * done / total) if total else 0
    bar    = "=" * filled
    head   = ">" if filled < width else "="
    empty  = " " * (width - filled - (1 if filled < width else 0))
    return "[{}{}{}] {}/{}".format(bar, head, empty, done, total)


def install_packages(py, force=False):
    if not REQ_FILE.exists():
        _line("[??]", "requirements.txt not found — skipping package install")
        return ""

    setup        = _read_setup()
    current_hash = _req_hash()

    if not force and setup.get("req_hash") == current_hash and _venv_pip().exists():
        _line("[OK]", "All packages up to date")
        return current_hash

    pkgs  = _parse_requirements()
    total = len(pkgs)
    errors = []

    print()
    print("  Installing {} package(s) — this takes ~1 min on first run\n".format(total))

    for idx, (pkg_line, pkg_name) in enumerate(pkgs, 1):
        bar     = _progress_bar(idx - 1, total)
        label   = "  {} Installing  {:<22}".format(bar, pkg_name + "...")
        # Print on same line, overwrite previous
        print(label, end="", flush=True)

        proc = subprocess.Popen(
            [str(py), "-m", "pip", "install", pkg_line,
             "-q", "--disable-pip-version-check"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=str(ROOT_DIR)
        )

        # Spin while pip works, keeping the line alive
        spin = ["|", "/", "-", "\\"]
        tick = 0
        while proc.poll() is None:
            spinner = spin[tick % len(spin)]
            print("\r  {} Installing  {:<22} {}".format(
                bar, pkg_name + "...", spinner), end="", flush=True)
            tick += 1
            time.sleep(0.15)

        # Overwrite with result
        if proc.returncode == 0:
            print("\r  {} [OK] {:<30}".format(
                _progress_bar(idx, total), pkg_name))
        else:
            out = proc.stdout.read() if proc.stdout else ""
            print("\r  {} [!!] {:<30}  FAILED".format(
                _progress_bar(idx, total), pkg_name))
            errors.append((pkg_name, out))

    print()

    if errors:
        print("  [!!] Some packages failed to install:\n")
        for name, out in errors:
            print("       {} :".format(name))
            for ln in out.strip().splitlines()[-6:]:
                print("         " + ln)
        print()
        _pause("Press Enter to exit...")
        sys.exit(1)

    print("  All packages installed successfully.")
    print()
    return current_hash


# ---------------------------------------------------------------------------
# Step 4: First-time vs incremental setup
# ---------------------------------------------------------------------------

def run_first_time_setup():
    _section("First-time setup (takes about 1 minute)")
    py       = setup_venv()
    upgrade_pip(py)
    req_hash = install_packages(py, force=True)
    _write_setup({
        "date":     time.strftime("%Y-%m-%d"),
        "os":       _OS,
        "python":   "{}.{}.{}".format(*sys.version_info[:3]),
        "req_hash": req_hash,
        "venv":     str(VENV_DIR),
    })
    _line("[OK]", "Setup complete — won't repeat on next launch")


def run_incremental_check():
    setup = _read_setup()
    if setup.get("os") != _OS:
        _line("[..]", "New machine detected ({}) — running setup...".format(_OS))
        run_first_time_setup()
        return

    py = setup_venv()
    if setup.get("req_hash") != _req_hash():
        _line("[..]", "New packages detected — updating...")
        new_hash = install_packages(py, force=True)
        setup["req_hash"] = new_hash
        _write_setup(setup)
    else:
        _line("[OK]", "All packages up to date")


# ---------------------------------------------------------------------------
# Step 5: Start Flask
# ---------------------------------------------------------------------------

_flask_proc = None


def start_flask():
    global _flask_proc

    py       = _venv_python()
    app_path = SOURCE_DIR / APP_ENTRY

    if not py.exists():
        py = Path(sys.executable)

    if not app_path.exists():
        print("\n  [!!] Cannot find app files in: {}".format(SOURCE_DIR))
        print("  Make sure the 'source' folder is present next to launch.py")
        _pause("Press Enter to exit...")
        sys.exit(1)

    # Kill any stale process on the port
    if _port_open("127.0.0.1", APP_PORT, timeout=0.5):
        _line("[..]", "Port {} in use — restarting app...".format(APP_PORT))
        _kill_port()
        time.sleep(1.5)

    _line("[..]", "Starting app...")

    log_file = ROOT_DIR / "launcher.log"
    log      = open(log_file, "w")

    # Run Flask from SOURCE_DIR so it finds db, templates, static
    _flask_proc = subprocess.Popen(
        [str(py), str(app_path)],
        cwd=str(SOURCE_DIR),
        stdout=log, stderr=log
    )

    for _ in range(40):
        time.sleep(0.5)
        if _flask_proc.poll() is not None:
            log.close()
            try:
                err = log_file.read_text(encoding="utf-8", errors="replace")
            except Exception:
                err = "(could not read log)"
            print("\n  [!!] App crashed on startup. Last log lines:\n")
            for ln in err.strip().splitlines()[-25:]:
                print("      " + ln)
            _pause("\nPress Enter to exit...")
            sys.exit(1)

        if _port_open("127.0.0.1", APP_PORT, timeout=0.5):
            _line("[OK]", "App is running at {}".format(APP_URL))
            return

    print("\n  [??] App is taking longer than expected.")
    print("  If the browser does not open, try: {}".format(APP_URL))


def _kill_port():
    try:
        if _OS == "Windows":
            out = subprocess.check_output(
                "netstat -ano | findstr :{}".format(APP_PORT),
                shell=True, text=True, stderr=subprocess.DEVNULL
            )
            for line in out.strip().splitlines():
                parts = line.split()
                if parts and parts[-1].isdigit():
                    subprocess.run("taskkill /PID {} /F".format(parts[-1]),
                                   shell=True, capture_output=True)
        else:
            subprocess.run(
                "lsof -ti:{} | xargs kill -9".format(APP_PORT),
                shell=True, capture_output=True
            )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Step 6: Open browser
# ---------------------------------------------------------------------------

def open_browser():
    _line("[OK]", "Opening {}".format(APP_URL))
    time.sleep(0.8)
    webbrowser.open(APP_URL)


# ---------------------------------------------------------------------------
# Keep-alive
# ---------------------------------------------------------------------------

def keep_alive():
    width = 52
    bar   = "-" * width
    print()
    print("  +" + bar + "+")
    print("  |  App running at {:<{}}  |".format(APP_URL, width - 18))
    print("  |  Keep this window open while using the app.  |")
    print("  |  Close this window (or Ctrl+C) to stop.      |")
    print("  +" + bar + "+")
    print()
    print("  First login:  username = admin   password = Universe")
    print()

    try:
        while True:
            time.sleep(2)
            if _flask_proc and _flask_proc.poll() is not None:
                print("\n  [!!] The app stopped unexpectedly.")
                print("  Check launcher.log for details.")
                _pause("Press Enter to exit...")
                break
    except KeyboardInterrupt:
        print("\n  Shutting down...")
        if _flask_proc:
            _flask_proc.terminate()
        print("  Goodbye!")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.chdir(ROOT_DIR)

    _banner("{} — Starting Up".format(APP_NAME))

    _section("Checking Python")
    check_python()
    ensure_pip()

    _section("Checking setup")
    if not SETUP_FILE.exists():
        run_first_time_setup()
    else:
        run_incremental_check()

    _section("Starting app")
    start_flask()
    open_browser()
    keep_alive()


if __name__ == "__main__":
    main()
