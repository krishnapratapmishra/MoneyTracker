# -*- coding: utf-8 -*-
#!/usr/bin/env python3
"""
Universe Magnet - Smart Launcher
Works on Windows, macOS, and Linux.
Double-click this file to start the app.
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
APP_NAME      = "Universe Magnet"
APP_HOST_NAME = "myuniversemagnet"
APP_PORT      = 7432
APP_URL       = "http://{}:{}".format(APP_HOST_NAME, APP_PORT)
APP_ENTRY     = "app.py"
MIN_PYTHON    = (3, 8)
_LAUNCHER_DIR = Path(__file__).parent.resolve()
# App files live in a source/ subfolder next to this launcher
PROJECT_DIR   = (_LAUNCHER_DIR / "source") if (_LAUNCHER_DIR / "source").exists() else _LAUNCHER_DIR
SETUP_FILE    = _LAUNCHER_DIR / ".setup_done"
REQ_FILE      = PROJECT_DIR / "requirements.txt"

# venv named per OS so all three can coexist on a shared drive
_OS      = platform.system()           # 'Windows' | 'Darwin' | 'Linux'
_OS_SLUG = {"Windows": "win", "Darwin": "mac", "Linux": "linux"}.get(_OS, "other")
VENV_DIR = PROJECT_DIR / ".venv-{}".format(_OS_SLUG)

HOSTS_ENTRY = "127.0.0.1    {}".format(APP_HOST_NAME)


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
    print("  Please download a newer Python from:")
    print("  https://www.python.org/downloads/")
    print()
    print("  Steps:")
    print("  1. Click the big yellow Download button")
    print("  2. Run the installer")
    if _OS == "Windows":
        print("  3. CHECK 'Add Python to PATH'  <-- important!")
    print("  4. Run this launcher again")
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
        _line("[..]", "pip not found - installing via ensurepip...")
        subprocess.run([sys.executable, "-m", "ensurepip", "--upgrade"],
                       check=True, capture_output=True)
        _line("[OK]", "pip installed")


# ---------------------------------------------------------------------------
# Step 3: Hosts file  (myuniversemagnet -> 127.0.0.1)
# ---------------------------------------------------------------------------

def _hosts_path():
    if _OS == "Windows":
        return Path(r"C:\Windows\System32\drivers\etc\hosts")
    return Path("/etc/hosts")


def _hosts_has_entry():
    try:
        return APP_HOST_NAME in _hosts_path().read_text(errors="ignore")
    except Exception:
        return False


def _add_hosts_entry():
    if _hosts_has_entry():
        _line("[OK]", "Network name '{}' already configured".format(APP_HOST_NAME))
        return True

    _line("[..]", "Configuring network name '{}'...".format(APP_HOST_NAME))

    if _OS == "Windows":
        return _add_hosts_windows()
    return _add_hosts_unix()


def _add_hosts_windows():
    hosts = _hosts_path()
    # Try direct write (works if already admin)
    try:
        with open(hosts, "a") as f:
            f.write("\n{}\n".format(HOSTS_ENTRY))
        _line("[OK]", "Network name configured")
        return True
    except PermissionError:
        pass

    # Re-run the hosts update via elevated PowerShell
    _line("[..]", "Requesting admin permission (one-time only)...")
    try:
        entry = HOSTS_ENTRY.replace("'", "''")
        ps = (
            "Add-Content -Path '{}' -Value '`n{}' -Encoding UTF8"
            .format(str(hosts).replace("\\", "\\\\"), entry)
        )
        subprocess.run(
            ["powershell", "-Command",
             "Start-Process powershell -ArgumentList "
             "'-NoProfile -Command {}' -Verb RunAs -Wait".format(ps)],
            timeout=60, capture_output=True
        )
        if _hosts_has_entry():
            _line("[OK]", "Network name configured")
            return True
    except Exception:
        pass

    _hosts_manual_guide()
    return False


def _add_hosts_unix():
    hosts = _hosts_path()
    try:
        with open(hosts, "a") as f:
            f.write("\n{}\n".format(HOSTS_ENTRY))
        _line("[OK]", "Network name configured")
        return True
    except PermissionError:
        pass

    _line("[..]", "Need sudo to update hosts file (one-time only)...")
    try:
        result = subprocess.run(
            ["sudo", "sh", "-c",
             'echo "\n{}" >> {}'.format(HOSTS_ENTRY, str(hosts))],
            timeout=60
        )
        if result.returncode == 0 and _hosts_has_entry():
            _line("[OK]", "Network name configured")
            return True
    except Exception:
        pass

    _hosts_manual_guide()
    return False


def _hosts_manual_guide():
    print()
    print("  [!!] Could not update the hosts file automatically.")
    print()
    print("  Please do this once manually:")
    print("  1. Open as Administrator:  {}".format(_hosts_path()))
    print("  2. Add this line at the bottom:")
    print("     {}".format(HOSTS_ENTRY))
    print("  3. Save and run this launcher again.")
    print()
    print("  Until then you can still use: http://localhost:{}".format(APP_PORT))
    _pause()


# ---------------------------------------------------------------------------
# Step 4: Virtual environment
# ---------------------------------------------------------------------------

def setup_venv():
    py = _venv_python()

    venv_ok = py.exists()
    if venv_ok:
        try:
            subprocess.run([str(py), "--version"], check=True, capture_output=True)
        except Exception:
            venv_ok = False
            _line("[..]", "venv appears broken - recreating...")
            shutil.rmtree(VENV_DIR, ignore_errors=True)

    if not venv_ok:
        _line("[..]", "Creating isolated Python environment ({})...".format(_OS_SLUG))
        subprocess.run(
            [sys.executable, "-m", "venv", str(VENV_DIR)],
            check=True, capture_output=True
        )
        _line("[OK]", "Environment created")

    return py


def _parse_packages(req_file):
    pkgs = []
    for line in req_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            name = line.split("=")[0].split(">")[0].split("<")[0].split("!")[0].strip()
            pkgs.append((name, line))
    return pkgs


def _progress_bar(done, total, width=28):
    filled = int(width * done / total) if total else 0
    return "[{}{}]".format("=" * filled, " " * (width - filled))


def install_packages(py, force=False):
    if not REQ_FILE.exists():
        _line("[??]", "requirements.txt not found - skipping package install")
        return ""

    setup        = _read_setup()
    current_hash = _req_hash()

    if not force and setup.get("req_hash") == current_hash and _venv_pip().exists():
        _line("[OK]", "All packages up to date")
        return current_hash

    pkgs = _parse_packages(REQ_FILE)
    total = len(pkgs)
    print()
    print("  Installing {} package(s) — this takes ~1 min on first run".format(total))
    print()

    spinner = ["|", "/", "-", "\\"]
    failed  = []

    for idx, (name, spec) in enumerate(pkgs):
        done = idx
        spin_i = 0

        # Print installing line
        bar = _progress_bar(done, total)
        sys.stdout.write("  {} {}/{}  Installing  {}...          {}\r".format(
            bar, done, total, name.ljust(16), spinner[0]))
        sys.stdout.flush()

        proc = subprocess.Popen(
            [str(py), "-m", "pip", "install", spec,
             "-q", "--disable-pip-version-check"],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=str(PROJECT_DIR)
        )

        while proc.poll() is None:
            spin_i = (spin_i + 1) % 4
            sys.stdout.write("  {} {}/{}  Installing  {}...          {}\r".format(
                bar, done, total, name.ljust(16), spinner[spin_i]))
            sys.stdout.flush()
            time.sleep(0.12)

        output = proc.stdout.read() if proc.stdout else ""
        done = idx + 1
        bar  = _progress_bar(done, total)

        if proc.returncode == 0:
            sys.stdout.write("  {} {}/{}  [OK] {:<30}\n".format(bar, done, total, name))
        else:
            sys.stdout.write("  {} {}/{}  FAILED {:<27}\n".format(bar, done, total, name))
            failed.append((name, output))
        sys.stdout.flush()

    print()
    if failed:
        print("  [!!] Some packages failed to install:\n")
        for name, err in failed:
            print("  --- {} ---".format(name))
            print("  " + "\n  ".join(err.strip().splitlines()[-10:]))
        print()
        _pause("Press Enter to exit...")
        sys.exit(1)

    print("  All packages installed successfully.")
    print()
    return current_hash


# ---------------------------------------------------------------------------
# Step 5: First-time vs incremental setup
# ---------------------------------------------------------------------------

def run_first_time_setup():
    _section("First-time setup (takes about 1 minute)")

    py       = setup_venv()
    req_hash = install_packages(py, force=True)
    hosts_ok = _add_hosts_entry()

    _write_setup({
        "date":           time.strftime("%Y-%m-%d"),
        "os":             _OS,
        "python":         "{}.{}.{}".format(*sys.version_info[:3]),
        "req_hash":       req_hash,
        "hostname_added": hosts_ok,
        "venv":           str(VENV_DIR),
    })
    _line("[OK]", "Setup complete - won't repeat on next launch")


def run_incremental_check():
    """Quick check on every subsequent launch."""
    setup = _read_setup()

    # Different OS = different machine on a shared drive -> full setup
    if setup.get("os") != _OS:
        _line("[..]", "New machine detected ({}) - running setup...".format(_OS))
        run_first_time_setup()
        return

    py = setup_venv()

    # Re-install only if requirements.txt changed
    if setup.get("req_hash") != _req_hash():
        _line("[..]", "New packages detected - updating...")
        new_hash = install_packages(py, force=True)
        setup["req_hash"] = new_hash
        _write_setup(setup)
    else:
        _line("[OK]", "All packages up to date")

    if not _hosts_has_entry():
        _add_hosts_entry()
    else:
        _line("[OK]", "Network name '{}' ready".format(APP_HOST_NAME))


# ---------------------------------------------------------------------------
# Step 6: Start Flask
# ---------------------------------------------------------------------------

_flask_proc = None


def start_flask():
    global _flask_proc

    py       = _venv_python()
    app_path = PROJECT_DIR / APP_ENTRY

    if not py.exists():
        py = Path(sys.executable)   # fallback to system Python

    if not app_path.exists():
        print("\n  [!!] {} not found in {}".format(APP_ENTRY, PROJECT_DIR))
        _pause("Press Enter to exit...")
        sys.exit(1)

    # Kill any stale process already on the port
    if _port_open("127.0.0.1", APP_PORT, timeout=0.5):
        _line("[..]", "Port {} in use - restarting app...".format(APP_PORT))
        _kill_port()
        time.sleep(1.5)

    _line("[..]", "Starting app...")

    log_file = PROJECT_DIR / "launcher.log"
    log      = open(log_file, "w")

    _flask_proc = subprocess.Popen(
        [str(py), str(app_path)],
        cwd=str(PROJECT_DIR),
        stdout=log, stderr=log
    )

    # Wait up to 20 s for Flask to respond
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

    print("\n  [??] App is taking longer than expected to start.")
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
                    subprocess.run(
                        "taskkill /PID {} /F".format(parts[-1]),
                        shell=True, capture_output=True
                    )
        else:
            subprocess.run(
                "lsof -ti:{} | xargs kill -9".format(APP_PORT),
                shell=True, capture_output=True
            )
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Step 7: Open browser
# ---------------------------------------------------------------------------

def open_browser():
    # Use 127.0.0.1 explicitly — avoids macOS/Safari IPv6 (::1) vs IPv4 mismatch
    url = APP_URL if _hosts_has_entry() else "http://127.0.0.1:{}".format(APP_PORT)
    _line("[OK]", "Opening {}".format(url))
    time.sleep(0.6)
    webbrowser.open(url)


def check_onboarding_hint():
    """Print a first-time setup hint once — on the very first launch only."""
    setup = _read_setup()
    if setup.get("onboarding_hint_shown"):
        return
    print()
    print("  +--------------------------------------------------+")
    print("  |  [>>]  First time here? A 2-minute setup wizard   |")
    print("  |        will appear in your browser automatically.  |")
    print("  |        Just follow the steps to get started!       |")
    print("  +--------------------------------------------------+")
    print()
    setup["onboarding_hint_shown"] = True
    _write_setup(setup)


# ---------------------------------------------------------------------------
# Keep-alive (closing this window stops the app)
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

    try:
        while True:
            time.sleep(2)
            if _flask_proc and _flask_proc.poll() is not None:
                print("\n  [!!] The app stopped unexpectedly.")
                print("  Check launcher.log in the project folder for details.")
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
    # Always run from the project root regardless of where user double-clicked
    os.chdir(PROJECT_DIR)

    _banner("{} - Starting Up".format(APP_NAME))

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
    check_onboarding_hint()
    keep_alive()


if __name__ == "__main__":
    main()
