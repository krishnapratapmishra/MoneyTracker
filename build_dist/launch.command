#!/bin/bash
# Universe Magnet — macOS Launcher
# Double-click this file to start the app. Terminal will open automatically.

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

echo ""
echo "  ================================================"
echo "  |   Universe Magnet — Starting Up              |"
echo "  ================================================"
echo ""

# Check for python3
if ! command -v python3 &>/dev/null; then
  echo "  Python 3 is not installed."
  echo ""
  echo "  Please install it:"
  echo "  1. Go to https://www.python.org/downloads/"
  echo "  2. Download and run the macOS installer"
  echo "  3. Once installed, double-click launch.command again"
  echo ""
  read -p "  Press Enter to open the download page..."
  open "https://www.python.org/downloads/"
  exit 1
fi

echo "  Python found: $(python3 --version)"
echo "  Starting Universe Magnet..."
echo ""

python3 launch.py
