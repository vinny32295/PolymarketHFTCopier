#!/usr/bin/env bash
# =============================================================================
# Polymarket Martingale Bot — Installer & Launcher
# =============================================================================
# This script:
#   1. Checks for Python 3.8+
#   2. Creates a virtual environment
#   3. Installs all dependencies
#   4. Launches the bot GUI
#
# Usage:
#   chmod +x install_and_run.sh
#   ./install_and_run.sh
#
# On Windows (Git Bash / WSL):
#   bash install_and_run.sh
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$SCRIPT_DIR/.venv"

echo "============================================="
echo "  Polymarket Martingale Bot — Setup"
echo "============================================="
echo ""

# --- Check Python ---
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        version=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null)
        major=$("$cmd" -c "import sys; print(sys.version_info.major)" 2>/dev/null)
        minor=$("$cmd" -c "import sys; print(sys.version_info.minor)" 2>/dev/null)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 8 ]; then
            PYTHON="$cmd"
            echo "[OK] Found $cmd (version $version)"
            break
        fi
    fi
done

if [ -z "$PYTHON" ]; then
    echo "[ERROR] Python 3.8+ is required but not found."
    echo "        Install it from https://python.org/downloads/"
    exit 1
fi

# --- Check tkinter ---
if ! "$PYTHON" -c "import tkinter" 2>/dev/null; then
    echo ""
    echo "[WARNING] tkinter is not installed."
    echo "          On Ubuntu/Debian: sudo apt install python3-tk"
    echo "          On Fedora:        sudo dnf install python3-tkinter"
    echo "          On macOS:         brew install python-tk"
    echo ""
    echo "          The bot requires tkinter for its GUI."
    read -p "          Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# --- Create virtual environment ---
if [ ! -d "$VENV_DIR" ]; then
    echo ""
    echo "Creating virtual environment in .venv/ ..."
    "$PYTHON" -m venv "$VENV_DIR"
    echo "[OK] Virtual environment created"
else
    echo "[OK] Virtual environment already exists"
fi

# --- Activate and install ---
echo ""
echo "Installing dependencies..."
source "$VENV_DIR/bin/activate"
pip install --upgrade pip -q
pip install -r "$SCRIPT_DIR/requirements.txt" -q
echo "[OK] Dependencies installed"

# --- Show summary ---
echo ""
echo "============================================="
echo "  Setup Complete!"
echo "============================================="
echo ""
echo "  Installed packages:"
pip list --format=columns 2>/dev/null | grep -E "web3|requests|py-clob" || true
echo ""

# --- Run ---
echo "Launching Polymarket Martingale Bot..."
echo "(Close the GUI window to stop)"
echo ""
cd "$SCRIPT_DIR"
"$PYTHON" polymarket_martingale.py
