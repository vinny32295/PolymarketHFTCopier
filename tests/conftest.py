"""Test configuration — ensure position persistence does not leak between tests."""

import os
import tempfile
import pytest

# Use a temp directory for the positions file so tests don't pollute
# the working directory or interfere with each other.
_test_positions_dir = tempfile.mkdtemp(prefix="polymarket_test_")
_test_positions_path = os.path.join(_test_positions_dir, "positions.json")


@pytest.fixture(autouse=True)
def _isolate_positions_file(monkeypatch):
    """Point POSITIONS_FILE to a temp path and clean up after each test."""
    import polymarket_martingale as bot
    monkeypatch.setattr(bot, "POSITIONS_FILE", _test_positions_path)
    yield
    # Remove any positions file created during the test
    try:
        os.remove(_test_positions_path)
    except FileNotFoundError:
        pass
