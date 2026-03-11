import sys
from pathlib import Path


def pytest_configure(config):
    """Ensure the `src` directory is on sys.path for imports."""
    root = Path(__file__).resolve().parents[1]
    src_dir = root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))
