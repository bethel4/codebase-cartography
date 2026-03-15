from pathlib import Path

from reports.front_page import write_front_page


def test_write_front_page(tmp_path: Path) -> None:
    out = tmp_path / "index.html"
    written = write_front_page(out_path=out, module_html="module.html", lineage_html="lineage.html")
    text = written.read_text(encoding="utf-8")
    assert written.exists()
    assert "module.html" in text
    assert "lineage.html" in text
    assert "Split view" not in text
