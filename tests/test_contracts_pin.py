from pathlib import Path

from openpine_contracts import list_schema_ids


def test_contracts_release_dependency_and_catalog() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert '"openpine-contracts==5.0.0rc5"' in text
    assert "6b5e67445e2772057cd877e158c7aa0c58bdfe37" in workflow
    assert "git+" not in text
    assert "openpine.marketdata.v2" in list_schema_ids()
