from pathlib import Path

from openpine_contracts import list_schema_ids


def test_contracts_release_dependency_and_catalog() -> None:
    text = Path("pyproject.toml").read_text(encoding="utf-8")
    workflows = [
        Path(path).read_text(encoding="utf-8")
        for path in (
            ".github/workflows/ci.yml",
            ".github/workflows/live-acceptance.yml",
        )
    ]
    assert '"openpine-contracts==5.0.0rc6"' in text
    assert all(
        "db1745756516b47466756c9c5d38fbbb95595a3b" in workflow for workflow in workflows
    )
    assert "git+" not in text
    assert "openpine.marketdata.v2" in list_schema_ids()
