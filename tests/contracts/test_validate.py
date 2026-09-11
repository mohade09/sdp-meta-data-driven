from pathlib import Path

import pytest

from meta_driven_etl.contracts.validate import (
    config_columns,
    config_grain,
    load_contracts,
    load_gold_config,
    validate_config,
)


def _write(path: Path, text: str) -> Path:
    path.write_text(text)
    return path


@pytest.fixture
def contract_dir(tmp_path: Path) -> Path:
    directory = tmp_path / "contracts"
    directory.mkdir()
    _write(directory / "README.md", "not yaml")
    _write(
        directory / "gold_sales.yml",
        """
schema:
  - physicalName: gold_meta_sales
    properties:
      - {name: region, physicalType: STRING, primaryKey: true}
      - {name: revenue, physicalType: BIGINT}
quality:
  - {name: positive_revenue, rule: sql, query: 'revenue > 0', severity: warning}
""",
    )
    return directory


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    return _write(
        tmp_path / "gold.yml",
        """
tables:
  sales:
    table_type: aggregate
    group_by: [source.region]
    metrics:
      - {name: revenue, expression: 'SUM(amount)', type: BIGINT}
    data_quality:
      - {name: positive_revenue, expression: 'revenue > 0', action: warn}
""",
    )


def test_loader_skips_non_yaml_files(contract_dir: Path) -> None:
    contracts = load_contracts(contract_dir)
    assert [contract.path.name for contract in contracts] == ["gold_sales.yml"]
    assert contracts[0].config_name == "sales"


def test_aggregate_column_and_grain_parser() -> None:
    table = {"table_type": "aggregate", "group_by": ["t.customer_id"], "metrics": [{"name": "orders"}]}
    assert config_columns(table) == {"customer_id", "orders"}
    assert config_grain(table) == {"customer_id"}


def test_sql_column_and_grain_parser() -> None:
    table = {"table_type": "sql", "sql": "SELECT t.region, COUNT(*) AS orders FROM t GROUP BY t.region"}
    assert {"region", "orders"}.issubset(config_columns(table))
    assert config_grain(table) == {"region"}


def test_valid_contract_matches_config(contract_dir: Path, config_path: Path) -> None:
    results = validate_config(load_contracts(contract_dir), load_gold_config(config_path))
    assert results == {contract_dir / "gold_sales.yml": []}


def test_reports_schema_grain_and_quality_drift(contract_dir: Path, config_path: Path) -> None:
    contract = load_contracts(contract_dir)[0]
    contract.document["schema"][0]["properties"][0]["name"] = "country"
    contract.document["quality"][0]["query"] = "revenue >= 0"
    errors = validate_config([contract], load_gold_config(config_path))[contract.path]
    assert any("absent" in error for error in errors)
    assert any("primaryKey" in error for error in errors)
    assert any("expression differs" in error for error in errors)


def test_reports_contract_without_config(contract_dir: Path, config_path: Path) -> None:
    contract = load_contracts(contract_dir)[0]
    contract.document["schema"][0]["physicalName"] = "gold_meta_unknown"
    errors = validate_config([contract], load_gold_config(config_path))[contract.path]
    assert errors == ["SKIPPED: no matching config table 'unknown' (source drift)"]
