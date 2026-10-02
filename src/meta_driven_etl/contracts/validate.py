"""Validate gold-layer ODCS contracts against pipeline config and live tables."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CATALOG = "vdm_classic_aerfvt_catalog"
PROFILE = "fe-vm-vdm-classic-aerfvt"
SCHEMAS = ("gold_dev", "gold")


@dataclass(frozen=True)
class Contract:
    path: Path
    document: dict[str, Any]

    @property
    def table_name(self) -> str:
        return self.document["schema"][0]["physicalName"]

    @property
    def config_name(self) -> str:
        return self.table_name.removeprefix("gold_meta_")

    @property
    def properties(self) -> list[dict[str, Any]]:
        return self.document["schema"][0]["properties"]


def load_contracts(directory: Path) -> list[Contract]:
    """Load all YAML contracts in deterministic order."""
    contracts: list[Contract] = []
    for path in sorted(directory.glob("*.yml")):
        document = yaml.safe_load(path.read_text())
        if not isinstance(document, dict):
            raise ValueError(f"{path}: expected a YAML mapping")
        contracts.append(Contract(path, document))
    return contracts


def load_gold_config(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text())
    if not isinstance(document, dict) or not isinstance(document.get("tables"), dict):
        raise ValueError(f"{path}: missing tables mapping")
    return document


def _column_name(expression: str) -> str:
    return expression.strip().strip("`").split(".")[-1].strip("`")


def config_columns(table: dict[str, Any]) -> set[str]:
    """Return output columns declared by aggregate config or referenced by SQL."""
    if table.get("table_type") == "aggregate":
        return {
            *(_column_name(item) for item in table.get("group_by", [])),
            *(metric["name"] for metric in table.get("metrics", [])),
        }
    sql = table.get("sql", "")
    aliases = set(re.findall(r"\bAS\s+`?([A-Za-z_]\w*)`?", sql, re.IGNORECASE))
    identifiers = set(re.findall(r"\b(?:[A-Za-z_]\w*\.)?([A-Za-z_]\w*)\b", sql))
    return aliases | identifiers


def config_grain(table: dict[str, Any]) -> set[str]:
    if table.get("table_type") == "aggregate":
        return {_column_name(item) for item in table.get("group_by", [])}
    sql = table.get("sql", "")
    groups = re.findall(
        r"\bGROUP\s+BY\s+(.+?)(?=\b(?:HAVING|ORDER|LIMIT)\b|\)|$)",
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    return {
        _column_name(item)
        for group in groups
        for item in group.replace("\n", " ").split(",")
        if re.fullmatch(r"\s*(?:[A-Za-z_]\w*\.)?[A-Za-z_]\w*\s*", item)
    }


def validate_contract(contract: Contract, tables: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    expected_table = f"gold_meta_{contract.config_name}"
    schema = contract.document.get("schema") or []
    if len(schema) != 1 or contract.table_name != expected_table:
        errors.append(f"physical table must be {expected_table}")
    table = tables.get(contract.config_name)
    if table is None:
        return [f"SKIPPED: no matching config table '{contract.config_name}' (source drift)"]

    contract_columns = {item["name"] for item in contract.properties}
    missing = contract_columns - config_columns(table)
    if missing:
        errors.append(f"contract columns absent from config/SQL: {', '.join(sorted(missing))}")

    primary_key = {item["name"] for item in contract.properties if item.get("primaryKey")}
    grain = config_grain(table)
    if not primary_key:
        errors.append("contract has no primaryKey/grain")
    elif not primary_key.issubset(grain):
        errors.append(
            "primaryKey does not match configured GROUP BY grain: "
            f"contract={sorted(primary_key)}, config={sorted(grain)}"
        )

    contract_quality = {
        item.get("name"): item for item in contract.document.get("quality", [])
    }
    severity = {"warn": "warning", "drop": "error", "fail": "error"}
    for expectation in table.get("data_quality", []):
        rule = contract_quality.get(expectation["name"])
        if rule is None:
            errors.append(f"missing quality rule '{expectation['name']}'")
            continue
        if rule.get("query") != expectation.get("expression"):
            errors.append(f"quality rule '{expectation['name']}' expression differs")
        wanted_severity = severity.get(expectation.get("action"), expectation.get("action"))
        if rule.get("severity") != wanted_severity:
            errors.append(
                f"quality rule '{expectation['name']}' severity differs: "
                f"contract={rule.get('severity')}, config={wanted_severity}"
            )
    return errors


def validate_config(contracts: list[Contract], config: dict[str, Any]) -> dict[Path, list[str]]:
    results = {contract.path: validate_contract(contract, config["tables"]) for contract in contracts}
    contracted = {contract.config_name for contract in contracts}
    for table_name in sorted(set(config["tables"]) - contracted):
        results.setdefault(Path("<config>"), []).append(
            f"config table '{table_name}' has no matching contract"
        )
    return results


def _run_json(command: list[str]) -> dict[str, Any]:
    completed = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
    return json.loads(completed.stdout)


def _warehouse_id() -> str:
    payload = _run_json(["databricks", "warehouses", "list", f"--profile={PROFILE}", "--output=json"])
    warehouses = payload.get("warehouses", payload if isinstance(payload, list) else [])
    candidates = [w for w in warehouses if w.get("state") in {"RUNNING", "STARTING", "STOPPED"}]
    if not candidates:
        raise RuntimeError("no SQL warehouse is available")
    candidates.sort(key=lambda w: (w.get("state") != "RUNNING", w.get("name", "")))
    return candidates[0]["id"]


def describe_table(table: str, warehouse_id: str) -> tuple[str, dict[str, str]]:
    last_error = "table not found"
    for schema in SCHEMAS:
        full_name = f"{CATALOG}.{schema}.{table}"
        body = json.dumps({
            "statement": f"DESCRIBE TABLE {full_name}",
            "warehouse_id": warehouse_id,
            "format": "JSON_ARRAY",
            "wait_timeout": "50s",
        })
        try:
            payload = _run_json([
                "databricks", "api", "post", "/api/2.0/sql/statements/",
                "--json", body, f"--profile={PROFILE}",
            ])
            state = payload.get("status", {}).get("state")
            if state != "SUCCEEDED":
                last_error = payload.get("status", {}).get("error", {}).get("message", str(state))
                continue
            rows = payload.get("result", {}).get("data_array", [])
            columns = {
                row[0]: row[1].upper() for row in rows
                if len(row) >= 2 and row[0] and not row[0].startswith("#")
            }
            return full_name, columns
        except (subprocess.SubprocessError, OSError, ValueError, KeyError) as exc:
            last_error = str(exc)
    raise RuntimeError(last_error)


def validate_live(contracts: list[Contract]) -> tuple[bool, list[str]]:
    """Best-effort live schema validation. Unavailability is a non-failure."""
    try:
        warehouse_id = _warehouse_id()
    except (subprocess.SubprocessError, OSError, ValueError, KeyError, RuntimeError) as exc:
        return True, [f"SKIPPED live validation: {exc}"]
    messages: list[str] = []
    clean = True
    for contract in contracts:
        try:
            full_name, physical = describe_table(contract.table_name, warehouse_id)
        except RuntimeError as exc:
            messages.append(f"SKIPPED {contract.table_name}: {exc}")
            continue
        expected = {p["name"]: str(p["physicalType"]).upper() for p in contract.properties}
        missing = sorted(set(expected) - set(physical))
        extra = sorted(set(physical) - set(expected))
        mismatched = sorted(
            f"{name} contract={expected[name]} live={physical[name]}"
            for name in set(expected) & set(physical)
            if expected[name] != physical[name]
        )
        if missing or extra or mismatched:
            clean = False
            messages.append(
                f"FAIL {full_name}: missing={missing}, extra={extra}, type drift={mismatched}"
            )
        else:
            messages.append(f"PASS {full_name}: {len(expected)} columns match")
    return clean, messages


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contracts-dir", type=Path, default=Path("contracts"))
    parser.add_argument(
        "--config", type=Path,
        default=Path("src/meta_driven_etl/config/gold_tables.yml"),
    )
    parser.add_argument("--live", action="store_true", help="also compare against Unity Catalog")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        contracts = load_contracts(args.contracts_dir)
        config = load_gold_config(args.config)
    except (OSError, ValueError, yaml.YAMLError, KeyError) as exc:
        print(f"FAIL unable to load contracts/config: {exc}")
        return 1
    results = validate_config(contracts, config)
    failed = False
    for path, errors in results.items():
        if errors:
            for error in errors:
                if error.startswith("SKIPPED:"):
                    print(f"WARN {path}: {error}")
                else:
                    failed = True
                    print(f"FAIL {path}: {error}")
        else:
            print(f"PASS {path}")
    print(f"Config validation: {'FAILED' if failed else 'PASSED'} ({len(contracts)} contracts)")
    if args.live:
        live_clean, messages = validate_live(contracts)
        print(*messages, sep="\n")
        failed = failed or not live_clean
    else:
        print("SKIPPED live validation (enable with --live)")
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
