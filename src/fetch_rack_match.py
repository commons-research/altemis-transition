#!/usr/bin/env python3
"""Fetch Altemis SBS rack data and match it against a CSV layout."""
from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

try:
    import requests
except ImportError:  # pragma: no cover - fallback for environments without requests
    requests = None
import urllib.request
import urllib.error


DEFAULT_API_URL = "http://10.10.10.1:5223/api/v1/projects/SBS%2096"


class RackMismatchError(RuntimeError):
    """Raised when the scanned rack ID does not match the expected one."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Match a CSV rack layout to the latest scan from the Altemis SBS reader."
    )
    parser.add_argument(
        "csv_path",
        type=Path,
        help="Path to the CSV file describing rack positions (e.g. data/csv/filled_racks/ALT00019226.csv)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Where to write the paired CSV. Defaults to <csv_path stem>_paired.csv alongside the input.",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_API_URL,
        help=f"Reader API endpoint to query (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="HTTP timeout in seconds when fetching from the reader (default: 5.0).",
    )
    parser.add_argument(
        "--json-file",
        type=Path,
        help=(
            "Optional path to a JSON payload captured from the reader. "
            "If provided, the script skips the API call and uses this file instead."
        ),
    )
    parser.add_argument(
        "--trust-env-proxies",
        action="store_true",
        help=(
            "Allow HTTP libraries to use proxy settings from the environment. "
            "Disabled by default so the script can talk directly to the reader."
        ),
    )
    parser.add_argument(
        "--backend",
        choices=("auto", "requests", "urllib", "curl"),
        default="auto",
        help="HTTP backend to use when talking to the reader (default: auto).",
    )
    return parser.parse_args()


def normalize_position(raw: str) -> str:
    """Normalise well identifiers to A01..H12 style for reliable matching."""
    if raw is None:
        raise ValueError("Position identifier cannot be None")

    token = raw.strip().upper()
    match = re.fullmatch(r"([A-Z]+)0*(\d+)", token)
    if not match:
        return token

    row_letters, column_digits = match.groups()
    return f"{row_letters}{column_digits.zfill(2)}"


def read_layout(csv_path: Path) -> List[Tuple[str, str]]:
    """Load rack layout entries as (normalized_position, sample_id)."""
    entries: List[Tuple[str, str]] = []
    seen: Dict[str, str] = {}
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        for idx, row in enumerate(reader, start=1):
            if not row:
                continue
            if len(row) < 2:
                raise ValueError(
                    f"Row {idx} in {csv_path} has fewer than two columns: {row!r}"
                )
            position_raw = row[0].strip()
            sample_id = row[1].strip()
            if not position_raw:
                raise ValueError(f"Row {idx} in {csv_path} is missing a position identifier")
            normalized = normalize_position(position_raw)
            if normalized in seen and seen[normalized] != sample_id:
                raise ValueError(
                    f"Duplicate position {normalized} with conflicting sample IDs: "
                    f"{seen[normalized]!r} vs {sample_id!r}"
                )
            seen[normalized] = sample_id
            entries.append((normalized, sample_id))
    if not entries:
        raise ValueError(f"No usable data found in {csv_path}")
    return entries


def fetch_payload(url: str, timeout: float, *, trust_env: bool, backend: str) -> Any:
    """Fetch JSON payload from the reader using the selected backend."""
    errors: List[Exception] = []
    for method in _resolve_backends(backend):
        try:
            if method == "requests":
                return _fetch_via_requests(url, timeout=timeout, trust_env=trust_env)
            if method == "urllib":
                return _fetch_via_urllib(url, timeout=timeout, trust_env=trust_env)
            if method == "curl":
                return _fetch_via_curl(url, timeout=timeout, trust_env=trust_env)
        except Exception as exc:  # pragma: no cover - backend failures are surfaced collectively
            errors.append(exc)
    if errors:
        raise errors[-1]
    raise RuntimeError("No HTTP backend is configured")


def _resolve_backends(backend: str) -> List[str]:
    if backend == "auto":
        return ["requests", "urllib", "curl"]
    return [backend]


def _fetch_via_requests(url: str, *, timeout: float, trust_env: bool) -> Any:
    if requests is None:
        raise RuntimeError("The requests library is not available")
    with requests.Session() as session:
        session.trust_env = trust_env
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()


def _fetch_via_urllib(url: str, *, timeout: float, trust_env: bool) -> Any:
    opener_args = () if trust_env else (urllib.request.ProxyHandler({}),)
    opener = urllib.request.build_opener(*opener_args)
    try:
        with opener.open(url, timeout=timeout) as response:  # type: ignore[arg-type]
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except urllib.error.URLError as exc:  # type: ignore[attr-defined]
        raise ConnectionError(f"Failed to fetch data from {url}: {exc}") from exc


def _fetch_via_curl(url: str, *, timeout: float, trust_env: bool) -> Any:
    max_time = max(1.0, timeout)
    cmd = [
        "curl",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        f"{max_time:g}",
    ]
    if not trust_env:
        cmd.extend(["--noproxy", "*"])
    cmd.append(url)
    try:
        completed = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError("curl command not found on PATH") from exc
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip()
        message = stderr or f"curl failed with exit code {exc.returncode}"
        raise ConnectionError(message) from exc
    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("curl returned invalid JSON") from exc


def iter_decode_items(node: Any) -> Iterable[Dict[str, Any]]:
    """Yield dict nodes that contain decode information regardless of nesting."""
    stack: List[Any] = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if "decode" in current and isinstance(current["decode"], dict):
                stack.extend(current.values())
                yield current
            else:
                stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)


def extract_scanner_results(payload: Any) -> Tuple[str, Dict[str, Dict[str, Any]]]:
    """Return (rack_id, position -> decode info) from the payload."""
    rack_id: Optional[str] = None
    wells: Dict[str, Dict[str, Any]] = {}

    for item in iter_decode_items(payload):
        item_type = item.get("itemType")
        decode = item.get("decode") or {}
        result = decode.get("result")
        if not result:
            continue

        if item_type == 2 and rack_id is None:
            rack_id = str(result).strip()
        elif item_type == 1:
            position_raw = str(item.get("id") or "").strip()
            if not position_raw:
                continue
            normalized = normalize_position(position_raw)
            wells[normalized] = {
                "result": str(result).strip(),
                "hasTube": bool(decode.get("hasTube", False)),
                "passed": bool(decode.get("passed", False)),
            }

    if rack_id is None:
        raise ValueError("Could not determine rack ID from the scanner payload")

    if not wells:
        raise ValueError("Scanner payload did not contain any tube decode entries")

    return rack_id, wells


def build_output_rows(
    layout: List[Tuple[str, str]],
    scanner_data: Dict[str, Dict[str, Any]],
) -> List[Tuple[str, str, str, str]]:
    """Combine layout entries with scanner reads.

    Returns rows as (Position, SampleID, ScannerResult, Status)
    where Status highlights missing tubes or decode failures.
    """
    rows: List[Tuple[str, str, str, str]] = []
    for position, sample_id in layout:
        scanner_entry = scanner_data.get(position)
        if not scanner_entry:
            rows.append((position, sample_id, "", "no_scan"))
            continue

        status_flags: List[str] = []
        if not scanner_entry.get("hasTube", False):
            status_flags.append("empty")
        if not scanner_entry.get("passed", False):
            status_flags.append("decode_failed")

        status = ",".join(status_flags) if status_flags else "ok"
        rows.append((position, sample_id, scanner_entry.get("result", ""), status))
    return rows


def write_output(rows: List[Tuple[str, str, str, str]], output_path: Path) -> None:
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Position", "SampleID", "ScannerResult", "Status"])
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    layout = read_layout(args.csv_path)

    if args.json_file:
        payload = json.loads(args.json_file.read_text(encoding="utf-8"))
    else:
        payload = fetch_payload(
            args.url,
            timeout=args.timeout,
            trust_env=args.trust_env_proxies,
            backend=args.backend,
        )

    rack_id, scanner_wells = extract_scanner_results(payload)

    expected_rack = Path(args.csv_path).stem
    if rack_id != expected_rack:
        raise RackMismatchError(
            f"Rack ID mismatch: scanned {rack_id!r} but expected {expected_rack!r}"
        )

    rows = build_output_rows(layout, scanner_wells)

    output_path = args.output
    if output_path is None:
        output_path = args.csv_path.with_name(f"{args.csv_path.stem}_paired.csv")

    write_output(rows, output_path)

    extra_positions = sorted(set(scanner_wells.keys()) - {pos for pos, _ in layout})
    if extra_positions:
        print(
            "Warning: scanner reported positions not present in the CSV layout: "
            + ", ".join(extra_positions),
            file=sys.stderr,
        )

    print(
        f"Matched {len(rows)} positions. Output written to {output_path} (scanned rack ID: {rack_id})."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RackMismatchError as err:
        print(f"ERROR: {err}", file=sys.stderr)
        sys.exit(2)
    except Exception as err:  # pragma: no cover - catch-all for CLI use
        print(f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)
