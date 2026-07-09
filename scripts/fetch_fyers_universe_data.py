from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from intraday_research.universe import load_universe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch FYERS historical data for every symbol in a research universe.")
    parser.add_argument("--auth-file", default="fyers_auth.json", help="Path to fyers_auth.json")
    parser.add_argument("--user-key", required=True, help="User key inside fyers_auth.json")
    parser.add_argument("--universe-file", required=True, help="Path to a JSON universe definition")
    parser.add_argument("--start", required=True, help="Start date in YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date in YYYY-MM-DD")
    parser.add_argument("--output-dir", default="data/fyers", help="Directory to write output files")
    parser.add_argument("--format", choices=["csv", "parquet"], default="parquet", help="Output file format")
    parser.add_argument("--chunk-days", type=int, default=30, help="Chunk size for FYERS history requests")
    parser.add_argument(
        "--refresh-auth-before-fetch",
        action="store_true",
        help="Refresh FYERS auth for the selected user before fetching the universe",
    )
    parser.add_argument(
        "--skip-invalid-symbols",
        action="store_true",
        help="Continue fetching the rest of the universe when FYERS rejects a symbol",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    universe = load_universe(args.universe_file)

    command = [
        sys.executable,
        str(REPO_ROOT / "scripts" / "fetch_fyers_intraday_data.py"),
        "--auth-file",
        args.auth_file,
        "--user-key",
        args.user_key,
        "--start",
        args.start,
        "--end",
        args.end,
        "--output-dir",
        args.output_dir,
        "--format",
        args.format,
        "--chunk-days",
        str(args.chunk_days),
    ]
    if args.refresh_auth_before_fetch:
        command.append("--refresh-auth-before-fetch")
    if args.skip_invalid_symbols:
        command.append("--skip-invalid-symbols")
    for symbol in universe.symbols:
        command.extend(["--symbol", symbol])

    print(f"Fetching universe {universe.name} with {len(universe.symbols)} symbols")
    subprocess.run(command, cwd=REPO_ROOT, check=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
