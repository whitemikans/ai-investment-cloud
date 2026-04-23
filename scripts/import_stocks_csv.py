from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.models import SessionLocal, Stock, create_all_tables

REQUIRED_COLUMNS = {"stock_code", "company_name", "sector", "market"}


def normalize_row(row: pd.Series) -> dict[str, str]:
    return {
        "stock_code": str(row["stock_code"]).strip().upper(),
        "company_name": str(row["company_name"]).strip(),
        "sector": str(row["sector"]).strip(),
        "market": str(row["market"]).strip().upper(),
    }


def import_stocks_from_csv(csv_path: Path) -> tuple[int, int]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {', '.join(sorted(missing))}")

    create_all_tables()

    inserted = 0
    updated = 0
    with SessionLocal() as session:
        for _, raw in df.iterrows():
            row = normalize_row(raw)
            if not row["stock_code"] or not row["company_name"]:
                continue

            stock = session.get(Stock, row["stock_code"])
            if stock is None:
                stock = Stock(**row)
                session.add(stock)
                inserted += 1
            else:
                stock.company_name = row["company_name"]
                stock.sector = row["sector"]
                stock.market = row["market"]
                updated += 1

        session.commit()

    return inserted, updated


def main() -> None:
    parser = argparse.ArgumentParser(description="Import stocks CSV into stocks table")
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("data/us_stocks_top50.csv"),
        help="Path to CSV file (default: data/us_stocks_top50.csv)",
    )
    args = parser.parse_args()

    inserted, updated = import_stocks_from_csv(args.csv)
    print(f"Import completed: inserted={inserted}, updated={updated}, source={args.csv}")


if __name__ == "__main__":
    main()
