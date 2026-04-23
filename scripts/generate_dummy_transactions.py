from __future__ import annotations

import random
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import func

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.db_utils import init_db, rebuild_portfolio
from db.models import SessionLocal, Stock, Transaction


@dataclass
class PriceProfile:
    base: float
    daily_vol: float


PRICE_MAP = {
    "AAPL": PriceProfile(190, 0.022),
    "MSFT": PriceProfile(420, 0.018),
    "NVDA": PriceProfile(1050, 0.035),
    "AMZN": PriceProfile(185, 0.024),
    "GOOGL": PriceProfile(175, 0.02),
    "META": PriceProfile(500, 0.03),
    "JPM": PriceProfile(200, 0.018),
    "XOM": PriceProfile(115, 0.02),
    "UNH": PriceProfile(500, 0.017),
    "PG": PriceProfile(165, 0.012),
}


def is_weekday(d: date) -> bool:
    return d.weekday() < 5


def random_trade_date(start: date, end: date) -> date:
    while True:
        d = start + timedelta(days=random.randint(0, (end - start).days))
        if is_weekday(d):
            return d


def gen_price(ticker: str, elapsed_days: int) -> float:
    profile = PRICE_MAP.get(ticker, PriceProfile(150, 0.02))
    trend = 1.0 + (elapsed_days / 365.0) * random.uniform(-0.15, 0.25)
    noise = random.uniform(-profile.daily_vol, profile.daily_vol)
    px = profile.base * trend * (1.0 + noise)
    return round(max(8.0, px), 2)


def main() -> None:
    random.seed(42)
    init_db()

    today = date.today()
    start = today - timedelta(days=365)

    with SessionLocal() as session:
        stocks = [s.stock_code for s in session.query(Stock).order_by(Stock.stock_code).limit(20).all()]
        if not stocks:
            raise RuntimeError("stocks table is empty. Import stocks first.")

        session.query(Transaction).delete()
        session.commit()

        holdings: dict[str, int] = {s: 0 for s in stocks}
        tx_rows: list[Transaction] = []

        target_count = 200
        for _ in range(target_count):
            trade_day = random_trade_date(start, today)
            elapsed = (trade_day - start).days

            sellable = [s for s, q in holdings.items() if q > 0]
            do_sell = bool(sellable) and random.random() < 0.38

            if do_sell:
                ticker = random.choice(sellable)
                max_sell = max(1, min(holdings[ticker], 120))
                qty = random.randint(1, max_sell)
                trade_type = "売"
                holdings[ticker] -= qty
            else:
                ticker = random.choice(stocks)
                qty = random.randint(5, 150)
                trade_type = "買"
                holdings[ticker] += qty

            price = gen_price(ticker, elapsed)
            commission = round(random.uniform(0.3, 6.0), 2)
            memo = random.choice(["押し目買い", "決算前調整", "リバランス", "利益確定", "ニュース反応", "定期積立"])

            tx_rows.append(
                Transaction(
                    stock_code=ticker,
                    trade_type=trade_type,
                    quantity=qty,
                    price=price,
                    commission=commission,
                    trade_date=trade_day,
                    memo=memo,
                )
            )

        session.add_all(tx_rows)
        session.commit()
        rebuild_portfolio(session)

        total = session.query(func.count(Transaction.id)).scalar() or 0
        buy_count = session.query(func.count(Transaction.id)).filter(Transaction.trade_type == "買").scalar() or 0
        sell_count = session.query(func.count(Transaction.id)).filter(Transaction.trade_type == "売").scalar() or 0

    print(f"Dummy transactions generated: total={total}, buy={buy_count}, sell={sell_count}")


if __name__ == "__main__":
    main()
