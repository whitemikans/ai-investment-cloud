from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from sqlalchemy import and_, func, text
from sqlalchemy.orm import Session

from .models import Dividend, Portfolio, SessionLocal, Snapshot, Stock, Transaction, create_all_tables, engine

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PORTFOLIO_CSV = PROJECT_ROOT / "portfolio_input.csv"

DEFAULT_STOCKS = [
    ("AAPL", "Apple", "Information Technology", "NASDAQ"),
    ("MSFT", "Microsoft", "Information Technology", "NASDAQ"),
    ("NVDA", "NVIDIA", "Information Technology", "NASDAQ"),
    ("GOOGL", "Alphabet", "Communication Services", "NASDAQ"),
    ("AMZN", "Amazon", "Consumer Discretionary", "NASDAQ"),
]


def _result_df(success: bool, message: str, **kwargs: object) -> pd.DataFrame:
    payload = {"success": success, "message": message}
    payload.update(kwargs)
    return pd.DataFrame([payload])


def get_session() -> Session:
    return SessionLocal()


def _ensure_stock(session: Session, stock_code: str) -> Stock:
    code = stock_code.strip().upper()
    stock = session.get(Stock, code)
    if stock is None:
        stock = Stock(stock_code=code, company_name=code, sector="Unknown", market="NASDAQ")
        session.add(stock)
        session.flush()
    return stock


def init_db() -> None:
    create_all_tables()
    with get_session() as session:
        if session.query(Stock).count() == 0:
            for code, name, sector, market in DEFAULT_STOCKS:
                session.add(Stock(stock_code=code, company_name=name, sector=sector, market=market))
            session.commit()

        if session.query(Transaction).count() == 0 and PORTFOLIO_CSV.exists():
            seed_transactions_from_csv(session, PORTFOLIO_CSV)


def seed_transactions_from_csv(session: Session, csv_path: Path) -> pd.DataFrame:
    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        return _result_df(False, f"CSV読込エラー: {exc}")

    count = 0
    for _, row in df.iterrows():
        code = str(row.get("ticker", "")).strip().upper()
        if not code:
            continue
        _ensure_stock(session, code)

        qty = int(float(row.get("shares", 0) or 0))
        price = float(row.get("avg_cost", 0) or 0)
        if qty <= 0:
            continue
        trade_date = pd.to_datetime(row.get("purchase_date", date.today())).date()

        session.add(
            Transaction(
                stock_code=code,
                trade_type="買",
                quantity=qty,
                price=price,
                commission=0.0,
                trade_date=trade_date,
                memo="CSV移行",
            )
        )
        count += 1

    session.commit()
    rebuild_portfolio(session)
    return _result_df(True, "CSV移行が完了しました。", inserted=count)


# ----------------------------
# Required APIs (DataFrame return)
# ----------------------------

def add_transaction(
    stock_code: str,
    trade_type: str,
    quantity: int,
    price: float,
    commission: float,
    trade_date: date,
    memo: str,
) -> pd.DataFrame:
    code = stock_code.strip().upper()
    ttype = trade_type.strip()

    if ttype not in {"買", "売"}:
        return _result_df(False, "trade_typeは'買'または'売'を指定してください。")
    if quantity <= 0 or price <= 0:
        return _result_df(False, "quantityとpriceは正の値が必要です。")

    with get_session() as session:
        stock = session.get(Stock, code)
        if stock is None:
            return _result_df(False, f"銘柄コード {code} はstocksテーブルに存在しません。")

        duplicate = (
            session.query(Transaction)
            .filter(
                Transaction.stock_code == code,
                Transaction.trade_date == trade_date,
                Transaction.quantity == int(quantity),
                Transaction.price == float(price),
            )
            .first()
        )
        if duplicate is not None:
            return _result_df(
                False,
                "同一銘柄・同一日付・同一株数・同一単価の取引が既に存在します。登録前に内容を確認してください。",
                requires_confirmation=True,
                duplicate_transaction_id=duplicate.id,
            )

        if ttype == "売":
            p = session.get(Portfolio, code)
            current_qty = int(p.total_quantity) if p else 0
            if current_qty < int(quantity):
                return _result_df(False, f"保有株数が不足しています（保有: {current_qty}株）。")

        is_future_date = trade_date > date.today()
        warning_message = (
            f"取引日 {trade_date.isoformat()} は未来日です。内容を確認してください。"
            if is_future_date
            else ""
        )

        tx = Transaction(
            stock_code=code,
            trade_type=ttype,
            quantity=int(quantity),
            price=float(price),
            commission=float(commission),
            trade_date=trade_date,
            memo=memo,
        )
        session.add(tx)
        session.commit()

        portfolio_df = update_portfolio(code, session=session)
        return _result_df(
            True,
            "取引を登録しました。",
            transaction_id=tx.id,
            stock_code=code,
            rows=len(portfolio_df),
            has_warning=is_future_date,
            warning_message=warning_message,
            requires_confirmation=False,
        )


def get_transactions(
    stock_code: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    trade_type: str | None = None,
) -> pd.DataFrame:
    with get_session() as session:
        q = session.query(Transaction, Stock.company_name, Stock.sector).join(Stock, Transaction.stock_code == Stock.stock_code)

        filters = []
        if stock_code:
            filters.append(Transaction.stock_code == stock_code.strip().upper())
        if start_date:
            filters.append(Transaction.trade_date >= start_date)
        if end_date:
            filters.append(Transaction.trade_date <= end_date)
        if trade_type and trade_type != "すべて":
            filters.append(Transaction.trade_type == trade_type)
        if filters:
            q = q.filter(and_(*filters))

        rows = []
        for tx, company_name, sector in q.order_by(Transaction.trade_date.desc(), Transaction.id.desc()).all():
            amount = float(tx.quantity * tx.price)
            rows.append(
                {
                    "id": tx.id,
                    "stock_code": tx.stock_code,
                    "company_name": company_name,
                    "sector": sector,
                    "trade_type": tx.trade_type,
                    "quantity": tx.quantity,
                    "price": tx.price,
                    "amount": amount,
                    "commission": tx.commission,
                    "trade_date": tx.trade_date,
                    "memo": tx.memo,
                }
            )
        return pd.DataFrame(rows)


def update_portfolio(stock_code: str, session: Session | None = None) -> pd.DataFrame:
    code = stock_code.strip().upper()
    external = session is not None
    if session is None:
        session = get_session()

    try:
        txs = (
            session.query(Transaction)
            .filter(Transaction.stock_code == code)
            .order_by(Transaction.trade_date.asc(), Transaction.id.asc())
            .all()
        )

        buy_qty = 0.0
        buy_cost = 0.0
        sell_qty = 0.0
        for tx in txs:
            if tx.trade_type == "買":
                buy_qty += float(tx.quantity)
                buy_cost += float(tx.quantity * tx.price + tx.commission)
            else:
                sell_qty += float(tx.quantity)

        total_qty = int(round(buy_qty - sell_qty))
        existing = session.get(Portfolio, code)

        if total_qty <= 0:
            if existing is not None:
                session.delete(existing)
                session.commit()
            return pd.DataFrame([{"stock_code": code, "total_quantity": 0, "avg_price": 0.0, "total_cost": 0.0}])

        avg_price = float(buy_cost / buy_qty) if buy_qty > 0 else 0.0
        total_cost = float(avg_price * total_qty)

        if existing is None:
            existing = Portfolio(stock_code=code)
            session.add(existing)

        existing.total_quantity = total_qty
        existing.avg_price = avg_price
        existing.total_cost = total_cost
        existing.updated_at = datetime.utcnow()
        session.commit()

        return pd.DataFrame(
            [
                {
                    "stock_code": code,
                    "total_quantity": total_qty,
                    "avg_price": avg_price,
                    "total_cost": total_cost,
                    "updated_at": existing.updated_at,
                }
            ]
        )
    finally:
        if not external:
            session.close()


def get_portfolio() -> pd.DataFrame:
    with get_session() as session:
        rows = (
            session.query(Portfolio, Stock.company_name, Stock.sector, Stock.market)
            .join(Stock, Portfolio.stock_code == Stock.stock_code)
            .order_by(Portfolio.stock_code.asc())
            .all()
        )

    data = []
    for p, company_name, sector, market in rows:
        data.append(
            {
                "stock_code": p.stock_code,
                "company_name": company_name,
                "sector": sector,
                "market": market,
                "total_quantity": int(p.total_quantity),
                "avg_price": float(p.avg_price),
                "total_cost": float(p.total_cost),
                "updated_at": p.updated_at,
            }
        )
    return pd.DataFrame(data)


def add_dividend(
    stock_code: str,
    amount_per_share: float,
    total_amount: float,
    tax_amount: float,
    ex_date: date | None,
    payment_date: date,
) -> pd.DataFrame:
    code = stock_code.strip().upper()

    if amount_per_share <= 0 or total_amount < 0 or tax_amount < 0:
        return _result_df(False, "amount_per_share/total_amount/tax_amountの値が不正です。")

    shares = int(round(total_amount / amount_per_share)) if amount_per_share > 0 else 0
    gross = float(total_amount)
    net = float(total_amount - tax_amount)

    with get_session() as session:
        _ensure_stock(session, code)
        div = Dividend(
            stock_code=code,
            dividend_per_share=float(amount_per_share),
            shares=max(shares, 0),
            tax_withheld=float(tax_amount),
            record_date=ex_date,
            payment_date=payment_date,
            gross_amount=gross,
            net_amount=net,
        )
        session.add(div)
        session.commit()
        return _result_df(True, "配当金を登録しました。", dividend_id=div.id, stock_code=code)


def get_dividends(year: int | None = None) -> pd.DataFrame:
    backend = engine.url.get_backend_name().lower()
    with get_session() as session:
        q = session.query(Dividend, Stock.company_name).join(Stock, Dividend.stock_code == Stock.stock_code)
        if year is not None:
            if backend == "sqlite":
                q = q.filter(func.strftime("%Y", Dividend.payment_date) == str(year))
            else:
                q = q.filter(func.extract("year", Dividend.payment_date) == int(year))

        rows = []
        for div, company_name in q.order_by(Dividend.payment_date.desc(), Dividend.id.desc()).all():
            rows.append(
                {
                    "id": div.id,
                    "stock_code": div.stock_code,
                    "company_name": company_name,
                    "amount_per_share": float(div.dividend_per_share),
                    "shares": int(div.shares),
                    "total_amount": float(div.gross_amount),
                    "tax_amount": float(div.tax_withheld),
                    "net_amount": float(div.net_amount),
                    "ex_date": div.record_date,
                    "payment_date": div.payment_date,
                }
            )
        return pd.DataFrame(rows)


def save_snapshot(
    total_value: float,
    total_cost: float,
    unrealized_pl: float,
    realized_pl: float,
    snapshot_date: date | None = None,
) -> pd.DataFrame:
    with get_session() as session:
        snap_date = snapshot_date or date.today()
        row = Snapshot(
            snapshot_date=snap_date,
            total_market_value=float(total_value),
            total_invested=float(total_cost),
            unrealized_pnl=float(unrealized_pl),
            realized_pnl=float(realized_pl),
        )
        session.add(row)
        session.commit()
        payload = {
            "snapshot_date": row.snapshot_date,
            "total_value": float(row.total_market_value),
            "total_cost": float(row.total_invested),
            "unrealized_pl": float(row.unrealized_pnl),
            "realized_pl": float(row.realized_pnl),
        }

    return pd.DataFrame([payload])


def get_snapshots(period: str = "monthly") -> pd.DataFrame:
    with get_session() as session:
        rows = session.query(Snapshot).order_by(Snapshot.snapshot_date.asc()).all()

    if not rows:
        return pd.DataFrame(columns=["snapshot_date", "total_value", "total_cost", "unrealized_pl", "realized_pl"])

    df = pd.DataFrame(
        [
            {
                "snapshot_date": r.snapshot_date,
                "total_value": float(r.total_market_value),
                "total_cost": float(r.total_invested),
                "unrealized_pl": float(r.unrealized_pnl),
                "realized_pl": float(r.realized_pnl),
            }
            for r in rows
        ]
    )
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"])
    df = df.sort_values("snapshot_date")

    rule_map = {"daily": "D", "weekly": "W", "monthly": "ME"}
    rule = rule_map.get(period, "ME")

    grouped = (
        df.set_index("snapshot_date")
        .resample(rule)
        .last()
        .dropna(how="all")
        .reset_index()
    )
    grouped["snapshot_date"] = grouped["snapshot_date"].dt.date
    return grouped


# ----------------------------
# Compatibility helpers for existing pages/scripts
# ----------------------------

def rebuild_portfolio(session: Session | None = None) -> pd.DataFrame:
    external = session is not None
    if session is None:
        session = get_session()

    try:
        stock_codes = [row[0] for row in session.query(Transaction.stock_code).distinct().all()]
        frames: list[pd.DataFrame] = []
        for code in stock_codes:
            frames.append(update_portfolio(code, session=session))

        current_codes = {code for code in stock_codes}
        for p in session.query(Portfolio).all():
            if p.stock_code not in current_codes:
                session.delete(p)
        session.commit()

        if not frames:
            return pd.DataFrame(columns=["stock_code", "total_quantity", "avg_price", "total_cost"])
        return pd.concat(frames, ignore_index=True)
    finally:
        if not external:
            session.close()


def list_stocks() -> list[Stock]:
    with get_session() as session:
        return session.query(Stock).order_by(Stock.stock_code).all()


def get_portfolio_base_df() -> pd.DataFrame:
    df = get_portfolio()
    if df.empty:
        return pd.DataFrame(columns=["ticker", "company_name", "sector", "avg_cost", "shares"])
    return df.rename(
        columns={
            "stock_code": "ticker",
            "total_quantity": "shares",
            "avg_price": "avg_cost",
        }
    )[["ticker", "company_name", "sector", "avg_cost", "shares"]]


def get_transactions_df(
    stock_code: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    trade_type: str | None = None,
) -> pd.DataFrame:
    df = get_transactions(stock_code=stock_code, start_date=start_date, end_date=end_date, trade_type=trade_type)
    if df.empty:
        return pd.DataFrame(columns=["約定日", "銘柄コード", "企業名", "セクター", "売買", "株数", "単価", "金額", "手数料", "メモ"])

    return df.rename(
        columns={
            "trade_date": "約定日",
            "stock_code": "銘柄コード",
            "company_name": "企業名",
            "sector": "セクター",
            "trade_type": "売買",
            "quantity": "株数",
            "price": "単価",
            "amount": "金額",
            "commission": "手数料",
            "memo": "メモ",
        }
    )[["約定日", "銘柄コード", "企業名", "セクター", "売買", "株数", "単価", "金額", "手数料", "メモ"]]


def get_dividends_df(year: int | None = None) -> pd.DataFrame:
    df = get_dividends(year=year)
    if df.empty:
        return pd.DataFrame(columns=["支払日", "銘柄コード", "企業名", "1株配当", "株数", "税引前", "税額", "税引後"])

    return df.rename(
        columns={
            "payment_date": "支払日",
            "stock_code": "銘柄コード",
            "company_name": "企業名",
            "amount_per_share": "1株配当",
            "shares": "株数",
            "total_amount": "税引前",
            "tax_amount": "税額",
            "net_amount": "税引後",
        }
    )[["支払日", "銘柄コード", "企業名", "1株配当", "株数", "税引前", "税額", "税引後"]]


def ensure_dummy_dividends() -> pd.DataFrame:
    with get_session() as session:
        if session.query(Dividend).count() > 0:
            return _result_df(True, "既に配当データがあります。", inserted=0)

    stocks = list_stocks()
    if not stocks:
        return _result_df(False, "stocksテーブルが空です。")

    today = date.today()
    inserted = 0
    for i in range(12):
        stock_code = stocks[i % len(stocks)].stock_code
        pay = today - timedelta(days=30 * i)
        amount_per_share = 0.2 + (i % 4) * 0.08
        shares = 50 + (i % 3) * 25
        total_amount = amount_per_share * shares
        tax_amount = total_amount * 0.20315
        result = add_dividend(
            stock_code=stock_code,
            amount_per_share=amount_per_share,
            total_amount=total_amount,
            tax_amount=tax_amount,
            ex_date=pay - timedelta(days=20),
            payment_date=pay,
        )
        if not result.empty and bool(result.iloc[0]["success"]):
            inserted += 1

    return _result_df(True, "ダミー配当データを作成しました。", inserted=inserted)


def create_snapshot() -> pd.DataFrame:
    return record_snapshot()


def _calculate_realized_pnl() -> float:
    """Estimate realized P/L from transactions table using average buy cost per ticker."""
    tx_df = get_transactions()
    if tx_df.empty:
        return 0.0

    realized_total = 0.0
    for code, g in tx_df.groupby("stock_code"):
        buys = g[g["trade_type"] == "買"]
        sells = g[g["trade_type"] == "売"]
        if buys.empty or sells.empty:
            continue

        buy_qty = float(buys["quantity"].sum())
        if buy_qty <= 0:
            continue
        buy_cost = float((buys["quantity"] * buys["price"] + buys["commission"]).sum())
        avg_buy_price = buy_cost / buy_qty
        sell_proceeds = float((sells["quantity"] * sells["price"] - sells["commission"]).sum())
        sold_qty = float(sells["quantity"].sum())
        realized_total += sell_proceeds - avg_buy_price * sold_qty
    return float(realized_total)


def record_snapshot(snapshot_date: date | None = None) -> pd.DataFrame:
    """Record a snapshot from current portfolio market prices and save it to snapshots table."""
    portfolio_df = get_portfolio_df_with_price()
    if portfolio_df.empty:
        return _result_df(False, "portfolioが空です。")

    total_value = float(portfolio_df["market_value"].fillna(0).sum())
    total_cost = float(portfolio_df["total_cost"].fillna(0).sum())
    unrealized_pl = total_value - total_cost
    realized_pl = _calculate_realized_pnl()

    return save_snapshot(
        total_value=total_value,
        total_cost=total_cost,
        unrealized_pl=unrealized_pl,
        realized_pl=realized_pl,
        snapshot_date=snapshot_date,
    )


def generate_dummy_snapshots(days: int = 180, overwrite: bool = False) -> pd.DataFrame:
    """Generate daily dummy snapshots for the past N days."""
    import random

    with get_session() as session:
        if overwrite:
            session.query(Snapshot).delete()
            session.commit()
        elif session.query(Snapshot).count() > 0:
            return _result_df(True, "既にスナップショットが存在するため生成をスキップしました。", inserted=0)

    base = record_snapshot()
    if base.empty or (not bool(base.iloc[0].get("success", True)) and "total_value" not in base.columns):
        portfolio_df = get_portfolio_df_with_price()
        if portfolio_df.empty:
            return _result_df(False, "portfolioが空のためダミースナップショットを生成できません。")
        current_total_value = float(portfolio_df["market_value"].fillna(0).sum())
        current_total_cost = float(portfolio_df["total_cost"].fillna(0).sum())
        current_realized = 0.0
    else:
        current_total_value = float(base.iloc[0].get("total_value", 0.0))
        current_total_cost = float(base.iloc[0].get("total_cost", 0.0))
        current_realized = float(base.iloc[0].get("realized_pl", 0.0))

    random.seed(42)
    today = date.today()
    rows = []
    for i in range(days, 0, -1):
        d = today - timedelta(days=i)
        trend = 0.88 + (days - i) / max(days, 1) * 0.12
        value_noise = 1.0 + random.uniform(-0.025, 0.025)
        cost_noise = 1.0 + random.uniform(-0.01, 0.01)

        total_cost = max(0.0, current_total_cost * trend * cost_noise)
        total_value = max(0.0, current_total_value * trend * value_noise)
        unrealized = total_value - total_cost
        realized = current_realized * ((days - i) / max(days, 1))
        rows.append((d, total_value, total_cost, unrealized, realized))

    with get_session() as session:
        for d, total_value, total_cost, unrealized, realized in rows:
            session.add(
                Snapshot(
                    snapshot_date=d,
                    total_market_value=float(total_value),
                    total_invested=float(total_cost),
                    unrealized_pnl=float(unrealized),
                    realized_pnl=float(realized),
                )
            )
        session.commit()

    return _result_df(True, "ダミースナップショットを生成しました。", inserted=len(rows))


def get_monthly_trade_count(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["month", "count"])
    work = df.copy()
    work["約定日"] = pd.to_datetime(work["約定日"])
    result = (
        work.assign(month=work["約定日"].dt.strftime("%Y-%m"))
        .groupby("month", as_index=False)
        .size()
        .rename(columns={"size": "count"})
    )
    return result


# ----------------------------
# pandas.read_sql based helpers
# ----------------------------
def get_transactions_df_sql(
    stock_code: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    trade_type: str | None = None,
) -> pd.DataFrame:
    """Read transactions list with optional filters using pandas.read_sql."""
    sql = """
    SELECT
      t.id,
      t.trade_date,
      t.stock_code,
      s.company_name,
      s.sector,
      t.trade_type,
      t.quantity,
      t.price,
      (t.quantity * t.price) AS amount,
      t.commission,
      t.memo
    FROM transactions t
    JOIN stocks s ON s.stock_code = t.stock_code
    WHERE 1=1
    """
    params: dict[str, object] = {}
    if stock_code:
        sql += " AND t.stock_code = :stock_code"
        params["stock_code"] = stock_code.strip().upper()
    if start_date:
        sql += " AND t.trade_date >= :start_date"
        params["start_date"] = str(start_date)
    if end_date:
        sql += " AND t.trade_date <= :end_date"
        params["end_date"] = str(end_date)
    if trade_type and trade_type != "すべて":
        sql += " AND t.trade_type = :trade_type"
        params["trade_type"] = trade_type
    sql += " ORDER BY t.trade_date DESC, t.id DESC"
    return pd.read_sql(text(sql), con=engine, params=params)


def get_portfolio_df_with_price() -> pd.DataFrame:
    """Read current portfolio and append current price/market value/PnL columns."""
    sql = """
    SELECT
      p.stock_code,
      s.company_name,
      s.sector,
      s.market,
      p.total_quantity,
      p.avg_price,
      p.total_cost,
      p.updated_at
    FROM portfolio p
    JOIN stocks s ON s.stock_code = p.stock_code
    ORDER BY p.stock_code
    """
    df = pd.read_sql(text(sql), con=engine)
    if df.empty:
        return df

    current_prices: list[float] = []
    for code in df["stock_code"].tolist():
        price = None
        try:
            info = yf.Ticker(str(code)).info or {}
            price = info.get("regularMarketPrice")
        except Exception:
            price = None
        if price is None:
            price = float("nan")
        current_prices.append(float(price))

    df["current_price"] = current_prices
    df["market_value"] = df["current_price"] * df["total_quantity"]
    df["unrealized_pl"] = df["market_value"] - df["total_cost"]
    df["unrealized_pl_pct"] = df.apply(
        lambda r: (r["unrealized_pl"] / r["total_cost"] * 100) if r["total_cost"] not in (0, None) else float("nan"),
        axis=1,
    )
    return df


def get_monthly_investment_amount_df() -> pd.DataFrame:
    """Read monthly investment amount summary (buy/sell/net) using pandas.read_sql."""
    backend = engine.url.get_backend_name().lower()
    if backend == "sqlite":
        sql = """
        SELECT
          strftime('%Y-%m', trade_date) AS month,
          SUM(CASE WHEN trade_type='買' THEN quantity * price + commission ELSE 0 END) AS buy_amount,
          SUM(CASE WHEN trade_type='売' THEN quantity * price - commission ELSE 0 END) AS sell_amount,
          SUM(CASE
                WHEN trade_type='買' THEN -(quantity * price + commission)
                ELSE  (quantity * price - commission)
              END) AS net_cash_flow
        FROM transactions
        GROUP BY strftime('%Y-%m', trade_date)
        ORDER BY month
        """
    else:
        sql = """
        SELECT
          to_char(trade_date, 'YYYY-MM') AS month,
          SUM(CASE WHEN trade_type='買' THEN quantity * price + commission ELSE 0 END) AS buy_amount,
          SUM(CASE WHEN trade_type='売' THEN quantity * price - commission ELSE 0 END) AS sell_amount,
          SUM(CASE
                WHEN trade_type='買' THEN -(quantity * price + commission)
                ELSE  (quantity * price - commission)
              END) AS net_cash_flow
        FROM transactions
        GROUP BY to_char(trade_date, 'YYYY-MM')
        ORDER BY month
        """
    return pd.read_sql(text(sql), con=engine)


def get_sector_holding_ratio_df() -> pd.DataFrame:
    """Read sector allocation ratio based on current portfolio cost basis using pandas.read_sql."""
    sql = """
    SELECT
      s.sector,
      SUM(p.total_cost) AS total_cost
    FROM portfolio p
    JOIN stocks s ON s.stock_code = p.stock_code
    GROUP BY s.sector
    ORDER BY total_cost DESC
    """
    df = pd.read_sql(text(sql), con=engine)
    if df.empty:
        return df
    total = float(df["total_cost"].sum())
    df["holding_ratio_pct"] = df["total_cost"] / total * 100 if total > 0 else 0.0
    return df
