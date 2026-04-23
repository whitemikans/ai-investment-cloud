from db.db_utils import init_db, ensure_dummy_dividends

if __name__ == "__main__":
    init_db()
    ensure_dummy_dividends()
    print("DB initialized: investment.db")
