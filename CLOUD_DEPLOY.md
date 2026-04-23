# Cloud Deploy Guide (#06)

## 1. 必須Secrets
Streamlit Cloud / GitHub Secrets に以下を設定します。

- `DATABASE_URL` (Supabase PostgreSQL接続文字列)
- `APP_PASSWORD` (ログイン用)
- `DISCORD_WEBHOOK_URL` (任意)
- `GEMINI_API_KEY` (任意)
- `FRED_API_KEY` (任意)
- `ESTAT_API_KEY` (任意)
- `GITHUB_REPOSITORY` (管理ページでActionsログ表示する場合)
- `GITHUB_TOKEN` (同上)

## 2. 形式注意
- `postgres://` / `postgresql://` 形式でも `config.py` で自動変換されます。

## 3. デプロイ前チェック
```bash
python scripts/cloud_setup_check.py
```

## 4. 初期化
```bash
python scripts/bootstrap_cloud.py
```

## 5. Streamlit起動
```bash
streamlit run app.py
```

## 6. GitHub Actions
`.github/workflows/` に以下を追加済みです。
- `daily_collect.yml`
- `market_close_collect.yml`
- `nightly_briefing.yml`
- `daily_snapshot.yml`
- `weekly_backup.yml`
- `monthly_cleanup.yml`

