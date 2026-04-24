# Cloud Deploy Guide (#06)

## DB switching rule
- Local run: `LOCAL_DATABASE_URL` (default `sqlite:///investment.db`)
- Streamlit Cloud run: `CLOUD_DATABASE_URL` -> `DATABASE_URL` fallback

## Streamlit Cloud Secrets
Set at least:
- `APP_PASSWORD`
- `CLOUD_DATABASE_URL` (Supabase PostgreSQL URI)

Optional:
- `DATABASE_URL` (backward-compatible fallback)
- `DISCORD_WEBHOOK_URL`
- `FRED_API_KEY`
- `ESTAT_API_KEY`
- `GITHUB_REPOSITORY`
- `GITHUB_TOKEN`

## Local run
If you want SQLite locally, do not set `LOCAL_DATABASE_URL` (or set `sqlite:///investment.db`).

## Health check
```bash
python scripts/cloud_setup_check.py
```

## Streamlit
```bash
streamlit run app.py
```
