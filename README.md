# Crypto Awakening Scanner — Railway + Supabase

This build replaces local `bot_data.json` persistence with Supabase Postgres.

## What is stored in Supabase

- `bot_subscribers` — Telegram chat IDs subscribed to alerts
- `bot_settings` — editable `/set` settings
- `bot_alerts` — last alerted stage/score and downgrade-confirmation state

The scanner still reads market data from Binance and MEXC and uses Telegram long polling.

## Required environment variables

- `TELEGRAM_BOT_TOKEN` — token from BotFather
- `ADMIN_IDS` — comma-separated Telegram numeric user IDs
- `DATABASE_URL` — Supabase Postgres **Session pooler** connection string
- `DB_SSL=require` — enforce TLS to Postgres
- `DB_POOL_MAX=5` — max application DB connections
- `MAX_CONCURRENCY=8` — market-data concurrency
- `LOG_LEVEL=INFO`

## Supabase setup

1. Create a Supabase project.
2. Open **Connect** in the project dashboard.
3. Choose **Session pooler** and copy the Postgres URI.
4. Replace the password placeholder with your database password. URL-encode reserved characters in the password.
5. Optionally run `supabase_schema.sql` in Supabase SQL Editor. The bot also creates the tables automatically on first start.

## Migrate existing bot_data.json

Do this locally before switching production traffic:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create a local `.env` with at least:

```env
DATABASE_URL=postgresql://...
DB_SSL=require
```

Put your existing `bot_data.json` next to the migration script, then run:

```bash
python migrate_json_to_supabase.py
```

Verify rows in Supabase Table Editor.

## Railway deployment

Recommended flow: push this folder to a GitHub repository and create a Railway service from that repository.
Railway will detect the `Dockerfile`; no web port or public domain is required because this is a long-running worker using Telegram polling.

In Railway -> service -> Variables, add:

```env
TELEGRAM_BOT_TOKEN=...
ADMIN_IDS=...
DATABASE_URL=...
DB_SSL=require
DB_POOL_MAX=5
MAX_CONCURRENCY=8
LOG_LEVEL=INFO
```

Deploy. The Docker container starts with:

```bash
python crypto_awakening_scanner.py
```

Before the Railway deployment begins polling Telegram, stop the local copy of the bot. Only one polling instance should use the same bot token.

## Verify production

In Railway logs, look for:

- `Supabase/Postgres store initialized`
- `Bot initialized`

Then in Telegram test:

- `/settings`
- `/top`
- `/history LSK`
- `/subscribe`

Change a harmless setting and change it back to verify persistence across a Railway redeploy.

## Local development against Supabase

Copy `.env.example` to `.env`, fill secrets, then:

```bash
pip install -r requirements.txt
python crypto_awakening_scanner.py
```

Never commit `.env` or the database password.
