import asyncio
import json
import os
from pathlib import Path

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.environ["DATABASE_URL"]
JSON_PATH = Path(os.getenv("BOT_DATA_PATH", "bot_data.json"))


async def main():
    if not JSON_PATH.exists():
        raise SystemExit(f"JSON file not found: {JSON_PATH}")

    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    conn = await asyncpg.connect(
        DATABASE_URL,
        ssl=os.getenv("DB_SSL", "require"),
        command_timeout=30,
    )
    try:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bot_subscribers (
                chat_id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bot_settings (
                key TEXT PRIMARY KEY,
                value DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bot_alerts (
                symbol TEXT PRIMARY KEY,
                last_alert_ts BIGINT NOT NULL DEFAULT 0,
                last_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                last_stage TEXT NOT NULL DEFAULT 'NONE',
                pending_downgrade_stage TEXT NOT NULL DEFAULT '',
                pending_downgrade_count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)

        subscribers = data.get("subscribers", {})
        if isinstance(subscribers, list):
            subscriber_ids = subscribers
        elif isinstance(subscribers, dict):
            subscriber_ids = list(subscribers.keys())
        else:
            subscriber_ids = []

        for chat_id in subscriber_ids:
            try:
                chat_id = int(chat_id)
            except (TypeError, ValueError):
                continue
            await conn.execute(
                "INSERT INTO bot_subscribers(chat_id) VALUES($1) ON CONFLICT DO NOTHING",
                chat_id,
            )

        for key, value in (data.get("settings", {}) or {}).items():
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            await conn.execute(
                """
                INSERT INTO bot_settings(key, value, updated_at)
                VALUES($1, $2, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value=EXCLUDED.value, updated_at=NOW()
                """,
                str(key), value,
            )

        for symbol, row in (data.get("alerts", {}) or {}).items():
            if not isinstance(row, dict):
                continue
            await conn.execute(
                """
                INSERT INTO bot_alerts(
                    symbol, last_alert_ts, last_score, last_stage,
                    pending_downgrade_stage, pending_downgrade_count, updated_at
                ) VALUES($1,$2,$3,$4,$5,$6,NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    last_alert_ts=EXCLUDED.last_alert_ts,
                    last_score=EXCLUDED.last_score,
                    last_stage=EXCLUDED.last_stage,
                    pending_downgrade_stage=EXCLUDED.pending_downgrade_stage,
                    pending_downgrade_count=EXCLUDED.pending_downgrade_count,
                    updated_at=NOW()
                """,
                str(symbol),
                int(row.get("last_alert_ts", 0) or 0),
                float(row.get("last_score", 0) or 0),
                str(row.get("last_stage", "NONE") or "NONE"),
                str(row.get("pending_downgrade_stage", "") or ""),
                int(row.get("pending_downgrade_count", 0) or 0),
            )

        print(
            f"Migration complete: {len(subscriber_ids)} subscribers, "
            f"{len(data.get('settings', {}) or {})} settings, "
            f"{len(data.get('alerts', {}) or {})} alert states."
        )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
