import asyncio
import json
import logging
import math
import os

from dotenv import load_dotenv
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from statistics import mean, median
from typing import Any

import httpx
import asyncpg
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

load_dotenv()

BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
DATABASE_URL = os.environ["DATABASE_URL"]
ADMIN_IDS = {
    int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()
}

BINANCE = "https://fapi.binance.com"
MEXC = "https://api.mexc.com"
HTTP_TIMEOUT = 15.0
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "8"))

# Defaults are deliberately tuned toward the FORM/SOPH pattern:
# derivatives participation increases before price fully expands.
DEFAULTS: dict[str, float] = {
    "oi_change_pct": 10.0,
    "oi_lookback_days": 3.0,
    "volume_ratio": 1.50,
    "max_price_change_pct": 15.0,  # legacy setting; no longer used for lifecycle cutoff
    "late_move_price_pct": 30.0,
    "ignition_price_pct": 10.0,
    "expansion_price_pct": 20.0,
    "max_positive_funding_pct": 0.05,
    "short_squeeze_funding_pct": -0.03,
    "min_compression_ratio": 1.10,
    "min_quote_volume_usdt": 1_000_000.0,
    "scan_interval_min": 15.0,
    "alert_cooldown_hours": 12.0,
    "downgrade_confirm_scans": 2.0,
    "top_n": 10.0,
    "atr_recent_days": 7.0,
    "atr_baseline_days": 30.0,
    # SOPH-derived additions
    "oi_efficiency_floor_pct": 2.0,
    "oi_eff_watch": 3.0,
    "oi_eff_awakening": 5.0,
    "oi_eff_high": 8.0,
    "persistence_days": 4.0,
    "persistence_min_up_days": 3.0,
    "high_oi_change_pct": 20.0,
    # VVV-derived OI-shock path. Volume is intentionally not a hard gate.
    "oi_shock_pct": 50.0,
    "oi_shock_high_pct": 100.0,
    "oi_shock_efficiency": 8.0,
    "oi_shock_high_efficiency": 10.0,
    "high_volume_ratio": 2.0,
    # CVC-derived parallel volume-accumulation detector.
    "volume_accum_ratio": 4.0,
    "volume_accum_max_abs_price_pct": 8.0,
    "volume_accum_min_oi_change_pct": -10.0,
    "volume_accum_watch_score": 65.0,
    # v8: intraday OI Velocity detector.
    "oi_velocity_1h_pct": 5.0,
    "oi_velocity_4h_pct": 10.0,
    "oi_velocity_24h_pct": 20.0,
    "oi_velocity_watch_score": 65.0,
    "oi_velocity_fast_score": 80.0,
    "oi_velocity_max_abs_price_24h_pct": 10.0,
    "oi_velocity_confirmation_volume_ratio": 1.5,
    "watch_score": 55.0,
    "awakening_score": 70.0,
    "high_score": 82.0,
    "breakout_lookback_days": 20.0,
    "breakout_near_pct": 8.0,
    "historical_atr_target_pct": 8.0,
}

EDITABLE = {
    "oi": "oi_change_pct",
    "oilookback": "oi_lookback_days",
    "volume": "volume_ratio",
    "price": "late_move_price_pct",
    "latemove": "late_move_price_pct",
    "ignitionprice": "ignition_price_pct",
    "expansionprice": "expansion_price_pct",
    "funding": "max_positive_funding_pct",
    "squeezefunding": "short_squeeze_funding_pct",
    "compression": "min_compression_ratio",
    "minvolume": "min_quote_volume_usdt",
    "interval": "scan_interval_min",
    "cooldown": "alert_cooldown_hours",
    "downgradeconfirm": "downgrade_confirm_scans",
    "top": "top_n",
    "efffloor": "oi_efficiency_floor_pct",
    "effwatch": "oi_eff_watch",
    "effawake": "oi_eff_awakening",
    "effhigh": "oi_eff_high",
    "persistdays": "persistence_days",
    "persistup": "persistence_min_up_days",
    "highoi": "high_oi_change_pct",
    "oishock": "oi_shock_pct",
    "oishockhigh": "oi_shock_high_pct",
    "shockeff": "oi_shock_efficiency",
    "shockeffhigh": "oi_shock_high_efficiency",
    "highvolume": "high_volume_ratio",
    "volaccum": "volume_accum_ratio",
    "volaccumprice": "volume_accum_max_abs_price_pct",
    "volaccumoi": "volume_accum_min_oi_change_pct",
    "volaccumscore": "volume_accum_watch_score",
    "velocity1h": "oi_velocity_1h_pct",
    "velocity4h": "oi_velocity_4h_pct",
    "velocity24h": "oi_velocity_24h_pct",
    "velocityscore": "oi_velocity_watch_score",
    "fastscore": "oi_velocity_fast_score",
    "velocityprice": "oi_velocity_max_abs_price_24h_pct",
    "velocityvolume": "oi_velocity_confirmation_volume_ratio",
    "watchscore": "watch_score",
    "awakescore": "awakening_score",
    "highscore": "high_score",
    "breakoutdays": "breakout_lookback_days",
    "breakoutnear": "breakout_near_pct",
    "histatr": "historical_atr_target_pct",
}

HELP_TEXT = (
    "<b>FORM/SOPH Signal Bot</b>\n\n"
    "Сканирует Binance USDT-M perpetuals, которые одновременно имеют MEXC futures. "
    "Главная идея: искать рост участия в деривативах до полного движения цены — "
    "OI/Price divergence, OI Efficiency, устойчивый рост OI и OI Shock. Параллельно работают Volume Accumulation detector и OI Velocity 1h/4h/24h для быстрых PUNDIX-подобных пробуждений.\n\n"
    "<b>Стадии</b>\n"
    "🟣 VOLUME WATCH — объём резко вырос, пока цена почти стоит и OI не разваливается\n"
    "⚡ VELOCITY WATCH — OI резко ускорился за 1h/4h/24h\n"
    "⚡🔥 FAST AWAKENING — OI Velocity подтверждён контекстом\n"
    "🟡 WATCH — раннее расхождение OI и цены\n"
    "🟠 AWAKENING — подтверждённое накопление OI до импульса\n"
    "🔥 IGNITION — цена начала реализовывать накопленный OI\n"
    "🚀 EXPANSION — импульс уже развивается, но ещё не вышел за late-move limit\n\n"
    "<b>Команды</b>\n"
    "/subscribe — получать алерты\n"
    "/unsubscribe — отключить алерты\n"
    "/top — текущий TOP активных сигналов\n"
    "/rawtop — диагностический TOP включая NONE\n"
    "/history LSK — дневная история Price/OI для монеты из последнего скана\n"
    "/scan — ручной скан (admin)\n"
    "/settings — текущие параметры\n"
    "/set &lt;параметр&gt; &lt;значение&gt; — изменить параметр (admin)\n"
    "/menu — кнопки\n\n"
    "Примеры:\n"
    "<code>/set oi 12</code>\n"
    "<code>/set effawake 5</code>\n"
    "<code>/set effhigh 8</code>\n"
    "<code>/set persistdays 4</code>\n"
    "<code>/set persistup 3</code>\n"
    "<code>/set highoi 20</code>\n"
    "<code>/set oishock 50</code>\n"
    "<code>/set oishockhigh 100</code>\n\n"
    "Все параметры: " + ", ".join(EDITABLE.keys())
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("form-soph-vvv-signal-bot")


@dataclass
class Signal:
    symbol: str
    price: float
    price_change_pct: float
    oi_change_pct: float
    volume_ratio: float
    volume_score: float
    volume_accumulation: bool
    oi_velocity_1h_pct: float
    oi_velocity_4h_pct: float
    oi_velocity_24h_pct: float
    velocity_score: float
    velocity_watch: bool
    fast_awakening: bool
    price_24h_pct: float
    funding_pct: float
    compression_ratio: float
    quote_volume: float
    oi_efficiency: float
    persistence_up_days: int
    persistence_total_days: int
    persistence_ratio: float
    distance_to_breakout_pct: float
    historical_atr_pct: float
    score: float
    stage: str
    history: list[dict[str, Any]]


class Store:
    """Supabase/Postgres-backed state store for Railway deployments."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: asyncpg.Pool | None = None

    async def init(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.database_url,
            min_size=1,
            max_size=max(2, int(os.getenv("DB_POOL_MAX", "5"))),
            command_timeout=30,
            ssl=os.getenv("DB_SSL", "require"),
        )
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_subscribers (
                    chat_id BIGINT PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value DOUBLE PRECISION NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_alerts (
                    symbol TEXT PRIMARY KEY,
                    last_alert_ts BIGINT NOT NULL DEFAULT 0,
                    last_score DOUBLE PRECISION NOT NULL DEFAULT 0,
                    last_stage TEXT NOT NULL DEFAULT 'NONE',
                    pending_downgrade_stage TEXT NOT NULL DEFAULT '',
                    pending_downgrade_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            for key, value in DEFAULTS.items():
                await conn.execute(
                    """
                    INSERT INTO bot_settings(key, value)
                    VALUES($1, $2)
                    ON CONFLICT (key) DO NOTHING
                    """,
                    key, float(value),
                )
        log.info("Supabase/Postgres store initialized")

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    def _pool(self) -> asyncpg.Pool:
        if self.pool is None:
            raise RuntimeError("Store is not initialized")
        return self.pool

    async def settings(self) -> dict[str, float]:
        rows = await self._pool().fetch("SELECT key, value FROM bot_settings")
        out = DEFAULTS.copy()
        for row in rows:
            if row["key"] in DEFAULTS:
                out[row["key"]] = float(row["value"])
        return out

    async def set_setting(self, key: str, value: float):
        await self._pool().execute(
            """
            INSERT INTO bot_settings(key, value, updated_at)
            VALUES($1, $2, NOW())
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value, updated_at = NOW()
            """,
            key, float(value),
        )

    async def subscribe(self, chat_id: int):
        await self._pool().execute(
            """
            INSERT INTO bot_subscribers(chat_id)
            VALUES($1)
            ON CONFLICT (chat_id) DO NOTHING
            """,
            int(chat_id),
        )

    async def unsubscribe(self, chat_id: int):
        await self._pool().execute(
            "DELETE FROM bot_subscribers WHERE chat_id=$1", int(chat_id)
        )

    async def subscribers(self) -> list[int]:
        rows = await self._pool().fetch(
            "SELECT chat_id FROM bot_subscribers ORDER BY created_at"
        )
        return [int(row["chat_id"]) for row in rows]

    async def alert_decision(
        self,
        symbol: str,
        score: float,
        stage: str,
        cooldown_hours: float,
        downgrade_confirm_scans: int = 2,
    ) -> tuple[bool, str, str]:
        """Return (should_send, event_type, previous_alerted_stage)."""
        now = int(time.time())
        stage_rank = {
            "NONE": 0,
            "VOLUME_WATCH": 1,
            "VELOCITY_WATCH": 2,
            "WATCH": 3,
            "AWAKENING": 4,
            "HIGH": 4,
            "FAST_AWAKENING": 5,
            "IGNITION": 6,
            "EXPANSION": 7,
        }
        confirm_needed = max(1, int(downgrade_confirm_scans))

        async with self._pool().acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT * FROM bot_alerts WHERE symbol=$1 FOR UPDATE", symbol
                )

                if row is None:
                    return (stage != "NONE", "new", "NONE")

                last_ts = int(row["last_alert_ts"] or 0)
                last_score = float(row["last_score"] or 0.0)
                last_stage = str(row["last_stage"] or "NONE")
                pending_stage = str(row["pending_downgrade_stage"] or "")
                pending_count = int(row["pending_downgrade_count"] or 0)
                current_rank = stage_rank.get(stage, 0)
                last_rank = stage_rank.get(last_stage, 0)

                if current_rank < last_rank:
                    new_count = pending_count + 1 if pending_stage == stage else 1
                    await conn.execute(
                        """
                        UPDATE bot_alerts
                        SET pending_downgrade_stage=$2,
                            pending_downgrade_count=$3,
                            updated_at=NOW()
                        WHERE symbol=$1
                        """,
                        symbol, stage, new_count,
                    )
                    if new_count >= confirm_needed:
                        return True, "downgrade", last_stage
                    return False, "downgrade_pending", last_stage

                if pending_stage or pending_count:
                    await conn.execute(
                        """
                        UPDATE bot_alerts
                        SET pending_downgrade_stage='',
                            pending_downgrade_count=0,
                            updated_at=NOW()
                        WHERE symbol=$1
                        """,
                        symbol,
                    )

                if current_rank > last_rank:
                    return True, "upgrade", last_stage

                cooldown_ok = now - last_ts >= cooldown_hours * 3600
                improved = score >= last_score + 8
                return cooldown_ok or improved, "repeat", last_stage

    async def mark_alert(self, symbol: str, score: float, stage: str):
        await self._pool().execute(
            """
            INSERT INTO bot_alerts(
                symbol, last_alert_ts, last_score, last_stage,
                pending_downgrade_stage, pending_downgrade_count, updated_at
            )
            VALUES($1, $2, $3, $4, '', 0, NOW())
            ON CONFLICT (symbol) DO UPDATE
            SET last_alert_ts=EXCLUDED.last_alert_ts,
                last_score=EXCLUDED.last_score,
                last_stage=EXCLUDED.last_stage,
                pending_downgrade_stage='',
                pending_downgrade_count=0,
                updated_at=NOW()
            """,
            symbol, int(time.time()), float(score), stage,
        )


store = Store(DATABASE_URL)
scan_lock = asyncio.Lock()
latest_top: list[Signal] = []


def is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in ADMIN_IDS)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct_change(a: float, b: float) -> float:
    return ((b / a) - 1.0) * 100.0 if a else 0.0


def true_ranges(klines: list[list[Any]]) -> list[float]:
    out: list[float] = []
    prev_close: float | None = None
    for k in klines:
        high, low, close = float(k[2]), float(k[3]), float(k[4])
        tr = high - low if prev_close is None else max(
            high - low, abs(high - prev_close), abs(low - prev_close)
        )
        out.append(tr)
        prev_close = close
    return out


def stage_label(stage: str) -> str:
    return {
        "VOLUME_WATCH": "🟣 VOLUME WATCH",
        "VELOCITY_WATCH": "⚡ VELOCITY WATCH",
        "WATCH": "🟡 WATCH",
        "AWAKENING": "🟠 AWAKENING",
        "HIGH": "🟠 AWAKENING",  # legacy state from older bot_data.json
        "FAST_AWAKENING": "⚡🔥 FAST AWAKENING",
        "IGNITION": "🔥 IGNITION",
        "EXPANSION": "🚀 EXPANSION",
    }.get(stage, "⚪ NONE")


def compute_stage(
    *,
    oi_change: float,
    volume_ratio: float,
    price_change: float,
    funding_pct: float,
    oi_efficiency: float,
    persistence_up_days: int,
    score: float,
    cfg: dict[str, float],
) -> str:
    """Lifecycle classifier: accumulation -> ignition -> expansion.

    The old model treated price expansion above 15% as a broken signal. That
    caused cases like LSK to go WATCH -> NONE exactly when accumulated OI began
    to express itself in price. The new model keeps early accumulation
    score-driven, then switches to phase logic once price starts moving.
    """
    funding_ok = funding_pct <= cfg["max_positive_funding_pct"]
    oi_ok = oi_change >= cfg["oi_change_pct"]
    late_move = price_change > cfg["late_move_price_pct"]

    if not funding_ok or not oi_ok or late_move:
        return "NONE"

    ignition_price = cfg["ignition_price_pct"]
    expansion_price = cfg["expansion_price_pct"]
    high_oi = cfg["high_oi_change_pct"]

    # EXPANSION: price is clearly moving after a meaningful OI build-up.
    # Efficiency is intentionally NOT a hard gate here: it naturally falls as
    # price catches up with OI, which is exactly what happened in LSK.
    if (
        price_change >= expansion_price
        and oi_change >= high_oi
        and score >= cfg["watch_score"]
    ):
        return "EXPANSION"

    # IGNITION: transition from divergence into actual price movement.
    # Keep a modest efficiency floor, but do not require the old WATCH 3x gate.
    if (
        price_change >= ignition_price
        and oi_change >= high_oi
        and oi_efficiency >= cfg["oi_efficiency_floor_pct"]
        and score >= cfg["watch_score"]
    ):
        return "IGNITION"

    # Before ignition we still want genuine OI/price divergence.
    if oi_efficiency < cfg["oi_eff_watch"]:
        return "NONE"

    if score >= cfg["awakening_score"]:
        return "AWAKENING"
    if score >= cfg["watch_score"]:
        return "WATCH"
    return "NONE"


def score_volume_accumulation(
    *,
    volume_ratio: float,
    price_change: float,
    oi_change: float,
    funding_pct: float,
    cfg: dict[str, float],
) -> float:
    """Independent 0..100 score for CVC-style volume-first accumulation."""
    vol_target = max(cfg["volume_accum_ratio"] * 2.0, 0.01)
    volume_component = 50 * clamp(volume_ratio / vol_target, 0, 1)

    price_limit = max(cfg["volume_accum_max_abs_price_pct"], 0.1)
    price_component = 25 * clamp(1 - abs(price_change) / price_limit, 0, 1)

    positive_limit = max(cfg["max_positive_funding_pct"], 1e-9)
    if funding_pct <= 0:
        funding_component = 15.0
    else:
        funding_component = 15 * clamp(1 - funding_pct / positive_limit, 0, 1)

    # We mainly require OI not to collapse. Flat OI gets most of these points;
    # positive OI is an extra confirmation, not a requirement.
    oi_floor = cfg["volume_accum_min_oi_change_pct"]
    if oi_change < oi_floor:
        oi_component = 0.0
    elif oi_change >= 10:
        oi_component = 10.0
    elif oi_change >= 0:
        oi_component = 7.0 + 3.0 * (oi_change / 10.0)
    else:
        span = max(abs(oi_floor), 0.1)
        oi_component = 7.0 * clamp((oi_change - oi_floor) / span, 0, 1)

    return round(clamp(volume_component + price_component + funding_component + oi_component, 0, 100), 1)


def is_volume_accumulation(
    *,
    volume_ratio: float,
    price_change: float,
    oi_change: float,
    funding_pct: float,
    volume_score: float,
    cfg: dict[str, float],
) -> bool:
    return (
        volume_ratio >= cfg["volume_accum_ratio"]
        and abs(price_change) <= cfg["volume_accum_max_abs_price_pct"]
        and oi_change >= cfg["volume_accum_min_oi_change_pct"]
        and funding_pct <= cfg["max_positive_funding_pct"]
        and volume_score >= cfg["volume_accum_watch_score"]
    )



def oi_change_over_hours(oi_hist: list[dict[str, Any]], hours: float) -> float:
    """Percent OI change from the closest snapshot at/before `hours` ago."""
    points: list[tuple[int, float]] = []
    for item in oi_hist:
        try:
            ts = int(item.get("timestamp", 0))
            value = float(item.get("sumOpenInterestValue", 0) or 0)
        except (TypeError, ValueError):
            continue
        if ts > 0 and value > 0:
            points.append((ts, value))
    if len(points) < 2:
        return 0.0
    points.sort(key=lambda x: x[0])
    end_ts, end_value = points[-1]
    target_ts = end_ts - int(hours * 3600 * 1000)
    candidates = [p for p in points[:-1] if p[0] <= target_ts]
    if not candidates:
        return 0.0
    start_ts, start_value = candidates[-1]
    if target_ts - start_ts > 45 * 60 * 1000:
        return 0.0
    return pct_change(start_value, end_value)


def score_oi_velocity(oi_1h: float, oi_4h: float, oi_24h: float, cfg: dict[str, float]) -> float:
    thresholds = [
        max(cfg["oi_velocity_1h_pct"], 0.01),
        max(cfg["oi_velocity_4h_pct"], 0.01),
        max(cfg["oi_velocity_24h_pct"], 0.01),
    ]
    raw = [max(oi_1h, 0.0) / thresholds[0], max(oi_4h, 0.0) / thresholds[1], max(oi_24h, 0.0) / thresholds[2]]
    ratios = [clamp(x, 0, 1) for x in raw]
    strongest = max(ratios)
    weighted = 0.40 * ratios[0] + 0.35 * ratios[1] + 0.25 * ratios[2]
    acceleration_bonus = 20 * clamp(max(raw) - 1.0, 0, 1)
    return round(clamp(50 * strongest + 50 * weighted + acceleration_bonus, 0, 100), 1)


def classify_oi_velocity(*, oi_1h: float, oi_4h: float, oi_24h: float, velocity_score: float, price_24h: float, volume_ratio: float, funding_pct: float, cfg: dict[str, float]) -> tuple[bool, bool]:
    hits = [
        oi_1h >= cfg["oi_velocity_1h_pct"],
        oi_4h >= cfg["oi_velocity_4h_pct"],
        oi_24h >= cfg["oi_velocity_24h_pct"],
    ]
    # Always surface a sharp OI anomaly once a configured horizon is crossed.
    velocity_watch = any(hits) and velocity_score >= cfg["oi_velocity_watch_score"]
    funding_ok = funding_pct <= cfg["max_positive_funding_pct"]
    not_late = abs(price_24h) <= cfg["oi_velocity_max_abs_price_24h_pct"]
    confirmation = sum(hits) >= 2 or volume_ratio >= cfg["oi_velocity_confirmation_volume_ratio"]
    fast = velocity_watch and funding_ok and not_late and confirmation and velocity_score >= cfg["oi_velocity_fast_score"]
    return velocity_watch, fast


def score_signal(
    *,
    oi_change: float,
    oi_efficiency: float,
    persistence_ratio: float,
    volume_ratio: float,
    funding_pct: float,
    compression_ratio: float,
    distance_to_breakout_pct: float,
    historical_atr_pct: float,
    cfg: dict[str, float],
) -> float:
    """0..100. Deliberately OI-heavy after FORM/SOPH/VVV retrospectives."""
    # 45% OI/Price divergence — the strongest signal in the model.
    divergence_component = 45 * clamp(
        oi_efficiency / max(cfg["oi_eff_high"], 0.01), 0, 1
    )

    # 20% raw OI expansion. Full points at the OI-shock threshold.
    oi_component = 20 * clamp(
        max(oi_change, 0.0) / max(cfg["oi_shock_pct"], 0.01), 0, 1
    )

    # 20% persistence — distinguishes sustained accumulation from a one-tick spike.
    persistence_component = 20 * clamp(persistence_ratio, 0, 1)

    # Only 5% volume. It can rise gradually, explode late, or even contract before a pump.
    volume_component = 5 * clamp(
        volume_ratio / max(cfg["high_volume_ratio"], 0.01), 0, 1
    )

    # 5% asymmetric funding component for the bullish pre-pump thesis.
    # Negative funding can be useful squeeze fuel; positive funding increasingly
    # suggests crowded longs.  A very positive rate is handled by compute_stage
    # as a hard gate, while negative funding is never rejected on magnitude alone.
    squeeze_target = min(cfg["short_squeeze_funding_pct"], -1e-9)
    positive_limit = max(cfg["max_positive_funding_pct"], 1e-9)
    if funding_pct <= 0:
        # 4 points at neutral, rising to 5 once funding reaches squeeze_target.
        funding_component = 4 + clamp(abs(funding_pct) / abs(squeeze_target), 0, 1)
    else:
        # 4 points at neutral, fading to 0 at the positive hard-limit.
        funding_component = 4 * clamp(1 - funding_pct / positive_limit, 0, 1)

    # The remaining 5% are contextual, never hard filters.
    comp_target = max(cfg["min_compression_ratio"], 1.0)
    compression_component = 2 * clamp(compression_ratio / comp_target, 0, 1)

    near = max(cfg["breakout_near_pct"], 0.1)
    breakout_component = 1.5 * clamp(1 - distance_to_breakout_pct / near, 0, 1)

    atr_target = max(cfg["historical_atr_target_pct"], 0.1)
    historical_vol_component = 1.5 * clamp(historical_atr_pct / atr_target, 0, 1)

    return round(
        clamp(
            divergence_component
            + oi_component
            + persistence_component
            + volume_component
            + funding_component
            + compression_component
            + breakout_component
            + historical_vol_component,
            0,
            100,
        ),
        1,
    )


async def get_json(client: httpx.AsyncClient, url: str, params: dict | None = None):
    r = await client.get(url, params=params)
    r.raise_for_status()
    return r.json()


async def get_universe(client: httpx.AsyncClient) -> list[str]:
    """Binance USDT-M perpetuals ∩ active MEXC USDT futures."""
    b, m = await asyncio.gather(
        get_json(client, f"{BINANCE}/fapi/v1/exchangeInfo"),
        get_json(client, f"{MEXC}/api/v1/contract/detail"),
    )

    binance_symbols = {
        x["symbol"]
        for x in b.get("symbols", [])
        if x.get("status") == "TRADING"
        and x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT"
    }

    raw = m.get("data", []) if isinstance(m, dict) else []
    mexc_symbols = set()
    for x in raw:
        sym = str(x.get("symbol", ""))
        if sym.endswith("_USDT"):
            state = x.get("state")
            if state in (None, 0, "0"):
                mexc_symbols.add(sym.replace("_", ""))

    overlap = sorted(binance_symbols & mexc_symbols)
    log.info(
        "Universe: Binance=%d MEXC=%d overlap=%d",
        len(binance_symbols),
        len(mexc_symbols),
        len(overlap),
    )
    return overlap


async def get_global_market_snapshots(client: httpx.AsyncClient):
    tickers, premium = await asyncio.gather(
        get_json(client, f"{BINANCE}/fapi/v1/ticker/24hr"),
        get_json(client, f"{BINANCE}/fapi/v1/premiumIndex"),
    )
    ticker_map = {x["symbol"]: x for x in tickers if "symbol" in x}
    premium_map = {x["symbol"]: x for x in premium if "symbol" in x}
    return ticker_map, premium_map


def utc_day_from_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime("%d.%m")


def build_metric_history(
    klines: list[list[Any]],
    oi_hist: list[dict[str, Any]],
    lookback: int,
    current_price: float,
) -> list[dict[str, Any]]:
    """Build a compact day-by-day history for the exact OI lookback window.

    OI history is the anchor because OI change is calculated from those Binance
    daily snapshots. Price is matched by UTC kline day; for the current/open day
    we use the live ticker price so the visible history explains Price change too.
    """
    price_by_day: dict[str, float] = {}
    current_day = datetime.now(timezone.utc).strftime("%d.%m")
    for k in klines:
        try:
            day = utc_day_from_ms(int(k[0]))
            price_by_day[day] = float(k[4])
        except (TypeError, ValueError, IndexError):
            continue
    price_by_day[current_day] = current_price

    points: list[dict[str, Any]] = []
    selected = oi_hist[-(lookback + 1):]
    prev_price: float | None = None
    prev_oi: float | None = None
    for item in selected:
        try:
            day = utc_day_from_ms(int(item.get("timestamp", 0)))
            oi_value = float(item.get("sumOpenInterestValue", 0) or 0)
        except (TypeError, ValueError):
            continue
        if oi_value <= 0:
            continue
        price = price_by_day.get(day)
        if price is None:
            continue
        points.append({
            "date": day,
            "price": price,
            "oi": oi_value,
            "price_day_pct": pct_change(prev_price, price) if prev_price else None,
            "oi_day_pct": pct_change(prev_oi, oi_value) if prev_oi else None,
        })
        prev_price, prev_oi = price, oi_value

    # If Binance OI's latest daily bucket does not carry today's timestamp, add
    # a live price-only row rather than pretending we have a live OI value.
    if points and points[-1]["date"] != current_day:
        last_price = points[-1]["price"]
        points.append({
            "date": f"{current_day}*",
            "price": current_price,
            "oi": None,
            "price_day_pct": pct_change(last_price, current_price),
            "oi_day_pct": None,
        })
    return points


async def analyze_symbol(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    symbol: str,
    ticker: dict,
    premium: dict,
    cfg: dict[str, float],
) -> Signal | None:
    quote_volume = float(ticker.get("quoteVolume", 0) or 0)
    if quote_volume < cfg["min_quote_volume_usdt"]:
        return None

    lookback = max(1, int(cfg["oi_lookback_days"]))
    persistence_days = max(2, int(cfg["persistence_days"]))
    baseline_days = max(14, int(cfg["atr_baseline_days"]))
    recent_days = max(3, int(cfg["atr_recent_days"]))
    breakout_days = max(5, int(cfg["breakout_lookback_days"]))

    kline_limit = min(120, max(
        baseline_days + recent_days + 12,
        breakout_days + lookback + 5,
    ))
    oi_limit = min(30, max(lookback + 2, persistence_days + 2, 8))

    async with sem:
        try:
            klines, oi_hist, oi_intraday = await asyncio.gather(
                get_json(
                    client,
                    f"{BINANCE}/fapi/v1/klines",
                    {"symbol": symbol, "interval": "1d", "limit": kline_limit},
                ),
                get_json(
                    client,
                    f"{BINANCE}/futures/data/openInterestHist",
                    {"symbol": symbol, "period": "1d", "limit": oi_limit},
                ),
                get_json(
                    client,
                    f"{BINANCE}/futures/data/openInterestHist",
                    {"symbol": symbol, "period": "15m", "limit": 100},
                ),
            )
        except (httpx.HTTPError, ValueError, TypeError) as e:
            log.debug("%s skipped: %s", symbol, e)
            return None

    if len(klines) < recent_days + 9 or len(oi_hist) < lookback + 1:
        return None

    # Sort OI just in case an upstream response changes ordering.
    try:
        oi_hist = sorted(oi_hist, key=lambda x: int(x.get("timestamp", 0)))
    except Exception:
        pass

    try:
        oi_intraday = sorted(oi_intraday, key=lambda x: int(x.get("timestamp", 0)))
    except Exception:
        oi_intraday = []

    closed = klines[:-1] if len(klines) > 1 else klines
    current_price = float(ticker.get("lastPrice", klines[-1][4]))
    price_24h = float(ticker.get("priceChangePercent", 0) or 0)
    oi_velocity_1h = oi_change_over_hours(oi_intraday, 1)
    oi_velocity_4h = oi_change_over_hours(oi_intraday, 4)
    oi_velocity_24h = oi_change_over_hours(oi_intraday, 24)

    # OI change over lookback.
    start_oi = float(oi_hist[-(lookback + 1)].get("sumOpenInterestValue", 0) or 0)
    end_oi = float(oi_hist[-1].get("sumOpenInterestValue", 0) or 0)
    if start_oi <= 0:
        return None
    oi_change = pct_change(start_oi, end_oi)

    # Price change over similar lookback.
    if len(closed) >= lookback + 1:
        start_price = float(closed[-(lookback + 1)][4])
    else:
        start_price = float(closed[0][4])
    price_change = pct_change(start_price, current_price)

    # SOPH metric: how much faster OI expands than price.
    eff_floor = max(cfg["oi_efficiency_floor_pct"], 0.1)
    oi_efficiency = max(oi_change, 0.0) / max(abs(price_change), eff_floor)

    # OI persistence: number of positive daily OI changes over the last N comparisons.
    oi_values = [float(x.get("sumOpenInterestValue", 0) or 0) for x in oi_hist]
    oi_values = [x for x in oi_values if x > 0]
    recent_oi = oi_values[-(persistence_days + 1):]
    positive_changes = 0
    comparisons = max(0, len(recent_oi) - 1)
    for a, b in zip(recent_oi, recent_oi[1:]):
        if b > a:
            positive_changes += 1
    persistence_ratio = positive_changes / comparisons if comparisons else 0.0

    # Current rolling 24h quote volume vs prior 7 closed daily quote volumes.
    past_qv = [float(k[7]) for k in closed[-7:] if float(k[7]) > 0]
    avg_qv = mean(past_qv) if past_qv else 0
    volume_ratio = quote_volume / avg_qv if avg_qv else 0.0

    funding_pct = float(premium.get("lastFundingRate", 0) or 0) * 100.0

    volume_score = score_volume_accumulation(
        volume_ratio=volume_ratio,
        price_change=price_change,
        oi_change=oi_change,
        funding_pct=funding_pct,
        cfg=cfg,
    )
    volume_accumulation = is_volume_accumulation(
        volume_ratio=volume_ratio,
        price_change=price_change,
        oi_change=oi_change,
        funding_pct=funding_pct,
        volume_score=volume_score,
        cfg=cfg,
    )

    velocity_score = score_oi_velocity(oi_velocity_1h, oi_velocity_4h, oi_velocity_24h, cfg)
    velocity_watch, fast_awakening = classify_oi_velocity(
        oi_1h=oi_velocity_1h, oi_4h=oi_velocity_4h, oi_24h=oi_velocity_24h,
        velocity_score=velocity_score, price_24h=price_24h, volume_ratio=volume_ratio,
        funding_pct=funding_pct, cfg=cfg,
    )

    # ATR compression and historical volatility proxy.
    trs = true_ranges(closed)
    closes = [float(k[4]) for k in closed]
    if len(trs) < recent_days + 14:
        return None

    recent_atr_pct = mean(trs[-recent_days:]) / max(mean(closes[-recent_days:]), 1e-12) * 100
    base_slice = trs[-(baseline_days + recent_days):-recent_days]
    base_close_slice = closes[-(baseline_days + recent_days):-recent_days]
    if not base_slice or not base_close_slice:
        base_slice = trs[:-recent_days]
        base_close_slice = closes[:-recent_days]

    baseline_atr_values = [
        tr / max(c, 1e-12) * 100 for tr, c in zip(base_slice, base_close_slice)
    ]
    baseline_atr_pct = median(baseline_atr_values) if baseline_atr_values else recent_atr_pct
    compression_ratio = baseline_atr_pct / max(recent_atr_pct, 1e-9)

    # Distance from local breakout high: 0 means at/above recent high.
    breakout_slice = closed[-breakout_days:] if len(closed) >= breakout_days else closed
    recent_high = max(float(k[2]) for k in breakout_slice) if breakout_slice else current_price
    distance_to_breakout_pct = max(0.0, (recent_high - current_price) / max(recent_high, 1e-12) * 100)

    score = score_signal(
        oi_change=oi_change,
        oi_efficiency=oi_efficiency,
        persistence_ratio=persistence_ratio,
        volume_ratio=volume_ratio,
        volume_score=volume_score,
        volume_accumulation=volume_accumulation,
        oi_velocity_1h_pct=oi_velocity_1h,
        oi_velocity_4h_pct=oi_velocity_4h,
        oi_velocity_24h_pct=oi_velocity_24h,
        velocity_score=velocity_score,
        velocity_watch=velocity_watch,
        fast_awakening=fast_awakening,
        price_24h_pct=price_24h,
        funding_pct=funding_pct,
        compression_ratio=compression_ratio,
        distance_to_breakout_pct=distance_to_breakout_pct,
        historical_atr_pct=baseline_atr_pct,
        cfg=cfg,
    )

    stage = compute_stage(
        oi_change=oi_change,
        volume_ratio=volume_ratio,
        price_change=price_change,
        funding_pct=funding_pct,
        oi_efficiency=oi_efficiency,
        persistence_up_days=positive_changes,
        score=score,
        cfg=cfg,
    )
    if stage not in {"IGNITION", "EXPANSION"} and fast_awakening:
        stage = "FAST_AWAKENING"
    elif stage == "NONE" and velocity_watch:
        stage = "VELOCITY_WATCH"
    elif stage == "NONE" and volume_accumulation:
        stage = "VOLUME_WATCH"

    history = build_metric_history(klines, oi_hist, lookback, current_price)

    return Signal(
        symbol=symbol,
        price=current_price,
        price_change_pct=price_change,
        oi_change_pct=oi_change,
        volume_ratio=volume_ratio,
        volume_score=volume_score,
        volume_accumulation=volume_accumulation,
        funding_pct=funding_pct,
        compression_ratio=compression_ratio,
        quote_volume=quote_volume,
        oi_efficiency=oi_efficiency,
        persistence_up_days=positive_changes,
        persistence_total_days=comparisons,
        persistence_ratio=persistence_ratio,
        distance_to_breakout_pct=distance_to_breakout_pct,
        historical_atr_pct=baseline_atr_pct,
        score=score,
        stage=stage,
        history=history,
    )


async def scan_market() -> list[Signal]:
    cfg = await store.settings()
    async with scan_lock:
        async with httpx.AsyncClient(
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": "oi-lifecycle-signal-bot/8.0"},
        ) as client:
            universe = await get_universe(client)
            ticker_map, premium_map = await get_global_market_snapshots(client)
            sem = asyncio.Semaphore(MAX_CONCURRENCY)

            tasks = []
            for symbol in universe:
                ticker = ticker_map.get(symbol)
                premium = premium_map.get(symbol, {})
                if ticker:
                    tasks.append(analyze_symbol(client, sem, symbol, ticker, premium, cfg))

            results: list[Signal] = []
            chunk_size = 60
            for i in range(0, len(tasks), chunk_size):
                chunk = await asyncio.gather(*tasks[i:i + chunk_size], return_exceptions=True)
                for item in chunk:
                    if isinstance(item, Signal):
                        results.append(item)

    # Keep the full best-model-score-sorted universe in memory. /top filters out NONE,
    # while /rawtop can still expose rejected high-score anomalies for diagnosis.
    results.sort(key=effective_score, reverse=True)

    global latest_top
    latest_top = results
    return results


def effective_score(s: Signal) -> float:
    """Ranking/alert score across independent OI and Volume models."""
    return max(s.score, s.volume_score, s.velocity_score)


def should_alert(s: Signal) -> bool:
    return s.stage in {"VOLUME_WATCH", "VELOCITY_WATCH", "WATCH", "AWAKENING", "HIGH", "FAST_AWAKENING", "IGNITION", "EXPANSION"}


def money(x: float) -> str:
    if x >= 1_000_000_000:
        return f"${x/1_000_000_000:.2f}B"
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def fmt_price(x: float) -> str:
    if x >= 100:
        return f"${x:,.2f}"
    if x >= 1:
        return f"${x:.4f}"
    if x >= 0.01:
        return f"${x:.6f}"
    return f"${x:.8f}"


def history_text(s: Signal, cfg: dict[str, float]) -> str:
    if not s.history:
        return ""
    rows = [f"📊 <b>Price / OI history ({int(cfg['oi_lookback_days'])}d, UTC)</b>"]
    for row in s.history:
        price_delta = "" if row.get("price_day_pct") is None else f" ({row['price_day_pct']:+.1f}%)"
        oi_value = row.get("oi")
        if oi_value is None:
            oi_part = "OI n/a"
        else:
            oi_delta = "" if row.get("oi_day_pct") is None else f" ({row['oi_day_pct']:+.1f}%)"
            oi_part = f"OI {money(float(oi_value))}{oi_delta}"
        rows.append(f"<code>{row['date']}</code>  Price {fmt_price(float(row['price']))}{price_delta} | {oi_part}")
    if any(str(r.get("date", "")).endswith("*") for r in s.history):
        rows.append("<i>* текущая цена; дневной OI-снимок Binance ещё не обновлён</i>")
    return "\n".join(rows)


def signal_text(s: Signal, cfg: dict[str, float]) -> str:
    mexc_symbol = s.symbol.replace("USDT", "_USDT")
    shock = "\n⚡ <b>OI SHOCK</b>" if s.oi_change_pct >= cfg["oi_shock_pct"] and s.oi_efficiency >= cfg["oi_shock_efficiency"] else ""
    squeeze = "\n🩳 <b>SHORT-SQUEEZE POTENTIAL</b>" if s.funding_pct <= cfg["short_squeeze_funding_pct"] else ""
    vol_badge = "\n🟣 <b>VOLUME ACCUMULATION</b>" if s.volume_accumulation else ""
    velocity = "\n⚡ <b>OI VELOCITY</b>" if s.velocity_watch else ""
    independent_hits = int(s.volume_accumulation) + int(s.velocity_watch) + int(s.stage in {"WATCH", "AWAKENING", "HIGH", "IGNITION", "EXPANSION"})
    conviction = "\n🔴 <b>HIGH CONVICTION: MULTI-SIGNAL</b>" if independent_hits >= 2 else ""
    return (
        f"{stage_label(s.stage)} <b>{s.symbol}</b>{shock}{squeeze}{vol_badge}{velocity}{conviction}\n\n"
        f"OI Score: <b>{s.score:.1f}/100</b>\n"
        f"Цена: <b>{fmt_price(s.price)}</b>\n"
        f"OI change: <b>{s.oi_change_pct:+.1f}%</b>\n"
        f"Price change: <b>{s.price_change_pct:+.1f}%</b>\n"
        f"OI Efficiency: <b>{s.oi_efficiency:.2f}×</b>\n"
        f"OI persistence: <b>{s.persistence_up_days}/{s.persistence_total_days} up days</b>\n"
        f"Volume awakening: <b>{s.volume_ratio:.2f}×</b>\n"
        f"Volume Score: <b>{s.volume_score:.1f}/100</b>\n"
        f"Velocity Score: <b>{s.velocity_score:.1f}/100</b>\n"
        f"OI Velocity: <b>1h {s.oi_velocity_1h_pct:+.1f}% | 4h {s.oi_velocity_4h_pct:+.1f}% | 24h {s.oi_velocity_24h_pct:+.1f}%</b>\n"
        f"Price 24h: <b>{s.price_24h_pct:+.1f}%</b>\n"
        f"Funding: <b>{s.funding_pct:+.4f}%</b>\n"
        f"Compression: <b>{s.compression_ratio:.2f}×</b>\n"
        f"Distance to local high: <b>{s.distance_to_breakout_pct:.1f}%</b>\n"
        f"Historical ATR: <b>{s.historical_atr_pct:.1f}%</b>\n"
        f"24h quote volume: <b>{money(s.quote_volume)}</b>\n\n"
        f"{history_text(s, cfg)}\n\n"
        f"Binance: https://www.binance.com/en/futures/{s.symbol}\n"
        f"MEXC: https://www.mexc.com/futures/{mexc_symbol}\n\n"
        f"<i>Сигнал отражает OI lifecycle, Volume Accumulation и/или OI Velocity pattern, а не рекомендацию покупать.</i>"
    )


def downgrade_text(s: Signal, previous_stage: str, confirm_scans: int, cfg: dict[str, float]) -> str:
    mexc_symbol = s.symbol.replace("USDT", "_USDT")
    cycle_complete = (
        previous_stage in {"IGNITION", "EXPANSION"}
        and s.stage == "NONE"
        and s.price_change_pct > cfg["late_move_price_pct"]
    )
    if cycle_complete:
        title = f"🏁 <b>CYCLE COMPLETE — {s.symbol}</b>"
        footer = "<i>Цена вышла за late-move limit: ранний сигнал считаем реализованным, а не сломанным.</i>"
    else:
        title = f"⚠️ <b>SIGNAL WEAKENING — {s.symbol}</b>"
        footer = "<i>Сигнал ослаб. Это уведомление о смене стадии, а не торговая рекомендация.</i>"
    return (
        f"{title}\n\n"
        f"{stage_label(previous_stage)} → {stage_label(s.stage)}\n"
        f"Подтверждено: <b>{max(1, int(confirm_scans))} скана подряд</b>\n\n"
        f"OI Score: <b>{s.score:.1f}/100</b> | Volume Score: <b>{s.volume_score:.1f}/100</b>\n"
        f"OI change: <b>{s.oi_change_pct:+.1f}%</b>\n"
        f"Price change: <b>{s.price_change_pct:+.1f}%</b>\n"
        f"OI Efficiency: <b>{s.oi_efficiency:.2f}×</b>\n"
        f"OI persistence: <b>{s.persistence_up_days}/{s.persistence_total_days}</b>\n"
        f"Volume awakening: <b>{s.volume_ratio:.2f}×</b> | Volume Score <b>{s.volume_score:.1f}</b>\n"
        f"Funding: <b>{s.funding_pct:+.4f}%</b>\n\n"
        f"{history_text(s, cfg)}\n\n"
        f"Binance: https://www.binance.com/en/futures/{s.symbol}\n"
        f"MEXC: https://www.mexc.com/futures/{mexc_symbol}\n\n"
        f"{footer}"
    )


def rejection_reasons(s: Signal, cfg: dict[str, float]) -> list[str]:
    reasons: list[str] = []
    if s.oi_change_pct < cfg["oi_change_pct"]:
        reasons.append(f"OI {s.oi_change_pct:+.1f}% < {cfg['oi_change_pct']:.1f}%")
    if s.oi_efficiency < cfg["oi_eff_watch"]:
        reasons.append(f"Eff {s.oi_efficiency:.2f}× < {cfg['oi_eff_watch']:.2f}×")
    if s.price_change_pct > cfg["late_move_price_pct"]:
        reasons.append(f"Price {s.price_change_pct:+.1f}% > late limit +{cfg['late_move_price_pct']:.1f}%")
    if s.funding_pct > cfg["max_positive_funding_pct"]:
        reasons.append(f"Funding {s.funding_pct:+.4f}% > +{cfg['max_positive_funding_pct']:.4f}%")
    if s.score < cfg["watch_score"]:
        reasons.append(f"Score {s.score:.1f} < {cfg['watch_score']:.0f}")
    return reasons


def top_text(items: list[Signal], n: int, cfg: dict[str, float]) -> str:
    active = [s for s in items if s.stage != "NONE"][:n]
    if not active:
        return "Сейчас нет активных Volume/OI/Velocity сигналов."
    rows = ["<b>Текущий signal TOP — OI lifecycle + Volume + Velocity</b>\n"]
    for i, s in enumerate(active, 1):
        squeeze = " | 🩳 SQUEEZE" if s.funding_pct <= cfg["short_squeeze_funding_pct"] else ""
        vol = " | 🟣 VOL" if s.volume_accumulation else ""
        vel = " | ⚡ VEL" if s.velocity_watch else ""
        combo = " | 🔴 MULTI" if sum([s.volume_accumulation, s.velocity_watch, s.stage in {"WATCH", "AWAKENING", "HIGH", "IGNITION", "EXPANSION"}]) >= 2 else ""
        rows.append(
            f"<b>{i}. {s.symbol}</b> — {stage_label(s.stage)} — OI {s.score:.1f} | VOL {s.volume_score:.1f} | VEL {s.velocity_score:.1f}{squeeze}{vol}{vel}{combo}\n"
            f"OI {s.oi_change_pct:+.1f}% | Eff {s.oi_efficiency:.2f}× | "
            f"Persist {s.persistence_up_days}/{s.persistence_total_days} | Vol {s.volume_ratio:.2f}×\n"
            f"Vel 1h {s.oi_velocity_1h_pct:+.1f}% / 4h {s.oi_velocity_4h_pct:+.1f}% / 24h {s.oi_velocity_24h_pct:+.1f}%\n"
            f"Price {s.price_change_pct:+.1f}% (24h {s.price_24h_pct:+.1f}%) | Fund {s.funding_pct:+.4f}% | "
            f"Comp {s.compression_ratio:.2f}× | HighDist {s.distance_to_breakout_pct:.1f}%"
        )
    return "\n\n".join(rows)


def raw_top_text(items: list[Signal], n: int, cfg: dict[str, float]) -> str:
    if not items:
        return "Пока нет результатов. Запусти /scan или дождись следующего цикла."
    rows = ["<b>RAW TOP — включая отфильтрованные NONE</b>\n"]
    for i, s in enumerate(items[:n], 1):
        extra = ""
        if s.stage == "NONE":
            reasons = rejection_reasons(s, cfg)
            if reasons:
                extra = "\n❌ " + "; ".join(reasons)
        elif s.funding_pct <= cfg["short_squeeze_funding_pct"]:
            extra = "\n🩳 Отрицательный funding: short-squeeze potential"
        rows.append(
            f"<b>{i}. {s.symbol}</b> — {stage_label(s.stage)} — OI {s.score:.1f} | VOL {s.volume_score:.1f} | VEL {s.velocity_score:.1f}{' | 🟣 VOL' if s.volume_accumulation else ''}{' | ⚡ VEL' if s.velocity_watch else ''}\n"
            f"OI {s.oi_change_pct:+.1f}% | Eff {s.oi_efficiency:.2f}× | "
            f"Persist {s.persistence_up_days}/{s.persistence_total_days} | Vol {s.volume_ratio:.2f}×\n"
            f"Vel 1h {s.oi_velocity_1h_pct:+.1f}% / 4h {s.oi_velocity_4h_pct:+.1f}% / 24h {s.oi_velocity_24h_pct:+.1f}%\n"
            f"Price {s.price_change_pct:+.1f}% (24h {s.price_24h_pct:+.1f}%) | Fund {s.funding_pct:+.4f}% | "
            f"Comp {s.compression_ratio:.2f}× | HighDist {s.distance_to_breakout_pct:.1f}%"
            f"{extra}"
        )
    return "\n\n".join(rows)


def settings_text(cfg: dict[str, float]) -> str:
    return (
        "<b>Параметры FORM/SOPH-сканера</b>\n\n"
        f"OI change ≥ <b>{cfg['oi_change_pct']:.2f}%</b> / {int(cfg['oi_lookback_days'])}d\n"
        f"Volume reference: <b>{cfg['volume_ratio']:.2f}×</b> / high <b>{cfg['high_volume_ratio']:.2f}×</b> (OI score context)\n"
        f"🟣 Volume Accumulation: Vol ≥ <b>{cfg['volume_accum_ratio']:.1f}×</b>, |Price| ≤ <b>{cfg['volume_accum_max_abs_price_pct']:.1f}%</b>, OI ≥ <b>{cfg['volume_accum_min_oi_change_pct']:.1f}%</b>, Volume Score ≥ <b>{cfg['volume_accum_watch_score']:.0f}</b>\n"
        f"⚡ OI Velocity: 1h ≥ <b>{cfg['oi_velocity_1h_pct']:.1f}%</b>, 4h ≥ <b>{cfg['oi_velocity_4h_pct']:.1f}%</b>, 24h ≥ <b>{cfg['oi_velocity_24h_pct']:.1f}%</b>; WATCH ≥ <b>{cfg['oi_velocity_watch_score']:.0f}</b>, FAST ≥ <b>{cfg['oi_velocity_fast_score']:.0f}</b>\n"
        f"Velocity FAST context: |Price 24h| ≤ <b>{cfg['oi_velocity_max_abs_price_24h_pct']:.1f}%</b>, Vol ≥ <b>{cfg['oi_velocity_confirmation_volume_ratio']:.1f}×</b> or ≥2 horizons\n"
        f"Late-move hard limit: Price ≤ <b>{cfg['late_move_price_pct']:.2f}%</b>\n"
        f"IGNITION starts near <b>+{cfg['ignition_price_pct']:.1f}%</b>; EXPANSION near <b>+{cfg['expansion_price_pct']:.1f}%</b> (with OI confirmation)\n"
        f"Positive Funding hard-limit ≤ <b>+{cfg['max_positive_funding_pct']:.4f}%</b>\n"
        f"Short-squeeze bonus starts near <b>{cfg['short_squeeze_funding_pct']:.4f}%</b> (negative funding is not a hard filter)\n"
        f"Compression target ≥ <b>{cfg['min_compression_ratio']:.2f}×</b> (не hard filter)\n"
        f"Min 24h quote volume ≥ <b>{money(cfg['min_quote_volume_usdt'])}</b>\n\n"
        f"<b>OI Efficiency</b> = max(OI%,0) / max(|Price%|, {cfg['oi_efficiency_floor_pct']:.1f}%)\n"
        f"WATCH ≥ <b>{cfg['oi_eff_watch']:.2f}×</b>\n"
        f"AWAKENING ≥ <b>{cfg['oi_eff_awakening']:.2f}×</b>\n"
        f"HIGH ≥ <b>{cfg['oi_eff_high']:.2f}×</b>\n"
        f"Persistence: ≥ <b>{int(cfg['persistence_min_up_days'])}/{int(cfg['persistence_days'])}</b> OI up days\n"
        f"HIGH OI change ≥ <b>{cfg['high_oi_change_pct']:.1f}%</b>\n"
        f"OI SHOCK ≥ <b>{cfg['oi_shock_pct']:.0f}%</b> @ Eff ≥ <b>{cfg['oi_shock_efficiency']:.1f}×</b>\n"
        f"OI SHOCK HIGH ≥ <b>{cfg['oi_shock_high_pct']:.0f}%</b> @ Eff ≥ <b>{cfg['oi_shock_high_efficiency']:.1f}×</b>\n\n"
        f"Scores: WATCH ≥ <b>{cfg['watch_score']:.0f}</b>, "
        f"AWAKENING ≥ <b>{cfg['awakening_score']:.0f}</b>, HIGH ≥ <b>{cfg['high_score']:.0f}</b>\n"
        f"Near breakout: within <b>{cfg['breakout_near_pct']:.1f}%</b> of {int(cfg['breakout_lookback_days'])}d high\n"
        f"Historical ATR target: <b>{cfg['historical_atr_target_pct']:.1f}%</b>\n\n"
        f"Scan interval: <b>{int(cfg['scan_interval_min'])} min</b>\n"
        f"Cooldown: <b>{cfg['alert_cooldown_hours']:.1f} h</b>\n"
        f"Downgrade confirmation: <b>{int(cfg['downgrade_confirm_scans'])} scans</b>\n"
        f"TOP size: <b>{int(cfg['top_n'])}</b>\n\n"
        "Изменение: <code>/set price 30</code>, <code>/set volaccum 4</code>, <code>/set volaccumprice 8</code>, <code>/set volaccumscore 65</code>"
    )


def menu_markup(subscribed: bool | None = None):
    sub_label = "🔕 Отписаться" if subscribed else "🔔 Подписаться"
    sub_data = "unsubscribe" if subscribed else "subscribe"
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📊 TOP", callback_data="top"),
            InlineKeyboardButton("⚙️ Настройки", callback_data="settings"),
        ],
        [InlineKeyboardButton(sub_label, callback_data=sub_data)],
    ])


async def user_is_subscribed(chat_id: int) -> bool:
    return chat_id in set(await store.subscribers())


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        HELP_TEXT,
        parse_mode=ParseMode.HTML,
        reply_markup=menu_markup(await user_is_subscribed(update.effective_chat.id)),
        disable_web_page_preview=True,
    )


async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "Панель управления:",
        reply_markup=menu_markup(await user_is_subscribed(update.effective_chat.id)),
    )


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await store.subscribe(update.effective_chat.id)
    await update.effective_message.reply_text("✅ Подписка на сигналы включена.")


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await store.unsubscribe(update.effective_chat.id)
    await update.effective_message.reply_text("🔕 Подписка отключена.")


async def cmd_settings(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        settings_text(await store.settings()), parse_mode=ParseMode.HTML
    )


def validate_setting(key: str, value: float, cfg: dict[str, float]) -> str | None:
    if key == "scan_interval_min" and value < 5:
        return "Минимальный интервал — 5 минут, чтобы не упираться в API rate limits."
    if key in {"top_n", "oi_lookback_days", "persistence_days", "persistence_min_up_days", "breakout_lookback_days", "downgrade_confirm_scans"} and value < 1:
        return "Значение должно быть ≥ 1."
    if key == "oi_lookback_days" and value > 29:
        return "OI lookback должен быть ≤ 29 дней."
    if key == "ignition_price_pct" and value >= cfg.get("expansion_price_pct", DEFAULTS["expansion_price_pct"]):
        return "ignitionprice должен быть меньше expansionprice."
    if key == "expansion_price_pct" and value <= cfg.get("ignition_price_pct", DEFAULTS["ignition_price_pct"]):
        return "expansionprice должен быть больше ignitionprice."
    if key == "late_move_price_pct" and value <= cfg.get("expansion_price_pct", DEFAULTS["expansion_price_pct"]):
        return "price (late limit) должен быть больше expansionprice."
    if key == "expansion_price_pct" and value >= cfg.get("late_move_price_pct", DEFAULTS["late_move_price_pct"]):
        return "expansionprice должен быть меньше price (late limit)."
    if key == "persistence_days" and value > 29:
        return "Persistence window должен быть ≤ 29 дней."
    if key == "persistence_min_up_days" and value > cfg.get("persistence_days", DEFAULTS["persistence_days"]):
        return "persistup не может быть больше persistdays. Сначала увеличь persistdays."
    if key == "persistence_days" and value < cfg.get("persistence_min_up_days", DEFAULTS["persistence_min_up_days"]):
        return "persistdays не может быть меньше текущего persistup. Сначала уменьши persistup."
    if value < 0 and key not in {"late_move_price_pct", "short_squeeze_funding_pct", "volume_accum_min_oi_change_pct"}:
        return "Для этого параметра значение должно быть ≥ 0."
    if key == "short_squeeze_funding_pct" and value >= 0:
        return "squeezefunding должен быть отрицательным, например -0.03."
    if key == "volume_accum_min_oi_change_pct" and value > 0:
        return "volaccumoi обычно должен быть ≤ 0: это допустимое падение OI, например -10."
    if key in {"watch_score", "awakening_score", "high_score", "volume_accum_watch_score", "oi_velocity_watch_score", "oi_velocity_fast_score"} and value > 100:
        return "Score должен быть в диапазоне 0..100."
    return None


async def cmd_set(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.effective_message.reply_text(
            "⛔ Изменять глобальные параметры может только ADMIN_IDS."
        )
        return
    if len(context.args) != 2:
        await update.effective_message.reply_text(
            "Формат: /set <параметр> <значение>\nНапример: /set effawake 5"
        )
        return

    alias = context.args[0].lower()
    key = EDITABLE.get(alias)
    if not key:
        await update.effective_message.reply_text(
            "Неизвестный параметр: " + alias + "\n" + ", ".join(EDITABLE)
        )
        return

    try:
        value = float(context.args[1].replace(",", "."))
    except ValueError:
        await update.effective_message.reply_text("Значение должно быть числом.")
        return

    cfg = await store.settings()
    error = validate_setting(key, value, cfg)
    if error:
        await update.effective_message.reply_text(error)
        return

    await store.set_setting(key, value)
    await update.effective_message.reply_text(f"✅ {alias} = {value:g}")

    if key == "scan_interval_min":
        reschedule_scanner(context.application, value)


async def cmd_top(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg = await store.settings()
    items = latest_top
    if not items:
        await update.effective_message.reply_text("Данных ещё нет — запускаю первый скан...")
        try:
            items = await scan_market()
        except Exception as e:
            log.exception("Manual initial scan failed")
            await update.effective_message.reply_text(f"Ошибка сканирования: {type(e).__name__}")
            return
    await update.effective_message.reply_text(
        top_text(items, int(cfg["top_n"]), cfg),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_rawtop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cfg = await store.settings()
    items = latest_top
    if not items:
        await update.effective_message.reply_text("Данных ещё нет — запускаю первый скан...")
        try:
            items = await scan_market()
        except Exception as e:
            log.exception("Manual initial raw scan failed")
            await update.effective_message.reply_text(f"Ошибка сканирования: {type(e).__name__}")
            return
    await update.effective_message.reply_text(
        raw_top_text(items, int(cfg["top_n"]), cfg),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Формат: /history LSK или /history LSKUSDT")
        return
    symbol = context.args[0].upper().replace("/", "")
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    item = next((x for x in latest_top if x.symbol == symbol), None)
    if item is None:
        await update.effective_message.reply_text(
            f"{symbol} нет в последнем проанализированном universe. Запусти /scan и попробуй снова."
        )
        return
    cfg = await store.settings()
    reasons = rejection_reasons(item, cfg) if item.stage == "NONE" else []
    reason_text = "" if not reasons else "\n\n❌ " + "; ".join(reasons)
    await update.effective_message.reply_text(
        f"{stage_label(item.stage)} <b>{item.symbol}</b> — OI <b>{item.score:.1f}</b> | Volume <b>{item.volume_score:.1f}</b> | Velocity <b>{item.velocity_score:.1f}</b>\n"
        f"OI change <b>{item.oi_change_pct:+.1f}%</b> | Price change <b>{item.price_change_pct:+.1f}%</b> | Eff <b>{item.oi_efficiency:.2f}×</b>\n"
        f"OI Velocity: <b>1h {item.oi_velocity_1h_pct:+.1f}% | 4h {item.oi_velocity_4h_pct:+.1f}% | 24h {item.oi_velocity_24h_pct:+.1f}%</b>\n\n"
        f"{history_text(item, cfg)}{reason_text}",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


async def cmd_scan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id if update.effective_user else None
    if not is_admin(uid):
        await update.effective_message.reply_text(
            "⛔ Ручной полный скан доступен только администратору."
        )
        return

    await update.effective_message.reply_text("🔎 Сканирую рынок...")
    try:
        items = await scan_market()
        cfg = await store.settings()
        hits = [s for s in items if should_alert(s)]
        stage_counts = {
            "EXPANSION": sum(1 for s in hits if s.stage == "EXPANSION"),
            "IGNITION": sum(1 for s in hits if s.stage == "IGNITION"),
            "AWAKENING": sum(1 for s in hits if s.stage in {"AWAKENING", "HIGH"}),
            "WATCH": sum(1 for s in hits if s.stage == "WATCH"),
        }
        await update.effective_message.reply_text(
            f"Готово. Проанализировано: {len(items)}; "
            f"EXPANSION: {stage_counts['EXPANSION']}, IGNITION: {stage_counts['IGNITION']}, AWAKENING: {stage_counts['AWAKENING']}, WATCH: {stage_counts['WATCH']}.\n\n"
            + top_text(items, int(cfg["top_n"]), cfg),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
    except Exception as e:
        log.exception("Manual scan failed")
        await update.effective_message.reply_text(f"Ошибка: {type(e).__name__}: {e}")


async def callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat.id
    if q.data == "subscribe":
        await store.subscribe(chat_id)
        await q.edit_message_reply_markup(reply_markup=menu_markup(True))
        await q.message.reply_text("✅ Подписка включена.")
    elif q.data == "unsubscribe":
        await store.unsubscribe(chat_id)
        await q.edit_message_reply_markup(reply_markup=menu_markup(False))
        await q.message.reply_text("🔕 Подписка отключена.")
    elif q.data == "settings":
        await q.message.reply_text(
            settings_text(await store.settings()), parse_mode=ParseMode.HTML
        )
    elif q.data == "top":
        cfg = await store.settings()
        await q.message.reply_text(
            top_text(latest_top, int(cfg["top_n"]), cfg), parse_mode=ParseMode.HTML
        )


async def scheduled_scan(context: ContextTypes.DEFAULT_TYPE):
    try:
        cfg = await store.settings()
        results = await scan_market()

        subs = await store.subscribers()
        if not subs:
            return

        for s in results:
            should_send, event_type, previous_stage = await store.alert_decision(
                s.symbol,
                effective_score(s),
                s.stage,
                cfg["alert_cooldown_hours"],
                int(cfg["downgrade_confirm_scans"]),
            )
            if not should_send:
                continue

            if event_type == "downgrade":
                text = downgrade_text(s, previous_stage, int(cfg["downgrade_confirm_scans"]), cfg)
            elif should_alert(s):
                text = signal_text(s, cfg)
            else:
                # A first-ever NONE state is never alert-worthy; this is only a safeguard.
                continue

            delivered = 0
            for chat_id in subs:
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode=ParseMode.HTML,
                        disable_web_page_preview=True,
                    )
                    delivered += 1
                except Exception as e:
                    log.warning("Delivery to %s failed: %s", chat_id, e)

            if delivered:
                await store.mark_alert(s.symbol, effective_score(s), s.stage)

    except Exception:
        log.exception("Scheduled scan failed")


def reschedule_scanner(app: Application, interval_min: float):
    if not app.job_queue:
        return
    for job in app.job_queue.get_jobs_by_name("market_scan"):
        job.schedule_removal()
    app.job_queue.run_repeating(
        scheduled_scan,
        interval=max(300, int(interval_min * 60)),
        first=10,
        name="market_scan",
    )


async def post_init(app: Application):
    await store.init()
    cfg = await store.settings()
    reschedule_scanner(app, cfg["scan_interval_min"])
    try:
        await app.bot.set_my_commands([
            ("menu", "панель управления"),
            ("subscribe", "подписаться на сигналы"),
            ("unsubscribe", "отписаться"),
            ("top", "TOP активных сигналов"),
            ("rawtop", "диагностика включая NONE"),
            ("history", "Price/OI history + OI Velocity"),
            ("settings", "показать параметры"),
            ("scan", "ручной скан (admin)"),
            ("set", "изменить параметр (admin)"),
        ])
    except Exception as exc:
        # Setting Telegram's command menu is optional. On Windows/proxy/unstable
        # networks Telegram can occasionally time out here. Do not prevent the
        # scanner from starting because of this cosmetic startup operation.
        log.warning("Could not update Telegram command menu: %s", exc)

    log.info("Bot initialized")


async def post_shutdown(app: Application):
    await store.close()
    log.info("Database pool closed")


def main():
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_start))
    app.add_handler(CommandHandler("menu", cmd_menu))
    app.add_handler(CommandHandler("subscribe", cmd_subscribe))
    app.add_handler(CommandHandler("unsubscribe", cmd_unsubscribe))
    app.add_handler(CommandHandler("settings", cmd_settings))
    app.add_handler(CommandHandler("set", cmd_set))
    app.add_handler(CommandHandler("top", cmd_top))
    app.add_handler(CommandHandler("rawtop", cmd_rawtop))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("scan", cmd_scan))
    app.add_handler(CallbackQueryHandler(callback))
    app.run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
