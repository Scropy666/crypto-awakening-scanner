CREATE TABLE IF NOT EXISTS public.bot_subscribers (
    chat_id BIGINT PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.bot_settings (
    key TEXT PRIMARY KEY,
    value DOUBLE PRECISION NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.bot_alerts (
    symbol TEXT PRIMARY KEY,
    last_alert_ts BIGINT NOT NULL DEFAULT 0,
    last_score DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_stage TEXT NOT NULL DEFAULT 'NONE',
    pending_downgrade_stage TEXT NOT NULL DEFAULT '',
    pending_downgrade_count INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bot_subscribers_created_at
    ON public.bot_subscribers(created_at);

CREATE INDEX IF NOT EXISTS idx_bot_alerts_updated_at
    ON public.bot_alerts(updated_at);

-- The bot uses a direct Postgres connection, not Supabase's public Data API.
-- Enabling RLS with no public policies prevents accidental anon/auth access
-- if these tables are exposed through the Data API.
ALTER TABLE public.bot_subscribers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bot_settings ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.bot_alerts ENABLE ROW LEVEL SECURITY;
