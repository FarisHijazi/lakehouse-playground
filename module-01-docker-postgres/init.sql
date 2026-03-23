-- =============================================================================
-- Podcast Platform Schema
-- =============================================================================
-- This schema is designed for an analytical workload on a podcast platform.
-- Design decisions are documented inline.
--
-- Key principles:
--   1. Use TEXT over VARCHAR -- Postgres stores them identically, and TEXT
--      avoids arbitrary length constraints that cause migration headaches.
--   2. Use TIMESTAMPTZ over TIMESTAMP -- always store timezone-aware timestamps.
--      Listeners span multiple timezones (SA, KW, AE, etc.).
--   3. Use domain-specific constraints -- CHECK constraints catch bad data at
--      the database level, not just in application code.
--   4. Natural keys as PRIMARY KEY where they exist (podcast_id, episode_id,
--      user_id) -- these are stable business identifiers, not surrogate keys.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- podcasts: Core podcast metadata
-- ---------------------------------------------------------------------------
-- One row per podcast. ~10 records. This is a slowly-changing dimension.
CREATE TABLE podcasts (
    podcast_id   TEXT PRIMARY KEY,             -- e.g. 'pod_001'
    name         TEXT NOT NULL,                -- Arabic name
    name_en      TEXT,                         -- English name (nullable for Arabic-only)
    category     TEXT NOT NULL,
    language     TEXT NOT NULL DEFAULT 'ar',   -- 'ar', 'en', 'mixed'
    host         TEXT NOT NULL,
    created_at   DATE NOT NULL,

    -- Guard against obviously invalid data
    CONSTRAINT chk_podcasts_language CHECK (language IN ('ar', 'en', 'mixed'))
);

COMMENT ON TABLE podcasts IS 'Podcast-level metadata. One row per show.';
COMMENT ON COLUMN podcasts.language IS 'Primary language: ar (Arabic), en (English), or mixed.';


-- ---------------------------------------------------------------------------
-- episodes: Individual episode records
-- ---------------------------------------------------------------------------
-- One row per episode. ~784 records. Linked to podcasts via podcast_id.
CREATE TABLE episodes (
    episode_id       TEXT PRIMARY KEY,         -- e.g. 'ep_0001'
    podcast_id       TEXT NOT NULL REFERENCES podcasts(podcast_id),
    title            TEXT NOT NULL,
    published_at     TIMESTAMPTZ NOT NULL,
    duration_seconds INTEGER NOT NULL,
    season           INTEGER,
    episode_number   INTEGER,

    -- Duration must be positive and reasonable (max 8 hours)
    CONSTRAINT chk_episodes_duration CHECK (
        duration_seconds > 0 AND duration_seconds <= 28800
    )
);

-- Episodes are almost always queried by podcast. This index supports
-- queries like "all episodes for pod_001 ordered by publish date."
CREATE INDEX idx_episodes_podcast_id ON episodes(podcast_id);
CREATE INDEX idx_episodes_published_at ON episodes(published_at);

COMMENT ON TABLE episodes IS 'One row per episode. FK to podcasts.';
COMMENT ON COLUMN episodes.duration_seconds IS 'Total episode duration in seconds. Max 8 hours (28800s).';


-- ---------------------------------------------------------------------------
-- users: Listener profiles
-- ---------------------------------------------------------------------------
-- One row per user. ~5000 records. The raw data is intentionally messy:
-- inconsistent date formats, missing cities, mixed gender values.
-- The load script should clean this; the schema enforces the clean contract.
CREATE TABLE users (
    user_id           TEXT PRIMARY KEY,        -- e.g. 'usr_000001'
    name              TEXT,
    email             TEXT,
    country           TEXT,                    -- ISO 2-letter code
    city              TEXT,                    -- nullable (some users have no city)
    platform          TEXT,                    -- signup platform
    signup_date       DATE,
    subscription_type TEXT,
    age               INTEGER,
    gender            TEXT,

    CONSTRAINT chk_users_subscription CHECK (
        subscription_type IN ('free', 'premium', 'trial')
    ),
    CONSTRAINT chk_users_age CHECK (age IS NULL OR (age >= 13 AND age <= 120))
);

CREATE INDEX idx_users_country ON users(country);
CREATE INDEX idx_users_signup_date ON users(signup_date);
CREATE INDEX idx_users_subscription ON users(subscription_type);

COMMENT ON TABLE users IS 'Listener profiles. Raw data has messy dates/genders -- cleaned on load.';


-- ---------------------------------------------------------------------------
-- listening_events: The fact table
-- ---------------------------------------------------------------------------
-- This is the core analytical table. ~200k rows from daily JSONL files.
-- In a production system this would be partitioned by date; here we keep
-- it simple with proper indexes.
CREATE TABLE listening_events (
    event_id         TEXT PRIMARY KEY,         -- UUID
    user_id          TEXT NOT NULL REFERENCES users(user_id),
    episode_id       TEXT NOT NULL REFERENCES episodes(episode_id),
    event_type       TEXT NOT NULL,            -- 'play', 'pause', 'resume', 'complete', 'skip'
    event_timestamp  TIMESTAMPTZ NOT NULL,
    listened_seconds INTEGER NOT NULL DEFAULT 0,
    platform         TEXT,                     -- 'ios', 'android', 'web', etc.
    country          TEXT,
    app_version      TEXT,

    CONSTRAINT chk_events_type CHECK (
        event_type IN ('play', 'pause', 'resume', 'complete', 'skip')
    ),
    CONSTRAINT chk_events_listened CHECK (listened_seconds >= 0)
);

-- The most common analytical queries on events:
--   1. "What did user X listen to?" -> idx on user_id
--   2. "How many listens for episode Y?" -> idx on episode_id
--   3. "Daily active listeners" -> idx on timestamp
--   4. Combined filter: user + time range
CREATE INDEX idx_events_user_id ON listening_events(user_id);
CREATE INDEX idx_events_episode_id ON listening_events(episode_id);
CREATE INDEX idx_events_timestamp ON listening_events(event_timestamp);
CREATE INDEX idx_events_type ON listening_events(event_type);

COMMENT ON TABLE listening_events IS 'Fact table. One row per listening event (~200k rows).';
COMMENT ON COLUMN listening_events.event_type IS 'One of: play, pause, resume, complete, skip.';


-- ---------------------------------------------------------------------------
-- cdn_logs: Content delivery network telemetry
-- ---------------------------------------------------------------------------
-- Tracks streaming quality metrics. Useful for reliability/SRE dashboards.
CREATE TABLE cdn_logs (
    log_id            TEXT PRIMARY KEY,
    event_id          TEXT,                    -- FK to listening_events (nullable, not all match)
    user_id           TEXT,
    log_timestamp     TIMESTAMPTZ NOT NULL,
    isp               TEXT,
    bitrate           TEXT,                    -- e.g. '128kbps', '64kbps'
    buffer_events     INTEGER DEFAULT 0,
    rebuffer_ratio    NUMERIC(6,4) DEFAULT 0,
    startup_time_ms   INTEGER,
    error_type        TEXT,                    -- nullable (null = no error)
    cdn_node          TEXT,
    bytes_transferred BIGINT

    -- No FK on event_id because some CDN logs reference events we may not have loaded.
    -- In production, this would be a soft reference checked at the application layer.
);

CREATE INDEX idx_cdn_timestamp ON cdn_logs(log_timestamp);
CREATE INDEX idx_cdn_user_id ON cdn_logs(user_id);
CREATE INDEX idx_cdn_error_type ON cdn_logs(error_type) WHERE error_type IS NOT NULL;

COMMENT ON TABLE cdn_logs IS 'CDN delivery telemetry. One row per stream delivery attempt.';
COMMENT ON COLUMN cdn_logs.rebuffer_ratio IS 'Fraction of playback time spent rebuffering (0.0 = perfect).';


-- ---------------------------------------------------------------------------
-- ad_events: Advertising impressions and revenue
-- ---------------------------------------------------------------------------
CREATE TABLE ad_events (
    ad_event_id      TEXT PRIMARY KEY,
    event_id         TEXT,                     -- FK to listening_events
    user_id          TEXT,
    ad_timestamp     TIMESTAMPTZ NOT NULL,
    ad_type          TEXT NOT NULL,            -- 'pre_roll', 'mid_roll', 'post_roll'
    action           TEXT NOT NULL,            -- 'impression', 'click', 'complete', 'skip'
    advertiser       TEXT,
    campaign_id      TEXT,
    revenue_sar      NUMERIC(10,4) DEFAULT 0,  -- Revenue in Saudi Riyals
    duration_seconds INTEGER,

    CONSTRAINT chk_ad_type CHECK (ad_type IN ('pre_roll', 'mid_roll', 'post_roll')),
    CONSTRAINT chk_ad_action CHECK (action IN ('impression', 'click', 'complete', 'skip')),
    CONSTRAINT chk_ad_revenue CHECK (revenue_sar >= 0)
);

CREATE INDEX idx_ad_events_user_id ON ad_events(user_id);
CREATE INDEX idx_ad_events_timestamp ON ad_events(ad_timestamp);
CREATE INDEX idx_ad_events_campaign ON ad_events(campaign_id);
CREATE INDEX idx_ad_events_advertiser ON ad_events(advertiser);

COMMENT ON TABLE ad_events IS 'Ad impression/interaction events with revenue tracking.';
COMMENT ON COLUMN ad_events.revenue_sar IS 'Revenue per event in Saudi Riyals (SAR).';
