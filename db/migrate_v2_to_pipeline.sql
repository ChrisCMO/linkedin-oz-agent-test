-- =============================================================================
-- Migration: v2 → Agentic Pipeline
-- Run this in Supabase Dashboard → SQL Editor
-- =============================================================================
-- Adds: events table, batch_reviews table, DB functions, missing columns
-- Renames unipile_account_id → provider_account_id (provider-agnostic)
-- =============================================================================

-- 1. Create events table (polymorphic event log)
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    event_type      TEXT NOT NULL,
    campaign_id     UUID,
    prospect_id     UUID,
    data            JSONB NOT NULL DEFAULT '{}',
    actor           TEXT NOT NULL DEFAULT 'system',
    processed       BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_prospect ON events(prospect_id, created_at DESC) WHERE prospect_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_campaign ON events(campaign_id, created_at DESC) WHERE campaign_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_tenant_type ON events(tenant_id, event_type, created_at DESC);

-- 2. Create batch_reviews table (magic link prospect review)
CREATE TABLE IF NOT EXISTS batch_reviews (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    campaign_id         UUID NOT NULL REFERENCES campaigns(id),
    token_hash          TEXT NOT NULL UNIQUE,
    prospect_ids        UUID[] NOT NULL,
    total_count         INTEGER NOT NULL DEFAULT 0,
    approved_count      INTEGER NOT NULL DEFAULT 0,
    skipped_count       INTEGER NOT NULL DEFAULT 0,
    blacklisted_count   INTEGER NOT NULL DEFAULT 0,
    sent_to_email       TEXT NOT NULL,
    sent_by             UUID REFERENCES users(id),
    expires_at          TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '14 days'),
    last_accessed_at    TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_reviews_tenant ON batch_reviews(tenant_id);
CREATE INDEX IF NOT EXISTS idx_batch_reviews_campaign ON batch_reviews(campaign_id);
CREATE INDEX IF NOT EXISTS idx_batch_reviews_token ON batch_reviews(token_hash);

-- 3. Make linkedin_accounts provider-agnostic
-- Rename unipile_account_id → provider_account_id (works with any LinkedIn API provider)
ALTER TABLE linkedin_accounts RENAME COLUMN unipile_account_id TO provider_account_id;
ALTER TABLE linkedin_accounts ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT 'unipile';
ALTER TABLE linkedin_accounts ADD COLUMN IF NOT EXISTS provider_config JSONB NOT NULL DEFAULT '{}';
ALTER TABLE linkedin_accounts ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE linkedin_accounts ADD COLUMN IF NOT EXISTS rate_limits JSONB NOT NULL DEFAULT '{}';
ALTER TABLE linkedin_accounts ADD COLUMN IF NOT EXISTS reconnected_at TIMESTAMPTZ;

-- 3b. Make invitations provider-agnostic
ALTER TABLE invitations RENAME COLUMN unipile_invitation_id TO external_invitation_id;

-- 4. Add missing columns to messages (for pipeline — prospect_id, linkedin_account_id, campaign_id, chat_id)
ALTER TABLE messages ADD COLUMN IF NOT EXISTS prospect_id UUID REFERENCES prospects(id);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS linkedin_account_id UUID REFERENCES linkedin_accounts(id);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS campaign_id UUID REFERENCES campaigns(id);
ALTER TABLE messages ADD COLUMN IF NOT EXISTS chat_id TEXT;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS was_edited BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE messages ADD COLUMN IF NOT EXISTS generation JSONB NOT NULL DEFAULT '{}';
ALTER TABLE messages ADD COLUMN IF NOT EXISTS external_message_id TEXT;

-- 5. Add missing columns to activity_log
ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS campaign_id UUID;
ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS prospect_id UUID;
ALTER TABLE activity_log ADD COLUMN IF NOT EXISTS action TEXT;

-- Backfill action from action_type for existing rows
UPDATE activity_log SET action = action_type WHERE action IS NULL AND action_type IS NOT NULL;

-- 6. Add missing columns to invitations
ALTER TABLE invitations ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;
-- external_invitation_id already created by rename in step 3b
ALTER TABLE invitations ADD COLUMN IF NOT EXISTS data JSONB NOT NULL DEFAULT '{}';

-- 7. Create updated_at trigger for new tables
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$ BEGIN
    CREATE TRIGGER trg_batch_reviews_updated_at
        BEFORE UPDATE ON batch_reviews
        FOR EACH ROW EXECUTE FUNCTION update_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- 8. DB Functions for rate limiting

-- Get effective daily limit (accounting for ramp-up and cooldown)
CREATE OR REPLACE FUNCTION get_effective_limit(
    p_account_id UUID,
    p_action TEXT
)
RETURNS INTEGER AS $$
DECLARE
    v_account RECORD;
    v_days INTEGER;
    v_limits JSONB;
    v_ramp JSONB;
BEGIN
    SELECT connected_at, reconnected_at, status, rate_limits
    INTO v_account FROM linkedin_accounts WHERE id = p_account_id;

    IF v_account IS NULL THEN RETURN 0; END IF;

    -- Blocked states
    IF v_account.status IN ('restricted', 'disconnected', 'reconnecting') THEN
        RETURN 0;
    END IF;

    -- 7-day cooldown after reconnect
    IF v_account.reconnected_at IS NOT NULL
       AND v_account.reconnected_at > NOW() - INTERVAL '7 days' THEN
        RETURN 0;
    END IF;

    v_limits := v_account.rate_limits -> p_action;
    IF v_limits IS NULL THEN RETURN NULL; END IF;
    IF (v_limits ->> 'daily') IS NULL THEN RETURN NULL; END IF;

    v_days := EXTRACT(DAY FROM NOW() - v_account.connected_at);
    v_ramp := v_limits -> 'ramp';

    IF v_ramp IS NOT NULL AND jsonb_typeof(v_ramp) = 'array' THEN
        IF v_days < 7  THEN RETURN (v_ramp ->> 0)::INTEGER; END IF;
        IF v_days < 14 THEN RETURN (v_ramp ->> 1)::INTEGER; END IF;
        IF v_days < 30 THEN RETURN (v_ramp ->> 2)::INTEGER; END IF;
    END IF;

    RETURN (v_limits ->> 'daily')::INTEGER;
END;
$$ LANGUAGE plpgsql STABLE;

-- Check if action is within rate limit
CREATE OR REPLACE FUNCTION check_rate_limit(
    p_account_id UUID,
    p_action TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    v_limit INTEGER;
    v_count INTEGER;
BEGIN
    v_limit := get_effective_limit(p_account_id, p_action);
    IF v_limit IS NULL THEN RETURN TRUE; END IF;
    IF v_limit = 0 THEN RETURN FALSE; END IF;

    SELECT COUNT(*) INTO v_count
    FROM activity_log
    WHERE linkedin_account_id = p_account_id
      AND (action = p_action OR action_type = p_action)
      AND success = TRUE
      AND created_at >= date_trunc('day', NOW());

    RETURN v_count < v_limit;
END;
$$ LANGUAGE plpgsql STABLE;

-- Check business hours for an account
CREATE OR REPLACE FUNCTION is_business_hours(p_account_id UUID)
RETURNS BOOLEAN AS $$
DECLARE
    v_tz TEXT;
    v_start INTEGER;
    v_end INTEGER;
    v_hour INTEGER;
BEGIN
    SELECT
        COALESCE(la.settings ->> 'timezone', 'America/Los_Angeles'),
        COALESCE(la.business_hours_start, 8),
        COALESCE(la.business_hours_end, 18)
    INTO v_tz, v_start, v_end
    FROM linkedin_accounts la
    WHERE la.id = p_account_id;

    v_hour := EXTRACT(HOUR FROM NOW() AT TIME ZONE v_tz);
    RETURN v_hour >= v_start AND v_hour < v_end;
END;
$$ LANGUAGE plpgsql STABLE;

-- 9. Seed rate limits for existing accounts
UPDATE linkedin_accounts
SET rate_limits = '{
    "connection": {"daily": 5, "weekly": 20, "ramp": [3, 4, 5], "note_chars": 200, "delay": [45, 120]},
    "message": {"daily": 50, "weekly": null, "ramp": [20, 30, 40], "delay": [45, 120]},
    "profile_view": {"daily": 80, "weekly": null, "ramp": [30, 50, 60], "delay": [30, 90]},
    "search": {"daily": null, "weekly": null, "ramp": null, "delay": [30, 60]}
}'::JSONB
WHERE rate_limits = '{}'::JSONB AND account_type = 'free';

-- 10. Enable RLS on new tables
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE batch_reviews ENABLE ROW LEVEL SECURITY;

-- Service role key bypasses RLS, so no policies needed for backend operations

-- Done!
SELECT 'Migration complete: events + batch_reviews tables created, columns added, DB functions installed' AS status;
