-- =============================================================================
-- Linky Platform — Production PostgreSQL Schema (v3)
-- =============================================================================
-- Design: 11 tables. JSONB for config. Text for statuses. Polymorphic events. Magic link batch reviews.
-- Target: Supabase PostgreSQL 15+
-- Created: 2026-03-18
-- Updated: 2026-03-19 — Added batch_reviews table (magic link prospect review)
--
-- TABLES:
--   1. tenants              — Multi-tenant boundary + all config
--   2. users                — Auth + preferences + account assignments
--   3. linkedin_accounts    — Provider-agnostic LinkedIn connections + rate limits + health
--   4. campaigns            — Outreach container + ICP + timing
--   5. companies            — Deduplicated company cache (Apollo enrichment)
--   6. prospects            — Central pipeline + scoring + sequence state
--   7. invitations          — Connection request tracking + detection
--   8. messages             — Connection notes + follow-ups + HITL approval
--   9. events               — Polymorphic event log (bus, replies, webhooks, agents, notifications, jobs)
--  10. activity_log         — API call tracking for rate limiting (partitioned)
--  11. batch_reviews        — Magic link prospect review sessions (no login required)
--
-- DESIGN PATTERNS:
--   - JSONB columns for all configuration (no config tables)
--   - Text columns for statuses (no enums — easier migrations)
--   - Polymorphic events table (one table, many event types)
--   - Denormalized company info on prospects (fast reads)
--   - Soft references via text IDs where FK isn't critical
--   - Partitioned activity_log (only hot table)
--   - Provider-agnostic: LinkedIn API provider (Unipile, LinkUp, HeyReach, etc.)
--     is a config value, not baked into column names
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- HELPER
-- =============================================================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TABLE 1: tenants
-- =============================================================================
-- Multi-tenant boundary. Every data table references tenant_id.
-- All configuration lives in JSONB — no separate config tables.
--
-- settings JSONB schema:
-- {
--   "timezone": "America/Los_Angeles",
--   "business_hours": { "start": 8, "end": 18 },
--   "score_threshold": 50,           — Min ICP score for client approval queue
--   "stale_queue_days": 7,           — Days before stale queue alert
--   "escalation_days": 21,           — Days before escalation to admin
--   "brand_voice": {
--     "firm_name": "VWC CPAs",
--     "tone": "Professional, warm, relationship-focused",
--     "service_lines": ["financial_statement_audit", "ebp_audit"],
--     "competitor_firms": ["Moss Adams", "BDO"],
--     "writing_style": "..."
--   },
--   "notification_channels": {
--     "slack_webhook_url": "...",
--     "email_enabled": true
--   }
-- }
-- =============================================================================

CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    slug        TEXT NOT NULL UNIQUE,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    settings    JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_tenants_updated
    BEFORE UPDATE ON tenants FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 2: users
-- =============================================================================
-- Two roles: 'admin' (yorCMO team, cross-tenant) and 'client' (own tenant).
-- Account assignments and preferences are JSONB — no junction tables.
--
-- role: 'admin' | 'client'
--
-- settings JSONB schema:
-- {
--   "notification_prefs": {
--     "connection_accepted": { "email": true, "slack": false },
--     "reply_detected": { "email": true, "slack": true },
--     "weekly_report": { "email": true }
--   },
--   "linkedin_account_ids": ["uuid1", "uuid2"],  — Assigned accounts (client scoping)
--   "primary_account_id": "uuid1"                 — Default account shown in UI
-- }
-- =============================================================================

CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID REFERENCES tenants(id),   -- NULL for admin (cross-tenant)
    clerk_user_id   TEXT UNIQUE,
    email           TEXT NOT NULL,
    full_name       TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'client' CHECK (role IN ('admin', 'client')),
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    settings        JSONB NOT NULL DEFAULT '{}',
    last_login_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_users_tenant ON users(tenant_id) WHERE tenant_id IS NOT NULL;

CREATE TRIGGER trg_users_updated
    BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 3: linkedin_accounts
-- =============================================================================
-- LinkedIn accounts connected via an API provider (Unipile, LinkUp, HeyReach, etc.)
-- The provider is a config value — NOT baked into column names.
-- Switching providers = change `provider` + `provider_account_id`, app adapter handles the rest.
--
-- provider: 'unipile' | 'linkup' | 'heyreach' | (future providers)
-- status: 'ok' | 'credentials' | 'restricted' | 'disconnected' | 'reconnecting'
-- account_type: 'free' | 'premium' | 'sales_navigator'
--
-- rate_limits JSONB schema:
-- {
--   "connection":   { "daily": 5,  "weekly": 20, "ramp": [3, 4, 5],  "note_chars": 200, "delay": [45, 120] },
--   "message":      { "daily": 50, "weekly": null, "ramp": [20, 30, 40], "delay": [45, 120] },
--   "profile_view": { "daily": 80, "weekly": null, "ramp": [30, 50, 60], "delay": [30, 90] },
--   "search":       { "daily": null, "weekly": null, "ramp": null, "delay": [30, 60] }
-- }
--
-- provider_config JSONB schema (provider-specific connection details):
-- Unipile:  {"dsn": "https://api25.unipile.com:15572", "api_key": "vault:unipile_key"}
-- LinkUp:   {"api_key": "vault:linkup_key", "base_url": "https://api.linkup.com"}
-- HeyReach: {"api_key": "vault:heyreach_key", "workspace_id": "..."}
-- =============================================================================

CREATE TABLE linkedin_accounts (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    -- Provider abstraction (swap Unipile for anything else without schema changes)
    provider            TEXT NOT NULL DEFAULT 'unipile',  -- 'unipile' | 'linkup' | 'heyreach'
    provider_account_id TEXT NOT NULL,             -- The provider's internal account ID (was: unipile_account_id)
    provider_config     JSONB NOT NULL DEFAULT '{}', -- Provider-specific connection details (DSN, API keys, etc.)
    -- LinkedIn identity (provider-agnostic — these are LinkedIn's own IDs)
    linkedin_member_id  TEXT,                      -- LinkedIn URN / provider_id (universal across providers)
    owner_name          TEXT NOT NULL,
    linkedin_slug       TEXT,
    account_type        TEXT NOT NULL DEFAULT 'free' CHECK (account_type IN ('free', 'premium', 'sales_navigator')),
    -- Status + health
    status              TEXT NOT NULL DEFAULT 'ok',
    connected_at        TIMESTAMPTZ NOT NULL,
    reconnected_at      TIMESTAMPTZ,              -- 7-day cooldown after reconnect
    last_health_check   TIMESTAMPTZ,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    -- Rate limits (provider-agnostic — these are LinkedIn's limits, not the provider's)
    rate_limits         JSONB NOT NULL DEFAULT '{}',
    settings            JSONB NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Unique per provider (same LinkedIn account could exist in two providers during migration)
    UNIQUE(provider, provider_account_id)
);

CREATE INDEX idx_accounts_tenant ON linkedin_accounts(tenant_id);

CREATE TRIGGER trg_accounts_updated
    BEFORE UPDATE ON linkedin_accounts FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- Auto-seed default rate limits based on account_type
CREATE OR REPLACE FUNCTION seed_rate_limits()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.rate_limits = '{}'::JSONB THEN
        CASE NEW.account_type
            WHEN 'free' THEN
                NEW.rate_limits = '{
                    "connection":   {"daily": 5,  "weekly": 20,   "ramp": [3, 4, 5],     "note_chars": 200, "delay": [45, 120]},
                    "message":      {"daily": 50, "weekly": null,  "ramp": [20, 30, 40],   "delay": [45, 120]},
                    "profile_view": {"daily": 80, "weekly": null,  "ramp": [30, 50, 60],   "delay": [30, 90]},
                    "search":       {"daily": null, "weekly": null, "ramp": null,           "delay": [30, 60]}
                }'::JSONB;
            WHEN 'premium' THEN
                NEW.rate_limits = '{
                    "connection":   {"daily": 25, "weekly": 100,  "ramp": [20, 20, 25],   "note_chars": 300, "delay": [45, 120]},
                    "message":      {"daily": 50, "weekly": null,  "ramp": [20, 30, 40],   "delay": [45, 120]},
                    "profile_view": {"daily": 80, "weekly": null,  "ramp": [30, 50, 60],   "delay": [30, 90]},
                    "search":       {"daily": null, "weekly": null, "ramp": null,           "delay": [30, 60]}
                }'::JSONB;
            WHEN 'sales_navigator' THEN
                NEW.rate_limits = '{
                    "connection":   {"daily": 25, "weekly": 100,  "ramp": [20, 20, 25],   "note_chars": 300, "delay": [45, 120]},
                    "message":      {"daily": 50, "weekly": null,  "ramp": [20, 30, 40],   "delay": [45, 120]},
                    "profile_view": {"daily": 150, "weekly": null, "ramp": [50, 80, 100],  "delay": [30, 90]},
                    "search":       {"daily": null, "weekly": null, "ramp": null,           "delay": [30, 60]}
                }'::JSONB;
        END CASE;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_accounts_seed_limits
    BEFORE INSERT ON linkedin_accounts FOR EACH ROW EXECUTE FUNCTION seed_rate_limits();

-- =============================================================================
-- TABLE 4: campaigns
-- =============================================================================
-- Outreach campaign. ICP criteria, Apollo search params, and timing all live
-- in JSONB — no separate icps table. Version ICPs by appending to a JSONB array.
--
-- status: 'draft' | 'active' | 'paused' | 'completed' | 'archived'
--
-- icp JSONB schema:
-- {
--   "version": 1,
--   "raw_input": "CFOs at manufacturing companies in Seattle...",
--   "target_titles": ["CFO", "Controller", "VP Finance"],
--   "target_seniorities": ["c_suite", "vp", "director"],
--   "target_industries": ["manufacturing"],
--   "target_locations": ["Seattle, WA"],
--   "employee_count_ranges": ["101,300"],
--   "excluded_industries": ["healthcare"],
--   "excluded_companies": ["Competitor Inc"],
--   "weights": { "title": 0.30, "industry": 0.20, "size": 0.15, "geo": 0.10, "seniority": 0.15, "triggers": 0.10 },
--   "apollo_search_params": { ... },
--   "scoring_rules": { "pe_penalty": -10 },
--   "title_synonyms": { "CFO": ["Chief Financial Officer", "Finance Director"] }
-- }
--
-- timing JSONB schema:
-- {
--   "msg1_delay_days": 1,
--   "msg2_delay_days": 14,
--   "msg3_delay_days": 14
-- }
--
-- settings JSONB schema:
-- {
--   "daily_enrichment_budget": 50,
--   "auto_search_enabled": true,
--   "auto_search_batch_size": 25,
--   "sender_profile": { "name": "Adrienne Nordland", "title": "Audit Partner" }
-- }
-- =============================================================================

CREATE TABLE campaigns (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    linkedin_account_id UUID NOT NULL REFERENCES linkedin_accounts(id),
    name                TEXT NOT NULL,
    description         TEXT,
    status              TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'active', 'paused', 'completed', 'archived')),
    icp                 JSONB NOT NULL DEFAULT '{}',
    timing              JSONB NOT NULL DEFAULT '{"msg1_delay_days": 1, "msg2_delay_days": 14, "msg3_delay_days": 14}',
    settings            JSONB NOT NULL DEFAULT '{}',
    stats_cache         JSONB NOT NULL DEFAULT '{}',   -- Periodically updated KPIs
    activated_at        TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_campaigns_tenant ON campaigns(tenant_id);
CREATE INDEX idx_campaigns_active ON campaigns(tenant_id, status) WHERE status = 'active';

CREATE TRIGGER trg_campaigns_updated
    BEFORE UPDATE ON campaigns FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 5: companies
-- =============================================================================
-- Deduplicated company cache. Prevents re-enriching the same company for
-- different prospects. Lightweight — just a cache of Apollo/Unipile data.
-- =============================================================================

CREATE TABLE companies (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    name            TEXT NOT NULL,
    domain          TEXT,
    industry        TEXT,
    employee_count  INTEGER,
    linkedin_url    TEXT,
    data            JSONB NOT NULL DEFAULT '{}',    -- Full enrichment payload (Apollo, Unipile)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_companies_domain ON companies(tenant_id, domain) WHERE domain IS NOT NULL;
CREATE INDEX idx_companies_tenant ON companies(tenant_id);

CREATE TRIGGER trg_companies_updated
    BEFORE UPDATE ON companies FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 6: prospects
-- =============================================================================
-- Central pipeline table. One row per person per campaign.
-- Company info denormalized for fast list display.
-- Sequence state lives here (no separate sequences table).
--
-- status (prospect state machine):
--   'sourced' → 'scored' → 'queued' → 'approved' → 'invite_sent' → 'connected'
--   → 'msg1_pending' → 'msg1_sent' → 'msg2_pending' → 'msg2_sent'
--   → 'msg3_pending' → 'msg3_sent' → 'completed'
--   At any point: → 'replied' (stops everything)
--   Terminal: 'skipped' | 'declined' | 'recycled' | 'blacklisted'
--
-- source: 'apollo' | 'linkedin' | 'import' | 'manual'
--
-- scoring JSONB schema:
-- {
--   "score": 85,
--   "breakdown": { "title": 28, "industry": 18, "size": 12, "geo": 10, "seniority": 12, "triggers": 5 },
--   "reasoning": "Strong title match (CFO), target industry...",
--   "icp_version": 1,
--   "scored_at": "2026-03-18T..."
-- }
--
-- sequence JSONB schema:
-- {
--   "chat_id": "xvLUrzSyWHa5kX0Lxv-7WA",
--   "current_step": 1,
--   "started_at": "2026-03-18T...",
--   "msg1_sent_at": "2026-03-18T...",
--   "msg2_scheduled_for": "2026-04-01T...",
--   "warmth_score": 5.0
-- }
-- =============================================================================

CREATE TABLE prospects (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id            UUID NOT NULL REFERENCES tenants(id),
    campaign_id          UUID NOT NULL REFERENCES campaigns(id),
    linkedin_account_id  UUID NOT NULL REFERENCES linkedin_accounts(id),
    company_id           UUID REFERENCES companies(id),
    -- Identity
    linkedin_provider_id TEXT,
    linkedin_slug        TEXT,
    linkedin_url         TEXT,
    apollo_person_id     TEXT,
    -- Profile (denormalized for display)
    first_name           TEXT,
    last_name            TEXT,
    email                TEXT,
    headline             TEXT,
    title                TEXT,
    seniority            TEXT,
    location             TEXT,
    -- Company (denormalized for display)
    company_name         TEXT,
    company_domain       TEXT,
    -- Pipeline
    status               TEXT NOT NULL DEFAULT 'sourced',
    source               TEXT NOT NULL DEFAULT 'apollo',
    scoring              JSONB NOT NULL DEFAULT '{}',
    sequence             JSONB NOT NULL DEFAULT '{}',
    -- Raw data preservation
    raw_data             JSONB,                         -- Full Apollo enrichment
    enriched_at          TIMESTAMPTZ,
    status_changed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- One prospect per campaign per LinkedIn identity
    UNIQUE(campaign_id, linkedin_provider_id)
);

CREATE INDEX idx_prospects_campaign_status ON prospects(campaign_id, status);
CREATE INDEX idx_prospects_account_status ON prospects(linkedin_account_id, status);
CREATE INDEX idx_prospects_queue ON prospects(tenant_id, linkedin_account_id)
    WHERE status = 'queued';
CREATE INDEX idx_prospects_linkedin ON prospects(tenant_id, linkedin_provider_id)
    WHERE linkedin_provider_id IS NOT NULL;

CREATE TRIGGER trg_prospects_updated
    BEFORE UPDATE ON prospects FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 7: invitations
-- =============================================================================
-- Connection request tracking. Critical because Unipile's GET /invitations/sent
-- returns 404. This is our only record of pending invitations.
--
-- status: 'sent' | 'accepted' | 'declined' | 'expired' | 'cancelled'
-- detection_method: 'webhook_message' | 'webhook_relation' | 'poll_relations' | 'poll_profile' | 'linkup' | 'manual'
-- =============================================================================

CREATE TABLE invitations (
    id                      UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id               UUID NOT NULL REFERENCES tenants(id),
    linkedin_account_id     UUID NOT NULL REFERENCES linkedin_accounts(id),
    prospect_id             UUID NOT NULL REFERENCES prospects(id),
    campaign_id             UUID NOT NULL REFERENCES campaigns(id),
    provider_id             TEXT NOT NULL,              -- Target LinkedIn provider_id
    -- Connection note
    note_text               TEXT,                       -- What was actually sent
    note_original_text      TEXT,                       -- AI draft (before client edits)
    -- Status
    status                  TEXT NOT NULL DEFAULT 'sent',
    sent_at                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    accepted_at             TIMESTAMPTZ,
    chat_id                 TEXT,                        -- Chat created on acceptance
    detection_method        TEXT,
    last_checked_at         TIMESTAMPTZ,                -- Last polling attempt
    -- Provider response (provider-agnostic)
    external_invitation_id  TEXT,                   -- The provider's invitation ID (was: unipile_invitation_id)
    data                    JSONB NOT NULL DEFAULT '{}', -- Provider-specific response data + extra tracking
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One pending invitation per prospect per account
CREATE UNIQUE INDEX idx_invitations_pending
    ON invitations(linkedin_account_id, prospect_id) WHERE status = 'sent';
-- Stale invitation detection (for expiry cron)
CREATE INDEX idx_invitations_stale ON invitations(sent_at) WHERE status = 'sent';
-- Polling: find invitations needing acceptance check
CREATE INDEX idx_invitations_poll ON invitations(linkedin_account_id, last_checked_at)
    WHERE status = 'sent';
CREATE INDEX idx_invitations_prospect ON invitations(prospect_id);

CREATE TRIGGER trg_invitations_updated
    BEFORE UPDATE ON invitations FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 8: messages
-- =============================================================================
-- Every outbound message: connection notes (step=0) and follow-ups (step=1,2,3).
-- HITL approval workflow: draft → pending_approval → approved → sent.
-- No separate sequences table — sequence state lives on prospects.sequence JSONB.
--
-- step: 0 = connection_note, 1 = msg1, 2 = msg2, 3 = msg3
-- status: 'draft' | 'pending_approval' | 'approved' | 'scheduled' | 'sending' | 'sent' | 'rejected' | 'failed' | 'cancelled'
-- =============================================================================

CREATE TABLE messages (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    prospect_id         UUID NOT NULL REFERENCES prospects(id),
    linkedin_account_id UUID NOT NULL REFERENCES linkedin_accounts(id),
    campaign_id         UUID NOT NULL REFERENCES campaigns(id),
    -- Message identity
    step                INTEGER NOT NULL CHECK (step BETWEEN 0 AND 3),
    -- Content
    original_text       TEXT NOT NULL,              -- AI-generated (preserved for learning)
    approved_text       TEXT,                       -- After client edits (what gets sent)
    -- HITL
    status              TEXT NOT NULL DEFAULT 'draft',
    approved_by         UUID REFERENCES users(id),
    approved_at         TIMESTAMPTZ,
    rejected_by         UUID REFERENCES users(id),
    rejected_at         TIMESTAMPTZ,
    rejection_reason    TEXT,
    -- Scheduling + delivery
    scheduled_for       TIMESTAMPTZ,
    sent_at             TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    failure_reason      TEXT,
    -- Provider response (provider-agnostic)
    external_message_id TEXT,                   -- The provider's message ID (was: unipile_message_id)
    chat_id             TEXT,                   -- Provider's chat/thread ID
    -- AI generation metadata (for Agent 6 learning)
    generation          JSONB NOT NULL DEFAULT '{}', -- {model, prompt_name, tokens, cost_usd}
    -- Client edit tracking
    was_edited          BOOLEAN NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- One message per step per prospect
    UNIQUE(prospect_id, step)
);

-- Client approval queue
CREATE INDEX idx_messages_approval ON messages(tenant_id, linkedin_account_id)
    WHERE status = 'pending_approval';
-- Scheduled sends
CREATE INDEX idx_messages_scheduled ON messages(scheduled_for)
    WHERE status IN ('approved', 'scheduled') AND sent_at IS NULL;
-- Failed (for retry)
CREATE INDEX idx_messages_failed ON messages(failed_at)
    WHERE status = 'failed';
CREATE INDEX idx_messages_prospect ON messages(prospect_id, step);

CREATE TRIGGER trg_messages_updated
    BEFORE UPDATE ON messages FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- TABLE 9: events
-- =============================================================================
-- Polymorphic event log. Single table replaces: reply_events, webhook_events,
-- agent_executions, notifications, job_executions, and the event bus.
--
-- event_type examples:
--   Pipeline:      'prospect_scored', 'prospect_approved', 'prospect_skipped'
--   Outreach:      'invite_sent', 'connection_accepted', 'message_sent'
--   Replies:       'reply_detected', 'reply_classified'
--   System:        'account_warning', 'rate_limit_hit', 'health_check'
--   Agents:        'agent_started', 'agent_completed', 'agent_failed'
--   Webhooks:      'webhook_received', 'webhook_processed'
--   Notifications: 'notification_sent', 'reminder_sent'
--   Jobs:          'job_started', 'job_completed', 'job_failed'
--   Analytics:     'weekly_report_generated', 'icp_refinement_suggested'
--
-- data JSONB is the event payload — schema varies by event_type.
--
-- Example: reply_detected
-- {
--   "chat_id": "abc123",
--   "reply_text": "Thanks for reaching out...",
--   "sentiment": "positive",
--   "classification": "interested",
--   "confidence": 0.92,
--   "reasoning": "Prospect expressed interest in learning more",
--   "detected_via": "webhook"
-- }
--
-- Example: agent_completed
-- {
--   "agent": "prospect_finder",
--   "duration_ms": 4500,
--   "model": "gpt-5-2026-03",
--   "tokens": { "input": 1500, "output": 500 },
--   "cost_usd": 0.005,
--   "result": { "prospects_found": 25, "prospects_scored": 25 }
-- }
--
-- Example: webhook_received
-- {
--   "source": "unipile",
--   "webhook_type": "new_message",
--   "raw_payload": { ... },
--   "processing_status": "processed",
--   "action_taken": "reply_detected for prospect xyz"
-- }
-- =============================================================================

CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id),
    event_type      TEXT NOT NULL,
    -- Optional scoping (not all events relate to a campaign/prospect)
    campaign_id     UUID,
    prospect_id     UUID,
    -- Payload
    data            JSONB NOT NULL DEFAULT '{}',
    -- Source
    actor           TEXT NOT NULL,                  -- 'system', 'cron', 'webhook', 'api', 'agent:prospect_finder', 'user:uuid'
    -- Processing (for event bus consumers)
    processed       BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at    TIMESTAMPTZ,
    error           TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Event bus: unprocessed events
CREATE INDEX idx_events_unprocessed ON events(created_at) WHERE processed = FALSE;
-- Query by type (e.g., all replies, all agent runs)
CREATE INDEX idx_events_type ON events(event_type, created_at DESC);
-- Query by prospect (timeline view)
CREATE INDEX idx_events_prospect ON events(prospect_id, created_at DESC) WHERE prospect_id IS NOT NULL;
-- Query by campaign
CREATE INDEX idx_events_campaign ON events(campaign_id, created_at DESC) WHERE campaign_id IS NOT NULL;
-- Query by tenant + type (e.g., "all replies for this tenant")
CREATE INDEX idx_events_tenant_type ON events(tenant_id, event_type, created_at DESC);

-- =============================================================================
-- TABLE 10: activity_log (PARTITIONED)
-- =============================================================================
-- Every external API call. Source of truth for rate limiting.
-- Partitioned by month — rate limit queries only scan current partition.
-- Kept separate from events because it's high-volume + partitioned.
--
-- action: 'connection' | 'message' | 'profile_view' | 'search' | 'company_lookup'
--       | 'relations_check' | 'chat_check' | 'health_check' | 'linkup_check'
-- =============================================================================

CREATE TABLE activity_log (
    id                  BIGSERIAL,
    tenant_id           UUID NOT NULL,
    linkedin_account_id UUID NOT NULL,
    action              TEXT NOT NULL,
    target_provider_id  TEXT,
    endpoint            TEXT NOT NULL,
    response_status     INTEGER,
    response_id         TEXT,                       -- invitation_id, chat_id, etc.
    success             BOOLEAN NOT NULL DEFAULT TRUE,
    duration_ms         INTEGER,
    error_message       TEXT,
    campaign_id         UUID,
    prospect_id         UUID,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Partitions (extend quarterly)
CREATE TABLE activity_log_2026_q1 PARTITION OF activity_log
    FOR VALUES FROM ('2026-01-01') TO ('2026-04-01');
CREATE TABLE activity_log_2026_q2 PARTITION OF activity_log
    FOR VALUES FROM ('2026-04-01') TO ('2026-07-01');
CREATE TABLE activity_log_2026_q3 PARTITION OF activity_log
    FOR VALUES FROM ('2026-07-01') TO ('2026-10-01');
CREATE TABLE activity_log_2026_q4 PARTITION OF activity_log
    FOR VALUES FROM ('2026-10-01') TO ('2027-01-01');

-- Rate limiting: "how many connections did account X send today?"
CREATE INDEX idx_activity_rate ON activity_log(linkedin_account_id, action, created_at)
    WHERE success = TRUE;

-- =============================================================================
-- TABLE 11: batch_reviews
-- =============================================================================
-- Magic link prospect review sessions. Admin sends client a tokenized URL
-- via email — client clicks, sees an Airtable-like table, approves/skips/
-- blacklists prospects. No login required. Token is the auth.
--
-- Security:
--   - Raw token: secrets.token_urlsafe(32) (256-bit entropy)
--   - Storage: SHA-256 hash only (raw token only exists in URL + email)
--   - Expiry: 14 days default
--   - Scope: token grants access to one batch's prospect_ids only
--   - No RLS: backend validates token with service-role key
--
-- prospect_ids: UUID array — snapshot of the batch at creation time.
-- Counters: maintained by backend on each prospect action.
-- =============================================================================

CREATE TABLE batch_reviews (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    campaign_id         UUID NOT NULL REFERENCES campaigns(id),
    token_hash          TEXT NOT NULL UNIQUE,       -- SHA-256 of raw token (never store raw)
    prospect_ids        UUID[] NOT NULL,            -- Snapshot of prospects in this batch
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

CREATE INDEX idx_batch_reviews_tenant ON batch_reviews(tenant_id);
CREATE INDEX idx_batch_reviews_campaign ON batch_reviews(campaign_id);
CREATE INDEX idx_batch_reviews_token ON batch_reviews(token_hash);

CREATE TRIGGER trg_batch_reviews_updated
    BEFORE UPDATE ON batch_reviews FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- =============================================================================
-- FUNCTIONS
-- =============================================================================

-- Get effective daily limit accounting for ramp-up and cooldown
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

    -- No daily limit configured
    IF (v_limits ->> 'daily') IS NULL THEN RETURN NULL; END IF;

    v_days := EXTRACT(DAY FROM NOW() - v_account.connected_at);
    v_ramp := v_limits -> 'ramp';

    -- Ramp-up: [week1, week2, week3+]
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
      AND action = p_action
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
    v_hours JSONB;
    v_hour INTEGER;
BEGIN
    SELECT
        COALESCE(la.settings ->> 'timezone', t.settings ->> 'timezone', 'America/Los_Angeles'),
        COALESCE(la.settings -> 'business_hours', t.settings -> 'business_hours', '{"start": 8, "end": 18}'::JSONB)
    INTO v_tz, v_hours
    FROM linkedin_accounts la JOIN tenants t ON t.id = la.tenant_id
    WHERE la.id = p_account_id;

    v_hour := EXTRACT(HOUR FROM NOW() AT TIME ZONE v_tz);
    RETURN v_hour >= (v_hours ->> 'start')::INTEGER AND v_hour < (v_hours ->> 'end')::INTEGER;
END;
$$ LANGUAGE plpgsql STABLE;

-- =============================================================================
-- RLS
-- =============================================================================

-- Helper: resolve tenant
CREATE OR REPLACE FUNCTION get_my_tenant_id()
RETURNS UUID AS $$
    SELECT tenant_id FROM users WHERE clerk_user_id = auth.uid()::TEXT LIMIT 1;
$$ LANGUAGE SQL SECURITY DEFINER STABLE;

-- Helper: am I admin?
CREATE OR REPLACE FUNCTION is_admin()
RETURNS BOOLEAN AS $$
    SELECT role = 'admin' FROM users WHERE clerk_user_id = auth.uid()::TEXT LIMIT 1;
$$ LANGUAGE SQL SECURITY DEFINER STABLE;

-- Helper: my assigned LinkedIn account IDs
CREATE OR REPLACE FUNCTION my_account_ids()
RETURNS UUID[] AS $$
    SELECT COALESCE(
        (SELECT ARRAY(
            SELECT (jsonb_array_elements_text(settings -> 'linkedin_account_ids'))::UUID
            FROM users WHERE clerk_user_id = auth.uid()::TEXT
        )),
        '{}'::UUID[]
    );
$$ LANGUAGE SQL SECURITY DEFINER STABLE;

-- Enable RLS
ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE linkedin_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE campaigns ENABLE ROW LEVEL SECURITY;
ALTER TABLE companies ENABLE ROW LEVEL SECURITY;
ALTER TABLE prospects ENABLE ROW LEVEL SECURITY;
ALTER TABLE invitations ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE events ENABLE ROW LEVEL SECURITY;
ALTER TABLE activity_log ENABLE ROW LEVEL SECURITY;

-- Tenants
CREATE POLICY admin_all ON tenants FOR ALL USING (is_admin());
CREATE POLICY client_own ON tenants FOR SELECT USING (id = get_my_tenant_id());

-- Users
CREATE POLICY admin_all ON users FOR ALL USING (is_admin());
CREATE POLICY client_own ON users FOR SELECT USING (tenant_id = get_my_tenant_id());

-- LinkedIn Accounts
CREATE POLICY admin_all ON linkedin_accounts FOR ALL USING (is_admin());
CREATE POLICY client_own ON linkedin_accounts FOR SELECT
    USING (tenant_id = get_my_tenant_id() AND id = ANY(my_account_ids()));

-- Campaigns
CREATE POLICY admin_all ON campaigns FOR ALL USING (is_admin());
CREATE POLICY client_read ON campaigns FOR SELECT
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));

-- Companies
CREATE POLICY admin_all ON companies FOR ALL USING (is_admin());
CREATE POLICY client_read ON companies FOR SELECT USING (tenant_id = get_my_tenant_id());

-- Prospects (clients can read + update for approval)
CREATE POLICY admin_all ON prospects FOR ALL USING (is_admin());
CREATE POLICY client_read ON prospects FOR SELECT
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));
CREATE POLICY client_update ON prospects FOR UPDATE
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));

-- Invitations
CREATE POLICY admin_all ON invitations FOR ALL USING (is_admin());
CREATE POLICY client_read ON invitations FOR SELECT
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));

-- Messages (clients can read + update for approval)
CREATE POLICY admin_all ON messages FOR ALL USING (is_admin());
CREATE POLICY client_read ON messages FOR SELECT
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));
CREATE POLICY client_update ON messages FOR UPDATE
    USING (tenant_id = get_my_tenant_id() AND linkedin_account_id = ANY(my_account_ids()));

-- Events
CREATE POLICY admin_all ON events FOR ALL USING (is_admin());
CREATE POLICY client_read ON events FOR SELECT USING (tenant_id = get_my_tenant_id());

-- Activity Log
CREATE POLICY admin_all ON activity_log FOR ALL USING (is_admin());
CREATE POLICY client_read ON activity_log FOR SELECT USING (tenant_id = get_my_tenant_id());
