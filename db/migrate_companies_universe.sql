-- =============================================================================
-- Migration: Company Universe + Batch Tracking
-- Run in Supabase Dashboard → SQL Editor
-- =============================================================================

-- 1. Company universe — master database of all companies ever evaluated
CREATE TABLE IF NOT EXISTS companies_universe (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    -- Identity
    name                TEXT NOT NULL,
    domain              TEXT,
    website             TEXT,
    linkedin_url        TEXT,
    -- Firmographics
    category            TEXT,
    industry            TEXT,
    location            TEXT,
    employees_linkedin  INTEGER,
    employees_apollo    INTEGER,
    revenue             TEXT,
    ownership           TEXT,
    -- LinkedIn profile
    li_followers        INTEGER,
    li_description      TEXT,
    li_tagline          TEXT,
    li_founded          TEXT,
    li_has_logo         BOOLEAN DEFAULT FALSE,
    -- Scoring
    icp_score           INTEGER,
    pipeline_action     TEXT DEFAULT 'REVIEW',
    score_breakdown     TEXT,
    reasoning           TEXT,
    why_this_score      TEXT,
    -- Tracking
    contacts_found      INTEGER DEFAULT 0,
    batch_id            UUID,
    batch_name          TEXT,
    source              TEXT DEFAULT 'csv_upload',
    -- Dedup
    dedup_key           TEXT NOT NULL,
    first_seen_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dedup index — one company per dedup key per tenant
CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_universe_dedup
    ON companies_universe(tenant_id, dedup_key);

-- Query indexes
CREATE INDEX IF NOT EXISTS idx_companies_universe_tenant
    ON companies_universe(tenant_id);
CREATE INDEX IF NOT EXISTS idx_companies_universe_score
    ON companies_universe(tenant_id, icp_score DESC);
CREATE INDEX IF NOT EXISTS idx_companies_universe_industry
    ON companies_universe(tenant_id, industry);
CREATE INDEX IF NOT EXISTS idx_companies_universe_batch
    ON companies_universe(batch_id);
CREATE INDEX IF NOT EXISTS idx_companies_universe_action
    ON companies_universe(tenant_id, pipeline_action);

-- 2. Batch tracking — history of every CSV upload
CREATE TABLE IF NOT EXISTS company_batches (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    name                TEXT NOT NULL,
    file_name           TEXT,
    total_rows          INTEGER DEFAULT 0,
    new_count           INTEGER DEFAULT 0,
    updated_count       INTEGER DEFAULT 0,
    skipped_count       INTEGER DEFAULT 0,
    uploaded_by         TEXT,
    notes               TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_batches_tenant
    ON company_batches(tenant_id, created_at DESC);

-- 3. Enable RLS (service role key bypasses)
ALTER TABLE companies_universe ENABLE ROW LEVEL SECURITY;
ALTER TABLE company_batches ENABLE ROW LEVEL SECURITY;

-- Done
SELECT 'Migration complete: companies_universe + company_batches tables created' AS status;

