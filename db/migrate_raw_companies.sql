-- =============================================================================
-- Migration: Separate raw_companies table for the enrichment/scoring pipeline
-- Run in Supabase Dashboard → SQL Editor
-- =============================================================================

-- 1. Raw companies table — the pipeline funnel
CREATE TABLE IF NOT EXISTS raw_companies (
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
    employees           INTEGER,
    revenue             TEXT,
    ownership           TEXT,
    -- LinkedIn profile data
    li_followers        INTEGER,
    li_description      TEXT,
    li_tagline          TEXT,
    li_founded          TEXT,
    -- Source tracking
    source              TEXT NOT NULL DEFAULT 'other',
    source_data         JSONB NOT NULL DEFAULT '{}',
    -- Pipeline status
    pipeline_status     TEXT NOT NULL DEFAULT 'raw',
    -- Enrichment results
    enrichment_data     JSONB DEFAULT '{}',
    enrichment_error    TEXT,
    enriched_at         TIMESTAMPTZ,
    -- Scoring results
    icp_score           INTEGER,
    pipeline_action     TEXT,
    score_breakdown     TEXT,
    reasoning           TEXT,
    why_this_score      TEXT,
    scoring_error       TEXT,
    scored_at           TIMESTAMPTZ,
    -- Promotion tracking
    promoted            BOOLEAN DEFAULT FALSE,
    promoted_at         TIMESTAMPTZ,
    promoted_to_id      UUID,
    -- Batch tracking
    batch_id            UUID,
    batch_name          TEXT,
    -- Dedup
    dedup_key           TEXT NOT NULL,
    -- Timestamps
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dedup index
CREATE UNIQUE INDEX IF NOT EXISTS idx_raw_companies_dedup
    ON raw_companies(tenant_id, dedup_key);

-- Pipeline queries
CREATE INDEX IF NOT EXISTS idx_raw_companies_pipeline
    ON raw_companies(tenant_id, pipeline_status);
CREATE INDEX IF NOT EXISTS idx_raw_companies_batch
    ON raw_companies(batch_id);
CREATE INDEX IF NOT EXISTS idx_raw_companies_score
    ON raw_companies(tenant_id, icp_score DESC) WHERE icp_score IS NOT NULL;

-- 2. RLS policies
ALTER TABLE raw_companies ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS open_read ON raw_companies;
DROP POLICY IF EXISTS open_write ON raw_companies;
CREATE POLICY open_read ON raw_companies FOR SELECT USING (true);
CREATE POLICY open_write ON raw_companies FOR ALL USING (true) WITH CHECK (true);

-- 3. Updated_at trigger
DO $$ BEGIN
    CREATE TRIGGER trg_raw_companies_updated
        BEFORE UPDATE ON raw_companies
        FOR EACH ROW EXECUTE FUNCTION update_updated_at();
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Done
SELECT 'Migration complete: raw_companies table created' AS status;
