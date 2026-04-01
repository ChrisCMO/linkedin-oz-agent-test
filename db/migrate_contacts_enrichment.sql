-- =============================================================================
-- Migration: Enrich prospects table for contact upload + contact batches
-- Run in Supabase Dashboard → SQL Editor
-- =============================================================================

-- 1. Add missing columns to prospects for enriched contact data
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS company_linkedin_url TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS company_li_followers INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS role_verified BOOLEAN;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS linkedin_connections INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS linkedin_followers INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS open_to_work BOOLEAN DEFAULT FALSE;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS email_status TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS apollo_company_id TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS zoominfo_contact_id TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS zoominfo_company_id TEXT;

-- Activity tracking
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS activity_score INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS activity_level TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS activity_recommendation TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS activity_insights TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS posts_last_30_days INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS reactions_last_30_days INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS last_activity_date DATE;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS days_since_last_activity INTEGER;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS linkedin_active_status TEXT;

-- Per-partner messages and connection notes
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS connection_notes JSONB DEFAULT '{}';
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS partner_messages JSONB DEFAULT '{}';

-- Data source tracking
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS data_source TEXT;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS contact_batch_id UUID;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS contact_batch_name TEXT;

-- Company universe link
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS company_universe_id UUID REFERENCES companies_universe(id);

-- 2. Contact batch tracking (separate from company batches)
CREATE TABLE IF NOT EXISTS contact_batches (
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

CREATE INDEX IF NOT EXISTS idx_contact_batches_tenant
    ON contact_batches(tenant_id, created_at DESC);

-- 3. RLS policies for new table
ALTER TABLE contact_batches ENABLE ROW LEVEL SECURITY;
CREATE POLICY anon_read ON contact_batches FOR SELECT USING (true);
CREATE POLICY service_all ON contact_batches FOR ALL USING (true);

-- 4. Index for contact dedup by LinkedIn URL
CREATE INDEX IF NOT EXISTS idx_prospects_linkedin_slug_tenant
    ON prospects(tenant_id, linkedin_slug) WHERE linkedin_slug IS NOT NULL;

-- Done
SELECT 'Migration complete: prospects enriched + contact_batches created' AS status;
