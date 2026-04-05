-- =============================================================================
-- Migration: Scoring Pipeline — pipeline_status + source_data
-- Run in Supabase Dashboard → SQL Editor
-- =============================================================================

-- 1. Add pipeline tracking columns to companies_universe
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS pipeline_status TEXT DEFAULT 'raw';
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS source_data JSONB DEFAULT '{}';
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS enrichment_error TEXT;
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS scoring_error TEXT;
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS enriched_at TIMESTAMPTZ;
ALTER TABLE companies_universe ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;

-- 2. Index for pipeline queries
CREATE INDEX IF NOT EXISTS idx_companies_universe_pipeline
    ON companies_universe(tenant_id, pipeline_status);

-- 3. Backfill existing scored companies
UPDATE companies_universe
SET pipeline_status = 'scored', scored_at = created_at
WHERE icp_score IS NOT NULL AND pipeline_status = 'raw';

-- 4. RLS policy for new columns (already open from previous migration)

-- Done
SELECT 'Migration complete: pipeline_status + source_data columns added' AS status;
