-- Discovery log — audit trail for contact discovery API calls per company.
-- Tracks Apollo, ZoomInfo, and X-ray searches with results and timing.

CREATE TABLE IF NOT EXISTS discovery_log (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    tenant_id UUID NOT NULL,
    company_id UUID NOT NULL,
    company_name TEXT,
    source TEXT NOT NULL,                -- 'apollo', 'zoominfo', 'xray'
    actor_or_endpoint TEXT,              -- API endpoint or Apify actor ID
    request_params JSONB,
    contacts_found INTEGER DEFAULT 0,
    contacts_verified INTEGER DEFAULT 0,
    contacts_rejected INTEGER DEFAULT 0,
    duration_ms INTEGER,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_discovery_log_company ON discovery_log(company_id);
CREATE INDEX IF NOT EXISTS idx_discovery_log_tenant ON discovery_log(tenant_id, created_at);

-- RLS: open for service role
ALTER TABLE discovery_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY "discovery_log_all" ON discovery_log FOR ALL USING (true) WITH CHECK (true);
