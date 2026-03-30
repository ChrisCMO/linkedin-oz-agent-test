-- =============================================================================
-- Migration: batch_reviews table for magic link prospect review
-- =============================================================================

CREATE TABLE batch_reviews (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id           UUID NOT NULL REFERENCES tenants(id),
    campaign_id         UUID NOT NULL REFERENCES campaigns(id),
    token_hash          TEXT NOT NULL UNIQUE,        -- SHA-256 of the raw token (never store raw)
    prospect_ids        UUID[] NOT NULL,             -- Snapshot of prospects in this batch
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

CREATE TRIGGER trg_batch_reviews_updated_at
    BEFORE UPDATE ON batch_reviews
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- No RLS needed — backend validates token with service-role key
COMMENT ON TABLE batch_reviews IS 'Magic link batch review sessions — clients review prospects without login';
