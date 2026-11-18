CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

CREATE TABLE IF NOT EXISTS raw_inputs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL DEFAULT '11111111-1111-1111-1111-111111111111',
    user_id VARCHAR(128) NOT NULL DEFAULT 'pedro_gentil',
    source_channel VARCHAR(64) NOT NULL,
    raw_content TEXT NOT NULL,
    external_evidence_id VARCHAR(128),
    metadata JSONB DEFAULT '{}'::jsonb,
    received_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT raw_content_not_empty CHECK (length(raw_content) > 0)
);

CREATE TABLE IF NOT EXISTS memory_objects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL DEFAULT '11111111-1111-1111-1111-111111111111',
    raw_input_id UUID REFERENCES raw_inputs(id) ON DELETE RESTRICT,
    category VARCHAR(32) NOT NULL CHECK (category IN ('FACT', 'DECISION', 'HYPOTHESIS', 'PREFERENCE', 'TASK')),
    content TEXT NOT NULL,
    confidence NUMERIC(3,2) NOT NULL CHECK (confidence >= 0.00 AND confidence <= 1.00),
    is_sensitive_health BOOLEAN DEFAULT FALSE,
    half_life_days INT DEFAULT 30,
    is_archived BOOLEAN DEFAULT FALSE,
    embedding VECTOR(1536),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT check_hypothesis_ceiling CHECK (category != 'HYPOTHESIS' OR confidence <= 0.50)
);

CREATE TABLE IF NOT EXISTS confirmation_actions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id UUID NOT NULL DEFAULT '11111111-1111-1111-1111-111111111111',
    raw_input_id UUID REFERENCES raw_inputs(id) ON DELETE RESTRICT,
    action_type VARCHAR(64) NOT NULL,
    target_channel VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'PENDING_APPROVAL'
        CHECK (status IN ('PENDING_APPROVAL', 'EXECUTED', 'REJECTED', 'EXPIRED', 'CIRCUIT_HEALED')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP + INTERVAL '30 minutes'),
    executed_at TIMESTAMP WITH TIME ZONE,
    expired_at TIMESTAMP WITH TIME ZONE,
    approved_by VARCHAR(128),
    rejection_reason TEXT
);

CREATE TABLE IF NOT EXISTS ai_provider_circuit_states (
    provider_id VARCHAR(64) PRIMARY KEY,
    circuit_state VARCHAR(16) NOT NULL DEFAULT 'CLOSED' CHECK (circuit_state IN ('CLOSED', 'OPEN', 'HALF_OPEN')),
    consecutive_failures INT NOT NULL DEFAULT 0,
    failure_threshold INT NOT NULL DEFAULT 5,
    cooldown_seconds INT NOT NULL DEFAULT 60,
    last_failure_at TIMESTAMP WITH TIME ZONE,
    last_state_change TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    total_fallback_dispatches INT NOT NULL DEFAULT 0
);

INSERT INTO ai_provider_circuit_states (provider_id, failure_threshold, cooldown_seconds)
VALUES
    ('anthropic/claude-3-7-sonnet', 5, 60),
    ('openai/gpt-4o', 5, 60),
    ('google/gemini-1.5-flash', 3, 30)
ON CONFLICT (provider_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS context_drift_audit_log (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    memory_object_id UUID REFERENCES memory_objects(id) ON DELETE RESTRICT,
    action_taken VARCHAR(32) NOT NULL CHECK (action_taken IN ('ARCHIVED', 'SUPERSEDED', 'DECAYED_CONFIDENCE', 'INVALIDATED')),
    previous_confidence NUMERIC(3,2),
    new_confidence NUMERIC(3,2),
    trigger_reason TEXT NOT NULL,
    evaluated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_inputs_tenant ON raw_inputs(tenant_id);
CREATE INDEX IF NOT EXISTS idx_memory_hnsw ON memory_objects USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS idx_confirmation_pending_ttl ON confirmation_actions(status, expires_at) WHERE status = 'PENDING_APPROVAL';
CREATE INDEX IF NOT EXISTS idx_drift_memory_eval ON context_drift_audit_log(memory_object_id, evaluated_at);

ALTER TABLE raw_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE memory_objects ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_raw_inputs ON raw_inputs
    USING (tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);

CREATE POLICY tenant_isolation_memory_objects ON memory_objects
    USING (tenant_id = NULLIF(current_setting('app.current_tenant', true), '')::uuid);
