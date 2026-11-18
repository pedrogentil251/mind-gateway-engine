-- Criacao do banco dedicado de inteligencia corporativa e auditoria
SELECT 'CREATE DATABASE corporate_intel'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'corporate_intel')\gexec

\c corporate_intel;

BEGIN;

CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 1. Tabela de Memoria Semantica Hibrida (RAG Corporativo & Contratos)
CREATE TABLE IF NOT EXISTS knowledge_vault (
    id BIGSERIAL PRIMARY KEY,
    tenant_cnpj VARCHAR(18) NOT NULL,
    doc_category VARCHAR(64) NOT NULL,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    content_tsv tsvector GENERATED ALWAYS AS (to_tsvector('portuguese', coalesce(title, '') || ' ' || coalesce(content, ''))) STORED,
    embedding vector(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_knowledge_tenant_cat ON knowledge_vault (tenant_cnpj, doc_category);
CREATE INDEX IF NOT EXISTS idx_knowledge_tsv_gin ON knowledge_vault USING GIN (content_tsv);

-- 2. Tabela Forense de Auditoria de Repasses (iFood API v3 & PAT)
CREATE TABLE IF NOT EXISTS financial_audit_ledger (
    audit_id BIGSERIAL PRIMARY KEY,
    merchant_id VARCHAR(64) NOT NULL,
    tenant_cnpj VARCHAR(18) NOT NULL,
    order_id VARCHAR(64) NOT NULL,
    payment_channel VARCHAR(32) NOT NULL, -- 'IFOOD_ONLINE', 'PAT_VOUCHER', 'DIRECT_CREDIT'
    order_date TIMESTAMPTZ NOT NULL,
    settlement_date TIMESTAMPTZ,
    gross_amount NUMERIC(12, 2) NOT NULL,
    contracted_commission_rate NUMERIC(6, 4) NOT NULL,
    charged_commission_amount NUMERIC(12, 2) NOT NULL,
    charged_mdr_rate NUMERIC(6, 4) NOT NULL,
    charged_mdr_amount NUMERIC(12, 2) NOT NULL,
    shared_promo_subsidy_charged NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    contracted_promo_cap NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    unreimbursed_post_prep_cancel NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    expected_net_settlement NUMERIC(12, 2) NOT NULL,
    actual_net_settlement NUMERIC(12, 2) NOT NULL,
    leakage_amount NUMERIC(12, 2) NOT NULL,
    leakage_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_hash CHAR(64) NOT NULL,
    audited_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (merchant_id, order_id, payment_channel)
);

CREATE INDEX IF NOT EXISTS idx_audit_merchant_leakage ON financial_audit_ledger (merchant_id, leakage_amount DESC) WHERE leakage_amount > 0;
CREATE INDEX IF NOT EXISTS idx_audit_cnpj_date ON financial_audit_ledger (tenant_cnpj, order_date DESC);

-- 3. Tabela de Maquina de Estados B2B (WhatsApp Outbound Evolution API)
CREATE TABLE IF NOT EXISTS b2b_outreach_leads (
    lead_id BIGSERIAL PRIMARY KEY,
    cnpj VARCHAR(18) UNIQUE NOT NULL,
    restaurant_name TEXT NOT NULL,
    decision_maker_name TEXT NOT NULL,
    whatsapp_e164 VARCHAR(20) UNIQUE NOT NULL,
    estimated_monthly_gmv NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    audited_leakage_total NUMERIC(12, 2) NOT NULL DEFAULT 0.00,
    funnel_state VARCHAR(32) NOT NULL DEFAULT 'STATE_0_HOOK',
    last_interaction_at TIMESTAMPTZ,
    retry_count INT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_leads_funnel_state ON b2b_outreach_leads (funnel_state, last_interaction_at);

-- 4. Funcao SQL Parametrizada de Busca Hibrida com Reciprocal Rank Fusion (RRF)
CREATE OR REPLACE FUNCTION hybrid_search_rrf(
    p_tenant_cnpj VARCHAR(18),
    p_query_text TEXT,
    p_query_embedding vector(1536),
    p_match_count INT DEFAULT 10,
    p_rrf_k INT DEFAULT 60,
    p_weight_vec FLOAT DEFAULT 0.65,
    p_weight_fts FLOAT DEFAULT 0.35
)
RETURNS TABLE (
    doc_id BIGINT,
    title TEXT,
    content TEXT,
    metadata JSONB,
    rrf_score FLOAT
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Eleva precisao de busca do grafo HNSW na sessao atual
    SET LOCAL hnsw.ef_search = 100;

    RETURN QUERY
    WITH vector_ranked AS (
        SELECT
            kv.id,
            ROW_NUMBER() OVER (ORDER BY kv.embedding <=> p_query_embedding ASC) AS rank_ix
        FROM knowledge_vault kv
        WHERE kv.tenant_cnpj = p_tenant_cnpj
        ORDER BY kv.embedding <=> p_query_embedding ASC
        LIMIT p_match_count * 3
    ),
    fts_ranked AS (
        SELECT
            kv.id,
            ROW_NUMBER() OVER (ORDER BY ts_rank_cd(kv.content_tsv, websearch_to_tsquery('portuguese', p_query_text)) DESC) AS rank_ix
        FROM knowledge_vault kv
        WHERE kv.tenant_cnpj = p_tenant_cnpj
          AND kv.content_tsv @@ websearch_to_tsquery('portuguese', p_query_text)
        ORDER BY ts_rank_cd(kv.content_tsv, websearch_to_tsquery('portuguese', p_query_text)) DESC
        LIMIT p_match_count * 3
    )
    SELECT
        kv.id AS doc_id,
        kv.title,
        kv.content,
        kv.metadata,
        (
            COALESCE(p_weight_vec / (p_rrf_k + v.rank_ix)::FLOAT, 0.0) +
            COALESCE(p_weight_fts / (p_rrf_k + f.rank_ix)::FLOAT, 0.0)
        )::FLOAT AS rrf_score
    FROM vector_ranked v
    FULL OUTER JOIN fts_ranked f ON v.id = f.id
    JOIN knowledge_vault kv ON kv.id = COALESCE(v.id, f.id)
    ORDER BY rrf_score DESC
    LIMIT p_match_count;
END;
$$;

COMMIT;

-- Alocacao de 2GB de RAM para compilacao rapida do grafo HNSW sem spill para disco
SET maintenance_work_mem = '2GB';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_knowledge_embedding_hnsw
ON knowledge_vault USING hnsw (embedding vector_cosine_ops)
WITH (m = 24, ef_construction = 128);
