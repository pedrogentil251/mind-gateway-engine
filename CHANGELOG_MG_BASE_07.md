# Change Log — Mind Gateway Engine (`MG-BASE-07` / `v0.4.0`)

## Baseline Oficial Congelada: `MG-BASE-07` (`v0.4.0`)
- **Runtime em Produção:** `https://web-production-bc7ad0f9.up.railway.app` (FastAPI + Python + `pg8000` SSL).
- **Persistência Relacional & Vetorial:** PostgreSQL 16 + `pgvector` no Supabase (`sktjlhmhoacrkifnusxr`).
- **Protocolo MG-RAW-01:** Ingestão imutável auditada em `raw_inputs` (`9b313fe7-af53-4278-916e-21bdd368399b`).
- **Filtro Epistemológico:** Rebaixamento automático de hipóteses/incertezas para `HYPOTHESIS` com teto de confiança `<= 0.50`.
- **Base Factual Parmê Lab:** 8 registos ativos (`PARME-001` a `PARME-007` com confiança `1.00` + trava permanente de aprovação do WhatsApp).
- **Confirmation Gate & Auditoria:** Ação `2471d2a7-e0fa-4fbc-a138-807fc6cf2ae9` retida em `PENDING_APPROVAL`, bloqueada para token inválido e executada apenas mediante `AUTORIZADO` com registo imutável `35e451cc-a769-4a55-b7ac-5f842b7bf1c9` em `confirmation_audit_log`.
- **Calibração Empírica & Orquestração:** Endpoint `/api/v1/calibrate` (`|E - R|` em `Decimal`) e fluxo exportável `workflows/n8n_whatsapp_parme_flow.json`.
