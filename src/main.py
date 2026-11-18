import os
import json
import uuid
from datetime import datetime, timezone
from typing import Optional
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import asyncpg

app = FastAPI(title="Mind Gateway Core Engine", version="0.4.0")

DATABASE_URL = os.getenv("DATABASE_URL")
pool: Optional[asyncpg.Pool] = None


@app.on_event("startup")
async def startup():
    global pool
    if DATABASE_URL:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)


@app.on_event("shutdown")
async def shutdown():
    if pool:
        await pool.close()


class IngestRequest(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    source_channel: str
    raw_content: str = Field(..., min_length=1)
    category: str = Field(default="HYPOTHESIS", pattern="^(FACT|DECISION|HYPOTHESIS|PREFERENCE|TASK)$")
    confidence: float = Field(default=0.50, ge=0.0, le=1.0)
    external_evidence_id: Optional[str] = None
    is_sensitive_health: bool = False


class ActionApprovalRequest(BaseModel):
    action_id: uuid.UUID
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    confirmation_keyword: str
    operator_id: str


def apply_epistemological_governance(req: IngestRequest) -> tuple[str, float]:
    uncertain_terms = ("acho que", "talvez", "hipótese", "estimativa", "provavelmente", "simulação")
    category = req.category
    confidence = req.confidence

    if category == "FACT" and not req.external_evidence_id:
        category = "HYPOTHESIS"
        confidence = min(confidence, 0.50)

    if category == "HYPOTHESIS" or any(term in req.raw_content.lower() for term in uncertain_terms):
        category = "HYPOTHESIS"
        confidence = min(confidence, 0.50)

    return category, confidence


@app.get("/health")
async def health():
    return {
        "status": "ONLINE",
        "baseline": "MG-BASE-07",
        "version": "0.4.0",
        "db_connected": pool is not None,
    }


@app.post("/api/v1/ingest", status_code=status.HTTP_201_CREATED)
async def ingest_raw_input(payload: IngestRequest):
    category, confidence = apply_epistemological_governance(payload)
    raw_id = uuid.uuid4()

    if pool:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO raw_inputs (id, tenant_id, source_channel, raw_content, external_evidence_id, metadata)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb)
                """,
                raw_id,
                payload.tenant_id,
                payload.source_channel,
                payload.raw_content.strip(),
                payload.external_evidence_id,
                json.dumps({"is_sensitive_health": payload.is_sensitive_health}),
            )
            await conn.execute(
                """
                INSERT INTO memory_objects (raw_input_id, tenant_id, category, content, confidence, is_sensitive_health)
                VALUES ($1, $2, $3, $4, $5, $6)
                """,
                raw_id,
                payload.tenant_id,
                category,
                payload.raw_content.strip(),
                confidence,
                payload.is_sensitive_health,
            )

    return {
        "status": "INGESTED",
        "raw_input_id": str(raw_id),
        "category": category,
        "confidence": confidence,
        "persisted": pool is not None,
    }


@app.post("/api/v1/gate/approve")
async def approve_confirmation_gate(req: ActionApprovalRequest):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")

    now = datetime.now(timezone.utc)
    if req.confirmation_keyword.strip().upper() != "AUTORIZADO":
        raise HTTPException(status_code=400, detail="Token inválido. Exige palavra-chave 'AUTORIZADO'.")

    async with pool.acquire() as conn:
        action = await conn.fetchrow(
            "SELECT * FROM confirmation_actions WHERE id = $1 AND tenant_id = $2",
            req.action_id,
            req.tenant_id,
        )
        if not action:
            raise HTTPException(status_code=404, detail="Comando não encontrado no Confirmation Gate.")
        if action["status"] != "PENDING_APPROVAL":
            raise HTTPException(status_code=400, detail=f"Estado inválido para aprovação: {action['status']}")
        if action["expires_at"] <= now:
            await conn.execute(
                "UPDATE confirmation_actions SET status = 'EXPIRED', expired_at = $1 WHERE id = $2",
                now,
                action["id"],
            )
            raise HTTPException(status_code=410, detail="Janela de aprovação expirada (TTL ultrapassado).")

        await conn.execute(
            """
            UPDATE confirmation_actions
            SET status = 'EXECUTED', executed_at = $1, approved_by = $2
            WHERE id = $3
            """,
            now,
            req.operator_id,
            action["id"],
        )

    return {
        "status": "EXECUTED",
        "action_id": str(req.action_id),
        "approved_by": req.operator_id,
        "executed_at": now.isoformat(),
    }
