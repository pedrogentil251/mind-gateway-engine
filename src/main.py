from decimal import Decimal
import json
from datetime import timedelta
from typing import Dict, Any
import json
from datetime import timedelta
from typing import Dict, Any
import asyncio
import json
import os
import re
import ssl
import uuid
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, unquote
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

if os.path.exists(".env"):
    for _line in open(".env"):
        if "=" in _line and not _line.strip().startswith("#"):
            _k, _v = _line.strip().split("=", 1)
            os.environ.setdefault(_k, _v)

try:
    import asyncpg
except ImportError:
    asyncpg = None

try:
    import pg8000.native as pg8000_native
except ImportError:
    pg8000_native = None

app = FastAPI(title="Mind Gateway Core Engine", version="0.4.0")
DATABASE_URL = os.getenv("DATABASE_URL")
pool: Optional[Any] = None


class Pg8000Adapter:
    def __init__(self, url: str):
        self.url = url

    def _connect(self):
        u = urlparse(self.url)
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return pg8000_native.Connection(
            user=unquote(u.username or ""),
            password=unquote(u.password or ""),
            host=u.hostname,
            port=u.port or 5432,
            database=u.path.lstrip("/"),
            ssl_context=ctx,
        )

    def _prep(self, query: str, args: tuple):
        q = re.sub(r"\$(\d+)", r":p\1", query)
        params = {f"p{i+1}": v for i, v in enumerate(args)}
        return q, params

    async def execute(self, query: str, *args):
        def _run():
            con = self._connect()
            try:
                q, params = self._prep(query, args)
                con.run(q, **params)
            finally:
                con.close()
        await asyncio.to_thread(_run)

    async def fetchrow(self, query: str, *args):
        def _run():
            con = self._connect()
            try:
                q, params = self._prep(query, args)
                rows = con.run(q, **params)
                if not rows:
                    return None
                cols = [c["name"] for c in con.columns]
                return dict(zip(cols, rows[0]))
            finally:
                con.close()
        return await asyncio.to_thread(_run)

    async def fetch(self, query: str, *args):
        def _run():
            con = self._connect()
            try:
                q, params = self._prep(query, args)
                rows = con.run(q, **params)
                if not rows:
                    return []
                cols = [c["name"] for c in con.columns]
                return [dict(zip(cols, r)) for r in rows]
            finally:
                con.close()
        return await asyncio.to_thread(_run)

    async def close(self):
        pass


class _DummyCtx:
    def __init__(self, target):
        self.target = target
    async def __aenter__(self):
        return self.target
    async def __aexit__(self, *args):
        pass


@app.on_event("startup")
async def startup():
    global pool
    if not DATABASE_URL:
        return
    if asyncpg:
        pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    elif pg8000_native:
        adapter = Pg8000Adapter(DATABASE_URL)
        adapter.acquire = lambda: _DummyCtx(adapter)
        pool = adapter


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


class ActionProposeRequest(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    raw_input_id: Optional[uuid.UUID] = None
    action_type: str
    target_channel: str = "whatsapp"
    payload: Dict[str, Any]
    ttl_minutes: int = Field(default=30, ge=1, le=1440)


class WhatsAppWebhookPayload(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    sender: str = "whatsapp_user"
    message: str
    message_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


@app.post("/api/v1/gate/propose", status_code=status.HTTP_201_CREATED)
async def propose_confirmation_gate(req: ActionProposeRequest):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    action_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=req.ttl_minutes)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO confirmation_actions
                (id, tenant_id, raw_input_id, action_type, target_channel, payload, status, created_at, expires_at)
            VALUES
                ($1, $2, $3, $4, $5, CAST($6 AS jsonb), 'PENDING_APPROVAL', $7, $8)
            """,
            action_id,
            req.tenant_id,
            req.raw_input_id,
            req.action_type,
            req.target_channel,
            json.dumps(req.payload),
            now,
            expires_at,
        )
    return {
        "status": "PENDING_APPROVAL",
        "action_id": str(action_id),
        "tenant_id": str(req.tenant_id),
        "action_type": req.action_type,
        "target_channel": req.target_channel,
        "expires_at": expires_at.isoformat(),
        "required_keyword": "AUTORIZADO",
    }


class ActionProposeRequest(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    raw_input_id: Optional[uuid.UUID] = None
    action_type: str
    target_channel: str = "whatsapp"
    payload: Dict[str, Any]
    ttl_minutes: int = Field(default=30, ge=1, le=1440)


class WhatsAppWebhookPayload(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    sender: str = "whatsapp_user"
    message: str
    message_id: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


@app.post("/api/v1/gate/propose", status_code=status.HTTP_201_CREATED)
async def propose_confirmation_gate(req: ActionProposeRequest):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    action_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=req.ttl_minutes)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO confirmation_actions
                (id, tenant_id, raw_input_id, action_type, target_channel, payload, status, created_at, expires_at)
            VALUES
                ($1, $2, $3, $4, $5, CAST($6 AS jsonb), 'PENDING_APPROVAL', $7, $8)
            """,
            action_id,
            req.tenant_id,
            req.raw_input_id,
            req.action_type,
            req.target_channel,
            json.dumps(req.payload),
            now,
            expires_at,
        )
    return {
        "status": "PENDING_APPROVAL",
        "action_id": str(action_id),
        "tenant_id": str(req.tenant_id),
        "action_type": req.action_type,
        "target_channel": req.target_channel,
        "expires_at": expires_at.isoformat(),
        "required_keyword": "AUTORIZADO",
    }


@app.post("/api/v1/gate/approve")
async def approve_confirmation_gate(req: ActionApprovalRequest):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    now = datetime.now(timezone.utc)
    keyword = req.confirmation_keyword.strip().upper()
    if keyword != "AUTORIZADO":
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
        audit_payload = json.dumps(action["payload"]) if isinstance(action["payload"], (dict, list)) else str(action["payload"])
        await conn.execute(
            """
            INSERT INTO confirmation_audit_log (action_id, approved_by, approval_token, action_payload)
            VALUES ($1, $2, $3, CAST($4 AS jsonb))
            """,
            action["id"],
            req.operator_id,
            keyword,
            audit_payload,
        )
    return {
        "status": "EXECUTED",
        "action_id": str(req.action_id),
        "approved_by": req.operator_id,
        "executed_at": now.isoformat(),
    }


@app.post("/api/v1/ingest/whatsapp", status_code=status.HTTP_201_CREATED)
async def ingest_whatsapp_webhook(payload: WhatsAppWebhookPayload):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    raw_id = uuid.uuid4()
    mem_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    category = "HYPOTHESIS"
    confidence = 0.50
    meta = {**payload.metadata, "sender": payload.sender}
    triggers = ("disparar", "campanha", "enviar whatsapp", "promocao", "promoção")
    requires_gate = any(t in payload.message.lower() for t in triggers)
    action_id = None
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO raw_inputs (id, tenant_id, user_id, source_channel, raw_content, external_evidence_id, metadata, received_at)
            VALUES ($1, $2, $3, 'whatsapp_webhook', $4, $5, CAST($6 AS jsonb), $7)
            """,
            raw_id,
            payload.tenant_id,
            payload.sender,
            payload.message,
            payload.message_id,
            json.dumps(meta),
            now,
        )
        await conn.execute(
            """
            INSERT INTO memory_objects (id, tenant_id, raw_input_id, category, content, confidence, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            mem_id,
            payload.tenant_id,
            raw_id,
            category,
            payload.message,
            confidence,
            now,
        )
        if requires_gate:
            action_id = uuid.uuid4()
            await conn.execute(
                """
                INSERT INTO confirmation_actions
                    (id, tenant_id, raw_input_id, action_type, target_channel, payload, status, created_at, expires_at)
                VALUES
                    ($1, $2, $3, 'WHATSAPP_PROMOTIONAL_DISPATCH', 'whatsapp', CAST($4 AS jsonb), 'PENDING_APPROVAL', $5, $6)
                """,
                action_id,
                payload.tenant_id,
                raw_id,
                json.dumps({"sender": payload.sender, "message": payload.message}),
                now,
                now + timedelta(minutes=30),
            )
    resp = {
        "status": "INGESTED",
        "channel": "whatsapp_webhook",
        "raw_input_id": str(raw_id),
        "memory_object_id": str(mem_id),
        "category": category,
        "confidence": confidence,
        "persisted": True,
    }
    if action_id:
        resp["gate_status"] = "PENDING_APPROVAL"
        resp["action_id"] = str(action_id)
    return resp


class CalibrationRequest(BaseModel):
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111")
    memory_object_id: Optional[uuid.UUID] = None
    metric_name: str
    expected_value: Decimal
    realized_value: Decimal
    notes: Optional[str] = None


@app.get("/api/v1/memory/facts")
async def list_verified_facts(
    tenant_id: uuid.UUID = uuid.UUID("11111111-1111-1111-1111-111111111111"),
    prefix: Optional[str] = None,
):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    async with pool.acquire() as conn:
        if prefix:
            rows = await conn.fetch(
                "SELECT id, category, content, confidence, created_at FROM memory_objects WHERE tenant_id = $1 AND content LIKE $2 ORDER BY id",
                tenant_id,
                f"{prefix}%",
            )
        else:
            rows = await conn.fetch(
                "SELECT id, category, content, confidence, created_at FROM memory_objects WHERE tenant_id = $1 AND confidence = 1.00 ORDER BY id",
                tenant_id,
            )
    return {
        "tenant_id": str(tenant_id),
        "count": len(rows),
        "items": [
            {
                "id": str(r["id"]),
                "category": r["category"],
                "content": r["content"],
                "confidence": float(r["confidence"]),
            }
            for r in rows
        ],
    }


@app.post("/api/v1/calibrate", status_code=status.HTTP_201_CREATED)
async def record_decision_calibration(req: CalibrationRequest):
    if not pool:
        raise HTTPException(status_code=503, detail="Banco de dados não conectado.")
    abs_deviation = abs(req.expected_value - req.realized_value).quantize(Decimal("0.01"))
    pct_deviation = (
        ((abs_deviation / abs(req.expected_value)) * Decimal("100")).quantize(Decimal("0.01"))
        if req.expected_value != 0
        else Decimal("0.00")
    )
    drift_alert = pct_deviation > Decimal("30.00")
    cal_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    note_text = req.notes or f"Calibracao {req.metric_name}"

    async with pool.acquire() as conn:
        col_rows = await conn.fetch(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'decision_calibrations'
            ORDER BY ordinal_position
            """
        )
        semantic_map = {
            "id": cal_id,
            "tenant_id": req.tenant_id,
            "memory_object_id": req.memory_object_id,
            "decision_id": req.memory_object_id,
            "action_id": req.memory_object_id,
            "metric_name": req.metric_name,
            "decision_name": req.metric_name,
            "decision_title": req.metric_name,
            "hypothesis_summary": req.metric_name,
            "context_summary": req.metric_name,
            "expected_value": req.expected_value,
            "expected_outcome": req.expected_value,
            "predicted_value": req.expected_value,
            "realized_value": req.realized_value,
            "realized_outcome": req.realized_value,
            "actual_value": req.realized_value,
            "observed_value": req.realized_value,
            "absolute_error": abs_deviation,
            "absolute_deviation": abs_deviation,
            "deviation": abs_deviation,
            "deviation_amount": abs_deviation,
            "error_amount": abs_deviation,
            "delta": abs_deviation,
            "deviation_percent": pct_deviation,
            "deviation_pct": pct_deviation,
            "error_pct": pct_deviation,
            "percentage_error": pct_deviation,
            "requires_reassessment": drift_alert,
            "drift_triggered": drift_alert,
            "notes": note_text,
            "lesson_learned": note_text,
            "lessons_learned": note_text,
            "calibrated_at": now,
            "evaluated_at": now,
            "created_at": now,
        }

        cols_to_insert = []
        values = []
        for r in col_rows:
            cname = r["column_name"]
            dtype = r["data_type"]
            nullable = r["is_nullable"]
            default = r["column_default"]
            has_val = cname in semantic_map and semantic_map[cname] is not None

            if not has_val and (nullable == "YES" or default is not None):
                continue

            raw_val = semantic_map.get(cname)
            if dtype == "uuid":
                val = raw_val if isinstance(raw_val, uuid.UUID) else uuid.uuid4()
            elif dtype in ("numeric", "decimal"):
                val = raw_val if isinstance(raw_val, Decimal) else Decimal(str(raw_val or "0.00"))
            elif dtype in ("double precision", "real"):
                val = float(raw_val) if raw_val is not None else 0.0
            elif dtype in ("integer", "bigint", "smallint"):
                val = int(raw_val) if raw_val is not None else 0
            elif dtype == "boolean":
                val = bool(raw_val) if raw_val is not None else False
            elif "timestamp" in dtype:
                val = raw_val if isinstance(raw_val, datetime) else now
            else:
                val = str(raw_val) if raw_val is not None else note_text

            cols_to_insert.append(cname)
            values.append(val)

        placeholders = ", ".join(f"${i+1}" for i in range(len(cols_to_insert)))
        await conn.execute(
            f"INSERT INTO decision_calibrations ({', '.join(cols_to_insert)}) VALUES ({placeholders})",
            *values,
        )

    return {
        "status": "CALIBRATED",
        "calibration_id": str(cal_id),
        "metric_name": req.metric_name,
        "expected_value": str(req.expected_value),
        "realized_value": str(req.realized_value),
        "absolute_deviation": str(abs_deviation),
        "deviation_percent": str(pct_deviation),
        "drift_alert_triggered": drift_alert,
        "persisted": True,
    }
