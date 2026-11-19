import os
import ssl
import json
import uuid
import time
import math
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Dict, Any, List

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

router = APIRouter(prefix="/api/v1/orchestrate", tags=["Broker Pay-As-You-Go"])

USD_TO_BRL = Decimal(os.getenv("USD_TO_BRL_RATE", "5.80"))
QUOTE_TTL_MINUTES = 30

MODEL_CATALOG: Dict[str, Dict[str, Any]] = {
    "tier1_fast": {
        "tier_label": "Tier 1 • Triagem & Rápido",
        "primary": "google/gemini-2.5-flash",
        "fallback": "meta-llama/llama-3.3-70b-instruct",
        "input_per_1m_usd": Decimal("0.15"),
        "output_per_1m_usd": Decimal("0.60"),
        "est_output_ratio": Decimal("1.4"),
        "min_output_tokens": 250,
    },
    "tier2_pro": {
        "tier_label": "Tier 2 • Código & Estratégia",
        "primary": "anthropic/claude-sonnet-4",
        "fallback": "openai/gpt-4o",
        "input_per_1m_usd": Decimal("3.00"),
        "output_per_1m_usd": Decimal("15.00"),
        "est_output_ratio": Decimal("2.2"),
        "min_output_tokens": 550,
    },
    "tier3_deep": {
        "tier_label": "Tier 3 • Arquitetura & Raciocínio",
        "primary": "anthropic/claude-opus-4",
        "fallback": "openai/o3",
        "input_per_1m_usd": Decimal("15.00"),
        "output_per_1m_usd": Decimal("75.00"),
        "est_output_ratio": Decimal("2.8"),
        "min_output_tokens": 800,
    },
}

PENDING_QUOTES: Dict[str, Dict[str, Any]] = {}
EXECUTION_HISTORY: List[Dict[str, Any]] = []
CIRCUIT_BREAKER_FAILURES: Dict[str, int] = {}


class EstimateRequest(BaseModel):
    prompt: str = Field(..., min_length=2)
    routing_mode: str = Field(default="auto")
    use_pig_context: bool = Field(default=True)
    tenant_id: str = Field(default="00000000-0000-0000-0000-000000000001")


class ExecuteRequest(BaseModel):
    quote_id: str
    approval_token: str = Field(..., min_length=4)


def _diagnose_openrouter_runtime() -> Dict[str, Any]:
    raw_key = os.getenv("OPENROUTER_API_KEY", "")
    api_key = raw_key.strip()

    if not api_key:
        return {
            "ok": False,
            "code": "CHAVE_AUSENTE",
            "title": "Variável OPENROUTER_API_KEY não configurada no Railway",
            "cause": "O servidor no Railway não encontrou nenhuma chave definida na variável de ambiente OPENROUTER_API_KEY.",
            "action": "Abra o Railway -> Variables -> adicione OPENROUTER_API_KEY com a chave gerada em openrouter.ai/keys e aguarde o reinício.",
        }

    if not api_key.startswith("sk-or-"):
        return {
            "ok": False,
            "code": "FORMATO_CHAVE_INVALIDO",
            "title": "Formato inválido na variável OPENROUTER_API_KEY",
            "cause": f"O valor configurado começa com '{api_key[:6]}...', mas chaves válidas do OpenRouter começam com 'sk-or-'.",
            "action": "Edite OPENROUTER_API_KEY no Railway removendo aspas, espaços extras ou colando a chave correta (sk-or-v1-...).",
        }

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/auth/key",
        headers={
            "Authorization": f"Bearer {api_key}",
            "HTTP-Referer": "https://web-production-bc7ad0f9.up.railway.app",
            "X-Title": "Mind Gateway OS Diagnostic",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            data = payload.get("data", payload)
            limit = data.get("limit")
            usage = data.get("usage", 0) or 0
            limit_remaining = data.get("limit_remaining")
            is_free_tier = data.get("is_free_tier", False)

            if limit_remaining is not None and float(limit_remaining) <= 0:
                return {
                    "ok": False,
                    "code": "LIMITE_CHAVE_ESGOTADO",
                    "title": "Limite de gasto da chave OpenRouter atingido",
                    "cause": f"A chave está ativa, mas o limite configurado nela ({limit} US$) já foi totalmente consumido (uso atual: {usage} US$).",
                    "action": "Acesse openrouter.ai/keys para aumentar ou remover o limite de gasto desta chave.",
                }

            return {
                "ok": True,
                "code": "CONEXAO_ATIVA",
                "title": "Chave OpenRouter autenticada com sucesso",
                "cause": "A variável OPENROUTER_API_KEY está ativa e reconhecida pela API do OpenRouter.",
                "action": "Pronto para executar chamadas.",
                "meta": {
                    "label": data.get("label"),
                    "usage_usd": usage,
                    "limit_usd": limit,
                    "limit_remaining_usd": limit_remaining,
                    "is_free_tier": is_free_tier,
                },
            }
    except urllib.error.HTTPError as http_err:
        err_body = ""
        try:
            err_body = http_err.read().decode("utf-8", errors="ignore")
        except Exception:
            pass

        if http_err.code == 401:
            return {
                "ok": False,
                "code": "CHAVE_REVOGADA_OU_INVALIDA",
                "title": "Chave OpenRouter inválida, revogada ou expirada (HTTP 401)",
                "cause": "O OpenRouter recusou a autenticação da chave atual. Ela foi revogada no painel, expirou ou foi copiada incompleta.",
                "action": "Gere uma nova chave em openrouter.ai/keys, atualize OPENROUTER_API_KEY no Railway e teste novamente.",
                "raw_detail": err_body[:240],
            }
        if http_err.code == 402:
            return {
                "ok": False,
                "code": "SALDO_ZERADO",
                "title": "Saldo zerado ou insuficiente no OpenRouter (HTTP 402)",
                "cause": "A chave é válida, mas a conta não possui créditos suficientes para despachar modelos pagos.",
                "action": "Adicione créditos em openrouter.ai/credits para liberar o consumo Pay-As-You-Go.",
                "raw_detail": err_body[:240],
            }
        return {
            "ok": False,
            "code": f"OPENROUTER_HTTP_{http_err.code}",
            "title": f"Erro HTTP {http_err.code} ao validar chave no OpenRouter",
            "cause": err_body[:240] or str(http_err),
            "action": "Verifique o status do OpenRouter e tente novamente em instantes.",
        }
    except Exception as exc:
        return {
            "ok": False,
            "code": "FALHA_REDE_OPENROUTER",
            "title": "Falha de rede ao consultar o OpenRouter",
            "cause": str(exc),
            "action": "Verifique a conexão de saída do servidor no Railway e clique em Testar Conexão.",
        }


@router.get("/health-check")
async def orchestrate_health_check():
    diag = _diagnose_openrouter_runtime()
    return {
        "epistemic_tag": "FATO",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "circuit_breaker_failures": CIRCUIT_BREAKER_FAILURES,
        "diagnostic": diag,
    }


def _classify_complexity(prompt: str) -> str:
    text = prompt.lower()
    deep_kw = [
        "arquitetura", "auditoria", "refator", "sistema completo",
        "estratégia", "contrato", "financeiro", "modelagem", "pipeline",
        "segurança", "banco de dados", "sql", "multi-tenant"
    ]
    pro_kw = [
        "código", "python", "api", "script", "proposta", "pitch",
        "copy", "analise", "relatório", "comparar", "plano", "workflow", "n8n"
    ]
    if len(prompt) > 900 or sum(1 for k in deep_kw if k in text) >= 2:
        return "tier3_deep"
    if len(prompt) > 220 or any(k in text for k in pro_kw) or any(k in text for k in deep_kw):
        return "tier2_pro"
    return "tier1_fast"


def _open_db_connection():
    db_url = os.getenv("DATABASE_URL", "").strip()
    if not db_url:
        return None, None
    parsed = urllib.parse.urlparse(db_url)
    user = urllib.parse.unquote(parsed.username or "")
    password = urllib.parse.unquote(parsed.password or "")
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    database = (parsed.path or "/postgres").lstrip("/")

    try:
        import pg8000.dbapi
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        conn = pg8000.dbapi.connect(
            user=user, password=password, host=host, port=port, database=database, ssl_context=ctx
        )
        return conn, "pg8000"
    except Exception:
        pass

    try:
        import psycopg
        conn = psycopg.connect(db_url, connect_timeout=4)
        return conn, "psycopg"
    except Exception:
        pass

    try:
        import psycopg2
        conn = psycopg2.connect(db_url, connect_timeout=4)
        return conn, "psycopg2"
    except Exception:
        return None, None


def _fetch_pig_context(limit: int = 5) -> str:
    conn, _ = _open_db_connection()
    if not conn:
        return ""
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT memory_type, category, extracted_summary, confidence_score
            FROM memory_objects
            WHERE status = 'ACTIVE'
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        if not rows:
            return ""
        return "\n".join(
            f"[{r[0]}/{r[1]} | conf={r[3]}] {r[2]}" for r in rows
        )
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return ""


def _persist_execution_record(record: Dict[str, Any]) -> bool:
    conn, _ = _open_db_connection()
    if not conn:
        return False
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS broker_executions (
                id UUID PRIMARY KEY,
                tier VARCHAR(40) NOT NULL,
                model_used VARCHAR(120) NOT NULL,
                fallback_triggered BOOLEAN DEFAULT FALSE,
                prompt_text TEXT NOT NULL,
                response_text TEXT NOT NULL,
                est_cost_usd NUMERIC(12,6) NOT NULL,
                real_cost_usd NUMERIC(12,6) NOT NULL,
                est_cost_brl NUMERIC(12,4) NOT NULL,
                real_cost_brl NUMERIC(12,4) NOT NULL,
                prompt_tokens INT NOT NULL,
                completion_tokens INT NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        cur.execute(
            """
            INSERT INTO broker_executions (
                id, tier, model_used, fallback_triggered, prompt_text, response_text,
                est_cost_usd, real_cost_usd, est_cost_brl, real_cost_brl,
                prompt_tokens, completion_tokens
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record["execution_id"],
                record["tier"],
                record["model_used"],
                record["fallback_triggered"],
                record["prompt"],
                record["output"],
                Decimal(str(record["est_cost_usd"])),
                Decimal(str(record["real_cost_usd"])),
                Decimal(str(record["est_cost_brl"])),
                Decimal(str(record["real_cost_brl"])),
                record["prompt_tokens"],
                record["completion_tokens"],
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return False


def _calc_cost_usd(in_tokens: int, out_tokens: int, spec: Dict[str, Any]) -> Decimal:
    in_cost = (Decimal(in_tokens) / Decimal("1000000")) * spec["input_per_1m_usd"]
    out_cost = (Decimal(out_tokens) / Decimal("1000000")) * spec["output_per_1m_usd"]
    return (in_cost + out_cost).quantize(Decimal("0.000001"), rounding=ROUND_HALF_UP)


@router.post("/estimate")
async def estimate_orchestration(req: EstimateRequest):
    tier_key = req.routing_mode if req.routing_mode in MODEL_CATALOG else _classify_complexity(req.prompt)
    spec = MODEL_CATALOG[tier_key]

    pig_context = _fetch_pig_context(5) if req.use_pig_context else ""
    full_prompt = req.prompt.strip()
    if pig_context:
        full_prompt = (
            f"CONTEXTO ATIVO DO PIG (MIND GATEWAY):\n{pig_context}\n\n"
            f"INTENÇÃO DO USUÁRIO:\n{req.prompt.strip()}"
        )

    est_in_tokens = max(12, math.ceil(len(full_prompt) / 3.6))
    est_out_tokens = max(spec["min_output_tokens"], int(Decimal(est_in_tokens) * spec["est_output_ratio"]))

    est_usd = _calc_cost_usd(est_in_tokens, est_out_tokens, spec)
    est_brl = (est_usd * USD_TO_BRL).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    tier3_usd = _calc_cost_usd(est_in_tokens, est_out_tokens, MODEL_CATALOG["tier3_deep"])
    savings_pct = Decimal("0.0")
    if tier3_usd > Decimal("0"):
        savings_pct = (((tier3_usd - est_usd) / tier3_usd) * Decimal("100")).quantize(
            Decimal("0.1"), rounding=ROUND_HALF_UP
        )

    quote_id = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=QUOTE_TTL_MINUTES)

    primary_model = spec["primary"]
    if CIRCUIT_BREAKER_FAILURES.get(primary_model, 0) >= 3:
        primary_model = spec["fallback"]

    diag = _diagnose_openrouter_runtime()

    quote_payload = {
        "quote_id": quote_id,
        "status": "PENDING_BUDGET_APPROVAL",
        "epistemic_tag": "ESTIMATIVA",
        "tier_key": tier_key,
        "tier_label": spec["tier_label"],
        "primary_model": primary_model,
        "fallback_model": spec["fallback"],
        "pig_context_attached": bool(pig_context),
        "est_input_tokens": est_in_tokens,
        "est_output_tokens": est_out_tokens,
        "est_cost_usd": float(est_usd),
        "est_cost_brl": float(est_brl),
        "savings_vs_tier3_pct": float(max(Decimal("0.0"), savings_pct)),
        "expires_at": expires_at.isoformat(),
        "raw_prompt": req.prompt.strip(),
        "compiled_prompt": full_prompt,
        "api_key_configured": diag["ok"],
        "diagnostic": diag,
    }
    PENDING_QUOTES[quote_id] = quote_payload
    return {k: v for k, v in quote_payload.items() if k != "compiled_prompt"}


def _call_openrouter(model: str, compiled_prompt: str, api_key: str) -> Dict[str, Any]:
    url = "https://openrouter.ai/api/v1/chat/completions"
    body = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Você é um agente especialista da Intelligence Network do Mind Gateway "
                    "(governado pelo motor Otimizzai V2.2). Separe fatos de hipóteses, "
                    "seja direto, preciso e orientado à execução."
                ),
            },
            {"role": "user", "content": compiled_prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://web-production-bc7ad0f9.up.railway.app",
        "X-Title": "Mind Gateway OS Broker",
    }
    req = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=55) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as http_err:
        err_body = ""
        try:
            err_body = http_err.read().decode("utf-8", errors="ignore")
        except Exception:
            pass
        raise RuntimeError(f"HTTP {http_err.code}: {err_body or http_err.reason}")


@router.post("/execute")
async def execute_orchestration(req: ExecuteRequest):
    if req.approval_token.strip().upper() != "AUTORIZADO":
        raise HTTPException(status_code=403, detail="Token de aprovação inválido. Exigido: AUTORIZADO.")

    quote = PENDING_QUOTES.get(req.quote_id)
    if not quote:
        raise HTTPException(status_code=404, detail="Orçamento não encontrado ou já executado.")

    if datetime.now(timezone.utc) > datetime.fromisoformat(quote["expires_at"]):
        PENDING_QUOTES.pop(req.quote_id, None)
        raise HTTPException(status_code=410, detail="Budget Gate expirado (TTL 30 min). Gere nova estimativa.")

    diag = _diagnose_openrouter_runtime()
    if not diag["ok"]:
        return JSONResponse(
            status_code=503,
            content={
                "detail": f"[{diag['code']}] {diag['title']} — {diag['cause']}",
                "diagnostic": diag,
            },
        )

    api_key = os.getenv("OPENROUTER_API_KEY", "").strip()
    spec = MODEL_CATALOG[quote["tier_key"]]
    primary = quote["primary_model"]
    fallback = quote["fallback_model"]
    used_model = primary
    fallback_triggered = False
    start_ms = time.time()

    try:
        raw_resp = _call_openrouter(primary, quote["compiled_prompt"], api_key)
        CIRCUIT_BREAKER_FAILURES[primary] = 0
    except Exception as primary_err:
        CIRCUIT_BREAKER_FAILURES[primary] = CIRCUIT_BREAKER_FAILURES.get(primary, 0) + 1
        fallback_triggered = True
        used_model = fallback
        try:
            raw_resp = _call_openrouter(fallback, quote["compiled_prompt"], api_key)
        except Exception as fallback_err:
            err_str = f"{primary_err} | {fallback_err}"
            if "402" in err_str or "credits" in err_str.lower() or "afford" in err_str.lower():
                runtime_diag = {
                    "ok": False,
                    "code": "SALDO_ZERADO",
                    "title": "Saldo insuficiente no OpenRouter para concluir a inferência (HTTP 402)",
                    "cause": "A chave é válida, mas o saldo disponível não cobre a reserva máxima de tokens deste modelo.",
                    "action": "Adicione créditos em openrouter.ai/credits ou selecione o Tier 1 (mais econômico).",
                }
            elif "401" in err_str:
                runtime_diag = {
                    "ok": False,
                    "code": "CHAVE_REVOGADA_OU_INVALIDA",
                    "title": "Falha de autenticação no OpenRouter (HTTP 401)",
                    "cause": "A chave configurada foi recusada durante o despacho aos modelos.",
                    "action": "Gere uma nova chave em openrouter.ai/keys e atualize OPENROUTER_API_KEY no Railway.",
                }
            else:
                runtime_diag = {
                    "ok": False,
                    "code": "CIRCUIT_BREAKER_OPEN",
                    "title": "Circuit Breaker aberto: falha no modelo primário e no fallback",
                    "cause": f"Primário ({primary}): {primary_err} | Fallback ({fallback}): {fallback_err}",
                    "action": "Aguarde alguns segundos e clique em Testar Conexão OpenRouter para verificar o status.",
                }
            return JSONResponse(
                status_code=502,
                content={
                    "detail": f"[{runtime_diag['code']}] {runtime_diag['title']}",
                    "diagnostic": runtime_diag,
                },
            )

    latency_ms = int((time.time() - start_ms) * 1000)
    choice_text = (
        raw_resp.get("choices", [{}])[0]
        .get("message", {})
        .get("content", "[Sem conteúdo retornado pelo modelo]")
    )
    usage = raw_resp.get("usage", {})
    real_in = int(usage.get("prompt_tokens", quote["est_input_tokens"]))
    real_out = int(usage.get("completion_tokens", quote["est_output_tokens"]))

    real_usd = _calc_cost_usd(real_in, real_out, spec)
    real_brl = (real_usd * USD_TO_BRL).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    delta_brl = abs(Decimal(str(quote["est_cost_brl"])) - real_brl).quantize(
        Decimal("0.0001"), rounding=ROUND_HALF_UP
    )

    execution_id = str(uuid.uuid4())
    record = {
        "execution_id": execution_id,
        "quote_id": req.quote_id,
        "status": "EXECUTED",
        "epistemic_tag": "FATO",
        "tier": quote["tier_label"],
        "model_used": used_model,
        "fallback_triggered": fallback_triggered,
        "latency_ms": latency_ms,
        "prompt": quote["raw_prompt"],
        "output": choice_text,
        "prompt_tokens": real_in,
        "completion_tokens": real_out,
        "est_cost_usd": quote["est_cost_usd"],
        "real_cost_usd": float(real_usd),
        "est_cost_brl": quote["est_cost_brl"],
        "real_cost_brl": float(real_brl),
        "calibration_delta_brl": float(delta_brl),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    record["db_persisted"] = _persist_execution_record(record)
    EXECUTION_HISTORY.insert(0, record)
    del EXECUTION_HISTORY[25:]
    PENDING_QUOTES.pop(req.quote_id, None)
    return record


@router.get("/history")
async def get_broker_history():
    return {
        "count": len(EXECUTION_HISTORY),
        "circuit_breaker_state": CIRCUIT_BREAKER_FAILURES,
        "items": EXECUTION_HISTORY[:10],
    }


BROKER_MOBILE_INJECTION = """
<style>
  #mg-broker-fab {
    position: fixed;
    bottom: 16px;
    right: 16px;
    z-index: 9999;
    background: linear-gradient(135deg, #f59e0b, #d97706);
    color: #09090b;
    font-weight: 800;
    font-size: 13px;
    border: none;
    border-radius: 999px;
    padding: 12px 18px;
    box-shadow: 0 8px 24px rgba(245, 158, 11, 0.38);
    cursor: pointer;
    letter-spacing: 0.3px;
  }
  #mg-broker-drawer {
    display: none;
    position: fixed;
    inset: 0;
    z-index: 10000;
    background: #09090b;
    color: #f4f4f5;
    overflow-y: auto;
    padding: 16px;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  }
  .mg-b-card {
    background: #121216;
    border: 1px solid #27272a;
    border-radius: 14px;
    padding: 14px;
    margin-bottom: 12px;
  }
  .mg-b-badge {
    display: inline-block;
    font-size: 11px;
    font-weight: 700;
    padding: 3px 8px;
    border-radius: 6px;
    background: rgba(245, 158, 11, 0.15);
    color: #fbbf24;
    border: 1px solid rgba(245, 158, 11, 0.35);
  }
  .mg-b-input, .mg-b-select {
    width: 100%;
    box-sizing: border-box;
    background: #18181d;
    color: #fafafa;
    border: 1px solid #3f3f46;
    border-radius: 10px;
    padding: 11px;
    font-size: 14px;
    margin-top: 8px;
  }
  .mg-b-btn {
    width: 100%;
    padding: 12px;
    border-radius: 10px;
    border: none;
    font-weight: 700;
    font-size: 14px;
    cursor: pointer;
    margin-top: 10px;
  }
  .mg-b-btn-amber { background: #f59e0b; color: #09090b; }
  .mg-b-btn-green { background: #10b981; color: #052e16; }
  .mg-b-btn-outline { background: #18181d; color: #e4e4e7; border: 1px solid #3f3f46; }
  .mg-b-row { display: flex; gap: 8px; align-items: center; justify-content: space-between; }
  .mg-b-mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 12px; }
  .mg-b-diag-box {
    margin-top: 10px;
    padding: 12px;
    border-radius: 10px;
    font-size: 13px;
    line-height: 1.45;
  }
  .mg-b-diag-err {
    background: rgba(239, 68, 68, 0.12);
    border: 1px solid rgba(239, 68, 68, 0.45);
    color: #fecaca;
  }
  .mg-b-diag-ok {
    background: rgba(16, 185, 129, 0.12);
    border: 1px solid rgba(16, 185, 129, 0.45);
    color: #a7f3d0;
  }
</style>

<button id="mg-broker-fab" onclick="mgToggleBroker(true)">⚡ 4. Broker Pay-As-You-Go</button>

<div id="mg-broker-drawer">
  <div class="mg-b-row" style="margin-bottom:12px;">
    <div>
      <div style="font-size:17px;font-weight:800;color:#fbbf24;">4. Broker Pay-As-You-Go</div>
      <div style="font-size:12px;color:#a1a1aa;">Arbitragem Multi-IA & Autodiagnóstico (v0.6.1)</div>
    </div>
    <button onclick="mgToggleBroker(false)" style="background:#27272a;color:#fff;border:none;border-radius:8px;padding:8px 12px;font-weight:700;">✕ Fechar</button>
  </div>

  <div class="mg-b-card">
    <div class="mg-b-row">
      <span class="mg-b-badge">ORQUESTRAÇÃO DE INTENÇÃO</span>
      <button type="button" onclick="mgBrokerVoice()" style="background:#1e1b4b;color:#a5b4fc;border:1px solid #4338ca;border-radius:8px;padding:5px 10px;font-size:12px;font-weight:700;">🎙️ Ditar</button>
    </div>
    <textarea id="mg-b-prompt" class="mg-b-input" rows="4" placeholder="Digite ou dite uma tarefa complexa para orquestrar entre Claude, GPT, Gemini ou Llama..."></textarea>

    <label style="display:block;font-size:12px;color:#a1a1aa;margin-top:10px;">Estratégia de Roteamento</label>
    <select id="mg-b-tier" class="mg-b-select">
      <option value="auto">⚡ Auto (Roteador Inteligente de Custo/Complexidade)</option>
      <option value="tier1_fast">🟢 Tier 1 — Triagem & Rápido (Gemini Flash / Llama 70B)</option>
      <option value="tier2_pro">🟡 Tier 2 — Código & Estratégia (Claude Sonnet / GPT-4o)</option>
      <option value="tier3_deep">🔴 Tier 3 — Arquitetura Profunda (Claude Opus / o3)</option>
    </select>

    <label style="display:flex;align-items:center;gap:8px;font-size:12px;color:#d4d4d8;margin-top:10px;">
      <input type="checkbox" id="mg-b-pig" checked />
      Injetar contexto ativo do PIG (Memória Contínua)
    </label>

    <button class="mg-b-btn mg-b-btn-amber" onclick="mgEstimateBroker()">Calcular Orçamento (Budget Gate)</button>
  </div>

  <div id="mg-b-diag-panel" class="mg-b-card" style="display:none;">
    <div class="mg-b-row">
      <span id="mg-b-diag-code" class="mg-b-badge">DIAGNÓSTICO</span>
      <span style="font-size:11px;color:#a1a1aa;">Autodiagnóstico Runtime</span>
    </div>
    <div id="mg-b-diag-body" class="mg-b-diag-box mg-b-diag-err"></div>
    <button id="mg-b-health-btn" class="mg-b-btn mg-b-btn-outline" onclick="mgCheckOpenRouterHealth()">🔄 Testar Conexão OpenRouter Agora</button>
  </div>

  <div id="mg-b-quote-box" class="mg-b-card" style="display:none;border-color:#f59e0b;">
    <div class="mg-b-row">
      <span class="mg-b-badge">BUDGET GATE • [ESTIMATIVA]</span>
      <span id="mg-b-savings" style="font-size:11px;color:#34d399;font-weight:700;"></span>
    </div>
    <div style="margin-top:10px;font-size:13px;line-height:1.5;">
      <div><strong>Rota:</strong> <span id="mg-b-q-tier"></span></div>
      <div><strong>Modelo Primário:</strong> <span id="mg-b-q-model" class="mg-b-mono" style="color:#fbbf24;"></span></div>
      <div><strong>Circuit Breaker Fallback:</strong> <span id="mg-b-q-fallback" class="mg-b-mono"></span></div>
      <div><strong>Tokens Previstos:</strong> <span id="mg-b-q-tokens" class="mg-b-mono"></span></div>
      <div style="font-size:16px;margin-top:6px;color:#34d399;">
        <strong>Custo Previsto:</strong> R$ <span id="mg-b-q-brl"></span> (<span id="mg-b-q-usd"></span> US$)
      </div>
    </div>
    <button id="mg-b-exec-btn" class="mg-b-btn mg-b-btn-green" onclick="mgExecuteBroker()">✅ AUTORIZAR E EXECUTAR EM 1 TOQUE</button>
  </div>

  <div id="mg-b-result-box" class="mg-b-card" style="display:none;border-color:#10b981;">
    <div class="mg-b-row">
      <span class="mg-b-badge" style="background:rgba(16,185,129,0.15);color:#34d399;border-color:#10b981;">EXECUTADO • [FATO]</span>
      <button onclick="mgCopyBrokerOutput()" style="background:#27272a;color:#fff;border:none;border-radius:6px;padding:4px 10px;font-size:12px;">📋 Copiar</button>
    </div>
    <div id="mg-b-r-audit" class="mg-b-mono" style="margin-top:8px;color:#a1a1aa;"></div>
    <pre id="mg-b-r-output" style="white-space:pre-wrap;word-wrap:break-word;background:#18181d;padding:12px;border-radius:10px;font-size:13px;line-height:1.5;margin-top:10px;color:#f4f4f5;"></pre>
  </div>
</div>

<script>
  let mgCurrentQuoteId = null;

  function mgToggleBroker(open) {
    document.getElementById('mg-broker-drawer').style.display = open ? 'block' : 'none';
  }

  function mgRenderDiagnostic(diag) {
    const panel = document.getElementById('mg-b-diag-panel');
    if (!diag) {
      panel.style.display = 'none';
      return;
    }
    const badge = document.getElementById('mg-b-diag-code');
    const body = document.getElementById('mg-b-diag-body');
    panel.style.display = 'block';
    panel.style.borderColor = diag.ok ? '#10b981' : '#ef4444';
    badge.innerText = diag.code || (diag.ok ? 'OK' : 'FALHA');
    badge.style.color = diag.ok ? '#34d399' : '#fca5a5';
    badge.style.borderColor = diag.ok ? '#10b981' : '#ef4444';
    badge.style.background = diag.ok ? 'rgba(16,185,129,0.15)' : 'rgba(239,68,68,0.15)';
    body.className = 'mg-b-diag-box ' + (diag.ok ? 'mg-b-diag-ok' : 'mg-b-diag-err');

    let extra = '';
    if (diag.ok && diag.meta) {
      const rem = diag.meta.limit_remaining_usd !== null && diag.meta.limit_remaining_usd !== undefined
        ? `US$ ${Number(diag.meta.limit_remaining_usd).toFixed(4)}`
        : 'Sem teto fixo na chave';
      extra = `<div style="margin-top:6px;font-size:12px;"><strong>Uso acumulado:</strong> US$ ${Number(diag.meta.usage_usd || 0).toFixed(4)} | <strong>Limite restante:</strong> ${rem}</div>`;
    }
    body.innerHTML =
      `<div style="font-weight:800;margin-bottom:4px;">${diag.ok ? '✅' : '🚨'} ${diag.title}</div>` +
      `<div><strong>Causa identificada:</strong> ${diag.cause}</div>` +
      `<div style="margin-top:6px;"><strong>Como resolver:</strong> ${diag.action}</div>` +
      extra;
  }

  async function mgCheckOpenRouterHealth() {
    const btn = document.getElementById('mg-b-health-btn');
    btn.disabled = true;
    btn.innerText = '⏳ Diagnosticando OpenRouter...';
    try {
      const res = await fetch('/api/v1/orchestrate/health-check');
      const data = await res.json();
      if (data.diagnostic) {
        mgRenderDiagnostic(data.diagnostic);
      }
    } finally {
      btn.disabled = false;
      btn.innerText = '🔄 Testar Conexão OpenRouter Agora';
    }
  }

  function mgBrokerVoice() {
    const SR = window.SpeechRecognition || window.webkitSpeechRecognition;
    if (!SR) {
      alert('Use o microfone nativo do teclado Gboard no campo de texto.');
      return;
    }
    const rec = new SR();
    rec.lang = 'pt-BR';
    rec.onresult = (e) => {
      const box = document.getElementById('mg-b-prompt');
      box.value = (box.value ? box.value + ' ' : '') + e.results[0][0].transcript;
    };
    rec.start();
  }

  async function mgEstimateBroker() {
    const prompt = document.getElementById('mg-b-prompt').value.trim();
    if (!prompt) { alert('Digite ou dite a tarefa primeiro.'); return; }
    const routing_mode = document.getElementById('mg-b-tier').value;
    const use_pig_context = document.getElementById('mg-b-pig').checked;

    const res = await fetch('/api/v1/orchestrate/estimate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ prompt, routing_mode, use_pig_context })
    });
    const data = await res.json();
    if (!res.ok) { alert(data.detail || 'Erro ao calcular orçamento.'); return; }

    mgCurrentQuoteId = data.quote_id;
    document.getElementById('mg-b-q-tier').innerText = data.tier_label;
    document.getElementById('mg-b-q-model').innerText = data.primary_model;
    document.getElementById('mg-b-q-fallback').innerText = data.fallback_model;
    document.getElementById('mg-b-q-tokens').innerText = `${data.est_input_tokens} in / ~${data.est_output_tokens} out`;
    document.getElementById('mg-b-q-brl').innerText = data.est_cost_brl.toFixed(4);
    document.getElementById('mg-b-q-usd').innerText = data.est_cost_usd.toFixed(6);
    document.getElementById('mg-b-savings').innerText = data.savings_vs_tier3_pct > 0 ? `-${data.savings_vs_tier3_pct}% vs Tier 3` : 'Tier Máximo';

    if (data.diagnostic && !data.diagnostic.ok) {
      mgRenderDiagnostic(data.diagnostic);
    } else {
      document.getElementById('mg-b-diag-panel').style.display = 'none';
    }

    document.getElementById('mg-b-quote-box').style.display = 'block';
    document.getElementById('mg-b-result-box').style.display = 'none';
  }

  async function mgExecuteBroker() {
    if (!mgCurrentQuoteId) return;
    const btn = document.getElementById('mg-b-exec-btn');
    btn.disabled = true;
    btn.innerText = '⏳ Orquestrando IA via OpenRouter...';

    try {
      const res = await fetch('/api/v1/orchestrate/execute', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ quote_id: mgCurrentQuoteId, approval_token: 'AUTORIZADO' })
      });
      const data = await res.json();
      if (!res.ok) {
        if (data.diagnostic) {
          mgRenderDiagnostic(data.diagnostic);
        } else {
          alert(data.detail || 'Falha na execução.');
        }
        return;
      }
      document.getElementById('mg-b-diag-panel').style.display = 'none';
      document.getElementById('mg-b-r-audit').innerText =
        `Modelo: ${data.model_used} ${data.fallback_triggered ? '(FALLBACK ATIVO)' : ''} | ` +
        `Tokens: ${data.prompt_tokens} in / ${data.completion_tokens} out | ` +
        `Custo Real: R$ ${data.real_cost_brl.toFixed(4)} (Desvio |E-R|: R$ ${data.calibration_delta_brl.toFixed(4)}) | ` +
        `Latência: ${data.latency_ms}ms`;
      document.getElementById('mg-b-r-output').innerText = data.output;
      document.getElementById('mg-b-result-box').style.display = 'block';
    } finally {
      btn.disabled = false;
      btn.innerText = '✅ AUTORIZAR E EXECUTAR EM 1 TOQUE';
    }
  }

  function mgCopyBrokerOutput() {
    const txt = document.getElementById('mg-b-r-output').innerText;
    navigator.clipboard.writeText(txt);
    alert('Resposta copiada!');
  }
</script>
"""


def register_broker_ui(app):
    @app.middleware("http")
    async def inject_broker_tab_middleware(request: Request, call_next):
        response = await call_next(request)
        if request.url.path == "/" and "text/html" in response.headers.get("content-type", ""):
            body_bytes = b""
            async for chunk in response.body_iterator:
                body_bytes += chunk
            html = body_bytes.decode("utf-8", errors="ignore")
            if "mg-broker-fab" not in html and "</body>" in html:
                html = html.replace("</body>", f"{BROKER_MOBILE_INJECTION}\n</body>")
            headers = dict(response.headers)
            headers.pop("content-length", None)
            return HTMLResponse(content=html, status_code=response.status_code, headers=headers)
        return response
