"""
Motor Deterministico de Conciliacao Financeira (iFood v3 + PAT 3.6% MDR)
e Disparo B2B Outbound via Evolution API com Exponential Backoff.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("PG_OS_AuditEngine")

RETRYABLE_HTTP_CODES: Tuple[int, ...] = (408, 429, 500, 502, 503, 504)


def with_exponential_backoff(
    func: Callable[..., Any],
    *args: Any,
    max_retries: int = 5,
    base_delay: float = 1.5,
    max_delay: float = 32.0,
    **kwargs: Any,
) -> Any:
    """Executa chamadas de rede com retentativa e backoff exponencial + jitter."""
    attempt = 0
    while True:
        try:
            return func(*args, **kwargs)
        except urllib.error.HTTPError as exc:
            attempt += 1
            if exc.code not in RETRYABLE_HTTP_CODES or attempt > max_retries:
                logger.error("Falha HTTP definitiva (%s): %s", exc.code, exc.reason)
                raise
            sleep_time = min(max_delay, (base_delay ** attempt) + random.uniform(0.1, 0.9))
            logger.warning(
                "HTTP %s detectado. Tentativa %d/%d em %.2fs...",
                exc.code,
                attempt,
                max_retries,
                sleep_time,
            )
            time.sleep(sleep_time)
        except urllib.error.URLError as exc:
            attempt += 1
            if attempt > max_retries:
                logger.error("Erro de rede critico apos %d tentativas: %s", max_retries, exc)
                raise
            sleep_time = min(max_delay, (base_delay ** attempt) + random.uniform(0.1, 0.9))
            logger.warning(
                "Falha de conexao (%s). Tentativa %d/%d em %.2fs...",
                exc.reason,
                attempt,
                max_retries,
                sleep_time,
            )
            time.sleep(sleep_time)


@dataclass(frozen=True)
class OrderSettlementRecord:
    """Representa um registro consolidado de pedido para auditoria forense."""

    merchant_id: str
    tenant_cnpj: str
    order_id: str
    payment_channel: str  # 'IFOOD_ONLINE', 'PAT_VOUCHER', 'DIRECT_CREDIT'
    order_date: datetime
    settlement_date: Optional[datetime]
    gross_amount: Decimal
    contracted_commission_rate: Decimal
    charged_commission_amount: Decimal
    charged_mdr_rate: Decimal
    charged_mdr_amount: Decimal
    shared_promo_subsidy_charged: Decimal
    contracted_promo_cap: Decimal
    unreimbursed_post_prep_cancel: Decimal
    actual_net_settlement: Decimal


@dataclass
class AuditedOrderResult:
    """Resultado deterministico da auditoria de um unico pedido."""

    record: OrderSettlementRecord
    expected_net_settlement: Decimal
    leakage_amount: Decimal
    leakage_reasons: List[Dict[str, Any]] = field(default_factory=list)
    evidence_hash: str = ""


class FinancialForensicAuditor:
    """Audita repasses de Marketplaces e conformidade com o teto legal de 3.6% do PAT."""

    def __init__(
        self,
        pat_max_mdr_rate: Decimal = Decimal(os.getenv("PAT_MAX_MDR_RATE", "0.0360")),
        pat_max_liquidation_days: int = int(os.getenv("PAT_MAX_LIQUIDATION_DAYS", "15")),
    ) -> None:
        self.pat_max_mdr_rate = pat_max_mdr_rate
        self.pat_max_liquidation_days = pat_max_liquidation_days

    @staticmethod
    def _quantize(value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    def audit_order(self, rec: OrderSettlementRecord) -> AuditedOrderResult:
        """Calcula desvios entre o contrato/legislacao PAT e o repasse efetivo."""
        reasons: List[Dict[str, Any]] = []

        # 1. Auditoria de Comissao de Marketplace
        expected_commission = self._quantize(rec.gross_amount * rec.contracted_commission_rate)
        commission_overcharge = max(Decimal("0.00"), rec.charged_commission_amount - expected_commission)
        if commission_overcharge > Decimal("0.01"):
            reasons.append(
                {
                    "code": "MARKETPLACE_COMMISSION_OVERCHARGE",
                    "description": "Comissao cobrada acima da aliquota contratual sobre base de produtos.",
                    "amount": str(commission_overcharge),
                }
            )

        # 2. Auditoria de Teto Legal do PAT (3.6% MDR e Prazo de 15 dias)
        if rec.payment_channel == "PAT_VOUCHER":
            allowed_mdr_rate = min(rec.charged_mdr_rate, self.pat_max_mdr_rate)
            expected_mdr_amount = self._quantize(rec.gross_amount * allowed_mdr_rate)
            mdr_overcharge = max(Decimal("0.00"), rec.charged_mdr_amount - expected_mdr_amount)
            if mdr_overcharge > Decimal("0.01"):
                reasons.append(
                    {
                        "code": "PAT_MDR_CAP_VIOLATION",
                        "description": f"Violacao do teto legal de {self.pat_max_mdr_rate * 100:.2f}% de MDR do PAT.",
                        "charged_rate": str(rec.charged_mdr_rate),
                        "legal_cap": str(self.pat_max_mdr_rate),
                        "amount": str(mdr_overcharge),
                    }
                )
            if rec.settlement_date is not None:
                days_to_settle = (rec.settlement_date - rec.order_date).days
                if days_to_settle > self.pat_max_liquidation_days:
                    reasons.append(
                        {
                            "code": "PAT_LIQUIDATION_DELAY_VIOLATION",
                            "description": (
                                f"Liquidacao em {days_to_settle} dias excede teto legal "
                                f"de {self.pat_max_liquidation_days} dias corridos do PAT."
                            ),
                            "days_delayed": days_to_settle - self.pat_max_liquidation_days,
                            "amount": "0.00",
                        }
                    )
        else:
            expected_mdr_amount = self._quantize(rec.gross_amount * rec.charged_mdr_rate)

        # 3. Auditoria de Subsidio de Promocao Compartilhada
        promo_overcharge = max(
            Decimal("0.00"),
            rec.shared_promo_subsidy_charged - rec.contracted_promo_cap,
        )
        if promo_overcharge > Decimal("0.01"):
            reasons.append(
                {
                    "code": "UNCONTRACTED_PROMO_SUBSIDY",
                    "description": "Desconto de campanha promocional superior ao teto acordado pelo lojista.",
                    "amount": str(promo_overcharge),
                }
            )

        # 4. Auditoria de Cancelamento Pos-Producao nao Ressarcido
        if rec.unreimbursed_post_prep_cancel > Decimal("0.01"):
            reasons.append(
                {
                    "code": "UNREIMBURSED_POST_PREP_CANCELLATION",
                    "description": "Cancelamento apos inicio/conclusao do preparo sem estorno de CMV.",
                    "amount": str(rec.unreimbursed_post_prep_cancel),
                }
            )

        expected_net = self._quantize(
            rec.gross_amount
            - expected_commission
            - expected_mdr_amount
            - min(rec.shared_promo_subsidy_charged, rec.contracted_promo_cap)
        )
        total_leakage = max(
            Decimal("0.00"),
            (expected_net - rec.actual_net_settlement) + rec.unreimbursed_post_prep_cancel,
        )

        payload_for_hash = (
            f"{rec.merchant_id}|{rec.order_id}|{rec.gross_amount}|"
            f"{expected_net}|{rec.actual_net_settlement}|{total_leakage}"
        )
        evidence_hash = hashlib.sha256(payload_for_hash.encode("utf-8")).hexdigest()

        return AuditedOrderResult(
            record=rec,
            expected_net_settlement=expected_net,
            leakage_amount=self._quantize(total_leakage),
            leakage_reasons=reasons,
            evidence_hash=evidence_hash,
        )

    def persist_audit_batch(self, db_dsn: str, audited_results: List[AuditedOrderResult]) -> None:
        """Persiste lote auditado com transacao atomica e SQL parametrizado."""
        if not audited_results:
            return

        insert_sql = """
            INSERT INTO financial_audit_ledger (
                merchant_id, tenant_cnpj, order_id, payment_channel,
                order_date, settlement_date, gross_amount,
                contracted_commission_rate, charged_commission_amount,
                charged_mdr_rate, charged_mdr_amount,
                shared_promo_subsidy_charged, contracted_promo_cap,
                unreimbursed_post_prep_cancel, expected_net_settlement,
                actual_net_settlement, leakage_amount, leakage_reasons, evidence_hash
            ) VALUES %s
            ON CONFLICT (merchant_id, order_id, payment_channel) DO UPDATE SET
                expected_net_settlement = EXCLUDED.expected_net_settlement,
                actual_net_settlement = EXCLUDED.actual_net_settlement,
                leakage_amount = EXCLUDED.leakage_amount,
                leakage_reasons = EXCLUDED.leakage_reasons,
                evidence_hash = EXCLUDED.evidence_hash,
                audited_at = NOW();
        """

        rows = [
            (
                res.record.merchant_id,
                res.record.tenant_cnpj,
                res.record.order_id,
                res.record.payment_channel,
                res.record.order_date,
                res.record.settlement_date,
                res.record.gross_amount,
                res.record.contracted_commission_rate,
            
cd ~/pg_os_enterprise_stack && docker compose up -d.
#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# PG-OS MASTER DEPLOYMENT SCRIPT: AI INFRASTRUCTURE, B2B AUDIT & COGNITIVE AUDIO
# ==============================================================================

PROJECT_DIR="${HOME}/pg_os_enterprise_stack"
mkdir -p "${PROJECT_DIR}/sql" "${PROJECT_DIR}/src" "${PROJECT_DIR}/output"
cd "${PROJECT_DIR}"

echo "[1/6] Gerando arquivo de variaveis de ambiente (.env)..."
if [ ! -f "${PROJECT_DIR}/.env" ]; then
cat << 'EOF' > "${PROJECT_DIR}/.env"
# POSTGRESQL & PGVECTOR CONFIG
POSTGRES_USER=pgadmin_exec
POSTGRES_PASSWORD=ChangeMe_StrongPass_2026!
POSTGRES_DB=n8n_system
AUDIT_DB=corporate_intel
POSTGRES_PORT=5432

# N8N CONFIG
N8N_ENCRYPTION_KEY=ChangeMe_32Char_Encryption_Key_2026
N8N_HOST=localhost
N8N_PORT=5678
GENERIC_TIMEZONE=America/Sao_Paulo

# EVOLUTION API (WHATSAPP) CONFIG
EVOLUTION_API_KEY=ChangeMe_Evolution_Global_Key_2026
EVOLUTION_INSTANCE=parme_b2b_auditor
EVOLUTION_PORT=8080

# IFOOD API V3 & AUDITORIA PAT CONFIG
IFOOD_CLIENT_ID=your_ifood_client_id
IFOOD_CLIENT_SECRET=your_ifood_client_secret
IFOOD_MERCHANT_ID=your_merchant_uuid
PAT_MAX_MDR_RATE=0.036
PAT_MAX_LIQUIDATION_DAYS=15

# LLM ROUTER (LOCAL OLLAMA/VLLM OR DEEPSEEK API)
LLM_BASE_URL=http://host.docker.internal:11434/v1
LLM_API_KEY=local_key_not_required
