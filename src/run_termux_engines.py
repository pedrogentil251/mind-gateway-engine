from __future__ import annotations
import hashlib, json, math, wave, struct
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict

@dataclass(frozen=True)
class OrderRecord:
    merchant_id: str
    order_id: str
    payment_channel: str
    order_date: datetime
    settlement_date: datetime
    gross_amount: Decimal
    contracted_commission_rate: Decimal
    charged_commission_amount: Decimal
    charged_mdr_rate: Decimal
    charged_mdr_amount: Decimal
    shared_promo_subsidy_charged: Decimal
    contracted_promo_cap: Decimal
    unreimbursed_post_prep_cancel: Decimal
    actual_net_settlement: Decimal

def run_forensic_audit(rec: OrderRecord) -> Dict[str, Any]:
    q = lambda v: v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    pat_cap = Decimal("0.0360")
    reasons = []
    exp_comm = q(rec.gross_amount * rec.contracted_commission_rate)
    if rec.charged_commission_amount - exp_comm > Decimal("0.01"):
        reasons.append({"falha": "COMISSAO_ACIMA_CONTRATO", "valor_retido": str(rec.charged_commission_amount - exp_comm)})
    exp_mdr = q(rec.gross_amount * (min(rec.charged_mdr_rate, pat_cap) if rec.payment_channel == "PAT_VOUCHER" else rec.charged_mdr_rate))
    if rec.charged_mdr_amount - exp_mdr > Decimal("0.01"):
        reasons.append({"falha": "VIOLACAO_TETO_PAT_3.6%", "valor_retido": str(rec.charged_mdr_amount - exp_mdr)})
    days = (rec.settlement_date - rec.order_date).days
    if rec.payment_channel == "PAT_VOUCHER" and days > 15:
        reasons.append({"falha": "ATRASO_LIQUIDACAO_PAT_15_DIAS", "dias_atraso": days - 15})
    if rec.shared_promo_subsidy_charged - rec.contracted_promo_cap > Decimal("0.01"):
        reasons.append({"falha": "SUBSIDIO_PROMO_INDEVIDO", "valor_retido": str(rec.shared_promo_subsidy_charged - rec.contracted_promo_cap)})
    exp_net = q(rec.gross_amount - exp_comm - exp_mdr - min(rec.shared_promo_subsidy_charged, rec.contracted_promo_cap))
    leakage = q(max(Decimal("0.00"), (exp_net - rec.actual_net_settlement) + rec.unreimbursed_post_prep_cancel))
    ev_hash = hashlib.sha256(f"{rec.order_id}|{leakage}".encode()).hexdigest()[:16]
    return {"pedido": rec.order_id, "repasse_esperado": str(exp_net), "repasse_real": str(rec.actual_net_settlement), "vazamento_total_brl": str(leakage), "hash_prova": ev_hash, "causas": reasons}

def generate_binaural_wav_pure_python(duration_sec: int = 30) -> str:
    sr = 24000
    num_samples = sr * duration_sec
    dl_dir = Path("/sdcard/Download") if Path("/sdcard/Download").exists() else Path(".")
    out_path = dl_dir / "PG_Binaural_Delta_2_67Hz_888Hz.wav"
    print(f"A gerar {duration_sec} segundos de audio acustico (Delta 2.67 Hz + 888 Hz)...")
    with wave.open(str(out_path), "wb") as wf:
        wf.setnchannels(2)
        wf.setsampwidth(2)
        wf.setframerate(sr)
        chunk_size = 2400
        for i in range(0, num_samples, chunk_size):
            frames = bytearray()
            for j in range(i, min(i + chunk_size, num_samples)):
                t = j / float(sr)
                l_val = (0.12 * math.sin(2.0 * math.pi * 220.00 * t)) + (0.04 * math.sin(2.0 * math.pi * 888.00 * t) * 0.5 * (1.0 + math.sin(2.0 * math.pi * 2.67 * t)))
                r_val = (0.12 * math.sin(2.0 * math.pi * 222.67 * t)) + (0.04 * math.sin(2.0 * math.pi * 888.00 * t) * 0.5 * (1.0 + math.sin(2.0 * math.pi * 2.67 * t)))
                l_pcm = int(max(-1.0, min(1.0, l_val)) * 32767)
                r_pcm = int(max(-1.0, min(1.0, r_val)) * 32767)
                frames.extend(struct.pack("<hh", l_pcm, r_pcm))
            wf.writeframes(frames)
    return str(out_path)

if __name__ == "__main__":
    sample = OrderRecord(
        merchant_id="PARME-BTU", order_id="IFOOD-99812", payment_channel="PAT_VOUCHER",
        order_date=datetime(2026, 9, 1, tzinfo=timezone.utc), settlement_date=datetime(2026, 10, 5, tzinfo=timezone.utc),
        gross_amount=Decimal("250.00"), contracted_commission_rate=Decimal("0.1200"), charged_commission_amount=Decimal("35.00"),
        charged_mdr_rate=Decimal("0.0650"), charged_mdr_amount=Decimal("16.25"), shared_promo_subsidy_charged=Decimal("15.00"),
        contracted_promo_cap=Decimal("5.00"), unreimbursed_post_prep_cancel=Decimal("0.00"), actual_net_settlement=Decimal("183.75")
    )
    print("\\n=== [1] RESULTADO AUDITORIA FORENSE (IFOOD + PAT 3.6%) ==/")
    print(json.dumps(run_forensic_audit(sample), indent=2, ensure_ascii=False))
    wav_file = generate_binaural_wav_pure_python(30)
    print(f"\\n=== [2] AUDIO COGNITIVO GERADO COM SUCESSO ===\\nFicheiro guardado em: {wav_file}\\n")
