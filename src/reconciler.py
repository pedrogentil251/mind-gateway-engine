#!/usr/bin/env python3
"""
Módulo Autônomo de Conciliação Pix/Pedidos - Zero Dependências
Stack: Standard Python 3 Library (http.server + sqlite3)
Compatibilidade: 100% Nativo no Termux / Android
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Any, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [PG-OS-PIX] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Reconciler")

DB_FILE = os.getenv("PGOS_DB_PATH", "reconciliation_ledger.db")
SERVER_PORT = int(os.getenv("PGOS_PORT", "8085"))
MAX_RETRIES = 3
BACKOFF = 0.4

def init_database(db_path: str) -> None:
    conn = sqlite3.connect(db_path)
    try:
        with conn:
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    customer_name TEXT NOT NULL,
                    expected_amount REAL NOT NULL,
                    status TEXT NOT NULL CHECK(status IN ('PENDING', 'PAID', 'DIVERGENT', 'CANCELLED')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pix_events (
                    end_to_end_id TEXT PRIMARY KEY,
                    txid TEXT,
                    amount REAL NOT NULL,
                    payer_info TEXT,
                    matched_order_id TEXT,
                    received_at TEXT NOT NULL,
                    FOREIGN KEY (matched_order_id) REFERENCES orders(order_id)
                );
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_pix_txid ON pix_events(txid);")
            logger.info("Banco SQLite configurado em modo WAL com protecao.")
    finally:
        conn.close()

class ReconciliationEngine:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _execute_with_retry(self, operation, *args, **kwargs) -> Any:
        last_error = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                conn = sqlite3.connect(self.db_path, timeout=10.0)
                try:
                    with conn:
                        return operation(conn, *args, **kwargs)
                finally:
                    conn.close()
            except sqlite3.OperationalError as exc:
                last_error = exc
                wait_time = BACKOFF * (2 ** (attempt - 1))
                logger.warning("Conflito I/O SQLite. Tentativa %d/%d. Retry em %.2fs", attempt, MAX_RETRIES, wait_time)
                time.sleep(wait_time)
        raise RuntimeError(f"Falha de persistencia: {last_error}")

    def register_order(self, order_id: str, customer_name: str, expected_amount: float, created_at: str) -> Dict[str, Any]:
        def _op(conn: sqlite3.Connection):
            now = datetime.now(timezone.utc).isoformat()
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO orders (order_id, customer_name, expected_amount, status, created_at, updated_at)
                VALUES (?, ?, ?, 'PENDING', ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    expected_amount = excluded.expected_amount,
                    updated_at = excluded.updated_at
                WHERE orders.status = 'PENDING';
            """, (order_id, customer_name, expected_amount, created_at, now))
            return {"status": "REGISTERED", "order_id": order_id, "expected_amount": expected_amount}
        return self._execute_with_retry(_op)

    def reconcile_pix(self, txid: str, e2eid: str, amount: float, payer_info: str) -> Dict[str, Any]:
        def _op(conn: sqlite3.Connection):
            now = datetime.now(timezone.utc).isoformat()
            cursor = conn.cursor()

            cursor.execute("SELECT end_to_end_id, matched_order_id FROM pix_events WHERE end_to_end_id = ?", (e2eid,))
            existing = cursor.fetchone()
            if existing:
                return {"action": "ALREADY_PROCESSED", "end_to_end_id": e2eid, "order_id": existing[1], "status": "DUPLICATE"}

            matched_order_id = None
            resolution_status = "UNMATCHED"

            if txid:
                cursor.execute("SELECT order_id, expected_amount FROM orders WHERE order_id = ? AND status = 'PENDING';", (txid,))
                row = cursor.fetchone()
                if row:
                    matched_order_id = row[0]
                    resolution_status = "PAID" if abs(float(row[1]) - amount) <= 0.01 else "DIVERGENT"

            if not matched_order_id:
                cursor.execute("""
                    SELECT order_id, expected_amount FROM orders 
                    WHERE status = 'PENDING' AND ABS(expected_amount - ?) <= 0.01 
                    ORDER BY created_at ASC LIMIT 1;
                """, (amount,))
                row = cursor.fetchone()
                if row:
                    matched_order_id = row[0]
                    resolution_status = "PAID"

            if matched_order_id:
                cursor.execute("UPDATE orders SET status = ?, updated_at = ? WHERE order_id = ?;", (resolution_status, now, matched_order_id))

            cursor.execute("""
                INSERT INTO pix_events (end_to_end_id, txid, amount, payer_info, matched_order_id, received_at)
                VALUES (?, ?, ?, ?, ?, ?);
            """, (e2eid, txid, amount, payer_info, matched_order_id, now))

            return {
                "action": "PROCESSED",
                "matched": bool(matched_order_id),
                "order_id": matched_order_id,
                "reconciliation_status": resolution_status,
                "amount": amount,
                "end_to_end_id": e2eid
            }
        return self._execute_with_retry(_op)

    def get_summary(self) -> Dict[str, Any]:
        def _op(conn: sqlite3.Connection):
            cursor = conn.cursor()
            cursor.execute("SELECT status, COUNT(*), COALESCE(SUM(expected_amount), 0.0) FROM orders GROUP BY status;")
            orders_summary = {row[0]: {"count": row[1], "total": round(row[2], 2)} for row in cursor.fetchall()}
            cursor.execute("SELECT COUNT(*), COALESCE(SUM(amount), 0.0) FROM pix_events;")
            pix_total = cursor.fetchone()
            return {
                "orders_by_status": orders_summary,
                "pix_events_ingested": {"count": pix_total[0], "volume": round(pix_total[1], 2)},
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        return self._execute_with_retry(_op)

engine = ReconciliationEngine(DB_FILE)

class ReconcilerHandler(BaseHTTPRequestHandler):
    def _send_json(self, status: int, data: Dict[str, Any]):
        response_bytes = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

    def do_GET(self):
        if self.path == "/api/v1/summary":
            self._send_json(200, engine.get_summary())
        else:
            self._send_json(404, {"error": "Rota nao encontrada"})

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)
        try:
            payload = json.loads(body.decode("utf-8")) if body else {}
        except Exception:
            self._send_json(400, {"error": "JSON malformado"})
            return

        if self.path == "/api/v1/orders":
            order_id = str(payload.get("order_id", "")).strip()
            if not order_id:
                self._send_json(400, {"error": "order_id obrigatorio"})
                return
            res = engine.register_order(
                order_id=order_id,
                customer_name=str(payload.get("customer_name", "Cliente Balcão")),
                expected_amount=float(payload.get("expected_amount", 0.0)),
                created_at=str(payload.get("created_at", datetime.now(timezone.utc).isoformat()))
            )
            self._send_json(201, res)

        elif self.path == "/api/v1/webhooks/pix":
            txid = str(payload.get("txid") or payload.get("correlationID") or payload.get("external_reference") or "").strip()
            e2eid = str(payload.get("endToEndId") or payload.get("e2eId") or payload.get("id") or str(time.time())).strip()
            amount = float(payload.get("valor") or payload.get("amount") or payload.get("transaction_amount") or 0.0)
            payer = str(payload.get("pagador", {}).get("nome") or payload.get("payer", {}).get("email") or "Desconhecido")

            res = engine.reconcile_pix(txid=txid, e2eid=e2eid, amount=amount, payer_info=payer)
            self._send_json(200, res)

        else:
            self._send_json(404, {"error": "Rota nao encontrada"})

    def log_message(self, format, *args):
        logger.info("%s - %s", self.address_string(), format % args)

if __name__ == "__main__":
    init_database(DB_FILE)
    server = HTTPServer(("0.0.0.0", SERVER_PORT), ReconcilerHandler)
    logger.info("Servidor de Conciliação PG-OS ativo na porta %d", SERVER_PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Encerrando servidor...")
        server.server_close()
