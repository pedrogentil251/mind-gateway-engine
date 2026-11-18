#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modo Execução - Sistema de Auditoria e Registo Persistente
"""

import sys
import logging
from datetime import datetime
from pathlib import Path

# Configuração de log persistente em ficheiro
logging.basicConfig(
    filename="modo_execucao_auditoria.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)


def registar_auditoria(acao: str, status: str):
    registo = f"AÇÃO: {acao} | STATUS: {status} | OPERADOR: Sistema Local"
    logging.info(registo)
    print(f"[REGISTADO] {registo}")

if __name__ == "__main__":
    print("\n--- A EXECUTAR AUDITORIA DO SISTEMA ---")
    registar_auditoria("Varredura de Oportunidades B2B", "CONCLUÍDO")
    registar_auditoria("Disparo de Propostas Comerciais", "SIMULADO")
    registar_auditoria("Sincronização de Ficheiros de Roteiro", "VALIDADO")
    print("Log gravado com sucesso no ficheiro 'modo_execucao_auditoria.log'.")
