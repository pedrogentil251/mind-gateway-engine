#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modo Execução - Gestor Local de Estado e Métricas
"""

import sys
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def verificar_estado_projeto():
    diretorio = Path.cwd()
    fich_txt = list(diretorio.glob("roteiro_*.txt"))
    fich_py = list(diretorio.glob("*.py"))
    
    print("\n========================================")
    print("   PAINEL DE CONTROLO - MODO EXECUÇÃO   ")
    print("========================================")
    logging.info(f"Diretório de trabalho: {diretorio}")
    logging.info(f"Scripts Python ativos: {len(fich_py)}")
    logging.info(f"Roteiros em texto gerados: {len(fich_txt)}")
    print("----------------------------------------")
    for f in fich_txt:
        print(f" [OK] Ativo pronto para produção: {f.name}")
    print("========================================\n")

if __name__ == "__main__":
    verificar_estado_projeto()

