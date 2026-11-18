#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Modo Execução - CLI Central de Controlo
"""

import sys
import subprocess

def mostrar_menu():
    print("\n========================================")
    print("      MODO EXECUÇÃO - CLI CENTRAL       ")
    print("========================================\n")
    print(" [1] Executar Motor de Oportunidades B2B")
    print(" [2] Gerar Lote de Roteiros")
    print(" [3] Exportar Roteiros para Ficheiros")
    print(" [4] Simular Disparo de Propostas")
    print(" [5] Executar Registo de Auditoria")
    print(" [6] Executar Backup de Ativos")
    print(" [7] Analisar Métricas do Funil")
    print(" [0] Sair")
    print("----------------------------------------")

def main():
    while True:
        mostrar_menu()
        opcao = input("Selecione uma opção: ").strip()
        
        if opcao == "1":
            subprocess.run(["python3", "motor_caixa.py"])
        elif opcao == "2":
            subprocess.run(["python3", "gerador_roteiros.py"])
        elif opcao == "3":
            subprocess.run(["python3", "exportador_roteiros.py"])
        elif opcao == "4":
            subprocess.run(["python3", "disparador_propostas.py"])
        elif opcao == "5":
            subprocess.run(["python3", "auditoria_sistema.py"])
        elif opcao == "6":
            subprocess.run(["python3", "backup_projeto.py"])
        elif opcao == "7":
            subprocess.run(["python3", "monitor_funil.py"])
        elif opcao == "0":
            print("A encerrar o Modo Execução. Até breve.")
            sys.exit(0)
        else:
            print("Opção inválida. Tente novamente.")

if __name__ == "__main__":
    main()


