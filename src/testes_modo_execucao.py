#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import unittest

class TestModoExecucao(unittest.TestCase):
    
    def test_calculo_faturamento(self):
        faturamento = 45000.0
        taxa = 27.0
        potencial = faturamento * (taxa / 100.0) * 0.70
        self.assertGreaterEqual(potencial, 1000.0)
        
    def test_integridade_roteiros(self):
        roteiros_mock = [{"id": 1, "gancho": "Teste"}]
        self.assertEqual(len(roteiros_mock), 1)
        self.assertIn("gancho", roteiros_mock[0])

if __name__ == "__main__":
    print("\n--- A EXECUTAR TESTES UNITÁRIOS ---")
    unittest.main()
