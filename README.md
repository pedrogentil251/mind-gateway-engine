# Mind Gateway

> *"The gateway between your mind and the intelligence of the world."*

O **Mind Gateway** é um Sistema Operacional Cognitivo e camada de governança de decisões para orquestração de Inteligência Artificial. Preserva a continuidade de contexto, a genealogia de decisões (*Decision Lineage*) e a calibração empírica ao longo do tempo.

## Arquitetura (7 Camadas)

1. **Interaction Layer:** Captura de entradas brutas (voz, texto, WhatsApp, CLI/webhooks).
2. **Intent Understanding Engine:** Extração de entidades e objetivos com tipagem forte via FastAPI e Pydantic V2.
3. **Personal Intelligence Graph (PIG):** Grafo semântico e relacional com rastreamento de linhagem, meia-vida de premissas (*Decision Half-Life*) e calibração empírica (`|E - R|`).
4. **Orchestration Layer / Dynamic Router:** Roteamento de custo/desempenho entre modelos com *Circuit Breaker* e *Fallback*.
5. **Intelligence Network:** Modelos e agentes especializados sob papéis funcionais.
6. **Execution Layer & Confirmation Gate:** Despacho via n8n, APIs e servidores MCP com trava humana obrigatória (`AUTORIZADO`) e TTL de 30 minutos para ações críticas.
7. **Feedback Loop & Decision Calibration:** Confronto de previsões com resultados reais.

## Governança Cognitiva (Otimizzai V2.2)

- **MG-RAW-01 (Ingestão Imutável):** Toda entrada bruta é preservada em tabela *append-only* (`raw_inputs`).
- **Filtro Anti-Contaminação:** Hipóteses e declarações sem evidência externa são limitadas automaticamente a confiança `<= 0.50`.
- **Protocolos R20–R24:** Releitura de fontes primárias (R20), rigor aritmético decimal `NUMERIC(12,2)` (R21), separação entre fato e projeção (R22), rastreabilidade por UUID (R23) e declaração explícita de incerteza (R24).
- **Isolamento Multi-Tenant:** Implementado nativamente via *Row-Level Security* (RLS) no PostgreSQL 16.

## Como Rodar Localmente

1. Copie o modelo de ambiente:

       cp .env.example .env

2. Suba o PostgreSQL 16 com `pgvector`:

       docker compose up -d

3. Instale as dependências e inicie a API:

       pip install -r requirements.txt
       uvicorn src.main:app --host 0.0.0.0 --port 8000
