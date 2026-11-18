from fastapi import FastAPI
from pydantic import BaseModel
import uuid

app = FastAPI(title="Mind Gateway - Intent Engine API")

class IngestPayload(BaseModel):
    tenant_id: str
    source: str
    raw_content: str

@app.post("/api/v1/ingest", status_code=201)
def ingest_raw_input(payload: IngestPayload):
    # Simulação do Filtro Anti-Contaminação (Otimizzai V2.2)
    uncertain_terms = ["acho que", "talvez", "hipótese", "estimativa"]
    category = "HYPOTHESIS" if any(term in payload.raw_content.lower() for term in uncertain_terms) else "FACT"
    confidence = 0.50 if category == "HYPOTHESIS" else 1.00

    return {
        "status": "INGESTED",
        "raw_input_id": str(uuid.uuid4()),
        "processed_context": {
            "category": category,
            "confidence_score": confidence,
            "content": payload.raw_content
        }
    }
