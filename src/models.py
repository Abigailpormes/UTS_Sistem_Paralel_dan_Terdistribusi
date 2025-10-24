# src/models.py
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any

class Event(BaseModel):
    topic: str = Field(...)
    event_id: str = Field(...)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = Field(default="unknown")
    payload: Dict[str, Any] = Field(default={})

    # ✅ Pindahkan example ke json_schema_extra
    model_config = {
        "json_schema_extra": {
            "example": {
                "topic": "system",
                "event_id": "abc123",
                "timestamp": "2025-10-23T10:00:00Z",
                "source": "pytest",
                "payload": {"msg": "Hello World"}
            }
        }
    }
