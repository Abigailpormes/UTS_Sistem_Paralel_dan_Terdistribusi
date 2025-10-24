import os
import time
import asyncio
import logging
import json
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from src.models import Event
from src.store import DedupStore
from src.consumer import Consumer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")


def create_app(db_path: str = None):
    """
    Fungsi untuk membuat instance FastAPI dengan konfigurasi database dan consumer.
    """
    # Gunakan environment variable jika ada, default ke ./data/dedup.db
    db_path = db_path or os.environ.get("DEDUP_DB", "./data/dedup.db")
    dedup = DedupStore(db_path)

    # Antrian dan statistik event
    q: asyncio.Queue = asyncio.Queue()
    stats = {
        "received": 0,
        "unique_processed": 0,
        "duplicate_dropped": 0,
        "topics": set(),
        "start_time": time.time(),
    }

    # Inisialisasi consumer
    consumer = Consumer(q, dedup, stats)

    # Lifespan handler: untuk startup & shutdown
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        def run_consumer_in_thread():
            asyncio.run(consumer.start())

        # Jalankan consumer di background thread
        thread = threading.Thread(target=run_consumer_in_thread, daemon=True)
        thread.start()
        logger.info("✅ Background consumer thread started")

        yield  # Aplikasi berjalan di antara sini

        # Shutdown handler
        await consumer.stop()
        dedup.close()
        logger.info("🔻 Consumer stopped and DB closed")

    # Buat instance FastAPI
    app = FastAPI(
        title="UTS Pub-Sub Aggregator",
        description="Sistem Publish-Subscribe untuk pengelolaan event dengan deduplikasi.",
        version="1.0.0",
        lifespan=lifespan,
    )

    # --- ROUTES ---

    @app.get("/")
    async def root():
        """
        Endpoint root — hanya untuk memastikan server aktif.
        """
        return {
            "message": "✅ UTS Pub-Sub Aggregator API is running!",
            "docs_url": "/docs",
            "available_endpoints": ["/publish", "/events", "/stats"],
        }

    @app.post("/publish")
    async def publish(request: Request):
        """
        Endpoint untuk menerima event baru dan memasukkannya ke queue.
        """
        body = await request.json()
        events = body if isinstance(body, list) else [body]
        validated = []

        for e in events:
            try:
                ev = Event(**e)
                validated.append(ev.model_dump())
            except Exception as ex:
                raise HTTPException(status_code=422, detail=f"Invalid event schema: {ex}")

        for ev in validated:
            stats["received"] += 1
            stats["topics"].add(ev["topic"])
            await q.put(ev)

        return JSONResponse({"accepted": len(validated)})

    @app.get("/events")
    async def get_events(topic: str = None):
        """
        Endpoint untuk menampilkan event yang tersimpan dalam database.
        Bisa difilter berdasarkan 'topic'.
        """
        rows = dedup.list_events(topic)
        result = []
        for r in rows:
            result.append(
                {
                    "topic": r[0],
                    "event_id": r[1],
                    "timestamp": r[2],
                    "source": r[3],
                    "payload": json.loads(r[4] or "{}"),
                }
            )
        return result

    @app.get("/stats")
    async def get_stats():
        """
        Endpoint untuk menampilkan statistik pemrosesan event.
        """
        return {
            "received": stats["received"],
            "unique_processed": stats["unique_processed"],
            "duplicate_dropped": stats["duplicate_dropped"],
            "topics": list(stats["topics"]),
            "uptime_seconds": int(time.time() - stats["start_time"]),
        }

    return app


# Global instance untuk uvicorn
app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.main:app", host="0.0.0.0", port=8080, reload=True)
