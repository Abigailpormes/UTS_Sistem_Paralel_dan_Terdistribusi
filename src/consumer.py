import asyncio
import logging

logger = logging.getLogger("consumer")


class Consumer:
    """
    Kelas Consumer untuk memproses event dari queue secara asynchronous.
    Menggunakan DedupStore untuk deduplikasi sebelum menyimpan event ke DB.
    """

    def __init__(self, q: asyncio.Queue, dedup, stats: dict):
        self.q = q
        self.dedup = dedup
        self.stats = stats
        self.running = True

    async def start(self):
        """Loop utama untuk memproses event dari queue."""
        logger.info("🌀 Consumer started and waiting for events...")
        while self.running:
            try:
                event = await self.q.get()
                saved = self.dedup.insert(event)

                if saved:
                    self.stats["unique_processed"] += 1
                    logger.info("[✅ PROCESSED] %s:%s", event["topic"], event["event_id"])
                else:
                    self.stats["duplicate_dropped"] += 1
                    logger.info("[⚠️ DUPLICATE] %s:%s", event["topic"], event["event_id"])

            except Exception as e:
                logger.error("❌ Error processing event: %s", e)
            finally:
                await asyncio.sleep(0.01)  # Hindari blocking total

    async def stop(self):
        """Hentikan loop consumer."""
        logger.info("🛑 Stopping consumer...")
        self.running = False
