# Pub-Sub Log Aggregator

## Deskripsi
Aplikasi sistem terdistribusi berbasis **Publish-Subscribe** untuk mengumpulkan log/event dengan **idempotent consumer** dan **deduplication**.

## Cara Menjalankan

### Jalankan Lokal
```bash
uvicorn src.main:app --reload --port 8080
