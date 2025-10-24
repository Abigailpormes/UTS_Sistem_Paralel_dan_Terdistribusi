import sqlite3
import os
import json
from src.config import DB_PATH

# Pastikan folder database tersedia
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


class DedupStore:
    """Kelas penyimpanan untuk memastikan event tidak duplikat."""

    def __init__(self, db_path=DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                topic TEXT,
                event_id TEXT,
                timestamp TEXT,
                source TEXT,
                payload TEXT,
                PRIMARY KEY (topic, event_id)
            )
        """)
        self.conn.commit()

    def insert(self, event):
        """Masukkan event baru, return True jika unik, False jika duplikat."""
        try:
            self.conn.execute(
                "INSERT INTO events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                (
                    event["topic"],
                    event["event_id"],
                    event.get("timestamp", ""),
                    event.get("source", ""),
                    json.dumps(event.get("payload", {}))
                )
            )
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False  # Event duplikat
        except Exception as e:
            print(f"❌ Insert error: {e}")
            return False

    def list_events(self, topic=None):
        """Ambil semua event (bisa difilter per topik)."""
        cursor = self.conn.cursor()
        if topic:
            cursor.execute("SELECT * FROM events WHERE topic=?", (topic,))
        else:
            cursor.execute("SELECT * FROM events")
        return cursor.fetchall()

    def close(self):
        """Tutup koneksi database."""
        self.conn.close()


# -------------------------------------------------------------------
# ✅ Fungsi pembungkus agar kompatibel dengan test_all.py
# -------------------------------------------------------------------

def init_db():
    """Inisialisasi database."""
    DedupStore(DB_PATH)  # Membuat tabel kalau belum ada


def insert_event(event):
    """Masukkan event ke database."""
    store = DedupStore(DB_PATH)
    store.insert(event)
    store.close()


def get_all_events():
    """Ambil semua event dari database."""
    store = DedupStore(DB_PATH)
    data = store.list_events()
    store.close()
    return data
