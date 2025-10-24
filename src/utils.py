from datetime import datetime

def log_event(message: str):
    """
    Menampilkan log dengan timestamp ke console.
    """
    print(f"[{datetime.now().isoformat()}] {message}")

def current_time():
    """
    Mengembalikan waktu ISO8601 untuk event.
    """
    return datetime.utcnow().isoformat()