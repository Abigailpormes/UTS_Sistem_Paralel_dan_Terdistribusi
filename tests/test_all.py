# tests/test_all.py
from fastapi.testclient import TestClient
from src.main import app
from src.config import DB_PATH
from src.store import init_db, insert_event, get_all_events
import os
import time
import json

client = TestClient(app)


# --- Test 1: Publish & Get basic event ---
def test_publish_and_get():
    event = {
        "topic": "test_topic",
        "event_id": "1",
        "source": "pytest",
        "payload": {"msg": "Hello"},
    }
    res = client.post("/publish", json=event)
    assert res.status_code == 200

    stats = client.get("/stats").json()
    assert "received" in stats


# --- Test 2: Deduplication handling ---
def test_dedup():
    event = {"topic": "dup_test", "event_id": "xyz", "source": "pytest", "payload": {}}
    client.post("/publish", json=event)
    client.post("/publish", json=event)
    stats = client.get("/stats").json()
    assert "duplicate_dropped" in stats


# --- Test 3: Database file persistence ---
def test_db_persistence():
    init_db()
    before = os.path.exists(DB_PATH)
    insert_event({
        "topic": "persist",
        "event_id": "10",
        "timestamp": "t",
        "source": "x",
        "payload": "{}",
    })
    after = os.path.exists(DB_PATH)
    assert before and after


# --- Test 4: Retrieve stored events ---
def test_get_events_endpoint():
    res = client.get("/events")
    assert res.status_code == 200
    assert isinstance(res.json(), list)


# --- Test 5: Publish multiple events ---
def test_publish_multiple():
    events = [
        {"topic": "multi", "event_id": str(i), "source": "pytest"} for i in range(3)
    ]
    res = client.post("/publish", json=events)
    assert res.status_code == 200
    data = res.json()
    assert data["accepted"] == 3


# --- Test 6: Invalid schema handling ---
def test_invalid_event_schema():
    bad_event = {"topic": "bad"}  # missing required fields
    res = client.post("/publish", json=bad_event)
    assert res.status_code == 422


# --- Test 7: Stats uptime increasing ---
def test_stats_uptime():
    first = client.get("/stats").json()["uptime_seconds"]
    time.sleep(1)
    second = client.get("/stats").json()["uptime_seconds"]
    assert second >= first


# --- Test 8: Event topics are tracked correctly ---
def test_topics_tracking():
    event = {"topic": "topic_test", "event_id": "t1", "source": "pytest", "payload": {}}
    client.post("/publish", json=event)
    stats = client.get("/stats").json()
    assert "topic_test" in stats["topics"]


# --- Test 9: Get events by topic filter ---
def test_get_events_by_topic():
    topic = "filter_test"
    insert_event({
        "topic": topic,
        "event_id": "abc123",
        "timestamp": "now",
        "source": "pytest",
        "payload": {"k": 1},
    })
    res = client.get(f"/events?topic={topic}")
    data = res.json()
    assert all(ev["topic"] == topic for ev in data)
