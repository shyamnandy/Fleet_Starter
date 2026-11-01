"""API tests."""


def test_health_endpoint_responds(client):
    response = client.get("/health")
    assert response.status_code in (200, 503)
    assert response.json()["status"] in ("UP", "DEGRADED")


def test_valid_heartbeat_is_accepted(client, heartbeat, publisher):
    response = client.post("/v1/heartbeats", json=heartbeat)
    assert response.status_code == 202
    assert len(publisher.sent) == 1
    topic, key, event = publisher.sent[0]
    assert topic == "telemetry.raw"
    assert key == heartbeat["vin"]
    assert event["schema"] == "v1"
    assert event["odometer_km"] == heartbeat["odometer"]


def test_invalid_heartbeat_is_rejected(client, heartbeat, publisher):
    heartbeat["vin"] = "1HGCM82633A0O4352"
    heartbeat["speed_kmph"] = -1
    response = client.post("/v1/heartbeats", json=heartbeat)
    assert response.status_code == 400
    assert response.json()["error"] == "validation_failed"
    fields = {detail["field"] for detail in response.json()["details"]}
    assert {"vin", "speed_kmph"} <= fields
    assert publisher.sent == []


def test_miles_and_ist_are_normalized(client, heartbeat, publisher):
    heartbeat.update(odometer=10, odometer_unit="mi", ts="2026-10-28T15:30:00+05:30")
    response = client.post("/v1/heartbeats", json=heartbeat)
    assert response.status_code == 202
    event = publisher.sent[0][2]
    assert event["odometer_km"] == 16.09344
    assert event["ts_utc"] == "2026-10-28T10:00:00Z"