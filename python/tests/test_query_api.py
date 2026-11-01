import base64
import datetime as dt


def test_latest_returns_vehicle_row(client, monkeypatch):
    row = {"vin": "1FAXXK7Y3R5A2B4C6", "ts_utc": dt.datetime(2026, 10, 28, tzinfo=dt.timezone.utc)}
    monkeypatch.setattr("app.main.repository.latest_for_vin", lambda vin: row)

    response = client.get(f"/v1/vehicles/{row['vin']}/latest")

    assert response.status_code == 200
    assert response.json()["vin"] == row["vin"]


def test_latest_returns_not_found(client, monkeypatch):
    monkeypatch.setattr("app.main.repository.latest_for_vin", lambda vin: None)

    response = client.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/latest")

    assert response.status_code == 404
    assert response.json()["error"] == "not_found"


def test_telemetry_uses_timestamp_cursor(client, monkeypatch):
    first_timestamp = dt.datetime(2026, 10, 28, 10, 0, tzinfo=dt.timezone.utc)
    second_timestamp = dt.datetime(2026, 10, 28, 9, 59, tzinfo=dt.timezone.utc)
    calls = []

    def telemetry_range(vin, from_ts, to_ts, limit, cursor):
        calls.append((vin, from_ts, to_ts, limit, cursor))
        return [{"vin": vin, "ts_utc": first_timestamp}], second_timestamp

    monkeypatch.setattr("app.main.repository.telemetry_range", telemetry_range)

    response = client.get(
        "/v1/vehicles/1FAXXK7Y3R5A2B4C6/telemetry?from=2026-10-28T00:00:00Z&limit=1"
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["vin"] == "1FAXXK7Y3R5A2B4C6"
    assert response.json()["next_cursor"]
    assert calls == [("1FAXXK7Y3R5A2B4C6", dt.datetime(2026, 10, 28, tzinfo=dt.timezone.utc), None, 1, None)]

    cursor = response.json()["next_cursor"]
    second_response = client.get(f"/v1/vehicles/1FAXXK7Y3R5A2B4C6/telemetry?cursor={cursor}")

    assert second_response.status_code == 200
    assert calls[-1][-1] == second_timestamp


def test_invalid_cursor_is_rejected(client, monkeypatch):
    monkeypatch.setattr("app.main.repository.telemetry_range", lambda *args: (_ for _ in ()).throw(AssertionError("query should not run")))

    response = client.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/telemetry?cursor=not-a-cursor")

    assert response.status_code == 400
    assert response.json()["error"] == "invalid_cursor"


def test_alerts_and_fleet_latest_return_items(client, monkeypatch):
    monkeypatch.setattr("app.main.repository.alerts_for_vin", lambda vin: [{"vin": vin, "type": "OVERSPEED"}])
    monkeypatch.setattr("app.main.repository.fleet_latest", lambda customer_id: [{"vin": "1FAXXK7Y3R5A2B4C6"}])

    alerts = client.get("/v1/vehicles/1FAXXK7Y3R5A2B4C6/alerts")
    fleet = client.get("/v1/fleets/1/latest")

    assert alerts.status_code == 200
    assert alerts.json()["items"][0]["type"] == "OVERSPEED"
    assert fleet.status_code == 200
    assert fleet.json()["items"][0]["vin"] == "1FAXXK7Y3R5A2B4C6"
