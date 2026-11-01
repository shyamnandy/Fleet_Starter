"""FastAPI app. Run:  uvicorn app.main:app --reload --port 8080   (from the python/ folder)

Milestones (see README.md):
  M1  POST /v1/heartbeats          validate -> normalise -> publish to Kafka (NO AI tools for M1)
  M3  GET  /v1/vehicles/{vin}/latest
      GET  /v1/vehicles/{vin}/telemetry?from=&to=&limit=&cursor=
      GET  /v1/vehicles/{vin}/alerts
      GET  /v1/fleets/{customer_id}/latest
"""
import base64
import datetime as dt
import logging

from fastapi import Depends, FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app import config
from app.db import db_is_up
from app.kafka_client import EventPublisher, get_publisher
from app.models import HeartbeatRequest
from app.normalize import normalize
from app import repository

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("api")

app = FastAPI(title="Fleet Telemetry API", version="0.1")


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError):
    """Turns FastAPI's 422 into the 400 error body our API contract uses."""
    details = [{"field": ".".join(str(p) for p in e["loc"][1:]) or "body", "message": e["msg"]} for e in exc.errors()]
    return JSONResponse(status_code=400, content={"error": "validation_failed", "details": details})


@app.get("/health")
def health():
    db = db_is_up()
    return JSONResponse(status_code=200 if db else 503,
                        content={"status": "UP" if db else "DEGRADED", "db": "up" if db else "down",
                                 "kafka_bootstrap": config.KAFKA_BOOTSTRAP})


# ---------------- M1: ingest (no AI tools) ----------------
@app.post("/v1/heartbeats", status_code=202)
def ingest_heartbeat(
    hb: HeartbeatRequest,
    publisher: EventPublisher = Depends(get_publisher),
):
    event = normalize(hb)
    publisher.publish(
        topic=config.TOPIC_RAW,
        key=event.vin,
        value=event.to_message(),
    )
    return {
        "status": "accepted",
        "vin": event.vin,
        "ts_utc": event.ts_utc.isoformat().replace("+00:00", "Z"),
    }


# ---------------- M3: query APIs (AI-assisted allowed)
def _encode_cursor(value):
    return base64.urlsafe_b64encode(value.isoformat().encode()).decode().rstrip("=")


def _decode_cursor(value):
    try:
        padded = value + "=" * (-len(value) % 4)
        parsed = dt.datetime.fromisoformat(base64.urlsafe_b64decode(padded).decode())
        if parsed.tzinfo is None:
            raise ValueError
        return parsed
    except (ValueError, UnicodeDecodeError, base64.binascii.Error) as error:
        raise ValueError("cursor must be a base64-encoded timestamp") from error


def _not_found(message):
    return JSONResponse(status_code=404, content={"error": "not_found", "message": message})


@app.get("/v1/vehicles/{vin}/latest")
def get_latest(vin: str):
    row = repository.latest_for_vin(vin)
    return row or _not_found(f"no telemetry found for vehicle {vin}")


@app.get("/v1/vehicles/{vin}/telemetry")
def get_telemetry(vin: str, from_: dt.datetime | None = Query(default=None, alias="from"),
                  to: dt.datetime | None = None, limit: int = Query(default=100, ge=1, le=500),
                  cursor: str | None = None):
    try:
        cursor_ts = _decode_cursor(cursor) if cursor else None
    except ValueError as error:
        return JSONResponse(status_code=400, content={"error": "invalid_cursor", "message": str(error)})
    rows, next_cursor = repository.telemetry_range(vin, from_, to, limit, cursor_ts)
    return {"items": rows, "next_cursor": _encode_cursor(next_cursor) if next_cursor else None}


@app.get("/v1/vehicles/{vin}/alerts")
def get_alerts(vin: str):
    return {"items": repository.alerts_for_vin(vin)}


@app.get("/v1/fleets/{customer_id}/latest")
def get_fleet_latest(customer_id: int):
    return {"items": repository.fleet_latest(customer_id)}
