"""Convert heartbeats to metric units and UTC."""
import datetime as dt

from app.models import HeartbeatRequest, TelemetryEvent

KM_PER_MILE = 1.609344


def normalize(hb: HeartbeatRequest) -> TelemetryEvent:
    odometer_km = hb.odometer * KM_PER_MILE if hb.odometer_unit == "mi" else hb.odometer
    return TelemetryEvent(
        vin=hb.vin,
        ts_utc=hb.ts.astimezone(dt.timezone.utc),
        speed_kmph=hb.speed_kmph,
        odometer_km=odometer_km,
        lat=hb.lat,
        lon=hb.lon,
        heading=hb.heading,
        soc_pct=hb.soc_pct,
        ignition=hb.ignition,
        source_oem=hb.source_oem,
    )