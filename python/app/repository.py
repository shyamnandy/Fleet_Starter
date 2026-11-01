"""Database queries for the telemetry application."""

from psycopg.rows import dict_row

from app.db import get_pool


def vehicle_exists(vin: str) -> bool:
    with get_pool().connection() as conn:
        return conn.execute("SELECT 1 FROM vehicles WHERE vin = %s", (vin,)).fetchone() is not None


def insert_telemetry(conn, event) -> bool:
    """Insert one event idempotently inside the caller's transaction."""
    result = conn.execute(
        """
        INSERT INTO telemetry (vin, ts_utc, speed_kmph, odometer_km, lat, lon,
                               heading, soc_pct, ignition, source_oem)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (vin, ts_utc) DO NOTHING
        """,
        (event.vin, event.ts_utc, event.speed_kmph, event.odometer_km, event.lat,
         event.lon, event.heading, event.soc_pct, event.ignition, event.source_oem),
    )
    return result.rowcount == 1


TELEMETRY_COLUMNS = "vin, ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem"


def latest_for_vin(vin):
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            return cursor.execute(
                f"SELECT {TELEMETRY_COLUMNS} FROM telemetry WHERE vin = %s ORDER BY ts_utc DESC LIMIT 1",
                (vin,),
            ).fetchone()


def telemetry_range(vin, from_ts=None, to_ts=None, limit=100, cursor=None):
    clauses, params = ["vin = %s"], [vin]
    if from_ts is not None:
        clauses.append("ts_utc >= %s"); params.append(from_ts)
    if to_ts is not None:
        clauses.append("ts_utc <= %s"); params.append(to_ts)
    if cursor is not None:
        clauses.append("ts_utc < %s"); params.append(cursor)
    params.append(limit + 1)
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            rows = cursor.execute(
                f"SELECT {TELEMETRY_COLUMNS} FROM telemetry WHERE {' AND '.join(clauses)} ORDER BY ts_utc DESC LIMIT %s",
                params,
            ).fetchall()
    return rows[:limit], (rows[limit]["ts_utc"] if len(rows) > limit else None)


def alerts_for_vin(vin):
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            return cursor.execute(
                "SELECT id, vin, type, started_at, details, created_at FROM alerts WHERE vin = %s ORDER BY started_at DESC, id DESC",
                (vin,),
            ).fetchall()


def fleet_latest(customer_id):
    """Fetch active vehicles and their latest telemetry in one SQL query."""
    with get_pool().connection() as conn:
        with conn.cursor(row_factory=dict_row) as cursor:
            return cursor.execute(
                """
                SELECT v.vin, v.customer_id, t.ts_utc, t.speed_kmph, t.odometer_km,
                       t.lat, t.lon, t.heading, t.soc_pct, t.ignition, t.source_oem
                FROM vehicles v
                LEFT JOIN LATERAL (
                    SELECT ts_utc, speed_kmph, odometer_km, lat, lon, heading, soc_pct, ignition, source_oem
                    FROM telemetry t WHERE t.vin = v.vin ORDER BY t.ts_utc DESC LIMIT 1
                ) t ON true
                WHERE v.customer_id = %s AND v.status = 'active'
                ORDER BY v.vin
                """, (customer_id,),
            ).fetchall()


def recent_telemetry(conn, vin):
    with conn.cursor(row_factory=dict_row) as cursor:
        return cursor.execute(
            f"SELECT {TELEMETRY_COLUMNS} FROM telemetry WHERE vin = %s ORDER BY ts_utc DESC",
            (vin,),
        ).fetchall()


def insert_overspeed_alert(conn, vin, started_at, limit_kmph):
    with conn.cursor(row_factory=dict_row) as cursor:
        return cursor.execute(
            """
            INSERT INTO alerts (vin, type, started_at, details)
            VALUES (%s, 'OVERSPEED', %s, jsonb_build_object('limit_kmph', %s))
            ON CONFLICT (vin, type, started_at) DO NOTHING
            RETURNING id, vin, type, started_at, details, created_at
            """, (vin, started_at, limit_kmph),
        ).fetchone()
