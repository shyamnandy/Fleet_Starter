# Fleet Telemetry Platform — Bootcamp Starter Kit

Build a small version of a connected-car data platform in one day: vehicles send signals → your **Ingest API** → **Kafka** → your **consumer** → **Postgres** → your **query APIs** and **alerts**.

This submission uses the `python/` track (FastAPI, Python 3.10+).

```
 device simulator ──HTTP──▶ Ingest API ──▶ Kafka: telemetry.raw (12 partitions, key = VIN)
                                                   │
                                                   ▼
                    Query API ◀── Postgres ◀── Consumer (group telemetry-writer) ──▶ telemetry.dlq (bad messages)
                                                   │
                                                   └──▶ alerts table + Kafka: alerts.out
```

---

## 0. Before Day 1 (pre-work — do this at home, the images are ~1.5 GB)

1. Install **Docker Desktop**, **git**, **Python 3.10+** (needed for the simulator on both tracks; macOS ships 3.9 as `python3`, so install a newer one, e.g. `brew install python@3.12`).
   Java track: **JDK 17+** (21 LTS recommended) and **Maven** (or IntelliJ IDEA, which bundles Maven).
2. Clone this repo, then from the repo root:
   ```bash
   docker compose up -d          # Kafka, Kafka UI, Postgres (+ seed data)
   ./scripts/check_env.sh        # must end with ALL GOOD
   ```
3. Start your track's app once (see §2) and open http://localhost:8080/health → `{"status":"UP"...}`.
4. Open Kafka UI at http://localhost:8081 — you should see topics `telemetry.raw`, `telemetry.dlq`, `alerts.out`.
5. Send your trainer a screenshot of `ALL GOOD` + the health response.

Windows: run the scripts from Git Bash or WSL.

---

## 1. What's already done for you

| Given | Where |
|---|---|
| Kafka (KRaft), Kafka UI, Postgres 15 with schema + ~56k telemetry rows | `docker-compose.yml`, `db/init/` |
| 200 vehicles across 12 customers (customer 1 = big fleet) | `data/vehicles.csv` |
| Device simulator (no pip install needed) | `simulator/simulate.py` |
| Kafka helper commands (topics, consume, lag) | `scripts/kafka.sh` |
| App skeleton: config from env vars, DB pool, Kafka producer/consumer wiring, `/health`, one passing test, 400 error format | `python/` |

**Not done (that's your job):** validation, normalisation, publishing, the consumer's DB writes, idempotency, DLQ, query APIs, alerts, tests.

---

## 2. Run your app

**Python**
```bash
cd python
python3 -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8080               # terminal 1: API
python -m app.consumer                                  # terminal 2: consumer (from M2)
pytest -q                                               # tests
```

**Java**
```bash
cd java
mvn spring-boot:run          # API + consumer in one process on :8080
mvn test                     # tests
```

Config comes from environment variables: `DB_HOST DB_PORT DB_NAME DB_USER DB_PASSWORD KAFKA_BOOTSTRAP OVERSPEED_KMPH`. Defaults point at docker compose.

---

## 3. Milestones (Day 2)

Commit after every milestone: `git commit -m "M1: ingest API with validation"`.

### M1 — Ingest API · **no AI tools** · 60 min
`POST /v1/heartbeats`
```json
{"vin":"1FA6CVNCNCXW927TC","ts":"2026-10-28T10:00:00Z","speed_kmph":54.2,"odometer":10234.5,
 "odometer_unit":"km","lat":12.97,"lon":77.59,"heading":90,"soc_pct":null,"ignition":true,"source_oem":"OEM-A"}
```
- Validate: VIN = 17 chars A–Z/0–9 without I, O, Q · `ts` has a timezone and is not > 5 min in the future · speed 0–300 · lat/lon ranges · odometer ≥ 0 · unit `km`/`mi`.
- Invalid → **400** `{"error":"validation_failed","details":[{"field":"vin","message":"..."}]}` and nothing is published.
- Normalise → `TelemetryEvent` (schema `v1`, odometer in **km**, time in **UTC**; OEM-B devices send miles).
- Publish JSON to `telemetry.raw`, **keyed by VIN**. Return **202**.
- ≥ 2 tests.

✅ Check: `python3 simulator/simulate.py --mode malformed` → every line 400 · `python3 simulator/simulate.py --vehicles 5 --count 5` → all 202 · messages visible in Kafka UI.

### M2 — Consumer → Postgres · 75 min (AI allowed from here — you must be able to explain every line)
- Consume `telemetry.raw` (group `telemetry-writer`), write to `telemetry`.
- **Idempotent**: the same `(vin, ts_utc)` twice → one row (look at the table constraints).
- Unparseable / wrong-schema message → publish to `telemetry.dlq` and continue. One bad message must never block a partition.
- Commit the Kafka offset **after** the DB commit.

✅ Check: `python3 simulator/simulate.py --mode duplicate --vehicles 5 --count 10` → row count grows by 50, not 100.
`./scripts/kafka.sh lag` → LAG 0 on all partitions. Run `EXPLAIN` on a `(vin, ts_utc)` range query in psql: does it use the index?

### M3 — Query APIs + alerts · 45 min
| Endpoint | Notes |
|---|---|
| `GET /v1/vehicles/{vin}/latest` | 404 + error body if none |
| `GET /v1/vehicles/{vin}/telemetry?from=&to=&limit=&cursor=` | keyset pagination on `ts_utc` → `next_cursor` (no OFFSET) |
| `GET /v1/vehicles/{vin}/alerts` | newest first |
| `GET /v1/fleets/{customer_id}/latest` | latest position of every active vehicle — **one** SQL query |

**Overspeed alert:** speed > 100 km/h for 3 consecutive pings → exactly **one** `OVERSPEED` row in `alerts` per run (+ message on `alerts.out`). Write the rule as a pure function and unit-test: exactly 100, run of 2, run broken by a slow ping, run that keeps growing.

✅ Check: `python3 simulator/simulate.py --mode overspeed` → one alert.

**Rules for AI:** write 3 tests by hand first · review every AI diff · reject at least one AI suggestion and note why in your README.


Implementation note: I reviewed each AI-assisted diff. I rejected a suggestion to use `OFFSET` pagination because it becomes slower and less stable as telemetry grows; the Python API uses a timestamp keyset cursor instead.
### Pod extension (pick the one your mentor assigns)
1. Geofence enter/exit alerts · 2. EV charging-session detection from `soc_pct` (OEM-D) · 3. Odometer anomaly (decrease or > 2,000 km/day) + daily billing export · 4. Customer webhook notifications with retries, exponential backoff and an `event_id` for de-duplication.

### M4 - Ship it - 20 min

Build and run the Python API from the `python/` directory:

```bash
docker build -t fleet-telemetry-api ./python
docker run --rm --network host fleet-telemetry-api
```

The container listens on port `8080`. It receives `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `KAFKA_BOOTSTRAP`, `OVERSPEED_KMPH`, and `OVERSPEED_MIN_PINGS` through the environment. Run the Kafka consumer as a second container using the same image and `python -m app.consumer`.

#### HLD

```text
Device simulator
      |
      v
POST /v1/heartbeats -> FastAPI -> Kafka: telemetry.raw (VIN key)
                                      |
                                      v
                              Python consumer
                              |             |
                              v             v
                         PostgreSQL      alerts.out
                              ^             |
                              |             v
GET query APIs --------------+       alert consumers
```

Kafka provides ordered per-vehicle input and durable buffering. The consumer validates messages, writes telemetry idempotently, evaluates alerts, and commits offsets after the database transaction. Query APIs read PostgreSQL directly.

#### Schema

- `customers` owns fleets and customer status.
- `vehicles` maps VINs to customers and active/deprovisioned status.
- `telemetry` stores normalized UTC readings; `(vin, ts_utc)` is unique for idempotency and range-query indexing.
- `alerts` stores alert type, VIN, run start, JSON details, and a unique `(vin, type, started_at)` key for alert deduplication.
- `devices`, `subscriptions`, and `audit_log` support device ownership, plans, and operational history.

#### Trade-offs

1. PostgreSQL is the system of record because it keeps the training setup simple and supports indexed time-range queries; a production system may move old telemetry to partitioned or object storage.
2. Kafka delivery is at-least-once with database idempotency instead of distributed transactions; this keeps the consumer understandable while safely handling redelivery.
3. Timestamp cursors use keyset pagination instead of `OFFSET`; pages remain stable and efficient as telemetry grows, but clients must retain the cursor.

#### What broke and how it was fixed

- Duplicate Kafka deliveries could create duplicate telemetry rows; `ON CONFLICT (vin, ts_utc) DO NOTHING` makes the insert idempotent.
- A malformed event could stop a partition; parse and schema failures now go to `telemetry.dlq`, then the offset is committed so later messages continue.
- A growing overspeed run could create multiple alerts if only the last three rows were inspected; the rule now scans the full newest contiguous run and the alert unique key makes retries safe.

#### At 10x load

Partition telemetry by time and VIN, add read replicas for query traffic, and use a bounded connection pool. Scale consumers horizontally across Kafka partitions, monitor consumer lag and database saturation, batch inserts where latency permits, and move historical telemetry to cheaper storage with a retention policy.

---

## Day 3 — SQL and data-fix practice

On the **morning of Day 3**, before the first session, run from the repo root (with `docker compose up -d` running):
```bash
./scripts/day3_setup.sh
```
It keeps your Day 2 data and adds what Day 3 needs: recent readings for the time-based questions, a few planted data problems, and your tickets. Running it again is safe (it says "already applied" and changes nothing).

- **Tickets** are in the `ops_tickets` table: `docker compose exec postgres psql -U fleet -d fleet -c 'select id, title, body from ops_tickets order by id'`
- **Files** are in `data/day3/`: `transfer_cityride_to_autofin.csv` (ticket FIX-3) and `app.log` (log task: count errors per service in the hour ending at the last line, `2026-10-27T10:00:00Z`).
- Follow the safe steps for every fix: SELECT and count first, back up, change inside a transaction, check, then commit.

`./scripts/day3_setup.sh --reset` rebuilds the database from scratch and then applies the Day 3 setup. **It deletes your Day 2 data**, so use it only if your database is broken.

---

## 4. Useful commands
```bash
./scripts/kafka.sh consume telemetry.raw     # see messages + partition + key
./scripts/kafka.sh consume telemetry.dlq
./scripts/kafka.sh lag                       # consumer lag per partition
docker compose exec postgres psql -U fleet -d fleet
docker compose down -v && docker compose up -d   # full reset (re-seeds the DB)
```

## 5. Questions you should be able to answer
Why key by VIN? What if one huge fleet's vehicles all hash to one partition? At-least-once vs exactly-once — which do you have, and why is it OK? How do you notice consumer lag? What happens with out-of-order timestamps? How would you store a year of data (hot vs cold)? How do you scale the consumer? What would you monitor and alert on?

> This is a **training project**. On your resume, describe it honestly (e.g. "Fleet Telemetry Platform — bootcamp project; built X, owned Y").
