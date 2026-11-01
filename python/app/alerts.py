"""Pure alert rules for telemetry rows."""


def _value(row, name):
    return row[name] if isinstance(row, dict) else getattr(row, name)


def overspeed_run_start(rows_newest_first, limit_kmph, min_pings):
    """Return the oldest timestamp in the current overspeed run, if complete."""
    if min_pings <= 0:
        raise ValueError("min_pings must be positive")
    run = []
    for row in rows_newest_first:
        if _value(row, "speed_kmph") <= limit_kmph:
            break
        run.append(row)
    if len(run) < min_pings:
        return None
    return _value(run[min_pings - 1], "ts_utc")
