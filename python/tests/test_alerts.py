import datetime as dt

from app.alerts import overspeed_run_start


def rows(speeds):
    newest = dt.datetime(2026, 10, 28, 10, 0, tzinfo=dt.timezone.utc)
    return [{"speed_kmph": speed, "ts_utc": newest - dt.timedelta(seconds=index * 5)} for index, speed in enumerate(speeds)]


def test_overspeed_exactly_at_limit_is_not_a_run():
    assert overspeed_run_start(rows([100, 100, 100]), 100, 3) is None


def test_overspeed_run_of_two_is_not_an_alert():
    assert overspeed_run_start(rows([105, 110]), 100, 3) is None


def test_overspeed_slow_ping_breaks_the_run():
    assert overspeed_run_start(rows([105, 110, 95, 120]), 100, 3) is None


def test_overspeed_growing_run_keeps_the_original_start():
    assert overspeed_run_start(rows([125, 120, 110, 105]), 100, 3) == rows([125, 120, 110])[2]["ts_utc"]
