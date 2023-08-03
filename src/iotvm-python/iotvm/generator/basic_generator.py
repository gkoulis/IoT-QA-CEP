import random
from typing import Optional, Union, Tuple
import logging
import requests

from ._scheduler import IScheduler

_logger = logging.getLogger("iotvm.generator.basic_generator")

_API_BASE_URL: str = "http://localhost:9001"
_API_SENSOR_TELEMETRY_EVENT_URL: str = (
    f"{_API_BASE_URL}/application/api/v1/http-transport/sensor-telemetry-events"
)


def _get_value(
    value: Union[float, Tuple[float, float]], round_n_digits: int = 2
) -> float:
    if type(value) == float:
        return value

    if type(value) == tuple:
        base, pct = value
        if pct <= 0:
            return base
        change = abs(base) * pct
        minimum = base - change
        maximum = base + change
        return round(random.uniform(minimum, maximum), round_n_digits)

    raise Exception("Invalid value!")


def _push_sensor_telemetry_event_web_request(
    sensor_id: str, timestamp: Optional[int], temperature: float, humidity: float
) -> None:
    requests.post(
        _API_SENSOR_TELEMETRY_EVENT_URL,
        json={
            "sensorTelemetryEvent": {
                "measurements": [
                    {"name": "temperature", "value": temperature, "unit": "celsius"},
                    {"name": "humidity", "value": humidity, "unit": "percentage"},
                ],
                "sensorId": sensor_id,
                "timestamp": timestamp,
                # TODO additional data (number of window, client, etc...)
            }
        },
    )


def _push(sensor_id: str, temperature: Union[float, Tuple], humidity: float) -> None:
    temperature: float = _get_value(value=temperature, round_n_digits=2)
    humidity: float = _get_value(value=humidity, round_n_digits=2)
    return _push_sensor_telemetry_event_web_request(
        sensor_id=sensor_id,
        timestamp=None,
        temperature=temperature,
        humidity=humidity,
    )


def _w1_run1() -> None:
    _logger.debug("W1 run1")
    pct: float = 0.02
    _push("sensor-1", (20.0, pct), 20.0)
    _push("sensor-2", (22.0, pct), 22.0)
    _push("sensor-3", (20.0, pct), 20.0)
    _push("sensor-4", (22.0, pct), 22.0)
    _push("sensor-5", (20.0, pct), 20.0)
    _push("sensor-6", (22.0, pct), 22.0)


def _w1_run2() -> None:
    _logger.debug("W1 run2")
    pct: float = 0.02
    _push("sensor-1", (20.5, pct), 20.5)
    _push("sensor-2", (22.5, pct), 22.5)
    _push("sensor-3", (20.5, pct), 20.5)
    _push("sensor-4", (22.5, pct), 22.5)
    _push("sensor-5", (20.5, pct), 20.5)
    _push("sensor-6", (22.5, pct), 22.5)


def _w2_run1() -> None:
    _logger.debug("W2 run1")
    pct: float = 0.02
    _push("sensor-1", (22.0, pct), 22.0)
    _push("sensor-2", (24.0, pct), 24.0)
    _push("sensor-3", (22.0, pct), 22.0)
    _push("sensor-4", (24.0, pct), 24.0)


def _w3_run1() -> None:
    _logger.debug("W3 run1")
    pct: float = 0.02
    # TODO Add loggers.
    _push("sensor-1", (23.0, pct), 23.0)
    _push("sensor-2", (25.0, pct), 25.0)
    _push("sensor-3", (23.0, pct), 23.0)


def _w4_run1() -> None:
    _logger.debug("W4 run1")
    pct: float = 0.01
    _push("sensor-1", (22.0, pct), 22.0)
    _push("sensor-2", (24.0, pct), 24.0)
    _push("sensor-3", (22.0, pct), 22.0)


def _w5_run1() -> None:
    _logger.debug("W5 run1")
    pass


def _w6_run1() -> None:
    _logger.debug("W6 run1")
    pass


class _Scheduler(IScheduler):
    def __init__(self):
        super().__init__(name="_Scheduler", blocking=True)

    #
    # Interface Implementation
    #

    def _prepare(self):
        pass

    def _schedule(self):
        minute_0: str = ",".join(map(lambda i: str(i), range(0, 60, 3)))
        minute_1: str = ",".join(map(lambda i: str(i), range(1, 60, 3)))
        minute_2: str = ",".join(map(lambda i: str(i), range(2, 60, 3)))

        self._scheduler.add_job(
            func=_w1_run1,
            trigger="cron",
            minute=minute_0,
            second="15",
            misfire_grace_time=2,
            id="_w1_run1",
            name="_w1_run1",
        )
        self._scheduler.add_job(
            func=_w1_run2,
            trigger="cron",
            minute=minute_0,
            second="20",
            misfire_grace_time=2,
            id="_w1_run2",
            name="_w1_run2",
        )
        self._scheduler.add_job(
            func=_w2_run1,
            trigger="cron",
            minute=minute_0,
            second="45",
            misfire_grace_time=2,
            id="_w2_run1",
            name="_w2_run1",
        )

        self._scheduler.add_job(
            func=_w3_run1,
            trigger="cron",
            minute=minute_1,
            second="15",
            misfire_grace_time=2,
            id="_w3_run1",
            name="_w3_run1",
        )
        self._scheduler.add_job(
            func=_w4_run1,
            trigger="cron",
            minute=minute_1,
            second="45",
            misfire_grace_time=2,
            id="_w4_run1",
            name="_w4_run1",
        )

        self._scheduler.add_job(
            func=_w5_run1,
            trigger="cron",
            minute=minute_2,
            second="15",
            misfire_grace_time=2,
            id="_w5_run1",
            name="_w5_run1",
        )
        self._scheduler.add_job(
            func=_w6_run1,
            trigger="cron",
            minute=minute_2,
            second="45",
            misfire_grace_time=2,
            id="_w6_run1",
            name="_w6_run1",
        )


def invoke() -> None:
    scheduler: _Scheduler = _Scheduler()

    try:
        scheduler.invoke()
    except KeyboardInterrupt:
        scheduler.signal_graceful_shutdown()

    scheduler.ensure_graceful_shutdown(sleep=3)
