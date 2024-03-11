import random
from typing import Any, Dict, List

from ._data_types import Measurement, EvaluatedSimulatedSensorOperation


def _measurement_value__rand() -> float:
    return random.random()


def _measurement_value__noise(base: float, pct: float = 0.01, round_n_digits: int = 2) -> float:
    assert type(base) == float
    assert type(pct) == float
    assert type(round_n_digits) == int
    if pct <= 0:
        return base
    change = abs(base) * pct
    minimum = base - change
    maximum = base + change
    return round(random.uniform(minimum, maximum), round_n_digits)


def _measurement_value__between(minimum: float, maximum: float, round_n_digits: int = 2) -> float:
    assert type(minimum) == float
    assert type(maximum) == float
    assert type(round_n_digits) == int
    return round(random.uniform(minimum, maximum), round_n_digits)


def _measurement_value__exact(value: float) -> float:
    assert type(value) == float
    return value


def _physical_quantity__humidity(value: float, unit: str = "PERCENTAGE") -> Measurement:
    assert type(value) == float
    assert type(unit) == str
    return Measurement(name="humidity", value=value, unit=unit)


def _physical_quantity__temperature(value: float, unit: str = "CELSIUS") -> Measurement:
    assert type(value) == float
    assert type(unit) == str
    return Measurement(name="temperature", value=value, unit=unit)


# noinspection PyPep8Naming
def _args_to_SimulatedSensorOperation(*args, operation: str, fail: bool) -> EvaluatedSimulatedSensorOperation:
    assert len(args) > 0
    measurements: List[Measurement] = []
    for measurement in args:
        assert type(measurement) == Measurement
        measurements.append(measurement)
    # sensor_id, timestamp, additional SHOULD BE CHANGED later.
    # noinspection PyTypeChecker
    return EvaluatedSimulatedSensorOperation(
        operation=operation,
        fail=fail,
        sensor_id="<undefined>",
        measurements=measurements,
        timestamp=0,
        additional={},
    )


def _operation__fail_push(*args) -> EvaluatedSimulatedSensorOperation:
    return _args_to_SimulatedSensorOperation(*args, operation="push", fail=True)


def _operation__push(*args) -> EvaluatedSimulatedSensorOperation:
    return _args_to_SimulatedSensorOperation(*args, operation="push", fail=False)


_MACRO_GLOBALS: Dict[str, callable] = {
    "__builtins__": None,
}
_MACRO_LOCALS: Dict[str, callable] = {
    # Measurement Value.
    "rand": _measurement_value__rand,
    "noise": _measurement_value__noise,
    "between": _measurement_value__between,
    "exact": _measurement_value__exact,
    # Physical Quantity.
    "temperature": _physical_quantity__temperature,
    "T": _physical_quantity__temperature,
    "humidity": _physical_quantity__humidity,
    "H": _physical_quantity__humidity,
    # Operations.
    "fail_push": _operation__fail_push,
    "push": _operation__push,
}


def evaluate_macro(
    string: str, sensor_id: str, timestamp: int, additional: Dict[str, Any]
) -> EvaluatedSimulatedSensorOperation:
    instance: EvaluatedSimulatedSensorOperation = eval(string, _MACRO_GLOBALS, _MACRO_LOCALS)
    instance.sensor_id = sensor_id
    instance.timestamp = timestamp
    instance.additional = additional
    return instance
