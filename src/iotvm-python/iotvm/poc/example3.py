"""
Author: Dimitris Gkoulis
Created at: Wednesday 7 June 2023
Modified at: Tuesday 27 June 2023
"""
import copy
import datetime
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

# noinspection PyUnresolvedReferences
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from sklearn.impute import SimpleImputer
from statsmodels.tsa.arima.model import ARIMA

logger = logging.getLogger("root")


# ####################################################################################################
# Constants
# ####################################################################################################


TIMESTAMP_FORMAT: str = "%Y-%m-%d %H:%M:%S"
TIMESTAMP_FORMAT_FOR_FILES: str = "%Y-%m-%d-%H-%M-%S"
DEFAULT_AIR_TEMPERATURE_UNIT: str = "celsius"


# ####################################################################################################
# Models
# ####################################################################################################


@dataclass
class SimpleMeasurement:
    """
    Simple Measurement.

    ...

    Attributes
    ----------

    name : str
        The measurement name.
    value : float
        The measurement value.
    unit : str
        The measurement unit.
    """

    name: str
    value: float
    unit: str

    def __str__(self):
        return f"Measurement: {self.name} = {self.value} {self.unit}"

    def __copy__(self):
        return SimpleMeasurement(name=self.name, value=self.value, unit=self.unit)


@dataclass
class SensorSpecs:
    """
    Attributes
    ----------

    accuracy : float
        0 to 1 float that indicates the accuracy as defined by factory specifications.
    """

    accuracy: float

    def __copy__(self):
        return SensorSpecs(accuracy=self.accuracy)


@dataclass
class Sensor:
    """
    Attributes
    ----------

    sensor_id : str
        Unique identifier of Sensor.
    location_id : str
        Identifier of the Sensor Location.
    sensor_specs : SensorSpecs
        Sensor specifications.
    """

    sensor_id: str
    location_id: str
    sensor_specs: SensorSpecs

    def __str__(self):
        return f"Sensor {self.sensor_id} at {self.location_id}"

    def __copy__(self):
        return Sensor(
            sensor_id=self.sensor_id,
            location_id=self.location_id,
            sensor_specs=self.sensor_specs.__copy__(),
        )


@dataclass
class SensorTelemetryMeasurementEvent:
    """
    SensorTelemetryMeasurementEvent.

    TODO Future Implementations:
    - Origin
    - Metadata
    - Multiple timestamps (and default)

    ...

    Attributes
    ----------
    sensor : Sensor
    measurement : SimpleMeasurement
    timestamp : datetime.datetime
    origin_type : str
    """

    sensor: Sensor
    measurement: SimpleMeasurement
    timestamp: datetime.datetime
    origin_type: str
    metadata: Dict = field(default_factory=lambda: {})

    def __str__(self):
        _timestamp: str = self.timestamp.strftime(TIMESTAMP_FORMAT)
        return f"SensorTelemetryMeasurementEvent {self.sensor.sensor_id} : {self.measurement.name} = {self.measurement.value} @ {_timestamp}"

    def __copy__(self):
        return SensorTelemetryMeasurementEvent(
            sensor=self.sensor.__copy__(),
            measurement=self.measurement.__copy__(),
            timestamp=self.timestamp,
            origin_type=self.origin_type,
            metadata=copy.deepcopy(self.metadata),
        )

    def __post_init__(self):
        assert self.origin_type in [
            "real",
            "past",
            "forecasted",
            "imputed",
        ], f"origin_type {self.origin_type} is not one of [real, past, forecasted, imputed]"


@dataclass
class WindowedAverageMeasurementValueEvent:
    """
    WindowedAverageMeasurementValueEvent

    TODO Future Implementations:
    - Quality Properties
    - Quality Traits or just Traits
    - Metadata
    - First timestamp
    - Last timestamp
    - Ground Truth
    - Commenting and identification.
    - Comparison...

    Attributes
    ----------
    key : str
    from_timestamp : datetime.datetime
    to_timestamp : datetime.datetime
    measurement : SimpleMeasurement
    reference : SimpleMeasurement or None
        Reference value aka ground truth.
        Used to calculate DQ accuracy metric.
    events : list
    events_by_sensor_id : dict
    number_of_sensors : int
        Number of Sensors installed in the physical environment.
    minimum_number_of_sensors : int
        The minimum number of sensors required to calculate the average value.
    metadata : dict
        Free form dictionary with metadata.
    """

    key: str
    from_timestamp: datetime.datetime
    to_timestamp: datetime.datetime
    measurement: SimpleMeasurement
    reference: Optional[SimpleMeasurement]
    events: List[SensorTelemetryMeasurementEvent]
    events_by_sensor_id: Dict[str, SensorTelemetryMeasurementEvent]
    number_of_sensors: int
    minimum_number_of_sensors: int

    completeness_decrease_per_past_time_window: float = 0.0
    completeness_decrease_per_forecasted_event: float = 0.0
    completeness_decrease_per_imputed_event: float = 0.0

    timeliness_decrease_per_past_time_window: float = 0.0
    timeliness_decrease_per_forecasted_event: float = 0.0
    timeliness_decrease_per_imputed_event: float = 0.0

    metadata: Dict = field(default_factory=lambda: {})

    def __str__(self):
        _from_timestamp: str = self.from_timestamp.strftime(TIMESTAMP_FORMAT)
        _to_timestamp: str = self.to_timestamp.strftime(TIMESTAMP_FORMAT)
        return f"WindowedAverageMeasurementValueEvent : {_from_timestamp} - {_to_timestamp} : {self.measurement.value}"

    def __copy__(self):
        return WindowedAverageMeasurementValueEvent(
            key=self.key,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp,
            measurement=self.measurement.__copy__(),
            reference=self.reference.__copy__() if self.reference is not None else None,
            events=[event.__copy__() for event in self.events],
            events_by_sensor_id={
                k: v.__copy__() for k, v in self.events_by_sensor_id.items()
            },
            number_of_sensors=self.number_of_sensors,
            minimum_number_of_sensors=self.minimum_number_of_sensors,
            completeness_decrease_per_past_time_window=self.completeness_decrease_per_past_time_window,
            completeness_decrease_per_forecasted_event=self.completeness_decrease_per_forecasted_event,
            completeness_decrease_per_imputed_event=self.completeness_decrease_per_imputed_event,
            timeliness_decrease_per_past_time_window=self.timeliness_decrease_per_past_time_window,
            timeliness_decrease_per_forecasted_event=self.timeliness_decrease_per_forecasted_event,
            timeliness_decrease_per_imputed_event=self.timeliness_decrease_per_imputed_event,
            metadata=self.metadata.copy(),
        )

    def __post_init__(self):
        assert self.number_of_sensors > 0
        assert self.minimum_number_of_sensors > 0
        assert self.minimum_number_of_sensors <= self.number_of_sensors

    def has_at_least_one_event(self) -> bool:
        return len(self.events_by_sensor_id) > 0

    def _update_measurement(self) -> None:
        events_count: int = len(self.events_by_sensor_id)
        if events_count == 0:
            # TODO Set null.
            self.measurement.value = 0.0
        total = 0.0
        for event in self.events_by_sensor_id.values():
            total = total + event.measurement.value
        average = total / events_count
        self.measurement.value = average

    def _update_completeness_decrease(self) -> None:
        self.metadata["completeness_decrease"] = 0.0

        _sum: float = 0.0
        for event in self.events_by_sensor_id.values():
            if event.origin_type == "real":
                pass

            elif event.origin_type == "past":
                _sum = _sum + (
                    self.completeness_decrease_per_past_time_window
                    * event.metadata["past_time_windows"]
                )

            elif event.origin_type == "forecasted":
                _sum = _sum + self.completeness_decrease_per_forecasted_event

            elif event.origin_type == "imputed":
                _sum = _sum + self.completeness_decrease_per_imputed_event

        self.metadata["completeness_decrease"] = _sum

    def _update_timeliness_decrease(self) -> None:
        self.metadata["timeliness_decrease"] = 0.0

        _sum: float = 0.0
        for event in self.events_by_sensor_id.values():
            if event.origin_type == "real":
                pass

            elif event.origin_type == "past":
                _sum = _sum + (
                    self.timeliness_decrease_per_past_time_window
                    * event.metadata["past_time_windows"]
                )

            elif event.origin_type == "forecasted":
                _sum = _sum + self.timeliness_decrease_per_forecasted_event

            elif event.origin_type == "imputed":
                _sum = _sum + self.timeliness_decrease_per_imputed_event

        self.metadata["timeliness_decrease"] = _sum

    def _update_completeness(self) -> None:
        expected: int = len(self.events_by_sensor_id)
        if expected >= self.minimum_number_of_sensors:
            real: int = expected
        else:
            real: int = self.minimum_number_of_sensors
        completeness_decrease: float = self.metadata["completeness_decrease"]
        completeness: float = 1 - ((real - expected) / real) - completeness_decrease
        self.metadata["completeness"] = completeness

    def _update_timeliness(self) -> None:
        total: int = len(self.events_by_sensor_id)
        if total == 0:
            self.metadata["timeliness"] = 0.0
            return
        timely: int = total  # TODO Temporary.
        timeliness_decrease: float = self.metadata["timeliness_decrease"]
        timeliness: float = 1 - ((total - timely) / total) - timeliness_decrease
        self.metadata["timeliness"] = timeliness

    def _update_metadata(self) -> None:
        total_count: int = 0
        real_count: int = 0
        past_count: int = 0
        forecasted_count: int = 0
        imputed_count: int = 0

        for event in self.events_by_sensor_id.values():
            total_count = total_count + 1

            if event.origin_type == "real":
                real_count = real_count + 1

            if not self.from_timestamp <= event.timestamp <= self.to_timestamp:
                past_count = past_count + 1

            if event.origin_type == "forecasted":
                forecasted_count = forecasted_count + 1

            if event.origin_type == "imputed":
                imputed_count = imputed_count + 1

        self.metadata["total_count"] = total_count
        self.metadata["real_count"] = real_count
        self.metadata["past_count"] = past_count
        self.metadata["forecasted_count"] = forecasted_count
        self.metadata["imputed_count"] = imputed_count

    def on_new_sensor_telemetry_measurement_event(
        self, event: SensorTelemetryMeasurementEvent, with_range_check: bool = True
    ) -> None:
        if with_range_check is True:
            if not self.from_timestamp <= event.timestamp <= self.to_timestamp:
                logger.warning(
                    f"Event does not belong in this time window (1). Aborting..."
                )
                return
        else:
            if event.timestamp > self.to_timestamp:
                logger.warning(
                    f"Event does not belong in this time window (2). Aborting..."
                )
                return

        self.events.append(event.__copy__())
        if event.sensor.sensor_id not in self.events_by_sensor_id:
            self.events_by_sensor_id[event.sensor.sensor_id] = event.__copy__()
        else:
            if (
                event.timestamp
                >= self.events_by_sensor_id[event.sensor.sensor_id].timestamp
            ):
                self.events_by_sensor_id[event.sensor.sensor_id] = event.__copy__()

        self._update_measurement()

        self._update_completeness_decrease()
        self._update_timeliness_decrease()

        self._update_completeness()
        self._update_timeliness()

        self._update_metadata()

    def debug_string(self) -> str:
        return (
            f"WindowedAverageMeasurementValueEvent "
            f"{self.from_timestamp.strftime(TIMESTAMP_FORMAT)} --- {self.to_timestamp.strftime(TIMESTAMP_FORMAT)} "
            f"\n"
            f"\taverage : {self.measurement.value} "
            f"\n"
            f"\t\tcompleteness : {self.metadata['completeness']}"
            f"\n"
            f"\t\ttimeliness   : {self.metadata['timeliness']}"
            f"\n"
            f"\t\t-"
            f"\n"
            f"\t\tcompleteness_decrease : {self.metadata['completeness_decrease']}"
            f"\n"
            f"\t\ttimeliness_decrease   : {self.metadata['timeliness_decrease']}"
            f"\n"
            f"\t\t-"
            f"\n"
            f"\t\ttotal_count      : {self.metadata['total_count']}"
            f"\n"
            f"\t\treal_count       : {self.metadata['real_count']}"
            f"\n"
            f"\t\tpast_count       : {self.metadata['past_count']}"
            f"\n"
            f"\t\tforecasted_count : {self.metadata['forecasted_count']}"
            f"\n"
            f"\t\timputed_count    : {self.metadata['imputed_count']}"
        )


# ####################################################################################################
# Logic / Execution.
# ####################################################################################################


# ##################################################
# Sensor Measurements.
# ##################################################


@dataclass
class _SensorMeasurement:
    """
    A Sensor Measurement.

    ...

    Attributes
    ----------

    timestamp : datetime.datetime
        The measurement timestamp aka event timestamp.
    sensor_id : str
        The ID of the Sensor that made the measurement.
    air_temperature : float
        The air temperature in the default unit.
    """

    timestamp: datetime.datetime
    sensor_id: str
    air_temperature: float

    def __str__(self):
        return f"_SensorMeasurement {self.sensor_id} : air_temperature = {self.air_temperature} @ {self.timestamp}"

    def __copy__(self):
        return _SensorMeasurement(
            timestamp=self.timestamp,
            sensor_id=self.sensor_id,
            air_temperature=self.air_temperature,
        )


def _load_sensor_measurements() -> List[_SensorMeasurement]:
    with open(
        "F:\\projects\\PhD\\iotvmqd\\iotvmdq-local-data\\sensors-measurements-0002.csv",
        "r",
    ) as input_file:
        contents: str = input_file.read()

    instances: List[_SensorMeasurement] = []

    lines: List[str] = contents.splitlines()
    for line in lines:
        if line.startswith("timestamp,"):
            continue
        if line.startswith("#"):
            continue
        if "," not in line:
            continue
        parts: List[str] = line.split(",")
        instances.append(
            _SensorMeasurement(
                sensor_id=parts[1],
                timestamp=datetime.datetime.strptime(parts[0], TIMESTAMP_FORMAT),
                air_temperature=float(parts[2]),
            )
        )

    return instances


# ##################################################
# Ground Truth / Reference Values.
# ##################################################


@dataclass
class _ReferenceMeasurement:
    """
    A ReferenceMeasurement aka ground truth for a specific window.

    ...

    Attributes
    ----------

    key : str
    from_timestamp : datetime.datetime
    to_timestamp : datetime.datetime
    air_temperature : float
    """

    key: str
    from_timestamp: datetime.datetime
    to_timestamp: datetime.datetime
    air_temperature: float

    def __str__(self):
        return f"_ReferenceMeasurement : air_temperature = {self.air_temperature} @ {self.key}"

    def __copy__(self):
        return _ReferenceMeasurement(
            key=self.key,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp,
            air_temperature=self.air_temperature,
        )


def _load_reference_measurements() -> List[_ReferenceMeasurement]:
    with open(
        "F:\\projects\\PhD\\iotvmqd\\iotvmdq-local-data\\reference-measurements-1m-resolution.csv",
        "r",
    ) as input_file:
        contents: str = input_file.read()

    instances: List[_ReferenceMeasurement] = []

    lines: List[str] = contents.splitlines()
    for line in lines:
        if line.startswith("from_timestamp,"):
            continue
        if line.startswith("#"):
            continue
        parts: List[str] = line.split(",")
        from_timestamp: datetime = datetime.datetime.strptime(
            parts[0], TIMESTAMP_FORMAT
        )
        key: str = _convert_timestamp_to_key(timestamp=from_timestamp)
        to_timestamp: datetime = datetime.datetime.strptime(parts[1], TIMESTAMP_FORMAT)
        air_temperature: float = float(parts[2])
        instances.append(
            _ReferenceMeasurement(
                key=key,
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                air_temperature=air_temperature,
            )
        )

    return instances


# ##################################################
# Time Windows.
# ##################################################


def _convert_timestamp_to_key(timestamp: datetime.datetime) -> str:
    _TIMESTAMP_FORMAT: str = "%Y-%m-%d %H:%M"
    return f"{timestamp.strftime(_TIMESTAMP_FORMAT)}:00"


@dataclass
class _FixedSizeTimeBasedWindow:
    key: str
    from_timestamp: datetime.datetime
    to_timestamp: datetime.datetime
    event: Optional[WindowedAverageMeasurementValueEvent]

    def __str__(self):
        _from_timestamp: str = self.from_timestamp.strftime(TIMESTAMP_FORMAT)
        _to_timestamp: str = self.to_timestamp.strftime(TIMESTAMP_FORMAT)
        return f"_FixedSizeTimeBasedWindow : {_from_timestamp} --- {_to_timestamp}"


def _generate_fixed_size_time_based_windows(
    year: int, month: int, day: int
) -> List[_FixedSizeTimeBasedWindow]:
    _year: str = str(year)
    _month: str = str(month) if month > 9 else f"0{str(month)}"
    _day: str = str(day) if day > 9 else f"0{str(day)}"

    instances: List[_FixedSizeTimeBasedWindow] = []

    for hour in range(0, 24):
        for minute in range(0, 60):
            _hour: str = str(hour)
            _minute: str = str(minute)
            _from_timestamp: str = f"{year}-{month}-{day} {_hour}:{_minute}:00"
            _to_timestamp: str = f"{year}-{month}-{day} {_hour}:{_minute}:59"
            from_timestamp: datetime.datetime = datetime.datetime.strptime(
                _from_timestamp, TIMESTAMP_FORMAT
            )
            to_timestamp: datetime.datetime = datetime.datetime.strptime(
                _to_timestamp, TIMESTAMP_FORMAT
            )
            key: str = _convert_timestamp_to_key(timestamp=from_timestamp)
            instance: _FixedSizeTimeBasedWindow = _FixedSizeTimeBasedWindow(
                key=key,
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp,
                event=None,
            )
            instances.append(instance)

    return instances


# ##################################################
# Sensors.
# ##################################################


_SENSORS: Dict[str, Sensor] = {
    "sensor-1": Sensor(
        sensor_id="sensor-1",
        location_id="zone-1",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
    "sensor-2": Sensor(
        sensor_id="sensor-2",
        location_id="zone-1",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
    "sensor-3": Sensor(
        sensor_id="sensor-3",
        location_id="zone-2",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
    "sensor-4": Sensor(
        sensor_id="sensor-4",
        location_id="zone-2",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
    "sensor-5": Sensor(
        sensor_id="sensor-5",
        location_id="zone-3",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
    "sensor-6": Sensor(
        sensor_id="sensor-6",
        location_id="zone-3",
        sensor_specs=SensorSpecs(accuracy=1.0),
    ),
}


def _get_sensor_by_id(sensor_id: str) -> Optional[Sensor]:
    if sensor_id not in _SENSORS:
        return None
    return _SENSORS[sensor_id].__copy__()


# ##################################################
# Execution Flow.
# ##################################################


def run(
    year: int,
    month: int,
    day: int,
    up__minimum_number_of_sensors: int,
    up__past_windows_lookup_limit: int,
    up__minimum_number_of_consecutive_events_for_prediction: int,
    up__enable_reference: bool,
    up__enable_soft_sensing: bool,
) -> None:
    # Load sensors measurements.
    # --------------------------------------------------

    sensors_measurements: List[_SensorMeasurement] = _load_sensor_measurements()

    # for sensor_measurement in sensors_measurements:
    #     print(sensor_measurement)

    # Load reference measurements (1m resolution).
    # --------------------------------------------------

    reference_measurements: List[_ReferenceMeasurement] = _load_reference_measurements()
    reference_measurements_by_key: Dict[str, _ReferenceMeasurement] = {
        v.key: v.__copy__() for v in reference_measurements
    }

    # Transform `_SensorMeasurement` instances to `SensorTelemetryMeasurementEvent` instances.
    # --------------------------------------------------

    sensor_telemetry_measurement_events: List[SensorTelemetryMeasurementEvent] = []

    for sensor_measurement in sensors_measurements:
        instance: SensorTelemetryMeasurementEvent = SensorTelemetryMeasurementEvent(
            sensor=_get_sensor_by_id(sensor_id=sensor_measurement.sensor_id),
            measurement=SimpleMeasurement(
                name="air_temperature",
                value=sensor_measurement.air_temperature,
                unit=DEFAULT_AIR_TEMPERATURE_UNIT,
            ),
            timestamp=sensor_measurement.timestamp,
            origin_type="real",
            metadata={},
        )
        assert instance.sensor is not None
        sensor_telemetry_measurement_events.append(instance)

    # for instance in sensor_telemetry_measurement_events:
    #     print(instance)

    # Create Time Windows.
    # Time Windows are actually slots for real business objects.
    # --------------------------------------------------

    fixed_size_time_based_windows: List[
        _FixedSizeTimeBasedWindow
    ] = _generate_fixed_size_time_based_windows(year=year, month=month, day=day)

    # for fixed_size_time_based_window in fixed_size_time_based_windows:
    #     print(fixed_size_time_based_window)

    fixed_size_time_based_windows_by_key: Dict[str, _FixedSizeTimeBasedWindow] = {
        v.key: v for v in fixed_size_time_based_windows
    }

    # Process event by event (soft sensing is optional).
    # --------------------------------------------------

    for sensor_telemetry_measurement_event in sensor_telemetry_measurement_events:
        key: str = _convert_timestamp_to_key(
            timestamp=sensor_telemetry_measurement_event.timestamp
        )
        assert key in fixed_size_time_based_windows_by_key, f"key {key} is not present!"

        reference: Optional[SimpleMeasurement] = None
        if up__enable_reference is True:
            if key in reference_measurements_by_key:
                reference_measurement: _ReferenceMeasurement = (
                    reference_measurements_by_key[key]
                )
                reference = SimpleMeasurement(
                    name="air_temperature",
                    value=reference_measurement.air_temperature,
                    unit=DEFAULT_AIR_TEMPERATURE_UNIT,
                )

        if fixed_size_time_based_windows_by_key[key].event is None:
            fixed_size_time_based_windows_by_key[
                key
            ].event = WindowedAverageMeasurementValueEvent(
                key=key,
                from_timestamp=fixed_size_time_based_windows_by_key[key].from_timestamp,
                to_timestamp=fixed_size_time_based_windows_by_key[key].to_timestamp,
                measurement=SimpleMeasurement(
                    name="air_temperature", value=0.0, unit=DEFAULT_AIR_TEMPERATURE_UNIT
                ),
                reference=reference,
                events=[],
                events_by_sensor_id={},
                number_of_sensors=len(_SENSORS),
                # TODO Get from user parameters.
                # TODO Enable callables and lambdas.
                completeness_decrease_per_past_time_window=0.0,
                completeness_decrease_per_forecasted_event=0.04,
                completeness_decrease_per_imputed_event=0.06,
                timeliness_decrease_per_past_time_window=0.02,
                timeliness_decrease_per_forecasted_event=0.0,
                timeliness_decrease_per_imputed_event=0.06,
                minimum_number_of_sensors=up__minimum_number_of_sensors,
            )

        fixed_size_time_based_windows_by_key[
            key
        ].event.on_new_sensor_telemetry_measurement_event(
            event=sensor_telemetry_measurement_event, with_range_check=True
        )

        if up__enable_soft_sensing is False:
            continue

        # Soft Sensing: check for missing events.
        # --------------------------------------------------

        missing_sensors_ids: List[str] = []

        for sensor_id in _SENSORS:
            if (
                sensor_id
                not in fixed_size_time_based_windows_by_key[
                    key
                ].event.events_by_sensor_id
            ):
                missing_sensors_ids.append(sensor_id)

        if len(missing_sensors_ids) == 0:
            continue

        # Soft Sensing: event from previous time window.
        # TODO Check again!
        # --------------------------------------------------

        for sensor_id in missing_sensors_ids:
            past_event: Optional[SensorTelemetryMeasurementEvent] = None
            for i in range(1, up__past_windows_lookup_limit + 1):
                prev_timestamp: datetime.datetime = (
                    fixed_size_time_based_windows_by_key[key].from_timestamp
                    - datetime.timedelta(minutes=i)
                )
                prev_key: str = _convert_timestamp_to_key(timestamp=prev_timestamp)
                # print(f"\t{sensor_id} : {key} VS {prev_timestamp.strftime(TIMESTAMP_FORMAT)}")  # TODO remove

                if fixed_size_time_based_windows_by_key[prev_key].event is None:
                    continue

                if (
                    sensor_id
                    not in fixed_size_time_based_windows_by_key[
                        prev_key
                    ].event.events_by_sensor_id
                ):
                    continue

                past_event = (
                    fixed_size_time_based_windows_by_key[prev_key]
                    .event.events_by_sensor_id[sensor_id]
                    .__copy__()
                )

                if past_event.origin_type != "real":
                    past_event = None
                    continue

                past_event.origin_type = "past"
                past_event.metadata["past_time_windows"] = i
                break

            if past_event is None:
                continue

            fixed_size_time_based_windows_by_key[
                key
            ].event.on_new_sensor_telemetry_measurement_event(
                event=past_event, with_range_check=False
            )

        # Soft Sensing: check for missing events.
        # TODO Duplicate code!
        # --------------------------------------------------

        missing_sensors_ids: List[str] = []

        for sensor_id in _SENSORS:
            if (
                sensor_id
                not in fixed_size_time_based_windows_by_key[
                    key
                ].event.events_by_sensor_id
            ):
                missing_sensors_ids.append(sensor_id)

        if len(missing_sensors_ids) == 0:
            continue

        # Soft Sensing: forecasting.
        # --------------------------------------------------

        for sensor_id in _SENSORS:
            # TODO Sample data.
            time_series: List[Dict] = [
                10.1,
                11.1,
                12.1,
                13.1,
                14.1,
                15.1,
                16.1,
                17.1,
                18.1,
                19.1,
                20.1,
                21.1,
                20.1,
                19.1,
                18.1,
                17.1,
                16.1,
                15.1,
                14.1,
                13.1,
                12.1,
                11.1,
                10.1,
                11.1,
                12.1,
                13.1,
                14.1,
                15.1,
                16.1,
                17.1,
                18.1,
                19.1,
                20.1,
                21.1,
                22.1,
                20.1,
                19.1,
                18.1,
                17.1,
                16.1,
                15.1,
                14.1,
                13.1,
                12.1,
                11.1,
                10.1,
                11.1,
                12.1,
                13.1,
                14.1,
                15.1,
                16.1,
                17.1,
                18.1,
                19.1,
                20.1,
                21.1,
            ]
            model = ARIMA(time_series, order=(1, 1, 1))
            model_fit = model.fit()
            yhat = model_fit.predict(len(time_series), len(time_series), typ="levels")

            forecasted_event: SensorTelemetryMeasurementEvent = (
                SensorTelemetryMeasurementEvent(
                    sensor=_get_sensor_by_id(sensor_id=sensor_id),
                    measurement=SimpleMeasurement(
                        name="air_temperature",
                        value=yhat[0],
                        unit=DEFAULT_AIR_TEMPERATURE_UNIT,
                    ),
                    timestamp=(
                        fixed_size_time_based_windows_by_key[key].from_timestamp
                        + datetime.timedelta(milliseconds=10)
                    ),
                    origin_type="forecasted",
                    metadata={},
                )
            )

            fixed_size_time_based_windows_by_key[
                key
            ].event.on_new_sensor_telemetry_measurement_event(
                event=forecasted_event, with_range_check=False
            )

        # Soft Sensing: check for missing events.
        # TODO Duplicate code!
        # --------------------------------------------------

        missing_sensors_ids: List[str] = []

        for sensor_id in _SENSORS:
            if (
                sensor_id
                not in fixed_size_time_based_windows_by_key[
                    key
                ].event.events_by_sensor_id
            ):
                missing_sensors_ids.append(sensor_id)

        if len(missing_sensors_ids) == 0:
            continue

        # Soft Sensing: imputation.
        # --------------------------------------------------

        for sensor_id in _SENSORS:
            imputed_value = 20.0  # TODO Add.

            forecasted_event: SensorTelemetryMeasurementEvent = (
                SensorTelemetryMeasurementEvent(
                    sensor=_get_sensor_by_id(sensor_id=sensor_id),
                    measurement=SimpleMeasurement(
                        name="air_temperature",
                        value=imputed_value,
                        unit=DEFAULT_AIR_TEMPERATURE_UNIT,
                    ),
                    timestamp=(
                        fixed_size_time_based_windows_by_key[key].from_timestamp
                        + datetime.timedelta(milliseconds=10)
                    ),
                    origin_type="imputed",
                    metadata={},
                )
            )

            fixed_size_time_based_windows_by_key[
                key
            ].event.on_new_sensor_telemetry_measurement_event(
                event=forecasted_event, with_range_check=False
            )

    # Process windows without events.
    # --------------------------------------------------

    # TODO Implement.

    # Display composite events with at least one primitive/basic event.
    # --------------------------------------------------

    for window in fixed_size_time_based_windows_by_key.values():
        if window.event is None:
            continue
        if window.event.has_at_least_one_event():
            print(window.event)
            print(window.event.debug_string())
            print("\n")
            pass
        # TODO To file.

    # Results Persistence.
    # --------------------------------------------------

    data = []
    for window in fixed_size_time_based_windows_by_key.values():
        row = {
            "key": window.key,
            "from_timestamp": window.from_timestamp.strftime(TIMESTAMP_FORMAT),
            "to_timestamp": window.to_timestamp.strftime(TIMESTAMP_FORMAT),
            **{k: None for k in _SENSORS.keys()},
            "average": None,
            # TODO Initialize quality properties dynamically!
            "completeness": None,
            "timeliness": None,
            "completeness_decrease": None,
            "timeliness_decrease": None,
            "total_count": None,
            "real_count": None,
            "past_count": None,
            "forecasted_count": None,
            "imputed_count": None,
        }

        if window.event is None:
            data.append(row)
            continue

        e: WindowedAverageMeasurementValueEvent = window.event

        for sensor_id in e.events_by_sensor_id:
            row[sensor_id] = e.events_by_sensor_id[sensor_id].measurement.value

        # row["average"] = e.measurement.value

        # TODO Set quality properties dynamically!
        row["completeness"] = e.metadata["completeness"]
        row["timeliness"] = e.metadata["timeliness"]

        row["completeness_decrease"] = e.metadata["completeness_decrease"]
        row["timeliness_decrease"] = e.metadata["timeliness_decrease"]

        row["total_count"] = e.metadata["total_count"]
        row["real_count"] = e.metadata["real_count"]
        row["past_count"] = e.metadata["past_count"]
        row["forecasted_count"] = e.metadata["forecasted_count"]
        row["imputed_count"] = e.metadata["imputed_count"]

        data.append(row)

    suffix: str = datetime.datetime.now().strftime(TIMESTAMP_FORMAT_FOR_FILES)
    pd.DataFrame(data=data).to_excel(f"example-{suffix}.xlsx")

    # Imputation Example.
    # TODO Remove.
    # --------------------------------------------------

    data = [
        [20.0, 21.0, 20.0, 21.0, 20.0, 21.0],
        [21.0, 20.0, 21.0, 20.0, 21.0, 20.0],
        [np.nan, 21.0, 20.0, 21.0, 20.0, 21.0],
    ]
    imputer = SimpleImputer(missing_values=np.nan, strategy="mean")
    result = imputer.fit_transform(data)
    # print(result)

    imputer = IterativeImputer(max_iter=10, random_state=0)
    result = imputer.fit_transform(data)
    # print(result)


def run_scenario1() -> None:
    year: int = 2023
    month: int = 1
    day: int = 1
    up__minimum_number_of_sensors: int = 6
    up__past_windows_lookup_limit: int = 2
    up__minimum_number_of_consecutive_events_for_prediction: int = 4
    up__enable_reference: bool = True
    up__enable_soft_sensing: bool = True

    run(
        year=year,
        month=month,
        day=day,
        up__minimum_number_of_sensors=up__minimum_number_of_sensors,
        up__past_windows_lookup_limit=up__past_windows_lookup_limit,
        up__minimum_number_of_consecutive_events_for_prediction=up__minimum_number_of_consecutive_events_for_prediction,
        up__enable_reference=up__enable_reference,
        up__enable_soft_sensing=up__enable_soft_sensing,
    )
