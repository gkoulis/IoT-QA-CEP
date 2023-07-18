"""
Author: Dimitris Gkoulis
Created at: Wednesday 7 June 2023
Modified at: Monday 12 June 2023
"""

import datetime
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import sys

import pandas as pd

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

    def __str__(self):
        _timestamp: str = self.timestamp.strftime(TIMESTAMP_FORMAT)
        return f"SensorTelemetryMeasurementEvent {self.sensor.sensor_id} : {self.measurement.name} = {self.measurement.value} @ {_timestamp}"

    def __copy__(self):
        return SensorTelemetryMeasurementEvent(
            sensor=self.sensor.__copy__(),
            measurement=self.measurement.__copy__(),
            timestamp=self.timestamp,
            origin_type=self.origin_type,
        )

    def __post_init__(self):
        assert self.origin_type in [
            "real",
            "fabricated",
        ], f"origin_type {self.origin_type} is not one of [real, fabricated]"


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
            self.measurement.value = 0.0
        total = 0.0
        for event in self.events_by_sensor_id.values():
            total = total + event.measurement.value
        average = total / events_count
        self.measurement.value = average

    def _update_accuracy1(self) -> None:
        if self.reference is None:
            self.metadata["accuracy1"] = 0.0
            return
        if self.reference.value == 0:
            # ΠΡΟΣΟΧΗ: Οι βαθμοί Κελσίου μπορεί να είναι μηδέν!
            self.metadata["accuracy1"] = 0.0
            return
        # accuracy1 = 1 - ((self.reference.value - self.measurement.value) / self.reference.value)
        accuracy1 = (
            self.reference.value - self.measurement.value
        ) / self.reference.value
        accuracy1 = 1 - abs(accuracy1)
        self.metadata["accuracy1"] = accuracy1

    def _update_timeliness1(self) -> None:
        total: int = len(self.events_by_sensor_id)
        if total == 0:
            self.metadata["timeliness1"] = 0.0
            return
        timely: int = 0
        for event in self.events_by_sensor_id.values():
            if self.from_timestamp <= event.timestamp <= self.to_timestamp:
                timely = timely + 1
        timeliness1: float = 1 - ((total - timely) / total)
        self.metadata["timeliness1"] = timeliness1

    def _update_completeness1(self) -> None:
        real: int = self.number_of_sensors
        expected: int = len(self.events_by_sensor_id)
        completeness1: float = 1 - ((real - expected) / real)
        self.metadata["completeness1"] = completeness1

    def _update_completeness2(self) -> None:
        expected: int = len(self.events_by_sensor_id)
        if expected >= self.minimum_number_of_sensors:
            real: int = expected
        else:
            real: int = self.minimum_number_of_sensors
        completeness2: float = 1 - ((real - expected) / real)
        self.metadata["completeness2"] = completeness2

    def _update_trustworthiness1(self) -> None:
        real: int = len(self.events_by_sensor_id)
        if real == 0:
            self.metadata["trustworthiness1"] = 0.0
            return
        expected: int = 0
        for event in self.events_by_sensor_id.values():
            if event.origin_type == "real":
                expected = expected + 1
        trustworthiness1: float = 1 - ((real - expected) / real)
        self.metadata["trustworthiness1"] = trustworthiness1

    def _update_trustworthiness2(self) -> None:
        real: int = len(self.events_by_sensor_id)
        if real == 0:
            self.metadata["trustworthiness2"] = 0.0
            return
        expected: int = 0
        for event in self.events_by_sensor_id.values():
            if (
                event.origin_type == "real"
                and self.from_timestamp <= event.timestamp <= self.to_timestamp
            ):
                expected = expected + 1
        trustworthiness2: float = 1 - ((real - expected) / real)
        self.metadata["trustworthiness2"] = trustworthiness2

    def _update_metadata(self) -> None:
        real_count: int = 0
        fabricated_count: int = 0
        old_count: int = 0
        for event in self.events_by_sensor_id.values():
            if event.origin_type == "real":
                real_count = real_count + 1
            if event.origin_type == "fabricated":
                fabricated_count = fabricated_count + 1
            if not self.from_timestamp <= event.timestamp <= self.to_timestamp:
                old_count = old_count + 1

        self.metadata["real_count"] = real_count
        self.metadata["fabricated_count"] = fabricated_count
        self.metadata["old_count"] = old_count

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
        self._update_accuracy1()
        self._update_timeliness1()
        self._update_completeness1()
        self._update_completeness2()
        self._update_trustworthiness1()
        self._update_trustworthiness2()
        self._update_metadata()

    def debug_string(self) -> str:
        return (
            f"WindowedAverageMeasurementValueEvent "
            f"{self.from_timestamp.strftime(TIMESTAMP_FORMAT)} --- {self.to_timestamp.strftime(TIMESTAMP_FORMAT)} "
            f"\n"
            f"\taverage : {self.measurement.value} "
            f"\n"
            f"\t\taccuracy1     : {self.metadata['accuracy1']}"
            f"\n"
            f"\t\ttimeliness1   : {self.metadata['timeliness1']}"
            f"\n"
            f"\t\tcompleteness1 : {self.metadata['completeness1']}"
            f"\n"
            f"\t\tcompleteness2 : {self.metadata['completeness2']}"
            f"\n"
            f"\t\ttrustworthiness1 : {self.metadata['trustworthiness1']}"
            f"\n"
            f"\t\ttrustworthiness2 : {self.metadata['trustworthiness2']}"
            f"\n"
            f"\t\t-"
            f"\n"
            f"\t\treal_count : {self.metadata['real_count']}"
            f"\n"
            f"\t\tfabricated_count : {self.metadata['fabricated_count']}"
            f"\n"
            f"\t\told_count : {self.metadata['old_count']}"
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
        "F:\\projects\\PhD\\iotvmqd\\iotvmdq-local-data\\sensors-measurements.csv", "r"
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
            older_event: Optional[SensorTelemetryMeasurementEvent] = None
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

                older_event = (
                    fixed_size_time_based_windows_by_key[prev_key]
                    .event.events_by_sensor_id[sensor_id]
                    .__copy__()
                )
                # older_event.origin_type = "real"  # This event is old but real (not fabricated).
                break

            if older_event is None:
                continue

            fixed_size_time_based_windows_by_key[
                key
            ].event.on_new_sensor_telemetry_measurement_event(
                event=older_event, with_range_check=False
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
            time_series: List[Dict] = []
            # TODO Use +1 instead of +2! FIX
            range_end: int = (
                up__past_windows_lookup_limit
                + up__minimum_number_of_consecutive_events_for_prediction
                + 2
            )
            for i in range(1, range_end):
                if i <= up__past_windows_lookup_limit:
                    continue

                prev_timestamp: datetime.datetime = (
                    fixed_size_time_based_windows_by_key[key].from_timestamp
                    - datetime.timedelta(minutes=i)
                )
                # print(f"\t{sensor_id} : {key} VS {prev_timestamp.strftime(TIMESTAMP_FORMAT)}")  # TODO remove
                prev_key: str = _convert_timestamp_to_key(timestamp=prev_timestamp)
                window: _FixedSizeTimeBasedWindow = (
                    fixed_size_time_based_windows_by_key[prev_key]
                )

                if window.event is None:
                    continue

                wamv_event: WindowedAverageMeasurementValueEvent = window.event

                if sensor_id not in wamv_event.events_by_sensor_id:
                    continue

                stm_event: SensorTelemetryMeasurementEvent = (
                    wamv_event.events_by_sensor_id[sensor_id]
                )

                # TODO Enable.
                # # Constraint: only real events.
                # if temp_event2 != "real":
                #     continue

                # TODO Enable.
                # # Constraint: only timely events.
                # if not temp_event1.from_timestamp <= temp_event2.timestamp <= temp_event1.to_timestamp:
                #     continue

                time_series.append(
                    {
                        # Use the window timestamp to help predictor infer the frequency.
                        "timestamp": window.from_timestamp,
                        "air_temperature": stm_event.measurement.value,
                    }
                )

            if (
                len(time_series)
                < up__minimum_number_of_consecutive_events_for_prediction
            ):
                # logger.warning("not enough data")
                continue

            # TODO Implement.

    # Process windows without events.
    # --------------------------------------------------

    # TODO Implement.

    # Display composite events with at least one primitive/basic event.
    # --------------------------------------------------

    for window in fixed_size_time_based_windows_by_key.values():
        if window.event is None:
            continue
        if window.event.has_at_least_one_event():
            # print(window.event)
            # print(window.event.debug_string())
            # print("\n")
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
            "accuracy1": None,
            "timeliness1": None,
            "completeness1": None,
            "completeness2": None,
            "trustworthiness1": None,
            "trustworthiness2": None,
            "real_count": None,
            "fabricated_count": None,
            "old_count": None,
        }

        if window.event is None:
            data.append(row)
            continue

        e: WindowedAverageMeasurementValueEvent = window.event

        for sensor_id in e.events_by_sensor_id:
            row[sensor_id] = e.events_by_sensor_id[sensor_id].measurement.value

        row["average"] = e.measurement.value

        # TODO Set quality properties dynamically!
        row["accuracy1"] = e.metadata["accuracy1"]
        row["timeliness1"] = e.metadata["timeliness1"]
        row["completeness1"] = e.metadata["completeness1"]
        row["completeness2"] = e.metadata["completeness2"]
        row["trustworthiness1"] = e.metadata["trustworthiness1"]
        row["trustworthiness2"] = e.metadata["trustworthiness2"]

        row["real_count"] = e.metadata["real_count"]
        row["fabricated_count"] = e.metadata["fabricated_count"]
        row["old_count"] = e.metadata["old_count"]

        data.append(row)

    suffix: str = datetime.datetime.now().strftime(TIMESTAMP_FORMAT_FOR_FILES)
    pd.DataFrame(data=data).to_excel(f"example-{suffix}.xlsx")


def run_scenario1() -> None:
    year: int = 2023
    month: int = 1
    day: int = 1
    up__minimum_number_of_sensors: int = 4
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
