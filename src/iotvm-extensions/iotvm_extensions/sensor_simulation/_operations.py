import time
from typing import Dict

import requests

import iotvm_extensions.config as config
from iotvm_extensions.helpers import ServerClientLocalFacade, \
    get_server_client_local_facade
from iotvm_extensions.sensing_recording.spec.ttypes import \
    RecordedSensorMeasurement, RecordedSensorData
from ._data_types import EvaluatedSimulatedSensorOperation

import logging

_logger = logging.getLogger("iotvm_extensions.sensor_simulation._operations")


# ####################################################################################################
# Internal.
# ####################################################################################################


def _push__sensor_telemetry_event__web_request(payload: Dict) -> None:
    requests.post(
        config.GATEWAY_API_SENSOR_TELEMETRY_EVENT_URL,
        json={"sensorTelemetryEvent": payload},
    )


def _record__sensor_telemetry_event__request(payload: RecordedSensorData) -> None:
    client: ServerClientLocalFacade = get_server_client_local_facade()
    client.recordSensorData(recordedSensorData=payload)


# ####################################################################################################
# API.
# ####################################################################################################


def simulate_sensor_operation(op: EvaluatedSimulatedSensorOperation, fail_silently: bool) -> None:
    assert op.operation == "push"

    timestamp_now: int = int(time.time_ns() / 1_000_000)

    payload_as_dict: Dict = {
        "measurements": [m.to_dict() for m in op.measurements],
        "sensorId": op.sensor_id,
        # @PHD_DOCS Three options:
        "timestamp": op.timestamp,
        # "timestamp": timestamp_now,
        # "timestamp": None,
        "additional": op.additional,
    }

    if op.fail is False:
        try:
            _push__sensor_telemetry_event__web_request(payload=payload_as_dict)
        except Exception as ex:
            _logger.error("Failed to execute: `_push__sensor_telemetry_event__web_request`", exc_info=ex)
            if fail_silently is False:
                raise ex

    payload: RecordedSensorData = RecordedSensorData(
        sensorId=op.sensor_id,
        measurements=[RecordedSensorMeasurement(name=m.name, value=m.value, unit=m.unit) for m in op.measurements],
        timestamp=op.timestamp,
        # additional data regarding sensing recording ONLY!
        additional={}
    )

    try:
        _record__sensor_telemetry_event__request(payload=payload)
    except Exception as ex:
        _logger.error("Failed to execute: `_record__sensor_telemetry_event__request`", exc_info=ex)
        if fail_silently is False:
            raise ex
