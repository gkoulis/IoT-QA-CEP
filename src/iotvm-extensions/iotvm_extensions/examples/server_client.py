import logging
import time

from thrift import Thrift

from iotvm_extensions.fabrication_forecasting.spec import FabricationForecastingService
from iotvm_extensions.sensing_recording.spec import SensingRecordingService
from iotvm_extensions.sensing_recording.spec.ttypes import (
    RecordedSensorMeasurement,
    RecordedSensorData,
)
from iotvm_extensions.server_client.client import ClientsFactory

_logger = logging.getLogger("iotvm_extensions.server_client.example")


def run_client_example() -> None:
    factory: ClientsFactory = ClientsFactory()
    factory.register_service(service_class=FabricationForecastingService)
    factory.register_service(service_class=SensingRecordingService)

    service: SensingRecordingService = factory.get_service(service_name="SensingRecordingService")

    if service is None:
        _logger.error("SensingRecordingService is None. Aborting...")
        return

    for i in range(0, 1_000):
        try:
            service.recordSensorData(
                recordedSensorData=RecordedSensorData(
                    sensorId=f"sensor-{i}",
                    measurements=[RecordedSensorMeasurement(name="temperature", value=10.1, unit="C")],
                    timestamp=time.time_ns(),
                    additional={},
                )
            )
        except Thrift.TException as ex:
            _logger.error("Caught Thrift.TException!", exc_info=ex)
        time.sleep(0.1)
