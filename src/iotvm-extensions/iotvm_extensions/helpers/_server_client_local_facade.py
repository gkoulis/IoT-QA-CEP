import logging
from typing import Dict, Optional

from iotvm_extensions.fabrication_forecasting.spec import (
    FabricationForecastingService,
)
from iotvm_extensions.fabrication_forecasting.spec.ttypes import (
    ForecastScope,
    ForecastRequest,
    ForecastResponse,
)
from iotvm_extensions.sensing_recording.spec import SensingRecordingService
from iotvm_extensions.sensing_recording.spec.ttypes import RecordedSensorData
from iotvm_extensions.server_client.client import ClientsFactory

_logger = logging.getLogger("iotvm_extensions.helpers._server_client_local_facade")


class ServerClientLocalFacade:
    def __init__(self):
        self._factory: ClientsFactory = ClientsFactory()
        self._factory.register_service(service_class=FabricationForecastingService)
        self._factory.register_service(service_class=SensingRecordingService)

    #
    # FabricationForecastingService
    #

    def ensure(self, scope: ForecastScope) -> None:
        service: FabricationForecastingService = self._factory.get_service(
            service_name="FabricationForecastingService"
        )
        if service is None:
            _logger.error("FabricationForecastingService is not available! Aborting `ensure`!")
            return
        service.ensure(scope=scope)

    def forecast(
        self, scope: ForecastScope, request: ForecastRequest
    ) -> Optional[ForecastResponse]:
        service: FabricationForecastingService = self._factory.get_service(
            service_name="FabricationForecastingService"
        )
        if service is None:
            _logger.error("FabricationForecastingService is not available! Aborting `forecast`!")
            return None
        return service.forecast(scope=scope, request=request)

    #
    # SensingRecordingService
    #

    def recordSensorData(self, recordedSensorData: RecordedSensorData) -> None:
        service: SensingRecordingService = self._factory.get_service(
            service_name="SensingRecordingService"
        )
        if service is None:
            _logger.error("SensingRecordingService is not available! Aborting `recordSensorData`!")
            return
        service.recordSensorData(recordedSensorData=recordedSensorData)


_SERVER_CLIENT_LOCAL_FACADES: Dict[str, ServerClientLocalFacade] = {}


def initialize_server_client_local_facade() -> None:
    _SERVER_CLIENT_LOCAL_FACADES["default"] = ServerClientLocalFacade()


def get_server_client_local_facade() -> ServerClientLocalFacade:
    return _SERVER_CLIENT_LOCAL_FACADES["default"]
