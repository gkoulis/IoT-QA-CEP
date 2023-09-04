import logging

from thrift.TMultiplexedProcessor import TMultiplexedProcessor
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from thrift.transport import TSocket, TTransport

import iotvm_extensions.config as config
from iotvm_extensions.fabrication_forecasting.api import (
    FabricationForecastingServiceImpl,
)
from iotvm_extensions.fabrication_forecasting.spec import FabricationForecastingService
from iotvm_extensions.sensing_recording.api import (
    SensingRecordingServiceImpl,
)
from iotvm_extensions.sensing_recording.spec import SensingRecordingService
from ._utilities import get_service_name

_logger = logging.getLogger("iotvm_extensions.server_client.server")


def start_server() -> None:
    _logger.info(
        f"Starting TThreadPoolServer with TMultiplexedProcessor = {config.SERVER_HOST}:{config.SERVER_PORT}"
    )
    m_processor: TMultiplexedProcessor = TMultiplexedProcessor()

    processors = [
        (
            get_service_name(FabricationForecastingService),
            FabricationForecastingService.Processor(
                handler=FabricationForecastingServiceImpl()
            ),
        ),
        (
            get_service_name(SensingRecordingService),
            SensingRecordingService.Processor(handler=SensingRecordingServiceImpl()),
        ),
    ]

    for service_name, instance in processors:
        _logger.info(f"Registering Processor : {service_name}")
        m_processor.registerProcessor(serviceName=service_name, processor=instance)

    transport = TSocket.TServerSocket(host=config.SERVER_HOST, port=config.SERVER_PORT)
    t_factory = TTransport.TBufferedTransportFactory()
    p_factory = TBinaryProtocol.TBinaryProtocolFactory()

    # TSimpleServer, TThreadedServer, TThreadPoolServer
    server = TServer.TThreadPoolServer(
        m_processor, transport, t_factory, p_factory, daemon=False
    )
    server.setNumThreads(num=4)

    server.serve()
