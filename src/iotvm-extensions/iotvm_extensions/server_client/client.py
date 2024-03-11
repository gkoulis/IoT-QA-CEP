import logging
from typing import Optional

from thrift.Thrift import TException
from thrift.protocol import TBinaryProtocol, TMultiplexedProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.transport.TTransport import TTransportException

import iotvm_extensions.config as config
from ._utilities import get_service_name

_logger = logging.getLogger("iotvm_extensions.server_client.client")


class ClientsFactory:
    def __init__(self):
        self._host: Optional[str] = None
        self._port: Optional[int] = None
        self._transport = None
        self._protocol = None
        self._clients = {}
        self._services_classes = []

    def _initialize(self) -> None:
        transport = TSocket.TSocket(host=config.SERVER_HOST, port=config.SERVER_PORT)
        self._transport = TTransport.TBufferedTransport(trans=transport)
        self._protocol = TBinaryProtocol.TBinaryProtocol(trans=self._transport)
        self._clients = {}

    def _close(self) -> None:
        if self._transport is not None:
            self._transport.close()
        self._transport = None
        self._protocol = None
        self._clients = {get_service_name(service_class=sc): None for sc in self._services_classes}

    def _reconnect(self) -> None:
        # raises TException.

        self._host = config.SERVER_HOST
        self._port = config.SERVER_PORT

        transport = TSocket.TSocket(host=config.SERVER_HOST, port=config.SERVER_PORT)
        self._transport = TTransport.TBufferedTransport(trans=transport)
        self._transport.open()

        self._protocol = TBinaryProtocol.TBinaryProtocol(trans=self._transport)

        for service_class in self._services_classes:
            service_name: str = get_service_name(service_class=service_class)
            mp: TMultiplexedProtocol = TMultiplexedProtocol.TMultiplexedProtocol(
                protocol=self._protocol, serviceName=service_name
            )
            self._clients[service_name] = service_class.Client(iprot=mp)

    def _try_to_reconnect(self) -> None:
        self._close()
        try:
            self._reconnect()
        except TException as ex:
            self._close()
            _logger.error(
                f"Failed to reconnect to extensions server {self._host}:{self._port}. Reason : {ex}. Services will be unavailable."
            )

    def _ping(self, client) -> bool:
        if client is None:
            return False
        try:
            client.ping()
            return True
        except TTransportException as ex:
            _logger.warning(f"TTransportException: {ex} (type : {ex.type})")
            return False
        except TException as ex:
            _logger.warning(f"TException: {ex}")
            return False

    #
    # Service (getters and setters).
    #

    def _get_service(self, service_name: str):
        if service_name not in self._clients:
            return None
        # It can be `None`.
        return self._clients[service_name]

    def register_service(self, service_class) -> None:
        self._services_classes.append(service_class)

    def get_service(self, service_name):
        service = self._get_service(service_name=service_name)

        pong: bool = self._ping(client=service)
        if pong is False:
            self._try_to_reconnect()

        service = self._get_service(service_name=service_name)
        return service
