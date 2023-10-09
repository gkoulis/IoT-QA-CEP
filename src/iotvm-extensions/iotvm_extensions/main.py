import fire

from typing import List
import logging

from ._logging import set_up_logging
from ._prerequisites import set_up_prerequisites


_logger = logging.getLogger("iotvm_extensions.main")


# noinspection PyMethodMayBeStatic
class CLI:
    def start_server(self) -> None:
        from iotvm_extensions.server_client.server import start_server

        start_server()

    def ensure_forecasters(self) -> None:
        from iotvm_extensions.server_client.client import ClientsFactory
        from iotvm_extensions.fabrication_forecasting.spec import (
            FabricationForecastingService,
        )
        from iotvm_extensions.fabrication_forecasting.spec.ttypes import ForecastScope

        factory: ClientsFactory = ClientsFactory()
        factory.register_service(service_class=FabricationForecastingService)

        service: FabricationForecastingService = factory.get_service(
            service_name="FabricationForecastingService"
        )

        if service is None:
            _logger.error("FabricationForecastingService is None!")
            return

        sensor_ids: List[str] = [
            "sensor-1",
            "sensor-2",
            "sensor-3",
            "sensor-4",
            "sensor-5",
            "sensor-6",
        ]
        physical_quantity: str = "temperature"
        topic_name: str = "ga.sensor_telemetry_measurement_event.0001.temperature"
        frequency_in_seconds_list: List[int] = [10, 20, 60]

        for sensor_id in sensor_ids:
            for frequency_in_seconds in frequency_in_seconds_list:
                service.ensure(
                    scope=ForecastScope(
                        sensorId=sensor_id,
                        physicalQuantity=physical_quantity,
                        topicName=topic_name,
                        frequencyInSeconds=frequency_in_seconds,
                    )
                )

    def delete_all_mongodb_documents(self) -> None:
        from iotvm_extensions.mongodb import MongoClient, get_default_mongodb_client

        client: MongoClient = get_default_mongodb_client()
        collections_names: List[str] = ["recorded_sensor_data", "universal"]
        for collection_name in collections_names:
            collection = client["iotvmdb"][collection_name]
            collection.delete_many({})


if __name__ == "__main__":
    set_up_logging()
    set_up_prerequisites()
    fire.Fire(CLI)
