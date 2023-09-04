import logging
from typing import Dict, List

from iotvm_extensions.mongodb import MongoClient, get_default_mongodb_client
from .spec import SensingRecordingService
from .spec.ttypes import RecordedSensorMeasurement, RecordedSensorData, PhysicalQuantityDataPoint

_logger = logging.getLogger("iotvm_extensions.sensing_recording.api")


def _RecordedSensorMeasurement_to_dict(instance: RecordedSensorMeasurement) -> Dict:
    return {
        "name": instance.name,
        "value": instance.value,
        "unit": instance.unit,
    }


def _RecordedSensorData_to_dict(instance: RecordedSensorData) -> Dict:
    return {
        "sensorId": instance.sensorId,
        "measurements": [
            _RecordedSensorMeasurement_to_dict(instance=nested)
            for nested in instance.measurements
        ],
        "timestamp": instance.timestamp,
        "additional": instance.additional,
    }


def _get_basic_aggregations_ctf_reals_mongodb_aggregation(client: MongoClient, sensor_ids: List[int], physical_quantity: str, from_timestamp: int, to_timestamp: int) -> List[PhysicalQuantityDataPoint]:
    collection = client["iotvmdb"]["recorded_sensor_data"]
    cursor = collection.aggregate(
        [
            {
                '$match': {
                    'sensorId': {
                        '$in': sensor_ids
                    },
                    'measurements.name': physical_quantity,
                    'timestamp': {
                        '$gte': from_timestamp,
                        '$lte': to_timestamp
                    }
                }
            }, {
            '$unwind': {
                'path': '$measurements'
            }
        }, {
            '$match': {
                'measurements.name': physical_quantity
            }
        }, {
            '$sort': {
                'timestamp': 1
            }
        }, {
            '$group': {
                '_id': '$sensorId',
                'value': {
                    '$last': '$measurements.value'
                },
                'timestamp': {
                    '$last': '$timestamp'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }
        ]
    )

    time_series: List[PhysicalQuantityDataPoint] = []
    for result in cursor:
        time_series.append(PhysicalQuantityDataPoint(
            value=result["value"],
            timestamp=result["timestamp"]
        ))

    return time_series


class SensingRecordingServiceImpl(SensingRecordingService.Iface):
    def __init__(self):
        self._mongo_client: MongoClient = get_default_mongodb_client()

    def recordSensorData(self, recordedSensorData: RecordedSensorData) -> None:
        if len(recordedSensorData.measurements) <= 0:
            _logger.warning(
                f"RecordedSensorData {recordedSensorData} has no measurements. "
                f"Aborting persistence..."
            )
            return

        document = _RecordedSensorData_to_dict(instance=recordedSensorData)

        collection = self._mongo_client["iotvmdb"]["recorded_sensor_data"]
        collection.insert_one(document=document)

    def getBasicAggregationsCTFReals(self, sensorIds: List[int], physicalQuantity: str, fromTimestamp: int, toTimestamp: int):
        return _get_basic_aggregations_ctf_reals_mongodb_aggregation(
            client=self._mongo_client,
            sensor_ids=sensorIds,
            physical_quantity=physicalQuantity,
            from_timestamp=fromTimestamp,
            to_timestamp=toTimestamp
        )
