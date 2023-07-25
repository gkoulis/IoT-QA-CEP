import pprint

import datetime
import pandas as pd
from typing import Any, Dict, List, Union, Optional
from pymongo import MongoClient



def run() -> None:
    client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=IoTVMPython&ssl=false')
    result = client['iotvmdb']['universal'].aggregate([
        {
            '$match': {
                'real.sensorId': 'sensor-1',
                'topicName': 'ga.sensor_telemetry_measurement_event.0001.temperature'
            }
        }, {
            '$project': {
                '_id': 1,
                'real.sensorId': 1,
                'real.measurement.name': 1,
                'real.measurement.value.double': 1,
                'real.timestamps.defaultTimestamp.long': 1
            }
        }, {
            '$addFields': {
                'sensorId': '$real.sensorId',
                'name': '$real.measurement.name',
                'value': '$real.measurement.value.double',
                'dateTime': {
                    '$toDate': '$real.timestamps.defaultTimestamp.long'
                }
            }
        }, {
            '$addFields': {
                'dateTimeWindow': {
                    '$dateTrunc': {
                        'date': '$dateTime',
                        'unit': 'minute',
                        'binSize': 1
                    }
                }
            }
        }, {
            '$project': {
                'real': 0
            }
        }, {
            '$sort': {
                'dateTime': 1
            }
        }, {
            '$group': {
                '_id': '$dateTimeWindow',
                'value': {
                    '$last': '$value'
                },
                'dateTime': {
                    '$last': '$dateTime'
                }
            }
        }, {
            '$sort': {
                '_id': 1
            }
        }
    ])

    # Transform MongoDB items to measurements.
    # --------------------------------------------------

    start_dt: datetime.datetime | None = None
    end_dt: datetime.datetime | None = None

    measurements_by_timestamp: Dict[pd.Timestamp, Dict] = {}
    unique_values = []  # TODO Remove.

    for measurement in result:
        if start_dt is None:
            start_dt = measurement["_id"]
        end_dt = measurement["_id"]

        window_timestamp: pd.Timestamp = pd.to_datetime(measurement["_id"])
        measurements_by_timestamp[window_timestamp] = {
            "window_dt": measurement["_id"],
            "window_timestamp": window_timestamp,
            "timestamp_dt": measurement["dateTime"],
            "value": measurement["value"]
        }

        if measurement["value"] not in unique_values:
            unique_values.append(measurement["value"])

    print(unique_values)

    # Datetime Index with all periods.
    # --------------------------------------------------

    datetime_index: pd.DatetimeIndex = pd.date_range(
        start=start_dt,
        end=end_dt,
        freq="30S"
    )
    timestamp_list: List[pd.Timestamp] = datetime_index.to_list()

    # Join datetime index and measurements.
    # Where measurements do not exist add NaN.
    # --------------------------------------------------

    df_data: List[Dict] = []
    for timestamp in timestamp_list:
        if timestamp in measurements_by_timestamp:
            df_data.append({
                "window": timestamp,
                "measurement": measurements_by_timestamp[timestamp]["value"]
            })
        else:
            df_data.append({
                "window": timestamp,
                "measurement": None
            })

    dataframe: pd.DataFrame = pd.DataFrame(
        data=df_data
    )

    print(dataframe)  # TODO Remove.

    # dataframe.interpolate(inplace=True)

    print(dataframe["measurement"].unique())  # TODO Remove.
