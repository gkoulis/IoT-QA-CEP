import zoneinfo
from typing import Dict, List, Optional

import pandas as pd

from iotvm_extensions.fabrication_forecasting.base import \
    build_key, SensorMeasurementForecaster
from iotvm_extensions.fabrication_forecasting.spec import \
    FabricationForecastingService
from iotvm_extensions.fabrication_forecasting.spec.ttypes import (
    ForecastScope,
    ForecastRequest,
    ForecastResponse,
    ForecastException,
)


def _key(scope: ForecastScope) -> str:
    return build_key(
        sensor_id=scope.sensorId,
        physical_quantity=scope.physicalQuantity,
        topic_name=scope.topicName,
        frequency_in_seconds=scope.frequencyInSeconds
    )


class FabricationForecastingServiceImpl(FabricationForecastingService.Iface):
    def __init__(self):
        self._forecasters: Dict[str, SensorMeasurementForecaster] = {}
        self._ensuring: Dict[str, bool] = {}

    def ensure(self, scope: ForecastScope) -> None:
        key: str = _key(scope=scope)

        if key not in self._ensuring:
            self._ensuring[key] = False

        if self._ensuring[key] is True:
            return

        self._ensuring[key] = True

        if key not in self._forecasters:
            self._forecasters[key] = None

        if self._forecasters[key] is None:
            self._forecasters[key] = SensorMeasurementForecaster(
                sensor_id=scope.sensorId,
                physical_quantity=scope.physicalQuantity,
                topic_name=scope.topicName,
                frequency_in_seconds=scope.frequencyInSeconds,
                debug=True,
                auto=False,
            )

        self._forecasters[key].refresh_train()

        self._ensuring[key] = False

    def forecast(
        self, scope: ForecastScope, request: ForecastRequest
    ) -> ForecastResponse:
        key: str = _key(scope=scope)

        if key not in self._forecasters:
            raise ForecastException(
                f"SensorMeasurementForecaster {key} does not exist!"
            )

        if self._ensuring[key] is True:
            raise ForecastException(
                f"SensorMeasurementForecaster {key} is temporary not available!"
            )

        if self._forecasters[key].is_ready() is False:
            raise ForecastException(f"SensorMeasurementForecaster {key} is not ready!")

        self._forecasters[key].refresh_data()

        future_periods: int = request.stepsAhead
        start_timestamp = pd.Timestamp(request.startTimestamp, tzinfo=zoneinfo.ZoneInfo("UTC"), unit="ms")
        end_timestamp = pd.Timestamp(request.endTimestamp, tzinfo=zoneinfo.ZoneInfo("UTC"), unit="ms")
        result: Optional[Dict] = self._forecasters[key].forecast(
            future_periods=future_periods,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
        )

        if result is None:
            raise ForecastException(
                f"SensorMeasurementForecaster {key} "
                f"could not provide forecast for period {start_timestamp} - {end_timestamp}!")

        return ForecastResponse(
            value=float(result["prediction"]),
            startTimestamp=int(result["window"].timestamp() * 1_000),
            endTimestamp=int(result["window_end"].timestamp() * 1_000),
            metrics=result["metrics"]
        )
