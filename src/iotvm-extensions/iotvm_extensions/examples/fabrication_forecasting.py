import time

import pandas as pd

from iotvm_extensions.fabrication_forecasting.base import SensorMeasurementForecaster
from iotvm_extensions.fabrication_forecasting.visualization import plot_f, plot_forecasts_df


# noinspection PyProtectedMember
def _display_results(forecaster: SensorMeasurementForecaster) -> None:
    # print(forecaster._dataframe)
    print(forecaster._last_forecast_df)
    print("_start_dt", forecaster._start_dt)
    print("_end_dt", forecaster._end_dt)
    print("_completeness", forecaster._completeness)
    print("\n\n")
    # plot_f(main_df=forecaster._dataframe.tail(n=1_000), forecast_df=forecaster._last_forecast_df.tail(n=1_000))
    plot_forecasts_df(dataframe=forecaster._last_forecast_df.tail(n=1_000))


def run_fabrication_forecasting_example() -> None:
    # dt = pd.Timedelta("PT30S")
    forecaster: SensorMeasurementForecaster = SensorMeasurementForecaster(
        sensor_id="sensor-1",
        physical_quantity="temperature",
        topic_name="ga.sensor_telemetry_measurement_event.0001.temperature",
        frequency_in_seconds=30,
        debug=True,
        auto=True,
    )

    future_periods: int = 20

    forecaster.refresh_train()
    forecaster.refresh_forecast(future_periods=future_periods)
    _display_results(forecaster=forecaster)

    time.sleep(60 * 5)

    forecaster.refresh_data()
    forecaster.refresh_forecast(future_periods=future_periods)
    _display_results(forecaster=forecaster)
