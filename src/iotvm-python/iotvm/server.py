import json

from flask import Flask, request, jsonify

from iotvm.forecasting.base import (
    MultiSensorMeasurementForecastingWrapper,
    WindowMeasurementForecast,
)

# TODO Scheduler to re-train models.
#  https://stackoverflow.com/questions/21214270/how-to-schedule-a-function-to-run-every-hour-on-flask


_M_WRAPPER: MultiSensorMeasurementForecastingWrapper = (
    MultiSensorMeasurementForecastingWrapper()
)


app = Flask(__name__)


@app.route(
    "/ensure-sensor-telemetry-measurement-value-forecasting-context", methods=["POST"]
)
def ensure_sensor_telemetry_measurement_value_forecasting_context():
    data = json.loads(request.data)
    params = {
        "sensor_id": data["sensor_id"],
        "physical_quantity": data["physical_quantity"],
        "topic_name": data["topic_name"],
        "frequency": data["frequency"],
    }
    _M_WRAPPER.initialize(**params)
    _M_WRAPPER.train(**params)
    return jsonify(params)


@app.route("/forecast-sensor-telemetry-measurement-value", methods=["POST"])
def forecast_sensor_telemetry_measurement_value():
    data = json.loads(request.data)
    params = {
        "sensor_id": data["sensor_id"],
        "physical_quantity": data["physical_quantity"],
        "topic_name": data["topic_name"],
        # TODO Temporary. FIX IT
        "frequency": data["frequency"][2:],
        "window_start_timestamp": data["window_start_timestamp"],
        "window_end_timestamp": data["window_end_timestamp"],
    }
    result: WindowMeasurementForecast = _M_WRAPPER.forecast(**params)
    response_data = {
        **params,
        **result.to_dict(),
    }
    return jsonify(response_data)
