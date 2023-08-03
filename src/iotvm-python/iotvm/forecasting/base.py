import datetime
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pmdarima as pm
from pmdarima.arima import ndiffs
from pymongo import MongoClient

_logger = logging.getLogger("iotvm.forecasting.base")


# TODO Move to another directory..file.
class MongoClientFactory:
    # TODO Create like binance!

    def __init__(self):
        self._clients: Dict[str, MongoClient] = {}

    def initialize(self) -> None:
        # TODO Only once
        self._clients["default"] = MongoClient(
            "mongodb://localhost:27017/?readPreference=primary&appname=IoTVMPython&ssl=false"
        )

    def get_default_client(self) -> MongoClient:
        return self._clients["default"]


mongo_client_factory: MongoClientFactory = MongoClientFactory()
mongo_client_factory.initialize()


@dataclass
class WindowedMeasurement:
    window_dt: datetime.datetime
    timestamp_dt: datetime.datetime
    measurement: float
    window_timestamp: pd.Timestamp = field(init=False)

    def __post_init__(self):
        self.window_dt = self.window_dt.replace(tzinfo=datetime.timezone.utc)
        self.timestamp_dt = self.timestamp_dt.replace(tzinfo=datetime.timezone.utc)
        self.window_timestamp = pd.to_datetime(self.window_dt)


@dataclass
class WindowMeasurementForecast:
    measurement: float
    forecast_window_start_timestamp: int
    forecast_window_end_timestamp: int

    def to_dict(self) -> Dict:
        return {
            "measurement": self.measurement,
            "forecast_window_start_timestamp": self.forecast_window_start_timestamp,
            "forecast_window_end_timestamp": self.forecast_window_end_timestamp,
        }


def _aggregate_and_transform(
    sensor_id: str,
    topic_name: str,
    default_timestamp_gte: Optional[int] = None,
    default_timestamp_lte: Optional[int] = None,
) -> Tuple[
    Dict[pd.Timestamp, WindowedMeasurement],
    Optional[datetime.datetime],
    Optional[datetime.datetime],
]:
    client: MongoClient = mongo_client_factory.get_default_client()
    collection = client["iotvmdb"]["universal"]

    # TODO Try millis in bin size.
    #  Δεν γίνεται! Επίσης πρέπει να είναι πάνω από 1 δευτερόλεπτο.

    default_timestamp_limit = {}
    if default_timestamp_gte is not None or default_timestamp_lte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"] = {}
    if default_timestamp_gte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"][
            "$gte"
        ] = default_timestamp_gte
    if default_timestamp_lte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"][
            "$lte"
        ] = default_timestamp_lte

    cursor = collection.aggregate(
        [
            {
                "$match": {
                    "real.sensorId": sensor_id,
                    # TODO add name temperature!
                    "topicName": topic_name,
                    **default_timestamp_limit,
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "real.sensorId": 1,
                    "real.measurement.name": 1,
                    "real.measurement.value.double": 1,
                    "real.timestamps.defaultTimestamp.long": 1,
                }
            },
            {
                "$addFields": {
                    "sensorId": "$real.sensorId",
                    "name": "$real.measurement.name",
                    "value": "$real.measurement.value.double",
                    "dateTime": {"$toDate": "$real.timestamps.defaultTimestamp.long"},
                }
            },
            {
                "$addFields": {
                    "dateTimeWindow": {
                        "$dateTrunc": {
                            # TODO Do not forget to change.
                            "date": "$dateTime",
                            "unit": "second",
                            "binSize": 30,
                        }
                    }
                }
            },
            {"$project": {"real": 0}},
            {"$sort": {"dateTime": 1}},
            {
                "$group": {
                    "_id": "$dateTimeWindow",
                    "value": {"$last": "$value"},
                    "dateTime": {"$last": "$dateTime"},
                }
            },
            {"$sort": {"_id": 1}},
        ]
    )

    # Transform MongoDB items to measurements.
    # --------------------------------------------------

    start_dt: datetime.datetime | None = None
    end_dt: datetime.datetime | None = None

    measurements_by_timestamp: Dict[pd.Timestamp, WindowedMeasurement] = {}

    for measurement in cursor:
        wm: WindowedMeasurement = WindowedMeasurement(
            window_dt=measurement["_id"],
            timestamp_dt=measurement["dateTime"],
            measurement=measurement["value"],
        )
        measurements_by_timestamp[wm.window_timestamp] = wm

        if start_dt is None:
            start_dt = wm.window_dt
        end_dt = wm.window_dt

    if start_dt is not None:
        start_dt = start_dt.replace(tzinfo=datetime.timezone.utc)

    if end_dt is not None:
        end_dt = end_dt.replace(tzinfo=datetime.timezone.utc)

    return measurements_by_timestamp, start_dt, end_dt


def _get_default_auto_arima_params(y_train, random_state: Optional[int] = None) -> Dict:
    return dict(
        y=y_train,
        X=None,
        start_p=2,
        d=None,
        start_q=2,
        max_p=5,
        max_d=2,
        max_q=5,
        start_P=1,
        D=None,
        start_Q=1,
        max_P=2,
        max_D=1,
        max_Q=2,
        max_order=5,
        m=1,
        seasonal=True,
        stationary=False,
        information_criterion="aic",
        alpha=0.05,
        test="kpss",
        seasonal_test="ocsb",
        stepwise=True,
        n_jobs=1,
        start_params=None,
        trend=None,
        method="lbfgs",
        maxiter=50,
        offset_test_args=None,
        seasonal_test_args=None,
        suppress_warnings=True,
        error_action="trace",
        trace=False,
        random=False,
        random_state=random_state,
        n_fits=10,
        return_valid_fits=False,
        out_of_sample_size=0,
        scoring="mse",
        scoring_args=None,
        with_intercept="auto",
        sarimax_kwargs=None,
    )


def _measurements_by_timestamp_to_dataframe(
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    now_dt: Optional[datetime.datetime],
    frequency: str,
    measurements_by_timestamp: Dict[pd.Timestamp, WindowedMeasurement],
) -> pd.DataFrame:
    # Datetime Index with all periods.
    # --------------------------------------------------

    datetime_index: pd.DatetimeIndex = pd.date_range(
        start=start_dt, end=end_dt, freq=frequency
    )
    timestamp_list: List[pd.Timestamp] = datetime_index.to_list()

    # Join datetime index and measurements.
    # Where measurements do not exist add NaN.
    # --------------------------------------------------

    df_data: List[Dict] = []
    for timestamp in timestamp_list:
        if timestamp in measurements_by_timestamp:
            df_data.append(
                {
                    "window": timestamp,
                    "measurement": measurements_by_timestamp[timestamp].measurement,
                    "missing": 0,
                }
            )
        else:
            df_data.append({"window": timestamp, "measurement": None, "missing": 1})

    # not_dt is None, return dataframe with data until `end_dt`.
    # --------------------------------------------------

    if now_dt is None:
        pd.DataFrame(data=df_data)

    # If there are windows between end_dt and now_dt,
    # add them to final result.
    # --------------------------------------------------

    datetime_index: pd.DatetimeIndex = pd.date_range(
        start=end_dt, end=now_dt, freq=frequency, inclusive="right"
    )
    timestamp_list: List[pd.Timestamp] = datetime_index.to_list()

    if len(timestamp_list) <= 0:
        print("NO DIFF!!!!!!!!!!!!!!!!!!")  # TODO Remove.
        return pd.DataFrame(data=df_data)

    print(timestamp_list)  # TODO hey!

    # TODO Last value strategy? To help interpolation!

    return pd.DataFrame(data=df_data)


def _key(
    sensor_id: str,
    physical_quantity: str,
    topic_name: str,
    frequency: str,
) -> str:
    return f"{sensor_id}:{physical_quantity}:{topic_name}:{frequency}"


class SensorMeasurementForecastingWrapper:
    """
    Simple wrapper for forecasting future sensor measurements.
    It also contains the necessary operations to tune, train and refresh the model.

    ...

    References
    ----------

    .. https://pandas.pydata.org/docs/reference/api/pandas.Timedelta.html
    .. http://alkaline-ml.com/2019-12-18-pmdarima-1-5-2/
    .. http://alkaline-ml.com/pmdarima/tips_and_tricks.html
    .. http://alkaline-ml.com/pmdarima/refreshing.html
    """

    def __init__(
        self,
        sensor_id: str,
        physical_quantity: str,
        topic_name: str,
        frequency: str,
        debug: bool,
    ) -> None:
        """
        Constructor.

        ...

        Parameters
        ----------

        sensor_id : str
            The ID of the sensor.
            All measurements belong to this sensor.
            This parameter is used to limit query.
        physical_quantity : str
            The physical quantity of the measurements.
            This parameter is used to limit query.
        topic_name : str
            The topic name through which the measurements are distributed.
            This parameter is used to limit query.
        frequency : str
            The Pandas frequency.
            Με βάση αυτή τη συχνότητα οργανώνονται τα measurements σε χρονικά παράθυρα για το ARIMA.
            Δεν είναι απαραίτητο να είναι ίδιο με το size του time window στο composite aggregation.
        """
        self._sensor_id: str = sensor_id
        self._physical_quantity: str = physical_quantity
        self._topic_name: str = topic_name
        self._frequency: str = frequency
        self._debug: bool = debug

        self._start_dt: datetime.datetime | None = None
        self._end_dt: datetime.datetime | None = None

        self._model = None
        self._dataframe: pd.DataFrame | None = None
        self._last_forecast_df: pd.DataFrame | None = None

    def _reset(self) -> None:
        self._start_dt = None
        self._end_dt = None
        self._model = None
        self._dataframe = None
        self._last_forecast_df = None

    def train(self) -> None:
        """
        DTs explained:

        Right now, UTC date time is 2 Aug 2023 10:00:05
        So, the range in which I will look for values is
        between 1 Aug 2023 10:00:05 and 2 Aug 2023 10:00:05.

        However, values exist only inside the window below:
        (first and last value respectively, count is not related right now)
            1 Aug 2023 13:02:01 until 2 Aug 2023 08:58:02

        So, `_start_dt` is the window dt of the timestamp of the 1st occurrence
        and `_end_dt` is the window dt of the current (UTC) timestamp.

        If the last window has no value,
        the mean of the available values is used.

        For the rest of the windows which contain NaN, interpolation is performed.
        """

        self._reset()

        utc_now: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        self._start_dt = utc_now - datetime.timedelta(hours=24)
        self._end_dt = utc_now

        measurements_by_timestamp, start_dt, end_dt = _aggregate_and_transform(
            sensor_id=self._sensor_id,
            topic_name=self._topic_name,
            default_timestamp_gte=int(self._start_dt.timestamp() * 1_000),
            default_timestamp_lte=int(self._end_dt.timestamp() * 1_000),
        )

        if len(measurements_by_timestamp) <= 0:
            # Reset, _start_dt and _end_dt are not valid.
            self._reset()
            _logger.warning("measurements_by_timestamp Dict is empty!")
            return

        # Είναι σημαντικό να χρησιμοποιηθούν τα `_start_dt` και `_end_dt`
        # που δίνονται από τα window timestamps του πρώτου και του τελευταίου
        # record αντίστοιχα. Και αυτό διότι το MongoDB aggregation εξασφαλίζει
        # την εγκυρότητα αλλά και την ομοιομορφία των χρονικών παραθύρων
        # που δημιουργεί. Συνεπώς, είναι κρίσιμο να έχω start_dt και end_dt
        # σωστά για να κάνω και άλλους υπολογισμούς και ενέργειες
        # που περιλαμβάνουν και τη δημιουργία μελλοντικών χρονικών στιγμών
        # για χρήση στις προβλέψεις.
        # Αργότερα, μετά από αυτή τη μέθοδο, θα επιλέξω τα window timestamps
        # του πρώτου και του τελευταίου dataframe row αντίστοιχα.
        dataframe: pd.DataFrame = _measurements_by_timestamp_to_dataframe(
            start_dt=start_dt,
            end_dt=end_dt,
            now_dt=utc_now,
            frequency=self._frequency,
            measurements_by_timestamp=measurements_by_timestamp,
        )

        self._start_dt = dataframe.head(n=1)["window"].iloc[0].to_pydatetime()
        self._end_dt = dataframe.tail(n=1)["window"].iloc[0].to_pydatetime()
        # TODO Keep both.
        #  _end_dt of the window of the utc_now
        #  _end_dt of the last VALID value
        # TODO Option to select the first non-missing value! (από το τέλος).

        # Handle NaN values.
        # --------------------------------------------------

        # dataframe.interpolate(inplace=True, limit=10)
        dataframe.interpolate(inplace=True)

        has_nan: bool = bool(dataframe.isnull().values.any())
        if has_nan is True:
            self._reset()
            _logger.warning("dataframe has NaN values!")
            return

        # Inspection.
        # --------------------------------------------------

        if self._debug is True:
            unique_values = list(dataframe["measurement"].unique())
            mean: float = float(dataframe["measurement"].mean())
            std: float = float(dataframe["measurement"].std())
            _logger.debug(
                f"Unique Values = {len(unique_values)}"
                f"\n"
                f"mean          = {mean}"
                f"\n"
                f"std           = {std}"
            )

        # ARIMA (pmdarima).
        # --------------------------------------------------

        self._dataframe = dataframe.copy(deep=True)

        y_train = dataframe["measurement"].values

        # train, test = train_test_split(y, train_size=0.8)
        random_state = random.randint(1, 1_000_000_000)
        _logger.debug(f"Training ARIMA with random_state={random_state}")
        params: Dict = _get_default_auto_arima_params(
            y_train=y_train, random_state=random_state
        )

        params["start_p"] = 5
        params["start_q"] = 3
        params["max_p"] = 5
        params["max_d"] = 2
        params["max_q"] = 5
        params["m"] = 1
        params["trace"] = 2 if self._debug is True else False
        params["return_valid_fits"] = False
        # TODO set True, and get aic.

        try:
            self._model = pm.auto_arima(**params)
        except Exception as ex:
            _logger.error("Failed to execute auto_arima!", exc_info=ex)
            self._reset()

    def refresh(self) -> None:
        if self._model is None:
            _logger.warning("Could not refresh model because it is None!")
            return

        utc_now: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        measurements_by_timestamp, start_dt, end_dt = _aggregate_and_transform(
            sensor_id=self._sensor_id,
            topic_name=self._topic_name,
            default_timestamp_gte=int(self._end_dt.timestamp() * 1_000) + 1,
            default_timestamp_lte=int(utc_now.timestamp() * 1_000),
        )

        self._end_dt = end_dt

        if len(measurements_by_timestamp) <= 0:
            _logger.warning("measurements_by_timestamp Dict is empty!")
            return

        dataframe: pd.DataFrame = _measurements_by_timestamp_to_dataframe(
            start_dt=start_dt,
            end_dt=end_dt,
            now_dt=utc_now,
            frequency=self._frequency,
            measurements_by_timestamp=measurements_by_timestamp,
        )

        self._dataframe = pd.DataFrame(
            data={
                "window": np.concatenate(
                    (self._dataframe["window"].values, dataframe["window"].values),
                    axis=0,
                ),
                "measurement": np.concatenate(
                    (
                        self._dataframe["measurement"].values,
                        dataframe["measurement"].values,
                    ),
                    axis=0,
                ),
            }
        )

        # TODO Validate this method.

        dataframe.interpolate(inplace=True)
        assert bool(dataframe.isnull().values.any()) is False, "Unreachable!"

        y_train = dataframe["measurement"].values

        _logger.debug(f"Updating model with {len(y_train)} new values")
        self._model.update(y_train)

    def forecast(self, future_periods: int) -> pd.DataFrame:
        if self._model is None:
            # TODO Better
            return pd.DataFrame()

        predictions = self._model.predict(future_periods)

        future_timestamps: List[pd.Timestamp] = pd.date_range(
            start=self._end_dt,
            periods=future_periods + 1,
            freq=self._frequency,
            inclusive="both",
        ).to_list()
        del future_timestamps[0]

        forecast_df: pd.DataFrame = pd.DataFrame(
            data={
                "window": future_timestamps,
                "measurement": predictions,
            }
        )

        self._last_forecast_df = forecast_df.copy(deep=True)

        return forecast_df


def _plot(main_df: pd.DataFrame, forecast_df: pd.DataFrame) -> None:
    plt.figure(figsize=(18, 8))

    # plt.scatter
    plt.plot(
        main_df["window"],
        main_df["measurement"],
        # linewidth=1,
        # linestyle="solid",
        color="black",
        label="Measurements",
    )
    plt.plot(
        forecast_df["window"],
        forecast_df["measurement"],
        # linewidth=1,
        # linestyle="solid",
        color="orange",
        label="Forecasts",
    )

    plt.title("Forecasting")
    plt.xlabel("Date and Time")
    plt.ylabel("Measurements")
    plt.legend(loc="upper left")

    plt.show()


class MultiSensorMeasurementForecastingWrapper:
    def __init__(self):
        self._wrappers: Dict[str, SensorMeasurementForecastingWrapper] = {}

    def initialize(
        self, sensor_id: str, physical_quantity: str, topic_name: str, frequency: str
    ) -> None:
        key: str = _key(
            sensor_id=sensor_id,
            physical_quantity=physical_quantity,
            topic_name=topic_name,
            frequency=frequency,
        )
        self._wrappers[key] = SensorMeasurementForecastingWrapper(
            sensor_id=sensor_id,
            physical_quantity=physical_quantity,
            topic_name=topic_name,
            frequency=frequency,
            debug=True,
        )

    def train(
        self, sensor_id: str, physical_quantity: str, topic_name: str, frequency: str
    ) -> None:
        key: str = _key(
            sensor_id=sensor_id,
            physical_quantity=physical_quantity,
            topic_name=topic_name,
            frequency=frequency,
        )
        self._wrappers[key].train()

    def forecast(
        self, sensor_id: str, physical_quantity: str, topic_name: str, frequency: str, window_start_timestamp: int, window_end_timestamp: int
    ) -> WindowMeasurementForecast:
        key: str = _key(
            sensor_id=sensor_id,
            physical_quantity=physical_quantity,
            topic_name=topic_name,
            frequency=frequency,
        )
        self._wrappers[key].refresh()
        # TODO Forecast specific time window! Auto-calculate the required time future periods.
        forecast_df: pd.DataFrame = self._wrappers[key].forecast(future_periods=1)
        # TODO Fix sos.
        return WindowMeasurementForecast(
            measurement=float(forecast_df.tail(n=1)["measurement"].iloc[0]),
            # TODO Change ASAP!
            forecast_window_start_timestamp=window_start_timestamp,
            forecast_window_end_timestamp=window_end_timestamp
        )


def run() -> None:
    # dt = pd.Timedelta("PT30S")
    wrapper: SensorMeasurementForecastingWrapper = SensorMeasurementForecastingWrapper(
        sensor_id="sensor-1",
        physical_quantity="temperature",
        topic_name="ga.sensor_telemetry_measurement_event.0001.temperature",
        frequency="30S",
        debug=True,
    )
    wrapper.train()

    forecast_df: pd.DataFrame = wrapper.forecast(future_periods=20)

    print(wrapper._dataframe)
    print(forecast_df)
    print(wrapper._start_dt)
    print(wrapper._end_dt)
    print("\n\n")
    _plot(main_df=wrapper._dataframe.tail(n=1_000), forecast_df=forecast_df)

    time.sleep(60 * 5)

    wrapper.refresh()
    forecast_df: pd.DataFrame = wrapper.forecast(future_periods=20)

    print(wrapper._dataframe)
    print(forecast_df)
    print(wrapper._start_dt)
    print(wrapper._end_dt)
    print("\n\n")
    _plot(main_df=wrapper._dataframe.tail(n=1_000), forecast_df=forecast_df)
