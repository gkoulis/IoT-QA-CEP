import datetime
import logging
import random
import warnings
import zoneinfo
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
import pmdarima as pm
from pmdarima.arima import ndiffs
from statsmodels.tools.sm_exceptions import ConvergenceWarning

from iotvm_extensions.mongodb import MongoClient, get_default_mongodb_client

_logger = logging.getLogger("iotvm_extensions.fabrication_forecasting.base")


def disable_statsmodels_convergence_warnings() -> None:
    warnings.simplefilter("ignore", ConvergenceWarning)


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
    physical_quantity: str,
    frequency_in_seconds: int,
    default_timestamp_gte: Optional[int] = None,
    default_timestamp_lte: Optional[int] = None,
) -> Tuple[Dict[pd.Timestamp, WindowedMeasurement], Optional[datetime.datetime], Optional[datetime.datetime],]:
    client: MongoClient = get_default_mongodb_client()
    collection = client["iotvmdb"]["universal"]

    default_timestamp_limit = {}
    if default_timestamp_gte is not None or default_timestamp_lte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"] = {}
    if default_timestamp_gte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"]["$gte"] = default_timestamp_gte
    if default_timestamp_lte is not None:
        default_timestamp_limit["real.timestamps.defaultTimestamp.long"]["$lte"] = default_timestamp_lte

    cursor = collection.aggregate(
        [
            {
                "$match": {
                    "real.sensorId": sensor_id,
                    "real.measurement.name": physical_quantity,
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
                            "date": "$dateTime",
                            "unit": "second",
                            "binSize": frequency_in_seconds,
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
    frequency_in_seconds: int,
    measurements_by_timestamp: Dict[pd.Timestamp, WindowedMeasurement],
) -> pd.DataFrame:
    frequency = f"{frequency_in_seconds}S"

    # Datetime Index with all periods.
    # --------------------------------------------------

    datetime_index: pd.DatetimeIndex = pd.date_range(start=start_dt, end=end_dt, freq=frequency)
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
        return pd.DataFrame(data=df_data)

    # If there are windows between end_dt and now_dt,
    # add them to final result.
    # --------------------------------------------------

    # @future TEMPORARY: Απενεργοποιημένο μέχρι να είμαι σίγουρος ότι είναι χρήσιμο!
    raise Exception("Unreachable!")

    # datetime_index: pd.DatetimeIndex = pd.date_range(
    #     start=end_dt, end=now_dt, freq=frequency, inclusive="right"
    # )
    # timestamp_list: List[pd.Timestamp] = datetime_index.to_list()
    #
    # if len(timestamp_list) <= 0:
    #     return pd.DataFrame(data=df_data)
    #
    # return pd.DataFrame(data=df_data)


def _merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, sort_by: Optional[str] = None) -> pd.DataFrame:
    df1_cols = df1.columns.tolist()
    df1_cols.sort()
    df2_cols = df2.columns.tolist()
    df2_cols.sort()
    assert df1_cols == df2_cols

    data = {**{column: [] for column in df1.columns.tolist()}}
    for column in data.keys():
        # data[column] = df1[column].values.tolist() + df2[column].values.tolist()
        data[column] = np.concatenate((df1[column].values, df2[column].values), axis=0)

    new_df: pd.DataFrame = pd.DataFrame(data=data)

    dt_columns = new_df.select_dtypes(include=["datetime64[ns]"]).columns
    for dt_column in dt_columns:
        new_df[dt_column] = new_df[dt_column].dt.tz_localize("UTC")

    if sort_by is not None:
        new_df.sort_values(by=[sort_by], inplace=True, ignore_index=True)

    return new_df


def _validate_frequency(dataframe: pd.DataFrame, column: str) -> None:
    if len(dataframe) == 0:
        # TODO add names to dataframes (as convention for debugging)!
        _logger.warning("Could not validate frequency because dataframe is empty!")
        return
    if len(dataframe) == 1:
        # TODO add names to dataframes (as convention for debugging)!
        single_value = dataframe[column].iloc[0]
        _logger.warning(f"Could not validate frequency because dataframe has only one row! (value : {single_value})")
        return
    assert len(dataframe[column]) == len(dataframe[column].unique())
    series = dataframe[column] - dataframe[column].shift(1)
    series = series.values.tolist()
    assert series[0] is None
    series.pop(0)
    series_set = set(series)
    assert len(series_set) == 1, f"{series_set} length is not one!"


def _auto_arima_model(params: Dict[str, Any], y_train: np.ndarray):
    params["y"] = y_train
    try:
        model = pm.auto_arima(**params)
    except Exception as ex:
        _logger.error("Failed to execute `pm.auto_arima`!", exc_info=ex)
        model = None

    return model


def _arima_model(params: Dict[str, Any], y_train: np.ndarray):
    try:
        model = pm.ARIMA(**params)
        model = model.fit(y=y_train)
    except Exception as ex:
        _logger.error("Failed to execute `pm.ARIMA` or `model.fit`!", exc_info=ex)
        model = None

    return model


def build_key(sensor_id: str, physical_quantity: str, topic_name: str, frequency_in_seconds: int) -> str:
    return f"{sensor_id}:{physical_quantity}:{topic_name}:{frequency_in_seconds}"


class SensorMeasurementForecaster:
    """
    Implementation for forecasting future sensor measurements.
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
        frequency_in_seconds: int,
        debug: bool,
        auto: bool,
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
        frequency_in_seconds : int
            Frequency in seconds.
            Με βάση αυτή τη συχνότητα οργανώνονται τα measurements σε χρονικά παράθυρα για το ARIMA.
            Δεν είναι απαραίτητο να είναι ίδιο με το size του time window στο composite aggregation.
        """
        self._sensor_id: str = sensor_id
        self._physical_quantity: str = physical_quantity
        self._topic_name: str = topic_name
        self._frequency_in_seconds: int = frequency_in_seconds
        self._key: str = build_key(
            sensor_id=sensor_id,
            physical_quantity=physical_quantity,
            topic_name=topic_name,
            frequency_in_seconds=frequency_in_seconds,
        )

        self._debug: bool = debug
        self._auto: bool = auto

        # Options.
        self._use_not_dt: bool = False

        self._start_dt: datetime.datetime | None = None
        self._end_dt: datetime.datetime | None = None

        self._model = None
        self._dataframe: pd.DataFrame | None = None
        self._completeness: float = 0.0
        self._last_forecast_df: pd.DataFrame | None = None
        self._should_refresh_forecast: bool = False

        self._ready: bool = False

    def get_key(self) -> str:
        return self._key

    def _reset(self) -> None:
        self._start_dt = None
        self._end_dt = None
        self._model = None
        self._dataframe = None
        self._completeness = 0.0
        self._last_forecast_df = None
        self._should_refresh_forecast = False
        self._ready = False

    def _calculate_completeness(self) -> None:
        if self._dataframe is None:
            self._completeness = 0.0
            return

        if self._dataframe.empty:
            self._completeness = 0.0
            return

        real: int = len(self._dataframe)
        expected: int = int(self._dataframe.query("missing == 0")["missing"].count())
        assert expected <= real
        self._completeness = 1 - ((real - expected) / real)

    def _handle_nan_values(self) -> None:
        assert bool(self._dataframe.tail(n=1).isnull().values.any()) is False, "Unreachable!"
        if self._dataframe.isnull().values.any():
            self._dataframe.interpolate(inplace=True)
        assert bool(self._dataframe.isnull().values.any()) is False, "Unreachable!"

    def describe(self) -> None:
        unique_values = list(self._dataframe["measurement"].unique())
        mean: float = float(self._dataframe["measurement"].mean())
        std: float = float(self._dataframe["measurement"].std())
        _logger.debug(
            f"Unique Values = {len(unique_values)}" f"\n" f"mean          = {mean}" f"\n" f"std           = {std}"
        )

    def is_ready(self) -> bool:
        return self._ready

    def get_completeness(self) -> float:
        return self._completeness

    def refresh_train(self) -> None:
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
        _logger.debug(f"Executing `refresh_train` for SensorMeasurementForecaster : {self._key}")

        self._reset()

        utc_now: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        self._start_dt = utc_now - datetime.timedelta(hours=24)
        self._end_dt = utc_now

        measurements_by_timestamp, start_dt, end_dt = _aggregate_and_transform(
            sensor_id=self._sensor_id,
            topic_name=self._topic_name,
            physical_quantity=self._physical_quantity,
            frequency_in_seconds=self._frequency_in_seconds,
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
            now_dt=utc_now if self._use_not_dt is True else None,
            frequency_in_seconds=self._frequency_in_seconds,
            measurements_by_timestamp=measurements_by_timestamp,
        )

        self._start_dt = dataframe.head(n=1)["window"].iloc[0].to_pydatetime()
        self._end_dt = dataframe.tail(n=1)["window"].iloc[0].to_pydatetime()

        # Finalize dataframe.
        # --------------------------------------------------

        self._dataframe = dataframe.copy(deep=True)
        del dataframe

        # Handle NaN values.
        # --------------------------------------------------

        self._handle_nan_values()

        # Validations.
        # --------------------------------------------------

        _validate_frequency(dataframe=self._dataframe, column="window")

        # Measure quality of dataframe.
        # --------------------------------------------------

        self._calculate_completeness()

        # Train model.
        # --------------------------------------------------

        y_train = self._dataframe["measurement"].values

        if self._auto is True:
            random_state = random.randint(1, 1_000_000_000)
            auto_arima_model_params = _get_default_auto_arima_params(y_train=None, random_state=random_state)

            auto_arima_model_params["trace"] = 2 if self._debug is True else False
            auto_arima_model_params["return_valid_fits"] = False
            # TODO Check -> http://alkaline-ml.com/pmdarima/tips_and_tricks.html

            _logger.debug(f"Training ARIMA with random_state={random_state}")
            self._model = _auto_arima_model(params=auto_arima_model_params, y_train=y_train)
        else:
            self._model = _arima_model(params={"order": (1, 0, 0)}, y_train=y_train)

        if self._model is None:
            self._reset()
        else:
            self._ready = True
            self._should_refresh_forecast = True

    def refresh_data(self) -> None:
        # _logger.debug(f"Executing `refresh_train` for SensorMeasurementForecaster : {self._key}")

        assert self._ready is True
        assert self._model is not None

        utc_now: datetime.datetime = datetime.datetime.now(tz=datetime.timezone.utc)

        # _logger.debug(f"Looking for values to update model in the dt range {self._end_dt} - {utc_now}")
        measurements_by_timestamp, start_dt, end_dt = _aggregate_and_transform(
            sensor_id=self._sensor_id,
            topic_name=self._topic_name,
            physical_quantity=self._physical_quantity,
            frequency_in_seconds=self._frequency_in_seconds,
            default_timestamp_gte=int(self._end_dt.timestamp() * 1_000),
            default_timestamp_lte=int(utc_now.timestamp() * 1_000),
        )

        # NOTICE: Το παραπάνω μπορεί να επιστρέψει ένα παράθυρο που υπάρχει ήδη,
        # δηλαδή, το τελευταίο. Αυτό δεν είναι απαραίτητα κακό. Σημαίνει,
        # ότι η υπάρχουσα τιμή του τελευταίου παραθύρου πρέπει να ανανεωθεί.
        # Αν θέλω να το απενεργοποιήσω, τότε βάζω ως default_timestamp_gte:
        # int(self._end_dt.timestamp() * 1_000) + (self._frequency_in_seconds * 1_000)
        # Αυτό μου εξασφαλίζει ότι δε επιστρέφει ποτέ ένα παράθυρο που υπάρχει ήδη
        # αλλά δε θα μου επιστρέφει ποτέ και τη νέα τιμή του τελευταίου παραθύρου
        # (δε σημαίνει ότι η τιμή αλλάζει πάντα).
        # UPDATE: Δυστυχώς η ανανέωση υπάρχουσας τιμής δε γίνεται
        # γιατί το `.update` του ARIMA προσθέτει μόνο νέες τιμές.
        # Και αυτό γιατί το auto_arima επιστρέφει fitted model.
        # Αν θέλω, πρέπει να αλλάξω την υλοποίηση και να δημιουργώ νέο model.
        # Δε νομίζω όμως να με συμφέρει.
        # Προς το παρόν, απλώς κάνω log για να δω πόσες φορές
        # η τελευταία τιμή πρέπει να αλλάξει
        # και κατά πόσο επηρεάζει την ακρίβεια του μοντέλου.

        if len(measurements_by_timestamp) <= 0:
            _logger.warning("measurements_by_timestamp Dict is empty!")
            return

        dataframe: pd.DataFrame = _measurements_by_timestamp_to_dataframe(
            start_dt=start_dt,
            end_dt=end_dt,
            now_dt=utc_now if self._use_not_dt is True else None,
            frequency_in_seconds=self._frequency_in_seconds,
            measurements_by_timestamp=measurements_by_timestamp,
        )
        assert len(dataframe) > 0

        # Handle existing last row and new first row.
        # --------------------------------------------------

        # @PHD_DOCS Κανονικά, πρέπει μόνο η τελευταία τιμή να μπορεί να διαφέρει.
        # Δηλαδή, δε γίνεται κάποια τιμή εκτός από την τελευταία να έχει αλλάξει.
        # Οπότε, πρέπει να γίνεται log, ώστε ο χρήστης να ξέρει πότε το μοντέλο
        # λειτουργεί με τιμές που έχουν αλλάξει. Πιστεύω ότι αυτό δεν επηρεάζει
        # την ακρίβεια σε μεγάλο βαθμό. Ωστόσο, όπως πάντα, υπάρχουν ακραίες
        # περιπτώσεις που μπορεί να την επηρεάζει. Επίσης, σε μεγάλα χρονικά
        # παράθυρα όπου αυτή η μέθοδος θα τρέξει χωρίς να έχει κλείσει το παράθυρο,
        # αναμένω ότι θα επηρεάζει την ακρίβειά της σε σημαντικό βαθμό και απαιτείται
        # πιθανότητα διαφορετική υλοποίηση.

        last_window = self._dataframe["window"].iloc[-1]

        existing_count: int = len(dataframe.query(f"window <= '{last_window}'"))
        if existing_count > 0:
            # TO-DO @_logger disabled temporarily.
            # _logger.debug(
            #     f"dataframe has {existing_count} existing values. These values will be removed."
            # )
            pass

        # @future : Log changed values! IMPORTANT!

        dataframe = dataframe.query(f"window > '{last_window}'").copy(deep=True)

        if dataframe.empty:
            # TO-DO @_logger disabled temporarily.
            # _logger.warning("dataframe is empty!")
            return

        # Ensure time-series.
        # --------------------------------------------------

        _validate_frequency(dataframe=dataframe, column="window")
        _validate_frequency(dataframe=self._dataframe, column="window")

        # Count new values.
        # --------------------------------------------------

        new_values_count: int = len(dataframe)
        new_values = dataframe["measurement"].values  # aka y_train
        assert new_values_count > 0

        # Update _end_dt.
        # --------------------------------------------------

        assert start_dt is not None
        assert end_dt is not None
        assert end_dt >= self._end_dt
        self._end_dt = end_dt

        # Merge new values with existing.
        # --------------------------------------------------

        self._dataframe = _merge_dataframes(df1=self._dataframe, df2=dataframe, sort_by="window")
        del dataframe

        # Measure quality of dataframe.
        # --------------------------------------------------

        self._calculate_completeness()

        # Handle NaN values.
        # --------------------------------------------------

        self._handle_nan_values()

        # Validations.
        # --------------------------------------------------

        _validate_frequency(dataframe=self._dataframe, column="window")

        # Update model.
        # --------------------------------------------------

        # TO-DO @_logger disabled temporarily.
        # _logger.debug(
        #     f"Updating {self._key} model with {new_values_count} new values "
        #     f"and new _end_dt {str(self._end_dt)}. "
        #     f"The last window is : {self._dataframe['window'].iloc[-1]}"
        # )
        self._model.update(new_values)
        self._should_refresh_forecast = True

    def refresh_forecast(self, future_periods: int) -> None:
        assert self._ready is True
        assert self._model is not None

        if self._should_refresh_forecast is False:
            return

        self._should_refresh_forecast = False

        predictions_in_sample = self._model.predict_in_sample()
        df1: pd.DataFrame = self._dataframe.copy(deep=True)
        df1["prediction"] = predictions_in_sample
        df1["future"] = 0

        predictions = self._model.predict(future_periods)
        assert len(predictions) == future_periods

        assert self._end_dt == df1["window"].iloc[-1]
        # start_ = self._end_dt
        start_ = df1["window"].iloc[-1]

        frequency: str = f"{self._frequency_in_seconds}S"
        future_timestamps: List[pd.Timestamp] = pd.date_range(
            start=start_,
            periods=future_periods + 1,
            freq=frequency,
            inclusive="both",
            tz=zoneinfo.ZoneInfo("UTC"),
        ).to_list()
        del future_timestamps[0]

        future_timestamps_alt = []
        for i in range(1, future_periods + 1):
            future_timestamps_alt.append(start_ + pd.Timedelta(seconds=(i * self._frequency_in_seconds)))

        assert future_timestamps_alt == future_timestamps

        df2: pd.DataFrame = pd.DataFrame(
            data={
                "window": future_timestamps,
                "measurement": predictions,
                "prediction": predictions,
            }
        )
        df2["missing"] = 1
        df2["future"] = 1

        self._last_forecast_df = _merge_dataframes(df1=df1, df2=df2, sort_by="window")

        nanoseconds: int = (self._frequency_in_seconds * 1_000_000_000) - 1
        t_delta: pd.Timedelta = pd.Timedelta(nanoseconds=nanoseconds)
        self._last_forecast_df["window_end"] = self._last_forecast_df["window"] + t_delta

    def forecast(
        self,
        steps_ahead: int,
        start_timestamp: pd.Timestamp,
        end_timestamp: pd.Timestamp,
    ) -> Optional[Dict]:
        assert steps_ahead > 0
        assert end_timestamp > start_timestamp
        inferred_window_delta: pd.Timedelta = end_timestamp - start_timestamp
        inferred_seconds: float = inferred_window_delta.total_seconds()

        if inferred_seconds > self._frequency_in_seconds:
            _logger.warning(
                f"inferred_seconds is greater than _frequency_in_seconds "
                f"({inferred_seconds} > {self._frequency_in_seconds})"
            )
            return None

        # query_string = f"(window <= '{start_timestamp}' < '{end_timestamp}' <= window_end) and (future == 1)"
        # query_string = f"(window <= '{start_timestamp}' <= window_end) and (future == 1)"
        query_string = f"(window <= '{start_timestamp}' <= window_end)"
        result_df: pd.DataFrame = self._last_forecast_df.query(query_string)

        fitted_count: int = len(self._last_forecast_df)
        # non_futured_count: int = int(self._last_forecast_df.query("future == 0").count())
        future_count: int = len(self._last_forecast_df.query("future == 1"))

        def _logger_debug(reason: str) -> None:
            _logger.debug(
                f"COULD NOT PROVIDE FORECAST: {reason}"
                f"\n"
                f"requested time window : {start_timestamp} - {end_timestamp} ({inferred_seconds} seconds)"
                f"\n"
                f"requested steps ahead : {steps_ahead}"
                f"\n"
                f"first window          : {self._last_forecast_df['window'].iloc[0]}"
                f"\n"
                f"last window           : {self._last_forecast_df['window'].iloc[-1]}"
                f"\n"
                f"fitted values         : {fitted_count}"
                f"\n"
                f"future values         : {future_count}"
                f"\n"
            )

        if result_df.empty:
            _logger_debug(reason="result_df is empty")
            return None

        if inferred_seconds <= self._frequency_in_seconds:
            assert len(result_df) == 1

        result_df = result_df.tail(n=1).copy(deep=True)
        assert len(result_df) == 1

        i1 = result_df.index[0]
        i2 = self._last_forecast_df.query("future == 0").index[-1]
        i_diff = i1 - i2
        # negative diff means that a forecasting was requested for an existing value!
        if i_diff > steps_ahead:
            _logger_debug(reason=f"i_diff {i_diff} > steps_ahead {steps_ahead} (i1 = {i1}, i2 = {i2})")
            return None

        result: Dict[str, Any] = result_df.to_dict(orient="records")[0]
        result["final_value"] = result["prediction"]
        if i_diff < 0:
            result["final_value"] = result["measurement"]

        # TODO change `timeDifference` if `future` clause is not present.
        #  χρησιμοποίησε βασικά για όλα το start timestamp!
        metrics = {
            "timeSteps": i_diff,
            "timeDifference": (result["window"] - self._dataframe["window"].iloc[-1]).total_seconds(),
            "completeness": self._completeness,
        }

        return {**result, "metrics": metrics}
