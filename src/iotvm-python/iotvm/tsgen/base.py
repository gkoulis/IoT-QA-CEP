"""
Synthetic Time-Series generator.

References
----------

mockseries library
    https://github.com/cyrilou242/mockseries
Nike timeseries generator library
    https://github.com/Nike-Inc/timeseries-generator/tree/master
Reference
    https://www.fsb.miamioh.edu/lij14/672_s16.pdf
Reference
    https://www.le.ac.uk/users/dsgp1/COURSES/TSERIES/2CYCLES.PDF
03 Time series with trend and seasonality components
    http://web.vu.lt/mif/a.buteikis/wp-content/uploads/2019/02/Lecture_03.pdf
Reference
    https://github.com/cetic/TSimulus
How to Identify and Remove Seasonality from Time Series Data with Python
    https://machinelearningmastery.com/time-series-seasonality-with-python/
    https://raw.githubusercontent.com/jbrownlee/Datasets/master/daily-min-temperatures.csv
"""

import datetime
from datetime import timedelta
from typing import Dict

import numpy as np
import pandas as pd

from mockseries.noise import GaussianNoise
from mockseries.seasonality import SinusoidalSeasonality, YearlySeasonality
from mockseries.trend import LinearTrend, FlatTrend
from mockseries.utils import datetime_range, plot_timeseries
from mockseries.utils.timedeltas import (
    JANUARY,
    FEBRUARY,
    MARCH,
    APRIL,
    MAY,
    JUNE,
    JULY,
    AUGUST,
    SEPTEMBER,
    OCTOBER,
    NOVEMBER,
    DECEMBER,
)


def generate_synthetic_timeseries() -> None:
    constraints = {
        JANUARY: 1.3,
        FEBRUARY: 2.1,
        MARCH: 6.9,
        APRIL: 10.0,
        MAY: 15.0,
        JUNE: 20.1,
        JULY: 21.9,
        AUGUST: 21.8,
        SEPTEMBER: 17.1,
        OCTOBER: 12,
        NOVEMBER: 7.0,
        DECEMBER: 2.7,
    }

    noise = GaussianNoise(mean=1, std=0.05)
    average = FlatTrend(12)
    warming = LinearTrend(0.1, timedelta(days=365.25), flat_base=1)
    yearly_seasonality_1 = SinusoidalSeasonality(
        amplitude=15, period=timedelta(days=365.25)
    )
    yearly_seasonality_2 = YearlySeasonality(constraints)
    yearly_seasonality = yearly_seasonality_1 + yearly_seasonality_2
    daily_seasonality = SinusoidalSeasonality(amplitude=5, period=timedelta(days=1))
    timeseries = noise * (average + (warming * yearly_seasonality) + daily_seasonality)

    year_params: Dict = {
        "num_years": 2,
        "start_time": datetime.datetime(2022, 1, 1),
    }

    average.preview_year(**year_params)
    yearly_seasonality_1.preview_year(**year_params)
    yearly_seasonality_2.preview_year(**year_params)
    yearly_seasonality.preview_year(**year_params)
    timeseries.preview_year(**year_params)

    time_points = datetime_range(
        granularity=timedelta(minutes=1),
        start_time=datetime.datetime(2023, 8, 1),
        end_time=datetime.datetime(2023, 8, 31),
    )
    time_series_values: np.ndarray = timeseries.generate(time_points=time_points)

    dataframe: pd.DataFrame = pd.DataFrame(
        data={"window": time_points, "estimation": time_series_values}
    )

    plot_timeseries(
        time_points=time_points,
        timeseries=time_series_values,
        save_path="estimations.png",
        graph_title="Estimations",
    )
    dataframe.to_excel("estimations.xlsx")
