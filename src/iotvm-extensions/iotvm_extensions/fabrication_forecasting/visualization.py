import matplotlib.pyplot as plt
import pandas as pd


def plot_f(main_df: pd.DataFrame, forecast_df: pd.DataFrame) -> None:
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


def plot_forecasts_df(dataframe: pd.DataFrame) -> None:
    plt.figure(figsize=(18, 8))

    # plt.scatter
    plt.plot(
        dataframe["window"],
        dataframe["measurement"],
        # linewidth=1,
        # linestyle="solid",
        color="black",
        label="Measurements",
    )
    plt.plot(
        dataframe["window"],
        dataframe["prediction"],
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
