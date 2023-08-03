import fire
import logging
from iotvm.forecasting.base import run
from iotvm.generator.basic_generator import invoke as invoke_basic_generator
from iotvm.tsgen.base import generate_synthetic_timeseries


def _set_up_logging() -> None:
    # TODO Temporary.
    logging.basicConfig(filename="app1.log", level=logging.INFO)

    # TODO Example.
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # TODO Better format!
    formatter = logging.Formatter("%(name)-12s: %(levelname)-8s %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)
    # logging.getLogger("matplotlib").setLevel(logging.INFO)

    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        if logger.name == "iotvm":
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)


# noinspection PyMethodMayBeStatic
class IoTVMCLI:
    def basic_generator(self) -> None:
        invoke_basic_generator()

    def forecast_example(self) -> None:
        run()

    def generate_synthetic_timeseries(self) -> None:
        generate_synthetic_timeseries()


if __name__ == "__main__":
    _set_up_logging()
    fire.Fire(IoTVMCLI)
