import fire
from iotvm.tsgen.base import generate_synthetic_timeseries


# noinspection PyMethodMayBeStatic
class IoTVMCLI:
    def generate_synthetic_timeseries(self) -> None:
        generate_synthetic_timeseries()


if __name__ == "__main__":
    fire.Fire(IoTVMCLI)
