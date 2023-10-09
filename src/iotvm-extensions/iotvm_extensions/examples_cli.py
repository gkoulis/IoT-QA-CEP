import fire

from ._logging import set_up_logging
from ._prerequisites import set_up_prerequisites


# noinspection PyMethodMayBeStatic
class CLI:
    def run_fabrication_forecasting_example(self) -> None:
        from iotvm_extensions.examples.fabrication_forecasting import (
            run_fabrication_forecasting_example,
        )

        run_fabrication_forecasting_example()

    def run_sensor_simulation_example(self) -> None:
        from iotvm_extensions.examples.sensor_simulation import (
            run_sensor_simulation_example,
        )

        run_sensor_simulation_example()

    def run_client_example(self) -> None:
        from iotvm_extensions.examples.server_client import run_client_example

        run_client_example()

    # TODO Temporary.
    def run_prototype_example(self) -> None:
        from iotvm_extensions.examples.prototype import (
            run_example_20231009,
            run_example_20231010,
        )

        run_example_20231010()


if __name__ == "__main__":
    set_up_logging()
    set_up_prerequisites()
    fire.Fire(CLI)
