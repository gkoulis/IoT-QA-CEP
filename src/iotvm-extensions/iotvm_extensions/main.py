import logging

import fire

from ._logging import set_up_logging
from ._prerequisites import set_up_prerequisites

_logger = logging.getLogger("iotvm_extensions.main")


# noinspection PyMethodMayBeStatic
class CLI: ...


if __name__ == "__main__":
    set_up_logging()
    set_up_prerequisites()
    fire.Fire(CLI)
