import logging
from typing import Dict

from pymongo import MongoClient

import iotvm_extensions.config as config

_logger = logging.getLogger("iotvm_extensions.mongodb._base")


_MONGODB_CLIENTS: Dict[str, MongoClient] = {}


def initialize_default_mongodb_client() -> None:
    _MONGODB_CLIENTS["default"] = MongoClient(config.MONGODB_CONNECTION_STRING)


def get_default_mongodb_client() -> MongoClient:
    return _MONGODB_CLIENTS["default"]
