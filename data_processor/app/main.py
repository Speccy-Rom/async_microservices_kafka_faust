import logging
import faust
import json
import config_loader as config_loader
import metrics
from prometheus_client import start_http_server

SERVICE_NAME = "data-processor"
config = config_loader.Config()

logging.basicConfig(
    level=logging.getLevelName(config.get(config_loader.LOGGING_LEVEL)),
    format=config.get(config_loader.LOGGING_FORMAT))

logger = logging.getLogger(__name__)
