import logging
from config import log_file_path, LOG_LEVEL
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("of_poller")
formatter = logging.Formatter("%(asctime)s - %(name)s - %(funcName)s - %(levelname)s - %(message)s")
stream_h = logging.StreamHandler()
stream_h.setFormatter(formatter)
stream_h.setLevel(LOG_LEVEL)
file_h = RotatingFileHandler(log_file_path.as_posix(), maxBytes=10000000, backupCount=5)
file_h.setFormatter(formatter)
file_h.setLevel(LOG_LEVEL)
logger.addHandler(file_h)
logger.addHandler(stream_h)
logger.setLevel(LOG_LEVEL)
