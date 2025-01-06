from pathlib import Path
import os

EMAIL = os.environ.get("EMAIL")
PASSWORD = os.environ.get("PASSWORD")
LOG_LEVEL = "DEBUG"
BIN_LENGTH_S = 60
WRITE_TIMER = 60
STATUS_POLLING_WAIT = 10
WEB_DRIVER_TIMEOUT = 4
data_file_path = Path(__file__).parent.parent.resolve() / "data" / "data.h5"
log_file_path = Path(__file__).parent.parent.resolve() / "logs" / "log.log"
creators_file_path = Path(__file__).parent.parent.resolve() / "src" / "creators.yaml"
