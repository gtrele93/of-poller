import yaml
from config import creators_file_path, REDIS_HOST
from logger import logger as default_logger
import logging
import time
import redis

module_logger = logging.getLogger(f"{default_logger.name}.utils")

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class CreatorConfig(metaclass=Singleton):
    def __init__(self):
        self.logger = logging.getLogger(f"{module_logger.name}.{self.__class__.__name__}")
        with open(creators_file_path, 'r') as file:
            self.creators_config = yaml.safe_load(file)
        self.logger.debug("Creators loaded: %s" % self.creators)
    
    @property
    def creators(self) -> list[str]:
        return [c["name"] for c in self.creators_config["creators"]]

    def get_pages(self, creator_name):
        for creator in self.creators_config["creators"]:
            if creator["name"] == creator_name:
                return creator["pages"]


class RedisConnection(metaclass=Singleton):
    def __init__(self):
        self.logger = logging.getLogger(f"{module_logger.name}.{self.__class__.__name__}")
        self.connection = None
        self.connect()

    def connect(self):
        wait = 0
        while self.connection is None:
            self.logger.debug(f"Waiting for connection {wait} secs")
            time.sleep(wait)
            try:
                self.connection = redis.Redis(
                    host=REDIS_HOST,
                    port=6379,
                    db=0
                )
            except Exception as e:
                self.logger.error(f"Error while connecting to Redis: {e}")
                wait = (wait + 1) ** 2
                if wait > 30:
                    wait = 0
        self.logger.info("Connection established")

    def get_connection(self):
        return self.connection


def get_creator_config() -> CreatorConfig:
    return CreatorConfig()


def get_redis_connection():
    return RedisConnection().get_connection()
