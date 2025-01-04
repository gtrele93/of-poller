import yaml
from config import creators_file_path
from logger import logger as default_logger
import logging

module_logger = logging.getLogger(f"{default_logger.name}.utils")

class Singleton(type):
    _instances = {}

    def __call(cls, *args, **kwargs):
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




def get_creator_config() -> CreatorConfig:
    return CreatorConfig()
