from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from datetime import datetime, UTC
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
import os
from config import WRITE_TIMER, POLLING_INTERVAL, data_file_path, WEB_DRIVER_TIMEOUT, EMAIL, PASSWORD, GECKODRIVER_PATH, RETRIES
from utils import get_creator_config
import tables as tb
import time
import logging
from logger import logger as default_logger
from pydantic import BaseModel
import multiprocessing as mp

module_logger = logging.getLogger(f"{default_logger.name}.creator_poller")
queue = mp.Queue()


class H5Data(tb.IsDescription):
    datetime = tb.UInt64Col()
    creator = tb.StringCol(255)
    page = tb.StringCol(255)
    online = tb.BoolCol()


class PollerData(BaseModel):
    datetime: datetime
    creator: str
    page: str
    online: bool


class CreatorPoller:
    def __init__(self, creator: str, pages: list[str], queue: mp.Queue):
        self.logger = logging.getLogger(f"{module_logger.name}.{self.__class__.__name__}")
        self.logger.info("Initializing CreatorPoller for creator %s" % creator)
        self.logger.debug("Initializing CreatorPoller for creator %s with pages %s" % (creator, pages))
        self.driver = self._driver_init()
        self.creator = creator
        self.pages = pages
        self.queue = queue

    def _driver_login(self, driver):
        self.logger.debug("Logging in to OnlyFans")
        try:
            email = WebDriverWait(driver, WEB_DRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.NAME, "email"))
            )
            password = WebDriverWait(driver, WEB_DRIVER_TIMEOUT).until(
                EC.presence_of_element_located((By.NAME, "password"))
            )
            email.clear()
            email.send_keys(EMAIL)
            password.clear()
            password.send_keys(PASSWORD)
            password.send_keys(Keys.RETURN)
            self.logger.debug("Logged in to OnlyFans")
        except TimeoutException:
            self.logger.info("Timeout while waiting for email and password fields")

    def _driver_init(self):
        os.environ["MOZ_HEADLESS"] = "1"
        options = Options()
        options.headless = True
        service = Service(executable_path=GECKODRIVER_PATH)
        driver = webdriver.Firefox(options=options, service=service)
        driver.get("https://www.onlyfans.com")
        self._driver_login(driver)
        self.logger.debug("Correctly initialized driver")
        return driver

    def get_page_online_status(self, page):
        self.logger.info(f"Checking page {page}")
        while True:
            try:
                is_online = False
                self.driver.get(page)
                try:
                    res = WebDriverWait(self.driver, WEB_DRIVER_TIMEOUT).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, "a.online_status_class"))
                    )
                    self.logger.debug("Page %s: %s" % (page, res.get_attribute('class')))
                    # Wait for the online status to update
                    time.sleep(POLLING_INTERVAL)
                    try:
                        # Fetch the online status element again in case it has changed in the meantime
                        res = self.driver.find_element(By.CSS_SELECTOR, "a.online_status_class")
                        self.logger.debug("Page %s: %s" % (page, res.get_attribute('class')))
                        self.driver.find_element(By.CSS_SELECTOR, "a.online")
                        self.logger.info(f"{self.creator} is online on page {page}")
                        is_online = True
                    except NoSuchElementException:
                        self.logger.info(f"{self.creator} is offline on page {page}")
                        is_online = False
                    data_point = PollerData(
                        datetime=datetime.now(UTC),
                        creator=self.creator,
                        page=page,
                        online=is_online
                    )
                    self.queue.put(data_point)
                except TimeoutException:
                    self.logger.info(f"Timeout while waiting for a.online_status_class for creator {self.creator}")
            except WebDriverException as e:
                self.logger.error(f"WebDriverException while checking {self.creator} online status. Exception: {e}")
                self.logger.info(f"Restarting {self.creator} WebDriver")
                self.driver = self._driver_init()

    @classmethod
    def get_creator_online_status(cls, creator: str, pages: list[str], queue: mp.Queue):
        poller = cls(creator, pages, queue)
        processes = []
        for page in pages:
            p = mp.Process(target=poller.get_page_online_status, args=(page,))
            p.start()
            processes.append(p)
        for p in processes:
            p.join()
        # await asyncio.gather(*[self.get_page_online_status(page) for page in self.pages])
        poller.driver.close()


def init_data_file():
    file = tb.open_file(data_file_path, "a")
    creator_config = get_creator_config()
    for creator in creator_config.creators:
        try:
            creator_group = file.create_group("/", creator, creator)
            file.create_table(creator_group, "data", H5Data, "Data")
        except tb.exceptions.NodeError as e:
            module_logger.error(f"Error when initializing data file. NodeError: {e}")
    module_logger.debug("Created/Opened data file")
    file.close()


def write_to_data(queue):
    module_logger.debug("Started service to write events to data file")
    buffer = []
    start = time.monotonic()
    while True:
        if (time.monotonic() - start) < WRITE_TIMER:
            module_logger.debug("Listening to queue for data events")
            data : PollerData = queue.get()
            buffer.append(data)
            module_logger.debug(f"Appended data event to buffer: {data}")
        else:
            if buffer:
                module_logger.info("Buffer is not empty, writing data to file")
                file = tb.open_file(data_file_path, "a")
                try:
                    creators_set = {event.creator for event in buffer}
                    for creator in creators_set:
                        module_logger.info(f"Writing data for creator {creator} to file")
                        table : tb.Table = file.get_node(f"/{creator}/data")
                        module_logger.debug(f"For creator {creator}, got table {table}")
                        for event in filter(lambda e: creator == e.creator, buffer):
                            module_logger.debug(f"Writing event {event} to table")
                            table.row['datetime'] = int(event.datetime.timestamp())
                            table.row['creator'] = event.creator
                            table.row['page'] = event.page
                            table.row['online'] = event.online
                            table.row.append()
                        table.flush()
                        module_logger.debug(f"Wrote data for creator {creator}")
                    start = time.monotonic()
                    buffer.clear()
                except Exception as e:
                    module_logger.error(f"Error while writing to file. Exception: {type(e)} {e}")
                file.close()


def check_all_creators_online_status():
    init_data_file()
    processes = []
    creator_config = get_creator_config()
    try:
        for creator in creator_config.creators:
            retries = 0
            while retries < RETRIES:
                # Sometimes OF gives an error page, keep trying until you get a login page
                try:
                    p = mp.Process(target=CreatorPoller.get_creator_online_status, args=(creator, creator_config.get_pages(creator), queue))
                    p.start()
                    processes.append(p)
                    # creator_pollers.append(CreatorPoller(creator, creator_config.get_pages(creator), queue))
                    break
                except Exception as e:
                    module_logger.error(f"Exception while creating poller for creator {creator}: {type(e)} {e}")
                    retries += 1
        for p in processes:
            p.join()
        # await asyncio.gather(*[poller.get_creator_online_status() for poller in creator_pollers], write_to_data(queue))
    except Exception as e:
        module_logger.error(f"Exception while awaiting pollers: {type(e)} {e}")


# def start_all_pollers():
#     asyncio.run(check_all_creators_online_status())


if __name__ == "__main__":
    check_all_creators_online_status()