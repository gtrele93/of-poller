from datetime import datetime, UTC
from config import STATUS_POLLING_WAIT, WEB_DRIVER_TIMEOUT, EMAIL, PASSWORD, REDIS_QUEUE
from utils import get_creator_config, get_redis_connection
import logging
from logger import logger as default_logger
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
import playwright.async_api as playwright
import redis
import msgpack


module_logger = logging.getLogger(f"{default_logger.name}.creator_poller")

class PollerData(BaseModel):
    timestamp: int
    creator: str
    page: str
    online: bool


class CreatorPoller:
    def __init__(self, creator: str, pages: list[str], redis_connection: redis.Redis):
        self.logger = logging.getLogger(f"{module_logger.name}.{self.__class__.__name__}")
        self.logger.info("Initializing CreatorPoller for creator %s" % creator)
        self.logger.debug("Initializing CreatorPoller for creator %s with pages %s" % (creator, pages))
        self.creator = creator
        self.pages = pages
        self.redis_connection = redis_connection

    def _page_restart(self, browser: playwright.Browser, page: playwright.Page):
        self.logger.debug("Recreating page")
        page.close()
        return browser.new_page()

    def _redis_push(self, data: PollerData):
        json_data = data.model_dump()
        self.logger.debug("Pushing data to redis queue: %s" % json_data)
        self.redis_connection.lpush(REDIS_QUEUE, msgpack.packb(json_data))

    def login(self, page: playwright.Page):
        self.logger.debug("Logging in to OnlyFans")
        email = page.locator('[name="email"]')
        password = page.locator('[name="password"]')
        try:
            email.wait_for(timeout=WEB_DRIVER_TIMEOUT*1000, state="visible")
            password.wait_for(timeout=WEB_DRIVER_TIMEOUT*1000, state="visible")
            email.clear()
            email.fill(value=EMAIL)
            password.clear()
            password.fill(value=PASSWORD)
            password.press("Enter")
            self.logger.debug("Logged in to OnlyFans")
        except playwright.TimeoutError:
            self.logger.warning("Timeout while logging in to OnlyFans")

    def get_page_online_status(self, browser: playwright.Browser, page_url: str):
        self.logger.info("Checking page %s" % page_url)
        page : playwright.Page = browser.new_page()
        is_online = False
        page.goto(page_url)
        self.login(page)
        while True:
            try:
                self.logger.debug("Going to page %s" % page_url)
                page.goto(page_url)
            except playwright.TimeoutError:
                self.logger.error("Timeout while going to page %s" % page_url)
                page = self._page_restart(browser, page)
                continue
            try:
                locator = page.locator("a.online_status_class").first
                locator.wait_for(timeout=STATUS_POLLING_WAIT*1000, state="visible")
                self.logger.debug("Found online_status_class element for page %s" % page_url)
                break
            except playwright.TimeoutError:
                self.logger.warning("Timeout while waiting for a.online_status_class for page %s" % page_url)
                page = self._page_restart(browser, page)
                continue
        # Fetch the online status element
        # Have to wait a bit for the element to be visible
        locator_online = page.locator("a.online").first
        try:
            locator_online.wait_for(timeout=STATUS_POLLING_WAIT*1000, state="visible")
            self.logger.debug("Found online element for page %s" % page_url)
            self.logger.info("%s is online on page %s" % (self.creator, page_url))
            is_online = True
        except playwright.TimeoutError:
            self.logger.debug("Timeout while waiting for a.online for page %s" % page_url)
            self.logger.info("%s is offline on page %s" % (self.creator, page_url))
            is_online = False
        data_point = PollerData(
            timestamp=int(datetime.now(UTC).timestamp()),
            creator=self.creator,
            page=page.url,
            online=is_online
        )
        self._redis_push(data_point)

    def get_creator_online_status(self):
        with sync_playwright() as p:
            browser = p.firefox.launch()
            for page_url in self.pages:
                self.get_page_online_status(browser, page_url)

def check_all_creators_online_status(redis_connection: redis.Redis):
    creator_pollers = []
    creator_config = get_creator_config()
    for creator in creator_config.creators:
        while True:
            # Sometimes OF gives an error page, keep trying until you get a login page
            try:
                creator_pollers.append(CreatorPoller(creator, creator_config.get_pages(creator), redis_connection))
                break
            except Exception as e:
                module_logger.error("Exception while creating poller for creator %s: %s %s" % (creator, type(e), e))
                continue
    while True:
        for poller in creator_pollers:
            try:
                poller.get_creator_online_status()
            except Exception as e:
                module_logger.error("Exception while executing poller for creator %s: %s %s" % (creator, type(e), e))
                raise e


if __name__ == "__main__":
    check_all_creators_online_status(get_redis_connection())