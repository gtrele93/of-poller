from datetime import datetime, UTC
from config import WRITE_TIMER, STATUS_POLLING_WAIT, data_file_path, WEB_DRIVER_TIMEOUT, EMAIL, PASSWORD
from utils import get_creator_config
import asyncio
import tables as tb
import time
import logging
from logger import logger as default_logger
from pydantic import BaseModel
from asyncio.queues import Queue as asyncioQueue
from playwright.async_api import async_playwright
from playwright.sync_api import sync_playwright
import playwright.async_api as playwright


module_logger = logging.getLogger(f"{default_logger.name}.creator_poller")
async_queue = asyncioQueue()


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
    def __init__(self, creator: str, pages: list[str], queue: asyncioQueue):
        self.logger = logging.getLogger(f"{module_logger.name}.{self.__class__.__name__}")
        self.logger.info("Initializing CreatorPoller for creator %s" % creator)
        self.logger.debug("Initializing CreatorPoller for creator %s with pages %s" % (creator, pages))
        self.creator = creator
        self.pages = pages
        self.queue = queue

    async def login(self, page: playwright.Page):
        self.logger.debug("Logging in to OnlyFans")
        email = page.locator('[name="email"]')
        password = page.locator('[name="password"]')
        try:
            await email.wait_for(timeout=WEB_DRIVER_TIMEOUT*1000, state="visible")
            await password.wait_for(timeout=WEB_DRIVER_TIMEOUT*1000, state="visible")
            await email.clear()
            await email.fill(value=EMAIL)
            await password.clear()
            await password.fill(value=PASSWORD)
            await password.press("Enter")
            self.logger.debug("Logged in to OnlyFans")
        except playwright.TimeoutError:
            self.logger.warning("Timeout while logging in to OnlyFans")

    async def get_page_online_status(self, browser: playwright.Browser, page_url: str):
        self.logger.info("Checking page %s" % page_url)
        page : playwright.Page = await browser.new_page()
        await page.goto(page_url)
        await self.login(page)
        while True:
            try:
                self.logger.debug("Reloading page %s" % page_url)
                await page.goto(page_url)
                is_online = False
            except playwright.TimeoutError:
                self.logger.error("Timeout while opening page %s, recreating page" % page_url)
                await page.close()
                page = await browser.new_page()
                await page.goto(page_url)
                await self.login(page)
                continue
            try:
                locator = page.locator("a.online_status_class").first
                await locator.wait_for(timeout=STATUS_POLLING_WAIT*1000, state="visible")
                self.logger.debug("Found online_status_class element for page %s" % page_url)
            except playwright.TimeoutError:
                self.logger.warning("Timeout while waiting for a.online_status_class for page %s, recreating page" % page_url)
                await page.close()
                page = await browser.new_page()
                await page.goto(page_url)
                await self.login(page)
                continue
            # Fetch the online status element
            # Have to wait a bit for the element to be visible
            locator_online = page.locator("a.online").first
            try:
                await locator_online.wait_for(timeout=STATUS_POLLING_WAIT*1000, state="visible")
                self.logger.debug("Found online element for page %s" % page_url)
                self.logger.info("%s is online on page %s" % (self.creator, page_url))
                is_online = True
            except playwright.TimeoutError:
                self.logger.debug("Timeout while waiting for a.online for page %s" % page_url)
                self.logger.info("%s is offline on page %s" % (self.creator, page_url))
                is_online = False
            data_point = PollerData(
                datetime=datetime.now(UTC),
                creator=self.creator,
                page=page.url,
                online=is_online
            )
            await self.queue.put(data_point)

    async def get_creator_online_status(self):
        async with async_playwright() as p:
            browser = await p.firefox.launch()
            await asyncio.gather(*[self.get_page_online_status(browser, page_url) for page_url in self.pages])


def init_data_file():
    file = tb.open_file(data_file_path, "a")
    creator_config = get_creator_config()
    for creator in creator_config.creators:
        try:
            creator_group = file.create_group("/", creator, creator)
            file.create_table(creator_group, "data", H5Data, "Data")
        except tb.exceptions.NodeError as e:
            module_logger.warning("Error when initializing data file. NodeError: %s" % e)
    module_logger.debug("Created/Opened data file")
    file.close()


async def write_to_data(queue):
    module_logger.debug("Started service to write events to data file")
    buffer = []
    start = time.monotonic()
    while True:
        if (time.monotonic() - start) < WRITE_TIMER:
            module_logger.debug("Listening to queue for data events")
            data : PollerData = await queue.get()
            buffer.append(data)
            module_logger.debug("Appended data event to buffer: %s" % data)
        else:
            if buffer:
                module_logger.info("Buffer is not empty, writing data to file")
                file = tb.open_file(data_file_path, "a")
                try:
                    creators_set = {event.creator for event in buffer}
                    for creator in creators_set:
                        module_logger.info("Writing data for creator %s to file" % creator)
                        table : tb.Table = file.get_node(f"/{creator}/data")
                        module_logger.debug("For creator %s, got table %s" % (creator, table))
                        for event in filter(lambda e: creator == e.creator, buffer):
                            module_logger.debug("Writing event %s to table" % event)
                            table.row['datetime'] = int(event.datetime.timestamp())
                            table.row['creator'] = event.creator
                            table.row['page'] = event.page
                            table.row['online'] = event.online
                            table.row.append()
                        table.flush()
                        module_logger.debug("Wrote data for creator %s to file" % creator)
                    start = time.monotonic()
                    buffer.clear()
                except Exception as e:
                    module_logger.error("Error while writing to file. Exception: %s %s" % (type(e), e))
                file.close()


async def check_all_creators_online_status():
    init_data_file()
    creator_pollers = []
    creator_config = get_creator_config()
    for creator in creator_config.creators:
        while True:
            # Sometimes OF gives an error page, keep trying until you get a login page
            try:
                creator_pollers.append(CreatorPoller(creator, creator_config.get_pages(creator), async_queue))
                break
            except Exception as e:
                module_logger.error("Exception while creating poller for creator %s: %s %s" % (creator, type(e), e))
                continue
    try:
        await asyncio.gather(*[poller.get_creator_online_status() for poller in creator_pollers], write_to_data(async_queue))
    except Exception as e:
        module_logger.error("Exception while awaiting pollers: %s %s" % (type(e), e))
        raise e


def start_all_pollers():
    asyncio.run(check_all_creators_online_status())


if __name__ == "__main__":
    start_all_pollers()