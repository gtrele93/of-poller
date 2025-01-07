from config import WRITE_TIMER, data_file_path, REDIS_QUEUE
from utils import get_creator_config, get_redis_connection
import tables as tb
import time
import logging
from logger import logger as default_logger
import redis
import msgpack


module_logger = logging.getLogger(f"{default_logger.name}.creator_poller")


class H5Data(tb.IsDescription):
    datetime = tb.UInt64Col()
    creator = tb.StringCol(255)
    page = tb.StringCol(255)
    online = tb.BoolCol()


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
    return get_redis_connection()


def _get_redis_data(redis_connection: redis.Redis):
    rawjob = redis_connection.rpop(REDIS_QUEUE)
    if rawjob is None:
        module_logger.debug("No data event in queue")
        return None
    data = msgpack.unpackb(rawjob)
    return data


def _write_data_to_table(table: tb.Table, events: str):
    for event in events:
        module_logger.debug("Writing event %s to table" % event)
        table.row['datetime'] = int(event['timestamp'])
        table.row['creator'] = event['creator']
        table.row['page'] = event['page']
        table.row['online'] = event['online']
        table.row.append()
    table.flush()


def write_to_data(redis_connection: redis.Redis):
    module_logger.debug("Started service to write events to data file")
    buffer = []
    start = time.monotonic()
    while True:
        time.sleep(0.5)
        if (time.monotonic() - start) < WRITE_TIMER:
            module_logger.debug("Listening to queue for data events")
            data = _get_redis_data(redis_connection)
            if data is not None:
                buffer.append(data)
                module_logger.debug("Appended data event to buffer: %s" % data)
        else:
            if buffer:
                module_logger.info("Buffer is not empty, writing data to file")
                file = tb.open_file(data_file_path, "a")
                try:
                    creators_set = {event['creator'] for event in buffer}
                    for creator in creators_set:
                        module_logger.info("Writing data for creator %s to file" % creator)
                        table : tb.Table = file.get_node(f"/{creator}/data")
                        module_logger.debug("For creator %s, got table %s" % (creator, table))
                        _write_data_to_table(table, filter(lambda e: creator == e['creator'], buffer))
                        module_logger.debug("Wrote data for creator %s to file" % creator)
                    start = time.monotonic()
                    buffer.clear()
                except Exception as e:
                    module_logger.error("Error while writing to file. Exception: %s %s" % (type(e), e))
                file.close()


if __name__ == "__main__":
    init_data_file()
    write_to_data(get_redis_connection())
