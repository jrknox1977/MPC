from classes import scrnewegg
from classes import dynamo_db_wrappers
import logging


def setup_logging():
    # ----> Initiate Logging <----
    log_file = '/var/log/MCP_Master_Log.log'
    klogger = logging.getLogger(__name__)
    handler = logging.FileHandler(log_file)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    klogger.addHandler(handler)
    klogger.setLevel(logging.INFO)
    return klogger


# ----> Create Logger <----
logger = setup_logging()
logger.info("MCP is running......")

# ----> Create Objects <----
new_scrape = scrnewegg.ScrNewegg()
ddb = dynamo_db_wrappers.DynamodbWrappers()

for entries in new_scrape.curr_entries:
    ddb.add_item(new_scrape.newegg_table_name, entries)
logger.info("Items added to DynamoDB")
