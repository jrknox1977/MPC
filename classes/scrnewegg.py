import feedparser
import logging
from boto3 import resource


# -----> SCRAPE NEW EGG CLASS <-----
class ScrNewegg:
    def __init__(self):
        # ----> Initiate Logging <----
        self.LOG_FILE = 'MCP_Master_Log.log'
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler(self.LOG_FILE)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.info("Newegg Scrape Started")

        # The boto3 dynamoDB resource
        self.newegg_table_name = 'NewEgg'
        self.dynamodb_resource = resource('dynamodb')
        self.newegg_table = self.dynamodb_resource.Table(self.newegg_table_name)

        # ----> Class Attributes <----
        self.url = "https://www.newegg.com/Product/RSS.aspx?Submit=RSSDailyDeals&Depa=0"
        self.curr_feed = feedparser.parse(self.url)
        self.last_updated = self.curr_feed['feed']['updated']
        self.logger.info("Feed last updated: " + self.last_updated)
        self.curr_entries = []
        self.get_entries()

    def get_entries(self):
        i = 1
        for entry in self.curr_feed['entries']:
            if str(entry['title']).find('RYZEN') >= 0 or \
                   str(entry['title']).find('GeForce') >= 0 or\
                   str(entry['title']).find('case') >= 0 or \
                   str(entry['title']).find('DDR4') >= 0 or \
                   str(entry['title']).find('Power Supply') >= 0:
                self.curr_entries.append({'title': entry['title'], 'link': entry['link'],
                                          'entry_date': self.last_updated + ' ' + str(i)})
                i += 1






