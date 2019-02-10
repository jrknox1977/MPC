import argparse
import boto3
import logging
import configparser
import time


# -----> MASTER CONTROL PROGRAM <-----
class mcp:
    def __init__(self):

        # -----> SETUP LOGGING <-----
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('/var/log/MCP_Master_Log.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
        self.logger.info("------------- MASTER CONTROL PROGRAM - ONLINE ------------- ")

        # -----> ARGPARSE <-----
        help_text = "MCP: MASTER CONTROL PROGRAM: automated system to rip picture from reddit and post them on twitter"
        arg_parser = argparse.ArgumentParser(description=help_text)
        arg_parser.add_argument("-V", "--version", help="Show program version", action="store_true")
        arg_parser.add_argument("--config", "-c", help="Configuration file name")

        args = arg_parser.parse_args()
        if args.version:
            print("MCP Version - IndyPy")
        if args.config:
            self.config = args.config
        else:
            print("\n    Please specify a configuration file.\n")
            exit()

        # -----> CONFIGPARSER <-----
        parser = configparser.ConfigParser()
        parser.read(self.config)
        self.client_id = parser['reddit']['client_id']
        self.client_secret = parser['reddit']['client_secret']
        self.username = parser['reddit']['username']
        self.password = parser['reddit']['password']
        self.user_agent = parser['reddit']['cat_bot']
        self.reddit_table_name = parser['dynamodb']['table_name']