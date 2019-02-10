import boto3
import sys
import logging

# -----> SETUP LOGGING <-----
LOG_FILE = '/var/log/MCP_Master_Log.log'
logger = logging.getLogger(__name__)
handler = logging.FileHandler(LOG_FILE)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)
logger.info("Exterminating EC2 instance(s): " + str(sys.argv[1:]))
ec2 = boto3.resource('ec2')

# iterate through instance IDs and terminate them
for id in sys.argv[1:]:
    instance = ec2.Instance(id)
    logger.info(instance.terminate())
    print("TERMINATION COMPLETED")
