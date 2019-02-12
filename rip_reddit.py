import boto3
from boto3 import resource
from boto3.dynamodb.conditions import Key
import logging
import os
import praw
import psutil
import time
import urllib.request
from splunk_handler import SplunkHandler


class RipReddit:
    def __init__(self):

        # ----> Splunk Log to HEC <----
        splunk = SplunkHandler(
            host=
            port='8088',
            token=
            index='mcp',
            verify=False
        )

        # -----> SETUP LOGGING <-----
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('/tmp/MCP_Master_Log.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.addHandler(splunk)
        self.logger.setLevel(logging.INFO)

        # -----> S3 INFO <-----
        self.s3 = resource('s3')

        # -----> DUPLICATE PROCESS PROTECTION <-----
        self.pid_file = '/tmp/rip_reddit.pid'
        self.buddy_pid = '/tmp/tweet.pid'
        self.pid_check()
        self.process_to_check = 'python36'

        # -----> REDDIT INFO <-----
        self.reddit = praw.Reddit(client_id=os.environ['CLIENT_ID'],
                                  client_secret=os.environ['CLIENT_SECRET'],
                                  username=os.environ['USERNAME'],
                                  password=os.environ['PASSWORD'],
                                  user_agent=os.environ['USER_AGENT'])

        self.subreddit = self.reddit.subreddit(os.environ['SUBREDDIT'])

        self.table_name = os.environ['REDDIT_TABLE_NAME']

        # -----> PICTURE RIP INFO <-----
        self.rekog = boto3.client('rekognition', region_name='us-east-2')
        self.image_path = '/tmp/images'
        if not os.path.exists(self.image_path):
            os.makedirs(self.image_path)
            self.logger.info("Created path: " + self.image_path)

    @staticmethod
    def curr_time():
        return int(round(time.time() * 1000))

    # -----> CHECK IF PROGRAM IS ALREADY RUNNING <-----
    def pid_check(self):
        if os.path.exists(self.pid_file):
            self.logger.error("REDDIT PID file Already exists. Exiting...")
            exit(0)
        else:
            with open(self.pid_file, 'w+') as f:
                f.write(str(os.getpid()))
                self.logger.info("Reddit Cat Scraper Initiated!")

    # -----> BUDDY CHECK <-----
    def buddy_check(self):
        num = 0
        for proc in psutil.process_iter():
            try:
                if self.process_to_check.lower() in proc.name().lower():
                    num += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        if num < 2:
            os.remove(self.buddy_pid)

    # -----> STREAM DATA FROM REDDIT <-----
    def reddit_stream(self, dyna, num):
        i = 0
        for submission in self.subreddit.stream.submissions():
            try:
                if submission.url == '':
                    self.logger.info("EMPTY SUBMISSION URL! <-----")
                    continue
                sub_url = submission.url
                if submission.url[0] == ':':
                    self.logger.info("FOUND A URL WITH THE WEIRD COLON THING!!")
                    sub_url = submission.url[1:]
                cat_stuff = {'epoch_time': str(self.curr_time()),
                             'submission_id': submission.id,
                             'submission_url': sub_url,
                             'downloaded': 'False',
                             'stored_to_s3': 'False',
                             'tweet': 'Not_Ready'}
                self.logger.info("HERE IS THE COMPLETED URL!!! ----->" + sub_url)
                dyna.add_item(self.table_name, cat_stuff)
                self.logger.info('Submission: ' + submission.id + ' received!')
                time.sleep(5)
            except Exception as e:
                self.logger.error('Something went wrong, here it is: ' + str(e))
            if i >= num:
                return
            else:
                i += 1

    # -----> AWS REKOGNITION <-----
    def get_rekognition_info(self, filename):
        try:
            with open(filename, 'rb') as imgfile:
                results = self.rekog.detect_labels(Image={'Bytes': imgfile.read()}, MinConfidence=1)
            return results
        except Exception as e:
            self.logger.error("Could not open file: " + filename + " " + str(e))
            return {}

    # -----> SLEEPY TIME! <-----
    def sleepy_time(self, num):
        for i in range(0, num):
            self.logger.info("REDDIT RIPPER in sleepy time for " + str(num - i) + " minutes.")
            time.sleep(60)

    # -----> RIP PICTURE <-----
    def rip_picture(self, dyna):
        pic_check = dyna.scan_table_allpages(self.table_name, filter_key='tweet', filter_value='READY')
        if len(pic_check) < 5:
            self.logger.info(" !!! There appear to be " + str(len(pic_check)) + " pictures ready to tweet!")
            items = dyna.scan_table_allpages(self.table_name, filter_key='downloaded', filter_value='False')
            for item in items:
                image_file_name = item['submission_url'].split('/')[-1]

                if image_file_name.lower().endswith('.jpg'):
                    full_path = self.image_path + '/' + image_file_name
                    try:
                        urllib.request.urlretrieve(item['submission_url'], full_path)
                        self.logger.info("Image Retrieved: " + image_file_name)
                    except Exception as e:
                        self.logger.error('=======\n' + item['submission_url'] + ' FAILED: ' + str(e) + '\n=======')
                        continue
                    # dyn.delete_item(table_name, 'epoch_time', item['epoch_time'])
                    image_info = self.get_rekognition_info(full_path)
                    for stuff in image_info['Labels']:
                        if stuff['Name'] == 'Person' or stuff['Name'] == 'Human':
                            self.logger.info("There is a person in this image! -----> DEREZ <-----")
                            try:
                                dyna.delete_item(self.table_name, 'epoch_time', item['epoch_time'])
                                time.sleep(5)
                            except Exception as e:
                                self.logger.info("There was an ERROR. Deleting the DB entry: " + str(e))
                        elif stuff['Name'] == 'Cat':
                            self.logger.info("There is a CAT in this image! -----> APPROPRIATING CAT <-------")
                            self.s3.meta.client.upload_file(full_path, 'master-control-program', 'images/'
                                                            + image_file_name)
                            try:
                                response = dyna.update_item('epoch_time', item['epoch_time'], 'downloaded',
                                                            'False', 'True')
                                self.logger.info("DOWNLOADED: Updating DynamoDB: "
                                                 + str(response['ResponseMetadata']['HTTPStatusCode']))
                                time.sleep(5)
                            except Exception as e:
                                self.logger.error('ERROR: ' + str(e))
                            try:
                                response = dyna.update_item('epoch_time', item['epoch_time'], 'tweet',
                                                            'NOT_READY', 'READY')
                                self.logger.info("READY FOR TWEET: Updating DynamoDB: "
                                                 + str(response['ResponseMetadata']['HTTPStatusCode']))
                                time.sleep(5)
                            except Exception as e:
                                self.logger.error('ERROR: ' + str(e))
                                time.sleep(5)
                            try:
                                response = dyna.update_item('epoch_time', item['epoch_time'], 'stored_to_s3',
                                                            'False', 'True')
                                self.logger.info("UPLOADED TO S3: Updating DynamoDB: "
                                                 + str(response['ResponseMetadata']['HTTPStatusCode']))
                                time.sleep(5)
                            except Exception as e:
                                self.logger.error('ERROR: ' + str(e))
                                time.sleep(5)
                            try:
                                response = dyna.update_item('epoch_time', item['epoch_time'], 'submission_id',
                                                            item['submission_id'], item['submission_id'])
                                self.logger.info("UPLOADED TO S3: Updating DynamoDB: "
                                                 + str(response['ResponseMetadata']['HTTPStatusCode']))
                                time.sleep(5)
                            except Exception as e:
                                self.logger.error('ERROR: ' + str(e))
                                time.sleep(5)
                            try:
                                response = dyna.update_item('epoch_time', item['epoch_time'], 'submission_url',
                                                            item['submission_url'], item['submission_url'])
                                self.logger.info("UPLOADED TO S3: Updating DynamoDB: "
                                                 + str(response['ResponseMetadata']['HTTPStatusCode']))
                                time.sleep(5)
                            except Exception as e:
                                self.logger.error('ERROR: ' + str(e))
                                time.sleep(5)
                self.sleepy_time(5)

    # -----> Delete Not Ready <-----
    def delete_not_ready(self, dyna):
        not_ready = dyna.scan_table_allpages(self.table_name, filter_key='tweet', filter_value='Not_Ready')
        self.logger.info(" !!! There appear to be " + str(len(not_ready)) + " records 'not ready' ----> DEREZ <---- ")
        for item in not_ready:
            self.logger.info("DEREZ --> " + item['epoch_time'])
            dyna.delete_item(self.table_name, 'epoch_time', item['epoch_time'])
            time.sleep(5)


# -----> DynamoDB WRAPPER FUNCTIONS <-----
class DynamodbWrappers:
    def __init__(self):
        self.dynamodb_resource = resource('dynamodb', region_name='us-east-2')
        self.curr_table = ''

    def create_table(self, table_name):
        table = self.dynamodb_resource.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'MD5',
                    'KeyType': 'S'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'submission_url',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'submission_id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'downloaded',
                    'AttributeType': 'S'
                }

            ]
        )
        # Wait until the table exists
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    def set_table_name(self, name):
        self.curr_table = self.dynamodb_resource.Table(name)

    def get_table_metadata(self, table_name):
        """
        Get some metadata about chosen table.
        """
        table = self.dynamodb_resource.Table(table_name)

        return {
            'num_items': table.item_count,
            'primary_key_name': table.key_schema[0],
            'status': table.table_status,
            'bytes_size': table.table_size_bytes,
            'global_secondary_indices': table.global_secondary_indexes
        }

    def read_table_item(self, table_name, pk_name, pk_value):
        """
        Return item read by primary key.
        """
        table = self.dynamodb_resource.Table(table_name)
        response = table.get_item(Key={pk_name: pk_value})
        return response

    def add_item(self, table_name, col_dict):
        """
        Add one item (row) to table. col_dict is a dictionary {col_name: value}.
        """
        table = self.dynamodb_resource.Table(table_name)
        response = table.put_item(Item=col_dict)
        return response

    def delete_item(self, table_name, pk_name, pk_value):
        """
        Delete an item (row) in table from its primary key.
        """
        table = self.dynamodb_resource.Table(table_name)
        response = table.delete_item(Key={pk_name: pk_value})
        return response

    def scan_table_firstpage(self, table_name, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table. Can specify filter_key (col name) and its value to be filtered.
        This gets only first page of results in pagination. Returns the response.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.scan(FilterExpression=filtering_exp)
        else:
            response = table.scan()
        return response

    def scan_table_allpages(self, table_name, filter_key=None, filter_value=None):
        """
        Perform a scan operation on table. Can specify filter_key (col name) and its value to be filtered.
        This gets all pages of results.
        Returns list of items.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.scan(FilterExpression=filtering_exp)
        else:
            response = table.scan()

        items = response['Items']
        while True:
            print(len(response['Items']))
            if response.get('LastEvaluatedKey'):
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items += response['Items']
            else:
                break
        return items

    def query_table(self, table_name, filter_key=None, filter_value=None):
        """
        Perform a query operation on the table. Can specify filter_key (col name) and its value to be filtered.
        Returns the response.
        """
        table = self.dynamodb_resource.Table(table_name)

        if filter_key and filter_value:
            filtering_exp = Key(filter_key).eq(filter_value)
            response = table.query(KeyConditionExpression=filtering_exp)
        else:
            response = table.query()
        return response

    def update_item(self, p_key, p_key_value, attribute_name, curr_attr_value, attribute_value):
        response = self.curr_table.update_item(
            Key={
                p_key: p_key_value,
            },
            UpdateExpression="SET " + attribute_name + " = :" + curr_attr_value,
            ExpressionAttributeValues={':' + curr_attr_value: attribute_value
                                       }
        )
        return response


rip = RipReddit()
dynamo = DynamodbWrappers()
dynamo.set_table_name(rip.table_name)
while True:
    rip.reddit_stream(dynamo, 20)
    rip.rip_picture(dynamo)
    rip.buddy_check()
    rip.delete_not_ready(dynamo)
    rip.sleepy_time(5)
