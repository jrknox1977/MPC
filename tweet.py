from boto3 import resource
from boto3.dynamodb.conditions import Key
from botocore.errorfactory import ClientError
import logging
import os
import psutil
import time
import tweepy


class TweetPics:
    def __init__(self):

        self.table_name = os.environ['REDDIT_TABLE_NAME']

        # -----> SETUP LOGGING <-----
        self.logger = logging.getLogger(__name__)
        handler = logging.FileHandler('/tmp/MCP_Master_Log.log')
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        # -----> S3 INFO <-----
        self.s3 = resource('s3')

        # -----> DUPLICATE PROCESS PROTECTION <-----
        self.pid_file = '/tmp/tweet.pid'
        self.buddy_pid = '/tmp/rip_reddit.pid'
        self.pid_check()
        self.process_to_check = 'python36'

        # -----> TWITTER AUTH <----
        mcp_auth = tweepy.OAuthHandler(os.environ['CONSUMER_KEY'], os.environ['CONSUMER_SECRET'])
        mcp_auth.set_access_token(os.environ['ACCESS_TOKEN'], os.environ['ACCESS_TOKEN_SECRET'])
        mcp_auth.secure = True
        self.mcp_tweet = tweepy.API(mcp_auth)

        self.kb_message = "#cats #catsoftwitter #python #reddit #IndyPy"

        self.myBot = self.mcp_tweet.get_user(screen_name="@Xonk_dp")
        self.logger.info("-------------> CONNECTED TO TWITTER!!! <-----------------")

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

    def sleepy_time(self, num):
        for i in range(0, num):
            self.logger.info("Tweet in sleepy time for " + str(num - i) + " minutes.")
            time.sleep(60)

    # -----> CHECK IF PROGRAM IS ALREADY RUNNING <-----
    def pid_check(self):
        if os.path.exists(self.pid_file):
            self.logger.error("TWEET PID file Already exists. Exiting...")
            exit(0)
        else:
            with open(self.pid_file, 'w+') as f:
                f.write(str(os.getpid()))
                self.logger.info("Twitter Bot Initiated!")

    def update_status_with_media(self, kb_message, image):
        self.mcp_tweet.update_with_media(image, kb_message)
        print("Image Uploaded")

    def tweet_pics(self, dyna):
        pics = dyna.scan_table_allpages(self.table_name, filter_key='tweet', filter_value='READY')
        self.logger.info(" ---> GRABBING PICS READY TO TWEET <---")
        for pic in pics:
            image_file_name = '/tmp/images/' + pic['submission_url'].split('/')[-1]
            self.logger.info("Tweet: Checking if Image Exists")
            if os.path.exists(image_file_name):
                self.update_status_with_media(self.kb_message, image_file_name)
                self.logger.info("!!! TWEETED: " + image_file_name + " !!!")
                self.logger.info("PICTURE TWEETED: Updating DynamoDB: "
                                 + str(dyna.update_item('epoch_time', pic['epoch_time'], 'tweet', 'READY', 'DONE')))
                self.sleepy_time(5)
            else:
                try:
                    self.logger.info("Tweet: Trying to grab file from S3 ")
                    self.s3.Bucket('master-control-program').download_file('images/' + pic['submission_url']
                                                                           .split('/')[-1], image_file_name)
                    self.logger.info("Tweet: Successfully Grabbed Image!")

                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        self.logger.info("The File Does not exist locally or in S3, DELETING DynamoDB record.")
                        dyna.delete_item(self.table_name, 'epoch_time', pic['epoch_time'])
                else:
                    dyna.delete_item(self.table_name, 'epoch_time', pic['epoch_time'])
                    self.update_status_with_media(self.kb_message, image_file_name)
                    self.logger.info("!!! TWEETED: " + image_file_name + " !!!")
                    self.logger.info("PICTURE TWEETED: Updating DynamoDB: "
                                     + str(dyna.update_item('epoch_time', pic['epoch_time'], 'tweet', 'READY', 'DONE')))
                    self.sleepy_time(5)


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


tweet = TweetPics()
dynamite = DynamodbWrappers()
dynamite.set_table_name(tweet.table_name)
while True:
    tweet.tweet_pics(dynamite)
    tweet.buddy_check()
