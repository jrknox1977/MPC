import praw
import logging
import time
import os
import boto3
from boto3 import resource
import urllib.request
from boto3.dynamodb.conditions import Key


class RipReddit:
    def __init__(self):

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
        return int(round(time.time()*1000))

    def reddit_stream(self, dyna):
        i = 0
        while True:
            for submission in self.subreddit.stream.submissions():
                try:
                    cat_stuff = {'epoch_time': str(self.curr_time()),
                                 'submission_id': submission.id,
                                 'submission_url': submission.url,
                                 'downloaded': 'False',
                                 'stored_to_s3': 'False',
                                 'tweet': 'Not_Ready'}
                    dyna.add_item(self.table_name, cat_stuff)
                    self.logger.info('Submission: ' + submission.id + ' received!')
                except Exception as e:
                    self.logger.error('Something went wrong, here it is: ' + str(e))
                if i >= 20:
                    self.rip_picture(dyna)
                    time.sleep(300)
                    i = 0
                else:
                    i += 1

    # -----> AWS REKOGNITION <-----
    def get_rekognition_info(self, filename):
        with open(filename, 'rb') as imgfile:
            results = self.rekog.detect_labels(Image={'Bytes': imgfile.read()}, MinConfidence=1)
        return results

    # -----> RIP PICTURE <-----
    def rip_picture(self, dyna):
        items = dyna.scan_table_allpages(self.table_name, filter_key='downloaded', filter_value='False')
        for item in items:
            image_file_name = item['submission_url'].split('/')[-1]

            if image_file_name.lower().endswith('.jpg'):
                full_path = self.image_path + '/' + image_file_name
                try:
                    urllib.request.urlretrieve(item['submission_url'], full_path)
                    self.logger.info("Image Retrieved: " + image_file_name)
                except Exception as e:
                    self.logger.error('=========================\n' + item['submission_url'] + 'FAILED: ' + str(e)
                                      + '\n=========================')
                # dyn.delete_item(table_name, 'epoch_time', item['epoch_time'])
                image_info = self.get_rekognition_info(full_path)
                for stuff in image_info['Labels']:
                    print(type(stuff))
                    if stuff['Name'] == 'Person' or stuff['Name'] == 'Human':
                        self.logger.info("There is a person in this image! -----> DEREZ <-----")
                        os.remove(full_path)
                        dyna.delete_item(self.table_name, 'epoch_time', item['epoch_time'])
                    elif stuff['Name'] == 'Cat':
                        self.logger.info("There is a CAT in this image! -----> APPROPRIATING CAT <-------")
                        self.s3.meta.client.upload_file(full_path, 'master-control-program', 'images/'
                                                        + image_file_name)
                        try:
                            self.logger.info("Update DynamoDB: "
                                             + str(dyna.update_item('epoch_time', item['epoch_time'],
                                                                    'downloaded', 'False', 'True')))
                            self.logger.info("Update DynamoDB: "
                                             + str(dyna.update_item('epoch_time', item['epoch_time'],
                                                                    'tweet', 'NOT_READY', 'READY')))

                        except Exception as e:
                            self.logger.error('ERROR: ' + str(e))


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
    rip.reddit_stream(dynamo)
