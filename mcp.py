import argparse
import boto3
import configparser
import logging
import time
from collections import defaultdict


# -----> MASTER CONTROL PROGRAM <-----
class MCP:
    def __init__(self):

        # ----> BOTO3 SETUP <----
        self.s3 = boto3.resource('s3')

        self.ec2 = boto3.resource('ec2')
        self.instance_name = 'MCP_cloud'
        self.image_id = 'ami-0cd3dfa4e37921605'  # 'ami-9c0638f9' - centos7
        self.image_type = 't2.micro'
        self.image_role = 'ec2-admin'

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
        arg_parser.add_argument("--get_info", "-i", help="Get info on currently running EC2 instances",
                                action="store_true")

        self.get_info = False
        args = arg_parser.parse_args()
        if args.get_info:
            self.get_info = True
        if args.version:
            print("MCP Version - IndyPy")
        if args.config:
            self.config = args.config
        else:
            print("\n    Please specify a configuration file using -c\n")
            exit()

        # -----> CONFIGPARSER <-----
        parser = configparser.ConfigParser()
        parser.read(self.config)
        self.client_id = parser['reddit']['client_id']
        self.client_secret = parser['reddit']['client_secret']
        self.username = parser['reddit']['username']
        self.password = parser['reddit']['password']
        self.user_agent = parser['reddit']['user_agent']
        self.subreddit = parser['reddit']['subreddit']
        self.reddit_table_name = parser['dynamodb']['table_name']
        self.access_token = parser['twitter']['access_token']
        self.access_token_secret = parser['twitter']['access_token_secret']
        self.consumer_key = parser['twitter']['consumer_key']
        self.consumer_secret = parser['twitter']['consumer_secret']

        # -----> EC2 STARTUP SCRIPT <-----
        self.user_data = '#!/bin/bash \nyum -y update && yum -y upgrade \nyum -y install python36 python36-devel ' \
                         'python36-pip python36-setuptools git gcc \n' \
                         'python36 -m pip install --upgrade pip \npython36 -m pip install boto3 tweepy praw psutil \n'\
                         'echo "ACCESS_TOKEN=' + self.access_token + '" >> /etc/environment\n' \
                         'echo "ACCESS_TOKEN_SECRET=' + self.access_token_secret + '" >> /etc/environment\n' \
                         'echo "CONSUMER_KEY=' + self.consumer_key + '" >> /etc/environment\n' \
                         'echo "CONSUMER_SECRET=' + self.consumer_secret + '" >> /etc/environment\n' \
                         'echo "CLIENT_ID=' + self.client_id + '" >> /etc/environment\n' \
                         'echo "CLIENT_SECRET=' + self.client_secret + '" >> /etc/environment\n' \
                         'echo "USERNAME=' + self.username + '" >> /etc/environment\n' \
                         'echo "PASSWORD=' + self.password + '" >> /etc/environment\n' \
                         'echo "USER_AGENT=' + self.user_agent + '" >> /etc/environment\n' \
                         'echo "SUBREDDIT=' + self.subreddit + '" >> /etc/environment\n' \
                         'echo "REDDIT_TABLE_NAME=' + self.reddit_table_name + '" >> /etc/environment\n' \
                         'aws s3 cp s3://master-control-program/tweet.py /tmp \n' \
                         'aws s3 cp s3://master-control-program/rip_reddit.py /tmp \n' \
                         'touch /tmp/MCP_Master_Log.log \n' \
                         'crontab -l | { cat; echo "*/5 * * * * python36 /tmp/rip_reddit.py"; } | crontab - \n' \
                         'crontab -l | { cat; echo "*/5 * * * * python36 /tmp/tweet.py"; } | crontab - \n'

        # -----> COPY FILES TO S3 <-----
        try:
            self.s3.meta.client.upload_file('rip_reddit.py', 'master-control-program', 'rip_reddit.py')
            self.s3.meta.client.upload_file('tweet.py', 'master-control-program', 'tweet.py')
        except Exception as e:
            self.logger.error('ERROR: ' + str(e))
        self.logger.info("Initialization complete...Waiting 60 seconds for files to move, etc.....")
        time.sleep(60)

    # -----> CREATE EC2 INSTANCE <-----
    def create_ec2(self):
        self.logger.info("Launching Instance")
        instance = self.ec2.create_instances(
            ImageId=self.image_id,
            MinCount=1,
            MaxCount=1,
            InstanceType=self.image_type,
            KeyName='MCP2',
            UserData=self.user_data,
            NetworkInterfaces=[
                {
                    'DeviceIndex': 0,
                    'SubnetId': 'subnet-0271b36be9fdc781f',
                    'Groups': [
                        'sg-086cc5688aa2e513d',
                    ],
                    'AssociatePublicIpAddress': True,
                }],
            IamInstanceProfile={
                'Name': self.image_role
            },
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': self.instance_name
                        },
                    ]
                }],
        )
        self.logger.info('EC2 INSTANCE CREATED')
        self.logger.info(instance)
        print(instance)

    # -----> GET EC2 INFO <-----
    def get_ec2_info(self):
        # Get information for all running instances
        running_instances = self.ec2.instances.filter(Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])

        ec2info = defaultdict()
        for instance in running_instances:
            for tag in instance.tags:
                if 'Name' in tag['Key']:
                    name = tag['Value']
            # Add instance info to a dictionary
            ec2info[instance.id] = {
                'Name': name,
                'Instance_ID': instance.id,
                'Type': instance.instance_type,
                'State': instance.state['Name'],
                'Private IP': instance.private_ip_address,
                'Public IP': instance.public_ip_address,
                'Launch Time': instance.launch_time
            }

        attributes = ['Name', 'Instance_ID', 'Type', 'State', 'Private IP', 'Public IP', 'Launch Time']
        for instance_id, instance in ec2info.items():
            for key in attributes:
                self.logger.info("{0}: {1}".format(key, instance[key]))
                print("{0}: {1}".format(key, instance[key]))


master_control_program = MCP()
if master_control_program.get_info:
    master_control_program.get_ec2_info()
    exit()
master_control_program.create_ec2()
time.sleep(60)
master_control_program.get_ec2_info()
