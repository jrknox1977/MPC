import boto3

# ----> BOTO3 EC2 SETUP <----
ec2 = boto3.resource('ec2')

user_data = '''#!/bin/bash
yum -y update && yum -y upgrade
yum -y install python36 python36-devel python36-pip python36-setuptools
python36 -m pip install --upgrade pip
python36 -m pip install boto3 beautifulsoup4 tweepy
'''

instance_name = 'MCP_cloud'
image_id = 'ami-0cd3dfa4e37921605'
image_type = 't2.micro'
image_role = 'EC2-S3'


# create a new EC2 instance
instances = ec2.create_instances(
     ImageId=image_id,
     MinCount=1,
     MaxCount=1,
     InstanceType=image_type,
     KeyName='MCP2',
     UserData=user_data,
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
            'Name': image_role
        },
     TagSpecifications=[
        {
            'ResourceType': 'instance',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': instance_name
                },
            ]
        }],
 )


