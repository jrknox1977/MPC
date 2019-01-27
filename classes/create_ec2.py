import boto3
import configparser
import sys

# ----> CONFIGPARSER SETUP <----
config = configparser.ConfigParser()
len(sys.argv)
if len(sys.argv) > 1:
    config.read(sys.argv[1])
else:
    config.read('aws_setup.ini')

aws_instance_name = config['aws_instance_names']['name']
aws_instance_type = config['aws_instance_types']['type']
ips = config['aws_ips']['ips'].split(',')

# ----> BOTO3 EC2 SETUP <----
ec2_client = boto3.client('ec2')
ec2 = boto3.resource('ec2')


instances[i] = ec2.create_instances(
    # BlockDeviceMappings=[
    #     {
    #         'DeviceName': '/dev/sda1',
    #         'Ebs': {
    #             'DeleteOnTermination': True,
    #             'VolumeSize': 40,
    #             'VolumeType': 'gp2',
    #         },
    #     },
    # ],
    ImageId=aws_image,
    MinCount=1,
    MaxCount=1,
    InstanceType=aws_instance_type,
    KeyName='CEMSight_Testing',
    NetworkInterfaces=[
        {
            'AssociatePublicIpAddress': True,
            'DeleteOnTermination': True,
            'DeviceIndex': 0,
            'Groups': ['sg-07a816799e212dbe9'],
            'SubnetId': 'subnet-05e6b41db911702e2',
            'PrivateIpAddresses': [
                {
                    'Primary': True,
                    'PrivateIpAddress': ips[i]
                },
            ],
        },
    ],
    TagSpecifications=[
        {
            'ResourceType': 'instance',
            'Tags': [
                {
                    'Key': 'Name',
                    'Value': aws_instance_name[i]
                },
            ]
        },
    ],
)
print('Instance ' + aws_instance_name[i] + ' successfully created.')

for i in range(5):
    instance_name = str(instances[i][0]).split("id='")
    instance_name = instance_name[1].replace("')", '')
    response = ec2_client.describe_instances(InstanceIds=[instance_name])
    print(response)
