import json
import boto3
from infrastructure.constants import REGION

ec2 = boto3.client("ec2", region_name=REGION)

def get_vpc_id_from_instances():
    ec2 = boto3.client("ec2", region_name=REGION)
    resp = ec2.describe_vpcs(Filters=[{"Name": "isDefault", "Values": ["true"]}])
    if resp["Vpcs"]:
        vpc_id = resp["Vpcs"][0]["VpcId"]
        print(f"VPC id found: {vpc_id}")
        return vpc_id
