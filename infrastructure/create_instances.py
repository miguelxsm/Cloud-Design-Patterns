import os
import boto3
from infrastructure.constants import REGION, KEY_PAIR_NAME

ec2 = boto3.resource("ec2", region_name=REGION)


def create_instance(instance_type, sg_id, role_tag):
    """
    Creates an EC2 instance with:
    - Type instance_type
    - Security Group sg_id
    - Startup script user_data (may be empty for now)
    """
    instances = ec2.create_instances(
        ImageId="ami-00ca32bbc84273381",  # Amazon Linux 2 AMI (HVM), SSD Volume Type
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[sg_id],
        TagSpecifications=[
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Role", "Value": role_tag}]
            }
        ],
        KeyName=KEY_PAIR_NAME
    )

    instance = instances[0]
    instance.wait_until_running()
    instance.reload()
    if not instance.public_ip_address:
        raise RuntimeError(f"Instance {instance.id} did not obtain a public IP. Check subnet settings.")
    
    
    return {
        "id": instance.id,
        "public_ip": instance.public_ip_address,
        "private_ip": instance.private_ip_address,
        "role": role_tag
    }

def create_main_instances(sg_name: str):
    print("Creating 3 t2.micro instances...")

    manager = create_instance(
                instance_type="t2.micro",
                sg_id=sg_name,
                role_tag="manager"
    )

    print("Instance manager successfully created", manager)

    workers = [create_instance(
                    instance_type="t2.micro",
                    sg_id=sg_name,
                    role_tag="worker"
                ) for _ in range(2) ]
    
    print("Instances of workers successfully created", workers)
    
    
    return {"manager": manager, "workers": workers}