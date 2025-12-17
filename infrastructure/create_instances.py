import os
import boto3
from infrastructure.constants import REGION, KEY_PAIR_NAME
from tools.utils import get_code

ec2 = boto3.resource("ec2", region_name=REGION)


def create_instance(instance_type, sg_id, role_tag, user_data=None):
    """
    Creates an EC2 instance with:
    - Type instance_type
    - Security Group sg_id
    - Optional startup script user_data
    """
    params = {
        "ImageId": "ami-0ecb62995f68bb549",  # Ubuntu
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
        "SecurityGroupIds": [sg_id],
        "TagSpecifications": [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Role", "Value": role_tag}]
            }
        ],
        "KeyName": KEY_PAIR_NAME
    }

    if user_data is not None:
        params["UserData"] = user_data

    instances = ec2.create_instances(**params)

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

    script_path = 'deployment/setup_instances.sh'
    code = get_code(script_path)

    print("Creating 3 t2.micro instances...")

    manager = create_instance(
                instance_type="t2.micro",
                sg_id=sg_name,
                user_data=code,
                role_tag="manager"
    )

    print("Instance manager successfully created", manager)

    workers = [create_instance(
                    instance_type="t2.micro",
                    sg_id=sg_name,
                    role_tag="worker",
                    user_data=code
                ) for _ in range(2) ]
    
    print("Instances of workers successfully created", workers)
    
    
    return {"manager": manager, "workers": workers}

def create_proxy_instance(sg_proxy_name: str):
    instance = create_instance(
        instance_type="t2.micro",
        sg_id = sg_proxy_name,
        role_tag="proxy",
    )

    return instance