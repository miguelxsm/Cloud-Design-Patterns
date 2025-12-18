import os
import boto3
from infrastructure.constants import REGION, KEY_PAIR_NAME, SQL_USER, SQL_PASSWORD
from tools.utils import get_code
from deployment.setup_instances import build_proxysql_user_data, build_manager_user_data, build_workers_user_data


ec2 = boto3.resource("ec2", region_name=REGION)


def create_instance(instance_type, sg_id, role_tag, user_data):
   
    instances = ec2.create_instances(
        ImageId="ami-0ecb62995f68bb549",
        InstanceType=instance_type,
        MinCount=1,
        MaxCount=1,
        SecurityGroupIds=[sg_id],
        UserData=user_data,
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

    code_manager = build_manager_user_data(
        mysql_user=SQL_USER,
        mysql_pass=SQL_PASSWORD,
        server_id=1
    )

    print("Creating 3 t2.micro instances...")

    manager = create_instance(
                instance_type="t2.micro",
                sg_id=sg_name,
                user_data=code_manager,
                role_tag="manager"
    )
    print("Instance manager successfully created", manager)

    code_worker1 = build_workers_user_data(
        mysql_user=SQL_USER,
        mysql_pass=SQL_PASSWORD,
        manager_ip=manager["private_ip"],
        server_id=2
    )
    worker1 = create_instance(
                    instance_type="t2.micro",
                    sg_id=sg_name,
                    role_tag="worker1",
                    user_data=code_worker1
                )

    code_worker2 = build_workers_user_data(
            mysql_user=SQL_USER,
            mysql_pass=SQL_PASSWORD,
            manager_ip=manager["private_ip"],
            server_id=3
        )

    worker2 =  create_instance(
                    instance_type="t2.micro",
                    sg_id=sg_name,
                    role_tag="worker2",
                    user_data=code_worker2
                )
    
    print("Instances of workers successfully created", worker1, worker2)
    
    
    return {"manager": manager, "worker1": worker1, "worker2":worker2}

def create_proxy_instance(sg_proxy_name: str, instances: dict):
    manager_ip = instances[0]
    workers = instances[1:]
    user_data = build_proxysql_user_data(
        manager_ip=manager_ip,
        worker_ips=workers,
        mysql_user=SQL_USER,
        mysql_pass=SQL_PASSWORD
        )
    
    instance = create_instance(
        instance_type="t2.large",
        sg_id = sg_proxy_name,
        role_tag="proxy",
        user_data=user_data
    )

    return instance

