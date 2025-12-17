import os
import time
import json
import requests
import pathlib
from infrastructure.create_security_group import create_security_group
from infrastructure.destroy_infrastructure import destroy_all
from infrastructure.constants import SG_MAIN_NAME, build_main_permissions, SG_PROXY_NAME, IP_PERMISSIONS_PROXY
from tools.instance_discovery import get_vpc_id_from_instances
from infrastructure.create_instances import create_main_instances, create_proxy_instance
from tools.utils import save_instance_ips
if __name__ == "__main__":

    try:
        print("--- CREATING SECURITY GROUP ---")
        vpc_id = get_vpc_id_from_instances()

        print("VPC ID: ", vpc_id)

        sg_proxy = create_security_group(
            SECURITY_GROUP_NAME=SG_PROXY_NAME,
            PERMISSIONS=IP_PERMISSIONS_PROXY,
            DESCRIPTION="Proxy security Group",
            VPC_ID=vpc_id
        )
        
        proxy_instance = create_proxy_instance(SG_PROXY_NAME)

        path = save_instance_ips({"proxy" : proxy_instance})

        print("Proxy Info saved in ", path)
        
        sg_main = create_security_group(
            SECURITY_GROUP_NAME=SG_MAIN_NAME,
            PERMISSIONS=build_main_permissions(sg_proxy),
            DESCRIPTION="Main security group",
            VPC_ID=vpc_id
            )
        
        instances = create_main_instances(SG_MAIN_NAME)

        save_instance_ips(instances)

        while True:
            ...


    except KeyboardInterrupt:
        print("CTRL + C Finishing Program...")
        destroy_all()