import os
import time
import json
import requests
import pathlib
from infrastructure.create_security_group import create_security_group
from infrastructure.destroy_infrastructure import destroy_all
from infrastructure.constants import SG_MAIN_NAME, IP_PERMISSIONS_SG_MAIN
from tools.instance_discovery import get_vpc_id_from_instances
from infrastructure.create_instances import create_main_instances

if __name__ == "__main__":

    try:
        print("--- CREATING SECURITY GROUP ---")
        vpc_id = get_vpc_id_from_instances()

        print("VPC ID: ", vpc_id)

        sg_main = create_security_group(
            SECURITY_GROUP_NAME=SG_MAIN_NAME,
            PERMISSIONS=IP_PERMISSIONS_SG_MAIN,
            DESCRIPTION="Main security group",
            VPC_ID=vpc_id
            )
        
        instances = create_main_instances(SG_MAIN_NAME)

        while True:
            ...


    except KeyboardInterrupt:
        print("CTRL + C Finishing Program...")
        destroy_all()