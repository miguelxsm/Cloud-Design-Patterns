import os
import time
import json
import requests
import pathlib
import argparse
from infrastructure.create_security_group import create_security_group, add_self_mysql_ingress
from infrastructure.destroy_infrastructure import destroy_all
from infrastructure.constants import SG_MAIN_NAME, build_main_permissions, SG_PROXY_NAME, IP_PERMISSIONS_PROXY, _REPO_ROOT
from infrastructure.create_instances import create_main_instances, create_proxy_instance
from tools.utils import save_instance_ips, get_vpc_id_from_instances
if __name__ == "__main__":

    create_sg = False
    create_instances = False
    create_proxy = False
    destroy = False

    parser = argparse.ArgumentParser(description="Final Assignment Cloud Computing")

    parser.add_argument("--sg", action="store_true", help="Create security groups")
    parser.add_argument("--instances", action="store_true", help="Create main instances")
    parser.add_argument("--proxy", action="store_true", help="Create proxy instance")
    parser.add_argument("--destroy", action="store_true", help="Destroy Infrastructure")


    args = parser.parse_args()

    if args.destroy:
        destroy_all()
        print("Successfully erased")
        exit(0)

    if not (args.sg or args.instances or args.proxy):
        create_sg = True
        create_instances = True
        create_proxy = True
    else:
        create_sg = args.sg
        create_instances = args.instances
        create_proxy = args.proxy

    vpc_id = get_vpc_id_from_instances()

    print("VPC ID: ", vpc_id)
    if create_sg:
        print("--- CREATING SECURITY GROUP FOR PROXY ---")
        sg_proxy = create_security_group(
            SECURITY_GROUP_NAME=SG_PROXY_NAME,
            PERMISSIONS=IP_PERMISSIONS_PROXY,
            DESCRIPTION="Proxy security Group",
            VPC_ID=vpc_id
        )
        print("--- CREATING SECURITY GROUP FOR MAIN ---")
        
        sg_main = create_security_group(
            SECURITY_GROUP_NAME=SG_MAIN_NAME,
            PERMISSIONS=build_main_permissions(sg_proxy),
            DESCRIPTION="Main security group",
            VPC_ID=vpc_id
            )
        
        add_self_mysql_ingress(sg_main)
        
    if create_instances:
        instances = create_main_instances(SG_MAIN_NAME)
        
        save_instance_ips(instances)


    if create_proxy:

        path = os.path.join(_REPO_ROOT, "deployment", "ips_info.json")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        ips = [data[key]["private_ip"] for key in data.keys() if key != 'proxy']

        print("private ips", ips)

        proxy_instance = create_proxy_instance(SG_PROXY_NAME, ips, "random")

        print("public_ip proxy: ", proxy_instance["public_ip"])
        path = save_instance_ips({"proxy" : proxy_instance})
        print("Proxy Info saved in ", path)



