import os
import pathlib
import requests
REGION = "us-east-1"

SG_MAIN_NAME = "SG_MAIN"
SG_PROXY_NAME = "SG_PROXY"
SG_GATEWAY_NAME = "SG_GATEWAY"


MY_IP = requests.get('https://ifconfig.me', timeout=5).text.strip()
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
KEY_PAIR_NAME =  "mainkey"



API_GATEWAY = "MY_API_KEY"
SQL_USER = "mysqluser"
SQL_PASSWORD = "mysqlpassword"
def build_main_permissions(sg_proxy_id: str):
    return [
        {
            "IpProtocol": "tcp", 
            "FromPort": 22,
            "ToPort": 22,
            "IpRanges": [{"CidrIp": f"{MY_IP}/32"}],
        },
        {
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "UserIdGroupPairs": [{"GroupId": sg_proxy_id}],
        },
        {
            "IpProtocol": "icmp",
            "FromPort": 8,
            "ToPort": -1,
            "UserIdGroupPairs": [{"GroupId": sg_proxy_id}],
        },
    ]

def build_proxy_permissions(sg_gateway_id):
    return [
    {
        "IpProtocol": "tcp",
        "FromPort": 22,
        "ToPort": 22,
        "IpRanges": [{"CidrIp": f"{MY_IP}/32"}],
    },
    {
        "IpProtocol": "tcp",
        "FromPort": 3306,
        "ToPort": 3306,
        "IpRanges": [{"CidrIp": f"{MY_IP}/32"}],
    },
    {
            "IpProtocol": "tcp",
            "FromPort": 3306,
            "ToPort": 3306,
            "UserIdGroupPairs": [{"GroupId": sg_gateway_id}],
        },
]

IP_PERMISSIONS_GATEWAY = [
    {
        "IpProtocol": "tcp",
        "FromPort": 22,
        "ToPort": 22,
        "IpRanges": [{"CidrIp": f"{MY_IP}/32"}],
    },

    {
        "IpProtocol": "tcp",
        "FromPort": 80,
        "ToPort": 80,
        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
    },
]