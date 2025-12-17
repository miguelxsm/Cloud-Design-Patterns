import os
import pathlib
import requests
REGION = "us-east-1"

SG_MAIN_NAME = "SG_MAIN"
SG_PROXY_NAME = "SG_PROXY"


MY_IP = requests.get('https://ifconfig.me', timeout=5).text.strip()
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DEFAULT_KEY = _REPO_ROOT / "labsuser.pem"
PRIVATE_KEY_PATH = os.environ.get("SSH_PRIVATE_KEY", str(_DEFAULT_KEY))
KEY_PAIR_NAME =  "mainkey"

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
    ]

IP_PERMISSIONS_PROXY = [
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
]

