import os
import pathlib
import requests

REGION = "us-east-1"
SG_MAIN_NAME = "SG_MAIN"
MY_IP = requests.get('https://ifconfig.me', timeout=5).text.strip()
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
_DEFAULT_KEY = _REPO_ROOT / "labsuser.pem"
PRIVATE_KEY_PATH = os.environ.get("SSH_PRIVATE_KEY", str(_DEFAULT_KEY))
KEY_PAIR_NAME =  "mainkey"

IP_PERMISSIONS_SG_MAIN = [
    {
        "IpProtocol": "tcp",
        "FromPort": 22,
        "ToPort": 22,
        "IpRanges": [{"CidrIp": f"{MY_IP}/32"}]
    }
]
