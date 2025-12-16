import os
import pathlib

REGION = "us-east-1"
SG_MAIN_NAME = "SG_MAIN"

_REPO_ROOT = pathlib.Path(__file__).resolve().parent
_DEFAULT_KEY = _REPO_ROOT / "labsuser.pem"
PRIVATE_KEY_PATH = os.environ.get("SSH_PRIVATE_KEY", str(_DEFAULT_KEY))
KEY_PAIR_NAME =  "mainkey"

IP_PERMISSIONS_SG_MAIN = [
    {
        "IpProtocol": "tcp",
        "FromPort": 80,
        "ToPort": 80,
        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
    },
    {
        "IpProtocol": "tcp",
        "FromPort": 22,
        "ToPort": 22,
        "IpRanges": [{"CidrIp": "0.0.0.0/0"}]
    }
]
