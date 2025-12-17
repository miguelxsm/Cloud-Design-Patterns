from infrastructure.constants import _REPO_ROOT
import os
import json
from typing import Dict, Any


def get_code(path: str):
    code_path = os.path.join(_REPO_ROOT, path)
    with open(code_path, "r") as f:
        code = f.read()
    return code


import os
import json
from typing import Dict, Any, Union, List

def save_instance_ips(topology: Dict[str, Any]) -> str:
    path = os.path.join(_REPO_ROOT, "deployment", "ips_info.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                data = {}
    else:
        data = {}

    def store(name: str, info: Dict[str, Any]) -> None:
        data[name] = {
            "id": info.get("id"),
            "public_ip": info.get("public_ip"),
            "private_ip": info.get("private_ip"),
        }

    for key, value in topology.items():
        if isinstance(value, dict):
            # manager / proxy
            store(key, value)
        elif isinstance(value, list):
            # workers list
            for idx, item in enumerate(value, start=1):
                if not isinstance(item, dict):
                    continue
                store(f"{key}_{idx}", item)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return path


    