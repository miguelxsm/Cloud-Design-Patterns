import time
import boto3
import os
from botocore.exceptions import ClientError
from infrastructure.constants import REGION, SG_MAIN_NAME

ec2 = boto3.client("ec2", region_name=REGION)

def _get_sg_id_by_name(name: str) -> str | None:
    try:
        resp = ec2.describe_security_groups(Filters=[{"Name": "group-name", "Values": [name]}])
        sgs = resp.get("SecurityGroups", [])
        return sgs[0]["GroupId"] if sgs else None
    except ClientError:
        return None

def _list_instance_ids_for_sgs(sg_ids: str) -> list[str]:
    if not sg_ids:
        return []
    filters = [{"Name": "instance.group-id", "Values": [sg_ids]}]
    ids = []
    paginator = ec2.get_paginator("describe_instances")
    for page in paginator.paginate(Filters=filters):
        for r in page.get("Reservations", []):
            for i in r.get("Instances", []):
                state = i.get("State", {}).get("Name")
                if state not in {"shutting-down","terminated"}:
                    ids.append(i["InstanceId"])
    return list(dict.fromkeys(ids))  # únicos


def _delete_sg_with_retry(sg_id: str, retries: int = 5) -> None:
    if not sg_id: 
        return
    for attempt in range(retries):
        try:
            ec2.delete_security_group(GroupId=sg_id)
            return
        except ClientError as e:
            code = e.response["Error"]["Code"]
            # Si aún está "in use", espera a que libere las ENI tras terminar instancias
            if code in {"DependencyViolation","InvalidGroup.InUse"} and attempt < retries - 1:
                time.sleep(2 + attempt)  # backoff corto
                continue
            if code == "InvalidGroup.NotFound":
                return
            raise

def destroy_all():
    sg_main_id   = _get_sg_id_by_name(SG_MAIN_NAME)

    instance_ids = _list_instance_ids_for_sgs(sg_main_id)
    _terminate_instances_and_wait(instance_ids)

    _delete_sg_with_retry(sg_main_id)

