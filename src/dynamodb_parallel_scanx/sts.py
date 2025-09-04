import boto3
from botocore.config import Config
from typing import Optional, Dict, Any

def build_session(role_arn: Optional[str],
                  region: Optional[str],
                  session_name: str = "dpscan-session",
                  external_id: Optional[str] = None,
                  duration_seconds: Optional[int] = None) -> boto3.Session:
    if not role_arn:
        return boto3.Session(region_name=region)
    sts = boto3.client("sts", region_name=region)
    params: Dict[str, Any] = {"RoleArn": role_arn, "RoleSessionName": session_name}
    if external_id: params["ExternalId"] = external_id
    if duration_seconds: params["DurationSeconds"] = int(duration_seconds)
    creds = sts.assume_role(**params)["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name=region,
    )

def build_ddb_client(sess: boto3.Session,
                     region: Optional[str],
                     max_pool: int = 128,
                     read_timeout: int = 60,
                     connect_timeout: int = 10,
                     retries_max: int = 10):
    cfg = Config(
        max_pool_connections=max_pool,
        read_timeout=read_timeout,
        connect_timeout=connect_timeout,
        retries={"mode": "adaptive", "max_attempts": retries_max},
    )
    return sess.client("dynamodb", region_name=region or sess.region_name, config=cfg)
