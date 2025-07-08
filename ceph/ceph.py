import json
import os
import subprocess as sp
import uuid
from abc import abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Optional, TypedDict, Union

import boto3
import fsspec

from pydantic import SecretStr

from typing_extensions import Self


class CephAdapter:
    """
    This Adapter makes accessing and administrating buckets in ceph easy
    """

    def __call__(self, *args: Any, **kwargs: Any) -> Self:
        return self

    def __init__(
        self,
        url: str,
        access_key: Union[str, SecretStr],
        secret_key: Union[str, SecretStr],
    ):

        self.access_key = (
            access_key if isinstance(access_key, SecretStr) else SecretStr(access_key)
        )
        self.secret_key = (
            secret_key if isinstance(secret_key, SecretStr) else SecretStr(secret_key)
        )
        self.url = url
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=self.access_key.get_secret_value(),
            aws_secret_access_key=self.secret_key.get_secret_value(),
            endpoint_url=url,
        )

        self.s3_session = boto3.Session(
            aws_access_key_id=self.access_key.get_secret_value(),
            aws_secret_access_key=self.secret_key.get_secret_value(),
        )

        handlers = self.s3_client.meta.events._emitter._handlers

        handlers_to_unregister = handlers.prefix_search("before-parameter-build.s3")
        handler_to_unregister = handlers_to_unregister[0]
        self.s3_client.meta.events._emitter.unregister(
            "before-parameter-build.s3", handler_to_unregister
        )

    def get_credentials(self):
        return {
            "url": self.url,
            "access_key": self.access_key.get_secret_value(),
            "secret_key": self.secret_key.get_secret_value(),
        }

    def list_buckets(self) -> list[str]:
        return [bucket["Name"] for bucket in self.s3_client.list_buckets()["Buckets"]]

    def create_user_bucket(
        self, bucket_name: str = "", user_name: str = "", tenant_name: str = ""
    ) -> None:
        self.s3_client.create_bucket(Bucket=bucket_name)
        self._bucket_acl_private(bucket_name)
        self.grant_bucket_to_user(user_name, bucket_name, tenant_name=tenant_name)

    def _bucket_acl_private(self, bucket_name: str = ""):
        self.s3_client.put_bucket_acl(ACL="private", Bucket=bucket_name)

    def delete_user_bucket(self, bucket_name: str = ""):
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def bucket_exists(self, bucket_name: str = "") -> bool:
        return bucket_name in self.list_buckets()

    def list_bucket_files(
        self, bucket_name: str = "", tenant_name: str = None, verbose=False
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )

        files = self.s3_client.list_objects(Bucket=tenant_bucket_name)
        if "Contents" not in files:
            return []
        return [file["Key"] if not verbose else file for file in files["Contents"]]

    def get_tenant(self):
        """
        This is an experimental function,
        but in most cases of populated Tenants it should
        return the proper tenant name.

        Returns:
            Tenant Name: str
        """
        first_or_default_bucket = self.s3_client.list_buckets()["Buckets"][0]["Name"]
        return self.s3_client.list_objects(Bucket=first_or_default_bucket)["Contents"][
            0
        ]["Owner"]["ID"].split("$")[-1]

    def upload_file(
        self,
        bucket_name: str = "",
        file_path: str = "",
        path_in_bucket: str = "",
        tenant_name: str = "    ",
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        path_in_bucket = path_in_bucket.removesuffix("/")
        self.s3_client.upload_file(
            file_path,
            tenant_bucket_name,
            os.path.join(path_in_bucket, file_path.split("/")[-1]),
        )

    def upload_path(
        self,
        bucket_name: str = "",
        path: str = "",
        path_in_bucket: str = "",
        recursive: bool = False,
    ):
        """
        This method uploads a path to a bucket.

        The path is uploaded to the bucket with the given name, and is placed
        in the bucket at the given path.

        parameters:
            bucket_name (str): bucket name to upload to.
            path (str): The path to the file to be uploaded.
            path_in_bucket (str): The path in the bucket where
            the file should be placed.
            recursive (bool): Whether to upload the path recursively.

        returns:
            None
        """
        path_in_bucket = path_in_bucket.removesuffix("/")
        files = Path(path).rglob("*") if recursive else Path(path).glob("*")

        for file in files:
            if not file.is_file():
                continue
            relative_path = "/".join(os.path.relpath(file, path).split("/")[:-1])
            new_path_in_bucket = os.path.join(path_in_bucket, relative_path)

            self.upload_file(
                bucket_name,
                str(file),
                "" if new_path_in_bucket == "/" else new_path_in_bucket,
            )

    def upload_stream(
        self,
        bucket_name: str = "",
        stream: Any = None,
        file_name: str = "",
        tenant_name: str = None,
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        self.s3_client.put_object(Bucket=tenant_bucket_name, Key=file_name, Body=stream)

    def delete_file(
        self, bucket_name: str = "", file_name: str = "", tenant_name: str = ""
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        self.s3_client.delete_object(Bucket=tenant_bucket_name, Key=file_name)

    def download_file(
        self,
        bucket_name: str = "",
        file_name: str = "",
        path: str = "",
        tenant_name: str = "",
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        self.s3_client.download_file(tenant_bucket_name, file_name, path)

    def download_stream(
        self, bucket_name: str = "", file_name: str = "", tenant_name: str = None
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        return self.s3_client.get_object(Bucket=tenant_bucket_name, Key=file_name)[
            "Body"
        ]

    def get_bucket(self, bucket_name: str = "", tenant_name: str = None):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        return self.s3_client.get_bucket(tenant_bucket_name)

    def get_fsspec(self, bucket_name: str = ""):
        return fsspec.filesystem(
            "s3",
            anon=False,
            key=self.get_credentials()["access_key"],
            secret=self.get_credentials()["secret_key"],
            client_kwargs={
                "endpoint_url": self.get_credentials()["url"],
            },
            bucket=bucket_name,
        )

    def get_signed_url(
        self,
        bucket_name,
        object_key,
        method="GET",
        expiration_time=3600,
        tenant_name: str = None,
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )

        method = f"{method.lower()}_object"
        signed_url = self.s3_client.generate_presigned_url(
            ClientMethod=method,
            Params={"Bucket": tenant_bucket_name, "Key": object_key},
            ExpiresIn=expiration_time,
        )

        return signed_url

    # Policies
    def grant_policy_to_bucket(
        self, bucket_name: str = "", policy_string: str = "", tenant_name: str = None
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        self.s3_client.put_bucket_policy(
            Bucket=tenant_bucket_name, Policy=policy_string
        )

    def grant_bucket_to_user(
        self, user_name: str, bucket_name: str = "", tenant_name=""
    ):
        policy_str = (
            CephPolicyBuilder(f"BASIC_{bucket_name.upper()}_{tenant_name}_{user_name}")
            .add_users_full_privileges(
                bucket_name, tenant_users=[(tenant_name, user_name)]
            )
            .build()
        )
        self.grant_policy_to_bucket(
            bucket_name=bucket_name, policy_string=json.dumps(policy_str)
        )

    def describe_bucket_policy(self, bucket_name: str = "", tenant_name: str = None):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )

        return json.loads(
            self.s3_client.get_bucket_policy(Bucket=tenant_bucket_name)["Policy"]
        )

    def set_bucket_public_readonly_access(
        self,
        bucket_name: str,
        object_names: list[str] = ["*"],
        tenant_name: str = None,
    ):
        tenant_bucket_name = self._combine_name(
            bucket_name=bucket_name, tenant_name=tenant_name
        )
        self._bucket_acl_private(bucket_name=tenant_bucket_name)
        public_readonly_access = (
            CephPolicyBuilder(policy_name="PublicReadOnly")
            .add_entry(
                bucket_name=tenant_bucket_name,
                object_names=object_names,
                privileges=["s3:ListBucket", "s3:GetObject"],
                tenant_users="*",
            )
            .build()
        )
        self.grant_policy_to_bucket(
            bucket_name=bucket_name,
            policy_string=json.dumps(public_readonly_access),
            tenant_name=tenant_name,
        )

    @staticmethod
    def _combine_name(bucket_name: str = "", tenant_name: str = None):
        tenant_name = f"{tenant_name}:" if tenant_name else ""
        return f"{tenant_name}{bucket_name}"


class CephPolicyBuilder:
    """
    The ceph policy builder is a handy tool for building s3 policies in a structured way.
    It allows for the creation of policies that can be used to grant or deny access to
    various resources in an s3 bucket. The policy builder can be used to create policies
    for buckets, objects and users.

    The policy builder can be used to create policies
    attached to buckets and objects, as ceph does not support user policies.

    This means you add user specific privileges to the policy, and then
    you grant it to the bucket. For this use all functions that look like:
    'add_user_..._privileges'


    If the user parameter is '*', the policy will be granted to all users.
    """

    class PolicyEntry(TypedDict):
        Effect: bool
        Action: list[str]
        Resource: list[str]
        Condition: Optional[dict[str, Any]]

    class Policy(TypedDict):
        Version: str
        Statement: list["CephPolicyBuilder.PolicyEntry"]

    policy_name: str = ""

    policy: Policy = {"Version": "2012-10-17", "Statement": []}

    @classmethod
    def make_policy_entry(
        cls,
        resource_names: list[str],
        actions: list[str],
        conditions: dict[str, Any] = None,
        tenant_users: str = list[tuple[str, str]],
        allow: bool = True,
    ) -> PolicyEntry:
        if isinstance(tenant_users, list):
            user_names = [
                f"arn:aws:iam::{tenant}:user/{user}" if tenant is not None else user
                for tenant, user in tenant_users
            ]
            principal = {"AWS": user_names} if user_names is not None else {}
        else:
            principal = "*" if tenant_users == "*" else {}

        return {
            "Sid": "Policy",
            "Effect": "Allow" if allow else "Deny",
            **({"Principal": principal}),
            "Action": [action for action in actions],
            "Resource": [f"arn:aws:s3:::{resource}" for resource in resource_names],
            **({"Condition": conditions} if conditions is not None else {}),
        }

    def __init__(self, policy_name: str):
        self.policy = {"Version": "2012-10-17", "Statement": []}
        self.policy_name = policy_name

    def build(self) -> Policy:
        return self.policy

    def write_policy_file(self, policy_name: str, path: str) -> str:
        abs_path = os.path.abspath(os.path.join(path, f"{policy_name}.json"))

        with open(abs_path, "w") as f:
            json.dump(self.policy, f)

        return abs_path

    def add_users_read_privileges(
        self,
        bucket_name: str = "",
        tenant_users: list[tuple[str, str]] = None,
    ) -> Self:
        assert tenant_users is not None, "Users must be specified"
        return self.add_entry(
            bucket_name=bucket_name,
            object_names=["*"],
            privileges=[
                "s3:ListBucket",
                "s3:GetObject",
            ],
            tenant_users=tenant_users,
            allow=True,
        )

    def add_users_write_privileges(
        self,
        bucket_name: str = "",
        tenant_users: list[tuple[str, str]] = None,
    ) -> Self:
        assert tenant_users is not None, "Users must be specified"
        return self.add_entry(
            bucket_name=bucket_name,
            object_names=["*"],
            privileges=[
                "s3:PutObject",
            ],
            tenant_users=tenant_users,
            allow=True,
        )

    def add_users_delete_privileges(
        self, bucket_name: str = "", tenant_users: list[tuple[str, str]] = None
    ) -> Self:
        assert tenant_users is not None, "Users must be specified"
        return self.add_entry(
            bucket_name=bucket_name,
            object_names=["*"],
            privileges=[
                "s3:DeleteObject",
            ],
            tenant_users=tenant_users,
            allow=True,
        )

    def add_users_full_privileges(
        self, bucket_name: str, tenant_users: list[tuple[str, str]] = None
    ) -> Self:
        assert tenant_users is not None, "Users must be specified"
        return self.add_entry(
            bucket_name=bucket_name,
            object_names=["*"],
            privileges=[
                "s3:ListBucket",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
            ],
            tenant_users=tenant_users,
            allow=True,
        )

    def add_entry(
        self,
        bucket_name: str,
        object_names: list[str] = ["*"],
        privileges: Union[str, list[str]] = "s3:*",
        conditions: dict[str, Any] = None,
        tenant_users: list[tuple[str, str]] = None,
        allow: bool = True,
    ) -> Self:
        self.policy["Statement"].append(
            CephPolicyBuilder.make_policy_entry(
                resource_names=[
                    bucket_name,
                    *[f"{bucket_name}/{object_name}" for object_name in object_names],
                ],
                actions=privileges,
                conditions=conditions,
                tenant_users=tenant_users,
                allow=allow,
            )
        )
        return self


class bucketError(Exception):
    pass
