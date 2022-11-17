from typing import Optional

import httpx
from fastapi.encoders import jsonable_encoder
from pydantic import Field, SecretStr

import prefect.settings
from prefect.blocks.notifications import NotificationBlock
from prefect.filesystems import S3, RemoteFileSystem
from prefect.logging import get_logger
from prefect.utilities.asyncutils import sync_compatible


class FlowResult(S3):
    """
    Store data as a file on a remote file system.

    Supports any remote file system supported by `fsspec`. The file system is specified
    using a protocol. For example, "s3://my-bucket/my-folder/" will use S3.

    Example:
        Load stored remote file system config:
        ```python
        from prefect.filesystems import RemoteFileSystem

        remote_file_system_block = RemoteFileSystem.load("BLOCK_NAME")
        ```
    """
    _block_type_name = "Flow Result"


class FlowSource(S3):
    """
    Store data as a file on a remote file system.

    Supports any remote file system supported by `fsspec`. The file system is specified
    using a protocol. For example, "s3://my-bucket/my-folder/" will use S3.

    Example:
        Load stored remote file system config:
        ```python
        from prefect.filesystems import RemoteFileSystem

        remote_file_system_block = RemoteFileSystem.load("BLOCK_NAME")
        ```
    """
    _block_type_name = "Flow Source"


class Minio(S3):
    """
    Store data as a file on a remote file system.

    Supports any remote file system supported by `fsspec`. The file system is specified
    using a protocol. For example, "s3://my-bucket/my-folder/" will use S3.

    Example:
        Load stored remote file system config:
        ```python
        from prefect.filesystems import RemoteFileSystem

        remote_file_system_block = RemoteFileSystem.load("BLOCK_NAME")
        ```
    """
    _block_type_name = "Minio"
    access_key_id: Optional[SecretStr] = Field(
        default=None,
        title="AWS Access Key ID",
        description="Equivalent to the AWS_ACCESS_KEY_ID environment variable.",
        example="AKIAIOSFODNN7EXAMPLE",
    )
    endpoint_url: str = Field(None, title="AWS Endpoint Url")
    secret_access_key: Optional[SecretStr] = Field(
        default=None,
        title="AWS Secret Access Key",
        description="Equivalent to the AWS_SECRET_ACCESS_KEY environment variable.",
        example="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    )

    @property
    def filesystem(self) -> RemoteFileSystem:
        settings = {}
        if self.access_key_id:
            settings["key"] = self.access_key_id.get_secret_value()
        if self.secret_access_key:
            settings["secret"] = self.secret_access_key.get_secret_value()
        if self.endpoint_url:
            settings["client_kwargs"] = {"endpoint_url": self.endpoint_url}
        self._remote_file_system = RemoteFileSystem(
            basepath=f"s3://{self.bucket_path}", settings=settings
        )
        return self._remote_file_system


class NovuNotificationBlock(NotificationBlock):
    """
    A base class for sending notifications using Novu.

    Attributes:
        apiKey: Api authorization key
        apiUrl: Used to send notifications to channels
        templateName: The trigger identifier of the template you wish to send.
                    This identifier can be found on the template page.
    """
    _block_type_name = "Novu"

    apiKey: SecretStr = Field(
        default=...,
        title="Novu ApiKey",
        description="Api authorization key",
        example="xxxxxx",
    )

    apiUrl: str = Field(
        default=...,
        title="Api URL",
        description="Used to send notifications to channels",
        example="https://api.novu.co/v1/events/trigger",
    )

    templateName: str = Field(
        default=...,
        title="Template name",
        description="The trigger identifier of the template you wish to send. "
                    "This identifier can be found on the template page.",
        example="slack",
    )

    def block_initialization(self) -> None:
        httpx_settings = {
            "verify": False,
            "headers": {
                "Authorization": f"ApiKey {self.apiKey.get_secret_value()}",
                "Content-Type": "application/json"
            }
        }
        self._client = httpx.AsyncClient(
            **httpx_settings,
        )
        self._logger = get_logger("prefect.notifications.novu")

    @sync_compatible
    async def notify(self, body: str, subject: Optional[str] = None, notification=None):
        notification_dict = dict(notification)
        tags = notification_dict.pop('flow_run_tags', [])
        target_id = None
        target_name = 'Bright'
        target_id_prefix = prefect.settings.PREFECT_ORION_SERVICES_FLOW_RUN_NOTIFICATIONS_TARGET_ID_PREFIX.value()
        target_name_prefix = prefect.settings.PREFECT_ORION_SERVICES_FLOW_RUN_NOTIFICATIONS_TARGET_NAME_PREFIX.value()
        enable_tag_value = prefect.settings.PREFECT_ORION_SERVICES_FLOW_RUN_NOTIFICATIONS_ENABLE_TAG.value()
        enable_tag = next((tag for tag in tags if tag == enable_tag_value), None)
        if not enable_tag:
            self._logger.info("This flow isn't enable notification, skipped")
            return
        target_tag = next((tag for tag in tags if tag.startswith(target_id_prefix)), None)
        target_name_tag = next((tag for tag in tags if tag.startswith(target_name_prefix)), None)
        if target_tag:
            target_id = target_tag[len(target_id_prefix):]
        if target_name_tag:
            target_name = target_name_tag[len(target_name_prefix):]
        if target_id:
            await self._client.post(self.apiUrl, json={
                "name": self.templateName,
                "to": target_id,
                "payload": jsonable_encoder({
                    "creator": target_name,
                    **notification_dict
                })
            })
        else:
            self._logger.warning("No flow creator found, skip")
