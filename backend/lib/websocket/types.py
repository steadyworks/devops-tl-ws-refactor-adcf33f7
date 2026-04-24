from enum import Enum
from typing import Literal, Optional, Union
from uuid import UUID

from pydantic import BaseModel

from backend.db.data_models import (
    PhotobookStatus,
)

# ---- WebSocket Event Types ----


class WebSocketEventType(str, Enum):
    # Client → Server
    ASSET_UPLOAD_STATUS_UPDATE = "asset_upload_status_update"
    PHOTOBOOK_STATUS_SUBSCRIBE = "photobook_status_subscribe"

    # Server → Client
    ASSET_REJECTED_INVALID_MIME = "asset_rejected_invalid_mime"
    ASSET_REJECTED_CORRUPT = "asset_rejected_corrupt"
    ASSET_FAILED_PERMANENTLY = "asset_failed_permanently"

    PHOTOBOOK_STATUS_UPDATE = "photobook_status_update"
    PHOTOBOOK_STATUS_ERROR = "photobook_status_error"


################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################


# ---- Client → Server Payloads ----
class PhotobookStatusSubscribePayload(BaseModel):
    photobook_id: UUID


class PhotobookStatusSubscribeMessage(BaseModel):
    event: Literal[WebSocketEventType.PHOTOBOOK_STATUS_SUBSCRIBE]
    photobook_id: UUID


class AssetUploadStatusFailed(BaseModel):
    asset_id: UUID
    error_msg: str


class AssetUploadStatusPayload(BaseModel):
    associated_photobook_id: Optional[UUID] = None
    succeeded: list[UUID]
    failed: list[AssetUploadStatusFailed]


class AssetUploadStatusMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_UPLOAD_STATUS_UPDATE]
    payload: AssetUploadStatusPayload


ClientToServerMessageUnion = Union[
    AssetUploadStatusMessage,
    PhotobookStatusSubscribeMessage,
]


class ClientToServerEnvelope(BaseModel):
    event: WebSocketEventType
    payload: Union[
        AssetUploadStatusPayload,
        PhotobookStatusSubscribePayload,
    ]

    model_config = {"json_schema_extra": {"discriminator": {"propertyName": "event"}}}


################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################
################################################################################


# ---- Server → Client Payloads ----
class AssetRejectedInvalidMIMEPayload(BaseModel):
    image_id: UUID
    message: str = "Unsupported MIME type"


class AssetRejectedCorruptPayload(BaseModel):
    image_id: UUID
    message: str = "Corrupt or unreadable image file"


class AssetFailedPermanentlyPayload(BaseModel):
    image_id: UUID
    message: str
    retryable: bool = False


class PhotobookStatusUpdatePayload(BaseModel):
    photobook_id: UUID
    status: Optional[PhotobookStatus]


class PhotobookStatusErrorPayload(BaseModel):
    photobook_id: Optional[UUID] = None
    message: str


# ---- Server → Client Envelopes ----


class AssetRejectedInvalidMIMEMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_REJECTED_INVALID_MIME]
    payload: AssetRejectedInvalidMIMEPayload


class AssetRejectedCorruptMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_REJECTED_CORRUPT]
    payload: AssetRejectedCorruptPayload


class AssetFailedPermanentlyMessage(BaseModel):
    event: Literal[WebSocketEventType.ASSET_FAILED_PERMANENTLY]
    payload: AssetFailedPermanentlyPayload


class PhotobookStatusUpdateMessage(BaseModel):
    event: Literal[WebSocketEventType.PHOTOBOOK_STATUS_UPDATE]
    payload: PhotobookStatusUpdatePayload


class PhotobookStatusErrorMessage(BaseModel):
    event: Literal[WebSocketEventType.PHOTOBOOK_STATUS_ERROR]
    payload: PhotobookStatusErrorPayload


ServerToClientMessageUnion = Union[
    AssetRejectedInvalidMIMEMessage,
    AssetRejectedCorruptMessage,
    AssetFailedPermanentlyMessage,
    PhotobookStatusUpdateMessage,
    PhotobookStatusErrorMessage,
]


class ServerToClientEnvelope(BaseModel):
    event: WebSocketEventType
    payload: Union[
        AssetRejectedInvalidMIMEPayload,
        AssetRejectedCorruptPayload,
        AssetFailedPermanentlyPayload,
        PhotobookStatusUpdatePayload,
        PhotobookStatusErrorPayload,
    ]

    model_config = {"json_schema_extra": {"discriminator": {"propertyName": "event"}}}
