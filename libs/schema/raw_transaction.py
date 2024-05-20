from datetime import datetime

from beanie import Document
from pydantic import BaseModel

from libs.data_types.platform import Platform


class Metadata(BaseModel):
    account: str
    timestamp: datetime
    platform: Platform


class RawTransaction(Document):
    from_address: str
    to_address: str
    amount: float
    metadata: Metadata

    class Settings:
        indexes = [
            'metadata.timestamp'
        ]
