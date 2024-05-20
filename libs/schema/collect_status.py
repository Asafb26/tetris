from datetime import datetime, UTC

import pymongo
from beanie import Document
from pymongo import IndexModel

from libs.data_types.platform import Platform

BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=UTC)


class CollectStatus(Document):
    account: str
    platform: Platform
    last_synced_at: datetime = BEGINNING_OF_TIME
    hold_until: datetime = BEGINNING_OF_TIME

    class Settings:
        indexes = [
            IndexModel(
                ['account', 'platform'],
                unique=True
            ),
            IndexModel(
                ['platform',
                 'hold_until',
                 ('last_synced_at', pymongo.ASCENDING)
                 ],
            )
        ]
