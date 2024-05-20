import asyncio
from datetime import timedelta, datetime
from enum import StrEnum
from random import randint

from beanie.odm.operators.update.general import Set
from pydantic import BaseModel

from core.worker import workflow_activity
from libs.data_types.platform import Platform
from libs.schema.collect_status import CollectStatus, BEGINNING_OF_TIME
from libs.schema.raw_transaction import RawTransaction, Metadata


class CollectAction(BaseModel):
    platform: Platform
    account: str


class CollectResult(StrEnum):
    CONTINUE = "CONTINUE"
    STOP = "STOP"


class CollectResponse(BaseModel):
    result: CollectResult
    num_of_tx: int
    to_date: datetime


@workflow_activity
async def collect(request: CollectAction) -> CollectResponse:
    collect_status = await CollectStatus.find(
        CollectStatus.platform == request.platform,
        CollectStatus.account == request.account
    ).first_or_none()

    last_synced_at = collect_status.last_synced_at if collect_status else BEGINNING_OF_TIME

    print(f"{request.platform}-{request.account} - Collecting transactions from {last_synced_at}")

    await asyncio.sleep(5)
    num_of_tx = 0
    num_of_iterations = randint(1, 5)
    timestamp = last_synced_at
    for i in range(num_of_iterations):
        timestamp = timestamp + timedelta(days=randint(0, 10))
        metadata = Metadata(account=request.account, timestamp=timestamp, platform=request.platform)
        random_from_address = str(randint(1, 1000000000000))
        random_to_address = str(randint(1, 1000000000000))
        random_amount = randint(1, 1000000000000)
        raw_tx = RawTransaction(from_address=random_from_address,
                                to_address=random_to_address,
                                amount=random_amount,
                                metadata=metadata)
        await raw_tx.save()
        num_of_tx += 1

    print(f"{request.platform}-{request.account} - Collected {num_of_tx} transactions")

    rand = randint(1, 100)
    if rand > 99:
        print(f"{request.platform}-{request.account} - No more transactions")
        result = CollectResult.STOP
    else:
        print(f'{request.platform}-{request.account} - More transactions to collect')
        result = CollectResult.CONTINUE

    await CollectStatus.find_one(
        CollectStatus.account == request.account,
        CollectStatus.platform == request.platform
    ).upsert(
        Set({CollectStatus.last_synced_at: timestamp}),
        on_insert=CollectStatus(
            account=request.account,
            platform=request.platform,
            last_synced_at=last_synced_at
        )
    )

    return CollectResponse(result=result, num_of_tx=num_of_tx, to_date=timestamp)
