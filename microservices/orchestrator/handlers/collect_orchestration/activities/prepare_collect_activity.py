from datetime import datetime

from beanie.odm.queries.find import FindMany
from pydantic import BaseModel
from temporalio import activity

from core.broker_client import BrokerClient
from core.worker import workflow_activity
from libs.data_types.platform import Platform
from libs.schema.collect_status import CollectStatus


class PrepareCollectAction(BaseModel):
    platform: Platform
    num_of_slots: int


class AccountToCollect(BaseModel):
    account: str
    last_synced_at: datetime


class PrepareCollectResponse(BaseModel):
    current_time: datetime = datetime.now()
    accounts_to_collect: list[AccountToCollect]


@workflow_activity
async def prepare_collect(action: PrepareCollectAction) -> PrepareCollectResponse:
    accounts_to_collect = [
        AccountToCollect(account=collect_status.account, last_synced_at=collect_status.last_synced_at)
        async for collect_status
        in _get_collect_statuses(action) if not await _is_currently_running(collect_status)
    ]

    return PrepareCollectResponse(
        current_time=datetime.now(),
        accounts_to_collect=accounts_to_collect
    )


def _get_collect_statuses(action) -> FindMany[CollectStatus]:
    return CollectStatus.find(
        CollectStatus.platform == action.platform,
        CollectStatus.hold_until <= datetime.now()
    ).sort(
        "+last_synced_at"
    ).limit(
        action.num_of_slots
    )


async def _is_currently_running(collect_status: CollectStatus) -> bool:
    client = await BrokerClient.client()

    async for workflow in client.list_workflows(
            f"Platform = '{collect_status.platform}' AND Account = '{collect_status.account}' AND ExecutionStatus = 'Running'",
            page_size=1):
        print(f"{collect_status.platform} {collect_status.account} is currently running on slot {workflow.id}")
        return True

    return False
