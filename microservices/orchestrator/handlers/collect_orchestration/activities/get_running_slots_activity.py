from datetime import datetime

from pydantic import BaseModel
from temporalio.client import WorkflowExecution

from core.broker_client import BrokerClient
from core.worker import workflow_activity
from libs.data_types.platform import Platform


class RunningSlot(BaseModel):
    id: int
    start_time: datetime


class GetRunningSlotsResponse(BaseModel):
    slots: list[RunningSlot]


class GetRunningSlotsAction(BaseModel):
    platform: Platform


@workflow_activity
async def get_running_slots(
        action: GetRunningSlotsAction) -> GetRunningSlotsResponse:
    client = await BrokerClient.client()
    slots = [_extract_slot(workflow) async for workflow
             in client.list_workflows(f"Platform = '{action.platform}' AND ExecutionStatus = 'Running'")]

    return GetRunningSlotsResponse(
        slots=slots
    )


def _extract_slot(workflow: WorkflowExecution) -> RunningSlot:
    return RunningSlot(
        id=int(workflow.id.split('-')[-1]),
        start_time=workflow.start_time
    )
