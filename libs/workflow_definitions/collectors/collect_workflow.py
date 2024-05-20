from pydantic import BaseModel

from core.workflow_definition import WorkflowDefinition
from libs.data_types.platform import Platform
from libs.workflow_definitions.queues import Queues


class CollectTask(BaseModel):
    platform: Platform
    wallet: str


class CollectDefinition(WorkflowDefinition):
    queue = Queues.COLLECTORS
    request = CollectTask
    response = int

    @classmethod
    def _workflow_identifiers(cls, platform: Platform, slot_id: int, *args, **kwargs) -> str:
        return f'collect-{platform}-{slot_id}'
