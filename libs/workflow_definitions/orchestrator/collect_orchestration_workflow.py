from libs.data_types.platform import Platform
from core.workflow_definition import WorkflowDefinition
from libs.workflow_definitions.queues import Queues


class CollectOrchestrationDefinition(WorkflowDefinition):
    request = None
    response = None
    queue = Queues.ORCHESTRATOR

    @classmethod
    def _workflow_identifiers(cls, platform: Platform, slot_id: str, *args, **kwargs) -> str:
        return f'collect-{platform}-{slot_id}'
